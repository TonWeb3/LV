"""
Live trading on Polymarket CLOB V2 via `polymarket-apis`.

Polymarket migrated to CLOB V2. New wallets trade through the gasless
**deposit-wallet** flow (signature_type=3 / POLY_1271):

  - Funds live in a **deposit wallet** derived from your EOA (deposit via Polymarket).
  - Your **private key / seed (EOA)** signs every order — it holds no funds or gas.
  - A **relayer API key** sponsors on-chain setup (wallet deploy + token approvals),
    so you never pay gas.

Legacy accounts (Polymarket proxy / Gnosis safe) are auto-detected and used as-is:
we pick whichever wallet actually holds pUSD. The EOA is derived from PRIVATE_KEY
(hex key or 12/24-word seed phrase), so there is no signature-type/funder to choose
by hand any more.

All clients are synchronous — call from the event loop via `asyncio.to_thread`.
"""

import threading
import time
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from .config import settings


def _to_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def _patch_clob_models():
    """Relax over-strict fields in polymarket-apis' models. The live CLOB API omits
    some fields the library marks required (e.g. blockaid_check_enabled), which would
    otherwise crash order placement with a pydantic ValidationError."""
    try:
        import polymarket_apis.types.clob_types as ct
        changed = False
        for name in ("blockaid_check_enabled",):
            f = ct.ClobMarketInfo.model_fields.get(name)
            if f is not None and f.is_required():
                f.default = False
                changed = True
        if changed:
            ct.ClobMarketInfo.model_rebuild(force=True)
    except Exception:
        pass


def _patch_order_builder():
    """Polymarket CLOB V2 strictly enforces:
    - Market buy maker amount: max 2 decimals (collateral/USDC spent)
    - Market buy taker amount: max 4 decimals (tokens/shares received)
    The polymarket-apis library's default ROUNDING_CONFIG specifies amount=5 for 0.001
    and amount=6 for 0.0001 tick sizes, which produces taker amounts with > 4 decimals.
    Polymarket rejects these with:
      HTTP 400 - invalid amounts, the market buy orders maker amount supports a max accuracy of 2 decimals, taker amount a max of 4 decimals.
    This patch clamps ROUNDING_CONFIG to a maximum of 4 decimals and ensures exact integer divisibility:
      maker amount is snapped to 10,000 (10^(6-2) -> max 2 decimals)
      taker amount is snapped to 100 (10^(6-4) -> max 4 decimals)
    """
    try:
        from polymarket_apis.utilities.order_builder.builder import OrderBuilder, ROUNDING_CONFIG, RoundConfig
        for k, rc in list(ROUNDING_CONFIG.items()):
            if rc.amount > 4:
                ROUNDING_CONFIG[k] = RoundConfig(price=rc.price, size=rc.size, amount=min(4, rc.amount))

        orig_get_market = OrderBuilder.get_market_order_amounts

        def safe_get_market_order_amounts(self, side: str, amount: float, price: float, round_config):
            res_side, maker, taker = orig_get_market(self, side, amount, price, round_config)
            if side == "BUY":
                maker = (maker // 10_000) * 10_000
                taker = (taker // 100) * 100
            elif side == "SELL":
                maker = (maker // 100) * 100
                taker = (taker // 10_000) * 10_000
            return res_side, maker, taker

        OrderBuilder.get_market_order_amounts = safe_get_market_order_amounts

        # Polymarket CLOB V2 rule: order.signer MUST match the EOA owning the API key
        # OrderBuilder._v2_order_signer left unpatched: returns self.funder for POLY_1271 (signature_type 3)
    except Exception as e:
        print(f"Warning: Failed to patch OrderBuilder: {e}")


def _patch_crypto_signing():
    """Accelerate EIP-712 cryptographic signing using compiled C coincurve (libsecp256k1).
    Bypasses pure-Python eth-account modular arithmetic, dropping signature creation to sub-0.1ms."""
    try:
        import coincurve
        from eth_account import Account
        from eth_account.datastructures import SignedMessage

        orig_unsafe_sign_hash = Account.unsafe_sign_hash

        def fast_unsafe_sign_hash(message_hash, private_key):
            try:
                if isinstance(private_key, str):
                    pk_bytes = bytes.fromhex(private_key[2:] if private_key.startswith("0x") else private_key)
                elif hasattr(private_key, "key"):
                    pk_bytes = bytes(private_key.key)
                else:
                    pk_bytes = bytes(private_key)

                msg_bytes = bytes.fromhex(message_hash[2:] if str(message_hash).startswith("0x") else message_hash) if isinstance(message_hash, str) else bytes(message_hash)

                pk = coincurve.PrivateKey(pk_bytes)
                sig65 = pk.sign_recoverable(msg_bytes, hasher=None)
                r = int.from_bytes(sig65[:32], "big")
                s = int.from_bytes(sig65[32:64], "big")
                v = sig65[64] + 27
                sig_bytes = sig65[:64] + bytes([v])
                return SignedMessage(
                    messageHash=msg_bytes,
                    r=r,
                    s=s,
                    v=v,
                    signature=sig_bytes,
                )
            except Exception:
                return orig_unsafe_sign_hash(message_hash, private_key)

        Account.unsafe_sign_hash = staticmethod(fast_unsafe_sign_hash)
    except Exception as e:
        print(f"Note: coincurve C-signing patch fallback: {e}")


_patch_clob_models()
_patch_order_builder()
_patch_crypto_signing()


_CACHED_RUST_MOD = None


def _ensure_rust_clob():
    """Return the cached native Rust `rust_clob` extension module.
    Cached in RAM after first import — executes in sub-microsecond time with zero filesystem I/O."""
    global _CACHED_RUST_MOD
    if _CACHED_RUST_MOD is not None:
        return _CACHED_RUST_MOD
    try:
        import rust_clob
        _CACHED_RUST_MOD = rust_clob
        return _CACHED_RUST_MOD
    except Exception as e:
        raise RuntimeError(f"rust_clob native module not available: {e}")


def derive_beacon_deposit_wallet(eoa_address: str) -> str:
    """Derive deterministic ERC-1967 Beacon Proxy deposit wallet address
    introduced in Polymarket's June 29, 2026 upgrade.
    Uses CREATE2 with Solady LibClone initCodeHashERC1967BeaconProxy."""
    from eth_utils import keccak, to_checksum_address, to_bytes
    from eth_abi import encode

    owner = to_checksum_address(eoa_address)
    factory = to_checksum_address("0x00000000000Fb5C9ADea0298D729A0CB3823Cc07")
    beacon = to_checksum_address("0x7A18EDfe055488A3128f01F563e5B479D92ffc3a")

    owner_b32 = to_bytes(hexstr=owner).rjust(32, b"\x00")
    args = encode(["address", "bytes32"], [factory, owner_b32])
    salt = keccak(args)

    prefix = bytes.fromhex("60523d8160223d3973")
    beacon_b20 = to_bytes(hexstr=beacon)
    suffix = bytes.fromhex("60195155f3363d3d373d3d363d602036600436635c60da1b60e01b36527fa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6cb3582b35133d50545afa5036515af43d6000803e604d573d6000fd5b3d6000f3")
    bytecode_hash = keccak(prefix + beacon_b20 + suffix)

    create2_input = b"\xff" + to_bytes(hexstr=factory) + salt + bytecode_hash
    derived_bytes = keccak(create2_input)[12:]
    return to_checksum_address(derived_bytes)


class ClobTrader:
    _FILLED_STATUSES = ("matched", "delayed")
    _PARTIAL_FILL_HINTS = (
        "fully filled", "could not be filled", "fill or kill", "killed",
        "unfilled", "insufficient liquidity", "not filled"
    )

    def __init__(self):
        self.clob = None          # PolymarketClobClient — order placement
        self.gasless = None       # PolymarketGaslessWeb3Client — relayer on-chain ops
        self.funder: Optional[str] = None         # funded wallet (deposit/proxy/safe)
        self.signature_type: Optional[int] = None
        self.ready = False
        self.last_error: Optional[str] = None
        self._approvals_done = False
        self._api_creds = None
        self._rust_ready = False
        self._lock = threading.Lock()

    def reset(self):
        """Drop cached clients so the next call re-initialises with fresh
        credentials (call after the key / relayer settings change)."""
        with self._lock:
            self.clob = None
            self.gasless = None
            self.funder = None
            self.signature_type = None
            self.ready = False
            self.last_error = None
            self._approvals_done = False
            self._api_creds = None
            self._rust_ready = False
    # ── wallet derivation / detection ───────────────────────────────────────────
    def _derive_creds(self):
        """CLOB API creds derived from the key (L1 auth). Cached. Doubles as the
        gasless client's builder_creds when no relayer key is set."""
        if self._api_creds is not None:
            return self._api_creds
        from polymarket_apis.clients.clob_client import PolymarketClobClient
        from eth_account import Account
        eoa = Account.from_key(settings.PRIVATE_KEY).address
        c = PolymarketClobClient(private_key=settings.PRIVATE_KEY, address=eoa,
                                 chain_id=137, signature_type=3)
        self._api_creds = c.create_or_derive_api_creds()
        return self._api_creds

    def _bind_gasless_funder(self, gasless, signature_type: int, funder: Optional[str]):
        if not funder:
            return gasless
        try:
            from web3 import Web3
            chk = Web3.to_checksum_address(funder)
            gasless.address = chk
            if signature_type == 3:
                gasless.get_expected_deposit_wallet = lambda: chk
                gasless.get_deposit_wallet_address = lambda: chk
            elif signature_type == 2:
                gasless.get_safe_proxy_wallet_address = lambda: chk
                if hasattr(gasless, "safe_abi"):
                    gasless.safe = gasless._contract(chk, gasless.safe_abi)
            elif signature_type == 1:
                gasless.get_poly_proxy_wallet_address = lambda: chk
        except Exception:
            pass
        return gasless

    def _new_gasless(self, signature_type: int, funder: Optional[str] = None):
        # Gasless on-chain client. Two distinct roles:
        #   - relayer key (or builder creds): SUBMITS txs and pays the gas
        #   - rpc_url: READS chain state (pUSD / USDC.e balance, wallet derivation). Use Alchemy
        #     when configured, else POLYGON_RPC_URL or library default.
        from polymarket_apis.clients.web3_client import PolymarketGaslessWeb3Client
        kwargs = {"private_key": settings.PRIVATE_KEY, "signature_type": signature_type}
        if settings.RELAYER_API_KEY:
            kwargs["relayer_api_key"] = settings.RELAYER_API_KEY
        else:
            kwargs["builder_creds"] = self._derive_creds()
        rpc = settings.alchemy_rpc_url() or getattr(settings, "POLYGON_RPC_URL", "")
        if rpc:
            kwargs["rpc_url"] = rpc
        client = PolymarketGaslessWeb3Client(**kwargs)
        if funder:
            self._bind_gasless_funder(client, signature_type, funder)
        return client

    def _get_onchain_balances(self, gasless, address: str) -> Tuple[float, float]:
        """Return (pusd_balance, usdc_e_balance) on-chain for `address`."""
        from web3 import Web3
        chk = Web3.to_checksum_address(address)
        pusd_bal = 0.0
        usdc_e_bal = 0.0
        try:
            pusd_bal = float(gasless.get_pusd_balance(address=chk))
        except Exception:
            pusd_bal = 0.0
        try:
            usdc_e_contract = gasless._contract(gasless.usdc_e_address, gasless.pusd_abi)
            usdc_e_bal = float(usdc_e_contract.functions.balanceOf(chk).call() / 1e6)
        except Exception:
            usdc_e_bal = 0.0
        return pusd_bal, usdc_e_bal

    def _candidate_wallets(self, gasless) -> List[Tuple[int, str, str]]:
        """(signature_type, address, label) candidates derived from the EOA:
        0. Explicit user override if configured
        1. Polymarket Official Profile Wallet (automatic via Gamma public profile)
        2. Modern Beacon Deposit Wallet (V2, st=3, post-June 29 standard)
        3. Legacy UUPS Deposit Wallet (st=3, pre-June 29)
        4. Legacy Proxy Wallet (st=1)
        5. Safe Proxy Wallet (st=2)
        6. Direct EOA Signer (st=0)
        Never includes unverified counterparty maker addresses from market trades."""
        out: List[Tuple[int, str, str]] = []

        # Derive EOA address
        eoa_addr = None
        try:
            from eth_account import Account
            eoa_addr = Account.from_key(settings.PRIVATE_KEY).address
        except Exception:
            eoa_addr = None

        # Explicit user override
        explicit_funder = getattr(settings, "FUNDER_ADDRESS", "").strip()
        if explicit_funder and explicit_funder.startswith("0x") and len(explicit_funder) == 42:
            try:
                from web3 import Web3
                chk = Web3.to_checksum_address(explicit_funder)
                detected_st = None
                try:
                    detected_st = gasless.detect_wallet_signature_type(chk)
                except Exception:
                    detected_st = None
                st_to_use = detected_st if detected_st in (0, 1, 2, 3) else 3
                out.append((st_to_use, chk, "Configured Funder (User Override)"))
                return out
            except Exception:
                pass

        # 1. Polymarket Official Gamma Profile Discovery (Works for all accounts: Email/Magic, Metamask, Turnkey, etc.)
        if eoa_addr:
            try:
                import urllib.request
                import json
                from web3 import Web3
                gamma_url = f"{settings.GAMMA_BASE_URL.rstrip('/')}/public-profile?address={eoa_addr}"
                req = urllib.request.Request(gamma_url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=5.0) as resp:
                    if resp.status == 200:
                        data = json.loads(resp.read().decode("utf-8"))
                        profile_proxy = data.get("proxyWallet")
                        if profile_proxy and profile_proxy.startswith("0x") and len(profile_proxy) == 42:
                            chk_profile = Web3.to_checksum_address(profile_proxy)
                            detected_st = None
                            try:
                                detected_st = gasless.detect_wallet_signature_type(chk_profile)
                            except Exception:
                                detected_st = None
                            st_to_use = detected_st if detected_st in (0, 1, 2, 3) else 3
                            out.append((st_to_use, chk_profile, "Polymarket Profile Wallet (Automatic)"))
            except Exception:
                pass

        # 2. Modern Beacon Proxy Deposit Wallet (st=3, June 29, 2026+ standard)
        if eoa_addr:
            try:
                beacon_wallet = derive_beacon_deposit_wallet(eoa_addr)
                if beacon_wallet and not any(a.lower() == beacon_wallet.lower() for _, a, _ in out):
                    out.append((3, beacon_wallet, "Deposit Wallet (Beacon V2)"))
            except Exception:
                pass

        # 3. Legacy UUPS Deposit Wallet (st=3, polymarket-apis 2.3.0 default)
        try:
            legacy_dep = gasless.get_expected_deposit_wallet()
            if legacy_dep and not any(a.lower() == legacy_dep.lower() for _, a, _ in out):
                out.append((3, legacy_dep, "Legacy Deposit Wallet (UUPS)"))
        except Exception:
            pass

        # 4. Polymarket Proxy Wallet (st=1)
        try:
            poly_proxy = gasless.get_poly_proxy_wallet_address()
            if poly_proxy and not any(a.lower() == poly_proxy.lower() for _, a, _ in out):
                out.append((1, poly_proxy, "Polymarket Proxy Wallet"))
        except Exception:
            pass

        # 5. Safe Proxy Wallet (st=2)
        try:
            safe_proxy = gasless.get_safe_proxy_wallet_address()
            if safe_proxy and not any(a.lower() == safe_proxy.lower() for _, a, _ in out):
                out.append((2, safe_proxy, "Gnosis Safe Proxy"))
        except Exception:
            pass

        # 6. Direct EOA (st=0)
        if eoa_addr and not any(a.lower() == eoa_addr.lower() for _, a, _ in out):
            out.append((0, eoa_addr, "Personal Signer (EOA)"))

        return out

    def _pick_funded_wallet(self, gasless) -> Tuple[int, str]:
        """Trade from where the money actually is: pick the candidate holding pUSD or USDC.e.
        Falls back to CLOB API balance check and then the Beacon deposit wallet when balances read 0."""
        explicit_funder = getattr(settings, "FUNDER_ADDRESS", "").strip()
        if explicit_funder and explicit_funder.startswith("0x") and len(explicit_funder) == 42:
            try:
                from web3 import Web3
                chk = Web3.to_checksum_address(explicit_funder)
                detected_st = None
                try:
                    detected_st = gasless.detect_wallet_signature_type(chk)
                except Exception:
                    detected_st = None
                return (detected_st if detected_st in (0, 1, 2, 3) else 3), chk
            except Exception:
                pass

        candidates = self._candidate_wallets(gasless)
        best = None  # (sig_type, addr, total_onchain_balance)
        for item in candidates:
            st, addr = item[0], item[1]
            pusd_bal, usdc_e_bal = self._get_onchain_balances(gasless, addr)
            total_bal = max(pusd_bal, usdc_e_bal)
            if total_bal > 0:
                # Immediate early exit on first funded wallet (eliminates 10+ redundant RPC calls)
                return st, addr
            if best is None or total_bal > best[2]:
                best = (st, addr, total_bal)

        if best and best[2] > 0:
            return best[0], best[1]

        # Fallback: check CLOB API collateral balance across user's genuine candidate wallets
        try:
            from polymarket_apis.clients.clob_client import PolymarketClobClient
            creds = self._derive_creds()
            for item in candidates:
                st, addr = item[0], item[1]
                try:
                    c = PolymarketClobClient(private_key=settings.PRIVATE_KEY, address=addr, chain_id=137, signature_type=st)
                    c.set_api_creds(creds)
                    clob_bal = float(c.get_pusd_balance())
                    if clob_bal > 0:
                        return st, addr
                except Exception:
                    continue
        except Exception:
            pass

        if candidates:
            # First candidate is the modern Beacon deposit wallet
            return candidates[0][0], candidates[0][1]

        # Ultimate fallback
        try:
            from eth_account import Account
            eoa = Account.from_key(settings.PRIVATE_KEY).address
            return 3, derive_beacon_deposit_wallet(eoa)
        except Exception:
            return 3, gasless.get_expected_deposit_wallet()

    def _init_clients(self):
        from polymarket_apis.clients.clob_client import PolymarketClobClient

        # Any gasless client can derive every candidate address; probe with deposit.
        probe = self._new_gasless(3)
        sig_type, funder = self._pick_funded_wallet(probe)

        gasless = probe if sig_type == 3 else self._new_gasless(sig_type, funder=funder)
        self._bind_gasless_funder(gasless, sig_type, funder)

        clob = PolymarketClobClient(
            private_key=settings.PRIVATE_KEY,
            address=funder,
            chain_id=137,
            signature_type=sig_type,
        )
        clob.set_api_creds(clob.create_or_derive_api_creds())

        # Optimize HTTP client transport: persistent Keep-Alive pool with HTTP/1.1 (bypasses HTTP/2 framing)
        try:
            import httpx
            limits = httpx.Limits(max_keepalive_connections=20, max_connections=30, keepalive_expiry=300.0)
            transport = httpx.HTTPTransport(http1=True, http2=False, limits=limits)
            clob.client = httpx.Client(base_url=clob.base_url, transport=transport, timeout=10.0)
        except Exception:
            pass

        # Initialize native Rust order signer & persistent HTTP/1.1 Keep-Alive sender
        rust_mod = _ensure_rust_clob()
        pk_hex = clob.signer.account.key.hex() if hasattr(clob.signer, "account") else clob.signer.private_key
        creds = clob.creds
        rust_mod.init_client(
            str(pk_hex),
            str(funder),
            int(sig_type),
            str(creds.key),
            str(creds.secret),
            str(creds.passphrase),
            str(clob.base_url),
            137,
        )
        self._rust_ready = True

        self.gasless = gasless
        self.clob = clob
        self.funder = funder
        self.signature_type = sig_type
        self.ready = True
        self.last_error = None

    def is_ready(self) -> bool:
        """Check if CLOB client is initialized and ready without triggering blocking setup."""
        return bool(self.ready and self.clob is not None and self._rust_ready)

    def ensure_ready(self) -> bool:
        if self.ready and self.clob is not None and self._rust_ready:
            return True
        with self._lock:
            if self.ready and self.clob is not None and self._rust_ready:
                return True
            if not settings.PRIVATE_KEY:
                self.last_error = "missing_private_key"
                return False
            try:
                self._init_clients()
                # Pre-warm native Rust crypto signing context
                rust_mod = _ensure_rust_clob()
                _ = rust_mod.build_signed_order("1", 1000000, 1000000, 0, "FAK", "0", False, False, False)
                return True
            except Exception as e:
                self.last_error = f"{type(e).__name__}: {e}"
                self.ready = False
                self.clob = None
                return False

    def ensure_setup(self) -> Dict[str, Any]:
        """One-time gasless on-chain setup: deploy the deposit wallet (if needed) and
        set token approvals, sponsored by the relayer key. Required once before the
        first live order on a fresh deposit wallet."""
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        if self._approvals_done:
            return {"ok": True, "skipped": "already_done"}
        if not settings.RELAYER_API_KEY:
            return {"ok": False, "error": "missing_relayer_api_key"}
        try:
            receipts = self.gasless.set_all_approvals()
            self._approvals_done = True
            return {"ok": True, "approvals": len(receipts or [])}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    def warm_token(self, token_id: str, tick_size: str = "0.01", neg_risk: bool = False):
        """Pre-warm token fee, tick size, and neg_risk metadata in the CLOB client
        so order creation and EIP-712 signing execute in ~7ms with zero network round trips."""
        if not self.clob or not token_id:
            return
        tid = str(token_id)
        try:
            from polymarket_apis.types.clob_types import FeeInfo
            if hasattr(self.clob, "_PolymarketReadOnlyClobClient__fee_infos"):
                if tid not in self.clob._PolymarketReadOnlyClobClient__fee_infos:
                    self.clob._PolymarketReadOnlyClobClient__fee_infos[tid] = FeeInfo(rate=0, exponent=0)
            if hasattr(self.clob, "_PolymarketReadOnlyClobClient__tick_sizes"):
                self.clob._PolymarketReadOnlyClobClient__tick_sizes[tid] = (tick_size, time.monotonic() + 86400)
            if hasattr(self.clob, "_PolymarketReadOnlyClobClient__neg_risk"):
                self.clob._PolymarketReadOnlyClobClient__neg_risk[tid] = neg_risk
        except Exception:
            pass

    # ── orders ──────────────────────────────────────────────────────────────────
    def _post_order_fast(
        self,
        token_id: str,
        maker_amount: int,
        taker_amount: int,
        side_int: int,
        order_type_str: str,
        expiration_str: str = "0",
        neg_risk: bool = False,
    ) -> Dict[str, Any]:
        """Sign EIP-712 / Solady ERC-1271 order and POST via native Rust (`rust_clob`),
        releasing the GIL during network I/O."""
        import json
        rust_mod = _ensure_rust_clob()
        res = rust_mod.post_order(
            str(token_id),
            int(maker_amount),
            int(taker_amount),
            int(side_int),
            str(order_type_str),
            str(expiration_str),
            bool(neg_risk),
            False,
            False,
        )
        http_status = int(res.get("http_status") or 0)
        resp_text = res.get("response_text") or ""
        if not res.get("ok_http"):
            err_detail = resp_text
            try:
                err_json = json.loads(resp_text) if resp_text else {}
                if isinstance(err_json, dict):
                    err_detail = (
                        err_json.get("error")
                        or err_json.get("errorMsg")
                        or err_json.get("message")
                        or resp_text
                    )
            except Exception:
                pass
            if http_status == 0:
                raise RuntimeError(res.get("transport_error") or "rust_clob transport error")
            err_detail = self.enrich_order_error(err_detail)
            raise RuntimeError(f"OrderPlacementError: Order placement failed: HTTP {http_status} - {err_detail}")
        data = json.loads(resp_text) if resp_text else {}
        if isinstance(data, dict):
            data["rust_sign_us"] = res.get("sign_time_us")
            data["rust_total_us"] = res.get("total_time_us")
        return data

    def place_limit_buy(self, token_id: str, size: float, price: float, expiration: Optional[int] = None) -> Dict[str, Any]:
        """Place a Good 'Til Date (GTD) limit BUY order for `size` shares at `price`.
        Sweeps asks up to `price` and rests any remaining unfilled size on the book
        until filled, cancelled, or the expiration timestamp is reached."""
        if not token_id:
            return {"ok": False, "error": "missing_token_id"}
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        if not self._approvals_done and settings.RELAYER_API_KEY:
            try:
                self.ensure_setup()
            except Exception:
                pass
        try:
            from polymarket_apis.utilities.order_builder.builder import ROUNDING_CONFIG
            order_price = round(float(price), 4)
            order_size = round(float(size), 2)
            exp = int(expiration) if expiration and expiration > (time.time() + 180) else int(time.time() + 300)
            self.warm_token(token_id)
            side_int, maker_amt, taker_amt = self.clob.builder.get_order_amounts(
                "BUY", order_size, order_price, ROUNDING_CONFIG["0.01"]
            )
            data = self._post_order_fast(
                token_id=str(token_id),
                maker_amount=maker_amt,
                taker_amount=taker_amt,
                side_int=side_int,
                order_type_str="GTD",
                expiration_str=str(exp),
                neg_risk=False,
            )
            order_id = data.get("order_id") or data.get("orderID") or data.get("orderId") or data.get("id")
            status = str(data.get("status", "")).lower()
            return {
                "ok": bool(order_id),
                "order_id": order_id,
                "status": status or "open",
                "price": order_price,
                "size": order_size,
                "expiration": exp,
                "response": data
            }
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    def place_limit_sell(self, token_id: str, size: float, price: float, expiration: Optional[int] = None) -> Dict[str, Any]:
        """Place a Good 'Til Date (GTD) limit SELL order for `size` shares at `price`."""
        if not token_id:
            return {"ok": False, "error": "missing_token_id"}
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        if not self._approvals_done and settings.RELAYER_API_KEY:
            try:
                self.ensure_setup()
            except Exception:
                pass
        try:
            from polymarket_apis.utilities.order_builder.builder import ROUNDING_CONFIG
            order_price = round(float(price), 4)
            order_size = round(float(size), 2)
            exp = int(expiration) if expiration and expiration > (time.time() + 180) else int(time.time() + 300)
            self.warm_token(token_id)
            side_int, maker_amt, taker_amt = self.clob.builder.get_order_amounts(
                "SELL", order_size, order_price, ROUNDING_CONFIG["0.01"]
            )
            data = self._post_order_fast(
                token_id=str(token_id),
                maker_amount=maker_amt,
                taker_amount=taker_amt,
                side_int=side_int,
                order_type_str="GTD",
                expiration_str=str(exp),
                neg_risk=False,
            )
            order_id = data.get("order_id") or data.get("orderID") or data.get("orderId") or data.get("id")
            status = str(data.get("status", "")).lower()
            return {
                "ok": bool(order_id),
                "order_id": order_id,
                "status": status or "open",
                "price": order_price,
                "size": order_size,
                "expiration": exp,
                "response": data
            }
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """Cancel an open order on the CLOB."""
        if not order_id or not self.ensure_ready():
            return {"ok": False, "error": "missing_order_id_or_client"}
        try:
            resp = self.clob.cancel_order(str(order_id))
            data = resp.model_dump() if hasattr(resp, "model_dump") else dict(resp or {})
            return {"ok": True, "response": data}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    def get_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Fetch order details from Polymarket CLOB /data/order/<id> with Level 2 authentication."""
        if not order_id or not self.ensure_ready() or not self.clob:
            return None
        try:
            from polymarket_apis.types.clob_types import RequestArgs
            from polymarket_apis.utilities.headers import create_level_2_headers
            endpoint = f"/data/order/{order_id}"
            request_args = RequestArgs(method="GET", request_path=endpoint)
            headers = create_level_2_headers(self.clob.signer, self.clob.creds, request_args)
            resp = self.clob.client.get(self.clob._build_url(endpoint), headers=headers)
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            pass
        return None

    def check_order_status(self, order_id: str) -> Dict[str, Any]:
        """Check if an order is still open or has been filled on the CLOB."""
        if not order_id or not self.ensure_ready():
            return {"status": "unknown"}
        try:
            # 1. Check open orders first
            orders = self.clob.get_orders(order_id=str(order_id))
            if orders and len(orders) > 0:
                ord_info = orders[0]
                d = ord_info.model_dump() if hasattr(ord_info, "model_dump") else dict(ord_info)
                size_matched = float(d.get("size_matched") or 0.0)
                original_size = float(d.get("original_size") or 0.0)
                if original_size > 0 and size_matched >= original_size:
                    return {"status": "FILLED", "size_matched": size_matched, "price": d.get("price")}
                return {"status": "OPEN", "size_matched": size_matched, "original_size": original_size, "price": d.get("price")}

            # 2. If not in open orders, query order directly from CLOB /data/order/<id>
            direct = self.get_order(str(order_id))
            if direct:
                status_raw = str(direct.get("status") or "").upper()
                size_matched = float(direct.get("size_matched") or 0.0)
                original_size = float(direct.get("original_size") or 0.0)
                price = float(direct.get("price") or 0.0)
                if status_raw in ("MATCHED", "FILLED") or (original_size > 0 and size_matched >= original_size):
                    return {"status": "FILLED", "size_matched": size_matched if size_matched > 0 else original_size, "price": price}
                elif status_raw in ("CANCELLED", "CANCELED"):
                    return {"status": "CANCELLED", "size_matched": size_matched}
                elif size_matched > 0:
                    return {"status": "OPEN", "size_matched": size_matched, "original_size": original_size, "price": price, "partial": True}

            # 3. Check actual executed trade fills on the CLOB (summing all partial pieces)
            fills = self.get_order_fills(str(order_id))
            if fills:
                total_shares = sum(float(f["size"]) for f in fills)
                total_usd = sum(float(f["size"]) * float(f["price"]) for f in fills)
                avg_price = total_usd / total_shares if total_shares > 0 else 0.0
                return {"status": "FILLED", "size_matched": total_shares, "price": avg_price}

            # IMPORTANT: Newly placed orders take 1-3 seconds to appear in indexer endpoints.
            # Do NOT assume CLOSED on empty/unindexed queries; return OPEN (pending propagation).
            return {"status": "OPEN", "pending": True}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def get_last_fill(self, token_id: str) -> Optional[Dict[str, Any]]:
        """Most recent on-chain trade for this token from the CLOB's own record —
        an independent confirmation that a reported fill really happened."""
        if not self.ensure_ready():
            return None
        try:
            trades = self.clob.get_trades(token_id=str(token_id))
        except Exception:
            return None
        if not trades:
            return None
        t = trades[0]
        d = t.model_dump() if hasattr(t, "model_dump") else dict(t)
        return {"trade_id": d.get("trade_id"), "side": d.get("side"),
                "price": _to_float(d.get("price")), "size": _to_float(d.get("size")),
                "status": d.get("status"), "match_time": d.get("match_time")}

    def get_order_fills(self, order_id: str, token_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Find all executed fills (including multi-piece fills) matching an order_id."""
        if not order_id or not self.ensure_ready() or not self.clob:
            return []
        try:
            trades = self.clob.get_trades(token_id=str(token_id)) if token_id else self.clob.get_trades()
            if not trades:
                return []
            target = str(order_id).lower()
            matching_fills = []
            for t in trades[:50]:
                td = t.model_dump() if hasattr(t, "model_dump") else dict(t)
                taker_id = str(td.get("taker_order_id") or "").lower()
                maker_orders = td.get("maker_orders") or []

                if taker_id == target:
                    matching_fills.append({
                        "size": float(td.get("size") or 0.0),
                        "price": float(td.get("price") or 0.0),
                        "side": td.get("side"),
                        "match_time": td.get("match_time")
                    })
                    continue

                for m in maker_orders:
                    md = m.model_dump() if hasattr(m, "model_dump") else dict(m)
                    if str(md.get("order_id") or "").lower() == target:
                        matching_fills.append({
                            "size": float(md.get("matched_amount") or td.get("size") or 0.0),
                            "price": float(md.get("price") or td.get("price") or 0.0),
                            "side": td.get("side"),
                            "match_time": td.get("match_time")
                        })
            return matching_fills
        except Exception:
            return []

    def get_token_fills_in_window(self, token_id: str, start_ts: float, end_ts: float) -> List[Dict[str, Any]]:
        """Find fills for a token that occurred within a given timestamp window."""
        if not token_id or not self.ensure_ready() or not self.clob:
            return []
        try:
            trades = self.clob.get_trades(token_id=str(token_id))
            if not trades:
                return []
            matching = []
            for t in trades[:30]:
                td = t.model_dump() if hasattr(t, "model_dump") else dict(t)
                mt = td.get("match_time")
                trade_ts = None
                if mt:
                    try:
                        if isinstance(mt, (int, float)):
                            trade_ts = float(mt) if mt < 1e11 else float(mt) / 1000.0
                        else:
                            trade_ts = datetime.fromisoformat(str(mt).replace('Z', '+00:00')).timestamp()
                    except Exception:
                        pass

                if trade_ts is None or (start_ts - 60 <= trade_ts <= end_ts + 60):
                    matching.append({
                        "size": float(td.get("size") or 0.0),
                        "price": float(td.get("price") or 0.0),
                        "side": td.get("side"),
                        "trade_id": td.get("trade_id")
                    })
            return matching
        except Exception:
            return []

    # ── redemption ──────────────────────────────────────────────────────────────
    def enable_auto_redeem(self) -> Dict[str, Any]:
        """Ask Polymarket to auto-redeem resolved positions. Best-effort: if the
        account doesn't support it we fall back to redeeming explicitly."""
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        if not hasattr(self.gasless, "auto_redeem_enable"):
            return {"ok": False, "error": "auto_redeem_unsupported"}
        try:
            res = self.gasless.auto_redeem_enable()
            return {"ok": True, "result": str(res)}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    def redeem(self, condition_id: str, amounts, neg_risk: bool = False) -> Dict[str, Any]:
        """Redeem a resolved position into pUSD.

        `amounts` is the per-outcome share list the CTF expects ([up, down]); the
        losing leg is simply zero.
        """
        if not condition_id:
            return {"ok": False, "error": "missing_condition_id"}
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        if not hasattr(self.gasless, "redeem_position"):
            return {"ok": False, "error": "redeem_unsupported_by_client"}
        try:
            amts = [float(a) for a in (amounts if isinstance(amounts, (list, tuple)) else [amounts])]
            tx = self.gasless.redeem_position(condition_id, amts, neg_risk)
            return {"ok": True, "tx": str(tx)}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    # ── capital extractor / withdrawal ─────────────────────────────────────────
    def get_funder(self) -> Optional[str]:
        """Return the active funded wallet address (deposit/proxy/safe)."""
        if self.funder:
            return self.funder
        if not settings.PRIVATE_KEY:
            return None
        try:
            probe = self._new_gasless(3)
            _, funder = self._pick_funded_wallet(probe)
            return funder
        except Exception:
            return None

    def get_funder_address(self) -> Optional[str]:
        return self.get_funder()

    def withdraw_pusd(self, recipient: str, amount: float) -> Dict[str, Any]:
        """Transfer pUSD or USDC.e from the funded wallet to `recipient` via gasless relayer."""
        if not recipient:
            return {"ok": False, "error": "missing_recipient_address"}
        try:
            from web3 import Web3
            recipient = Web3.to_checksum_address(recipient)
        except Exception as e:
            return {"ok": False, "error": f"invalid_recipient_address: {e}"}
        if amount <= 0:
            return {"ok": False, "error": "invalid_withdraw_amount"}
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        try:
            from web3 import Web3
            amount_int = int(round(float(amount) * 1e6))
            target_gasless = self.gasless
            target_funder = self.funder or self.gasless.address
            pusd_bal, usdc_e_bal = self._get_onchain_balances(target_gasless, target_funder)

            # If active funder appears below amount on-chain, scan all candidate wallets (Deposit, Proxy, Safe)
            if pusd_bal < amount and usdc_e_bal < amount:
                for cand_st, cand_addr in self._candidate_wallets(self.gasless):
                    c_pusd, c_usdce = self._get_onchain_balances(self.gasless, cand_addr)
                    if c_pusd >= amount or c_usdce >= amount:
                        target_funder = cand_addr
                        pusd_bal, usdc_e_bal = c_pusd, c_usdce
                        target_gasless = self._new_gasless(cand_st, funder=cand_addr)
                        break

            # 1. If wallet holds pUSD >= amount, transfer pUSD
            if pusd_bal >= amount:
                data = target_gasless._encode_transfer_pusd(recipient, amount_int)
                tx = target_gasless._execute(
                    target_gasless.pusd_address,
                    data,
                    "pUSD Transfer",
                    metadata="pusd_transfer",
                )
                return {"ok": True, "tx": str(tx), "token": "pUSD", "from": target_funder}

            # 2. If wallet holds bridged USDC.e >= amount, transfer USDC.e
            if usdc_e_bal >= amount:
                data = target_gasless._encode_transfer_pusd(recipient, amount_int)
                tx = target_gasless._execute(
                    target_gasless.usdc_e_address,
                    data,
                    "pUSD Transfer",
                    metadata="pusd_transfer",
                )
                return {"ok": True, "tx": str(tx), "token": "USDC.e", "from": target_funder}

            # 3. Fallback if RPC balance read returned 0.0 but CLOB API reports sufficient collateral
            clob_bal = 0.0
            try:
                if self.clob is not None:
                    clob_bal = float(self.clob.get_pusd_balance())
            except Exception:
                clob_bal = 0.0

            if clob_bal >= amount:
                data = target_gasless._encode_transfer_pusd(recipient, amount_int)
                try:
                    tx = target_gasless._execute(
                        target_gasless.pusd_address,
                        data,
                        "pUSD Transfer",
                        metadata="pusd_transfer",
                    )
                    return {"ok": True, "tx": str(tx), "token": "pUSD", "from": target_funder}
                except Exception:
                    tx = target_gasless._execute(
                        target_gasless.usdc_e_address,
                        data,
                        "pUSD Transfer",
                        metadata="pusd_transfer",
                    )
                    return {"ok": True, "tx": str(tx), "token": "USDC.e", "from": target_funder}

            return {
                "ok": False,
                "error": f"Insufficient on-chain balance in {target_funder}: pUSD={pusd_bal:.2f}, USDC.e={usdc_e_bal:.2f} (need {amount:.2f})"
            }
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    def is_tx_confirmed(self, tx_hash: str) -> bool:
        """Check whether an on-chain transaction has confirmed on Polygon."""
        if not tx_hash:
            return False
        try:
            if self.gasless and hasattr(self.gasless, "web3") and self.gasless.web3:
                receipt = self.gasless.web3.eth.get_transaction_receipt(tx_hash)
                return receipt is not None and receipt.get("status") == 1
            from web3 import Web3
            rpc = settings.alchemy_rpc_url() or settings.POLYGON_RPC_URL
            w3 = Web3(Web3.HTTPProvider(rpc))
            receipt = w3.eth.get_transaction_receipt(tx_hash)
            return receipt is not None and receipt.get("status") == 1
        except Exception:
            return False

    # ── diagnostics / balance ───────────────────────────────────────────────────
    def get_api_key(self) -> Optional[str]:
        """Return the active L2 CLOB API key string if available."""
        if self.clob and hasattr(self.clob, "creds") and getattr(self.clob.creds, "key", None):
            return str(self.clob.creds.key).strip()
        if self._api_creds and getattr(self._api_creds, "key", None):
            return str(self._api_creds.key).strip()
        return None

    def enrich_order_error(self, err: Any) -> str:
        """Enrich Polymarket error messages with specific signer address and API key prefix."""
        if not err:
            return ""
        err_str = str(err)
        import re
        if "the order signer address has to be the address of the API KEY" in err_str or (
            "order signer address" in err_str.lower() and "address of the api key" in err_str.lower()
        ):
            signer_addr = self.get_eoa_address()
            if not signer_addr and self.clob and hasattr(self.clob, "signer"):
                signer_addr = getattr(self.clob.signer, "address", "") or getattr(getattr(self.clob.signer, "account", None), "address", "")
            if not signer_addr and settings.PRIVATE_KEY:
                try:
                    from eth_account import Account
                    signer_addr = Account.from_key(settings.PRIVATE_KEY).address
                except Exception:
                    pass
            signer_addr = signer_addr or "unknown"

            api_key_str = self.get_api_key() or ""
            api_prefix = api_key_str[:7] if api_key_str else "unknown"

            if f"the order signer address {signer_addr} has to be the address of the API KEY {api_prefix}" in err_str:
                return err_str

            pattern = re.compile(
                r"the order signer address\s*(?:\{put the address\}|0x[0-9a-fA-F]{40})?\s*has to be the address of the API KEY(?:\s*\{put in the first 7 characters of the API\}|\s*[a-zA-Z0-9_\-]+)?",
                re.IGNORECASE
            )
            if pattern.search(err_str):
                return pattern.sub(
                    f"the order signer address {signer_addr} has to be the address of the API KEY {api_prefix}",
                    err_str
                )
            else:
                return re.sub(
                    r"the order signer address.*?address of the API KEY",
                    f"the order signer address {signer_addr} has to be the address of the API KEY {api_prefix}",
                    err_str,
                    flags=re.IGNORECASE
                )
        return err_str

    def get_eoa_address(self) -> Optional[str]:
        """The EOA address derived from PRIVATE_KEY — the wallet you control (the
        key/seed owner). It signs orders but holds no funds or gas."""
        if not settings.PRIVATE_KEY:
            return None
        try:
            from eth_account import Account
            return Account.from_key(settings.PRIVATE_KEY).address
        except Exception:
            return None

    def test_connection(self) -> Dict[str, Any]:
        """Derive the EOA + its candidate wallets and report pUSD balances — shows
        which wallet holds the funds and which signature type will be used. Read-only
        (no relayer key required)."""
        if not settings.PRIVATE_KEY:
            return {"ok": False, "error": "missing_private_key"}
        try:
            from eth_account import Account
            eoa = Account.from_key(settings.PRIVATE_KEY).address
        except Exception as e:
            return {"ok": False, "error": f"invalid_key: {type(e).__name__}: {e}"}
        try:
            probe = self._new_gasless(3)
            wallets = []
            candidates = self._candidate_wallets(probe)
            for item in candidates:
                st, addr = item[0], item[1]
                lbl = item[2] if len(item) > 2 else "Wallet"
                try:
                    p_bal, u_bal = self._get_onchain_balances(probe, addr)
                    bal = max(p_bal, u_bal)
                except Exception:
                    bal = None
                wallets.append({"signature_type": st, "address": addr, "label": lbl, "pusd_balance": bal})
            sig_type, funder = self._pick_funded_wallet(probe)

            # Query CLOB API directly for true tradeable balance
            clob_bal = None
            try:
                from polymarket_apis.clients.clob_client import PolymarketClobClient
                c = PolymarketClobClient(
                    private_key=settings.PRIVATE_KEY,
                    address=funder,
                    chain_id=137,
                    signature_type=sig_type
                )
                c.set_api_creds(c.create_or_derive_api_creds())
                clob_bal = float(c.get_pusd_balance())
            except Exception:
                pass

            if clob_bal is not None and clob_bal >= 0:
                for w in wallets:
                    if w["address"].lower() == funder.lower():
                        w["pusd_balance"] = clob_bal

            return {
                "ok": True,
                "eoa": eoa,
                "chosen_signature_type": sig_type,
                "funder": funder,
                "relayer_key_set": bool(settings.RELAYER_API_KEY),
                "wallets": wallets,
            }
        except Exception as e:
            return {"ok": False, "eoa": eoa, "error": f"{type(e).__name__}: {e}"}

    def get_usdc_balance(self) -> Optional[float]:
        """pUSD balance of the funded wallet (dollars), or None. pUSD is Polymarket's
        V2 collateral; this is the deposit wallet's tradeable balance."""
        if not settings.PRIVATE_KEY:
            return None
        now = time.time()
        if getattr(self, "_cached_balance", None) is not None and (now - getattr(self, "_last_balance_ts", 0)) < 15.0:
            return self._cached_balance

        if getattr(self, "_balance_fetching", False):
            return getattr(self, "_cached_balance", None)
        self._balance_fetching = True
        try:
            # 1. Direct CLOB API query
            if self.ensure_ready() and self.clob is not None:
                try:
                    c_bal = float(self.clob.get_pusd_balance())
                    if c_bal is not None and c_bal >= 0:
                        self._cached_balance = c_bal
                        self._last_balance_ts = now
                        return c_bal
                except Exception:
                    pass

            # 2. Gasless on-chain query if funder is known
            if self.ready and self.gasless is not None and self.funder:
                try:
                    p_bal, u_bal = self._get_onchain_balances(self.gasless, self.funder)
                    bal = max(p_bal, u_bal)
                    self._cached_balance = bal
                    self._last_balance_ts = now
                    return bal
                except Exception:
                    pass

            # 3. Derive funder once if unknown
            if not self.funder:
                try:
                    probe = self._new_gasless(3)
                    sig_type, funder = self._pick_funded_wallet(probe)
                    self.funder = funder
                    self.signature_type = sig_type
                    p_bal, u_bal = self._get_onchain_balances(probe, funder)
                    bal = max(p_bal, u_bal)
                    self._cached_balance = bal
                    self._last_balance_ts = now
                    return bal
                except Exception:
                    pass

            return getattr(self, "_cached_balance", None)
        finally:
            self._balance_fetching = False


clob_trader = ClobTrader()
