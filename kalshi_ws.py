"""
kalshi_ws.py — Live Kalshi orderbook cache via WebSocket

Maintains a real-time snapshot of all subscribed tickers' orderbooks by
receiving orderbook_snapshot + orderbook_delta messages from Kalshi's
WebSocket API. The main bot checks this cache first in kalshi_get_orderbook;
if the cache is stale or the ticker isn't subscribed it falls back to REST.

Usage:
    from kalshi_ws import ws_manager
    ws_manager.start()                        # call once at startup
    ws_manager.subscribe(["TICKER1", ...])    # call whenever new tickers are found
    ob = ws_manager.get_orderbook("TICKER1")  # returns REST-compatible dict or None
"""

import asyncio
import base64
import json
import logging
import os
import threading
import time
from typing import Dict, List, Optional, Set

logger = logging.getLogger(__name__)

WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
RECONNECT_DELAY_SECONDS = 5
MAX_RECONNECT_DELAY_SECONDS = 60
# How old a cached orderbook can be before we force a REST fallback
WS_CACHE_MAX_AGE_SECONDS = 30.0


class KalshiWebSocketManager:
    """
    Maintains live orderbook snapshots for subscribed Kalshi tickers.
    Runs in a background daemon thread using asyncio.
    Thread-safe: all public methods acquire _lock before touching shared state.
    """

    def __init__(self):
        self._lock = threading.Lock()
        # ticker -> {"yes": {price_cents: qty}, "no": {price_cents: qty}}
        self._orderbooks: Dict[str, dict] = {}
        # ticker -> unix timestamp of last update
        self._ts: Dict[str, float] = {}
        # tickers we are currently subscribed to on the live connection
        self._subscribed: Set[str] = set()
        # tickers waiting to be sent in the next subscribe message
        self._pending_subscribe: Set[str] = set()

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._connected = False
        self._msg_id = 0

        # Auth — loaded lazily in start()
        self._private_key = None
        self._key_id: str = ""

    # ── Public API ────────────────────────────────────────────────────────────

    def start(self):
        """Start the background WebSocket thread. Safe to call multiple times."""
        if self._thread and self._thread.is_alive():
            return
        if not self._load_auth():
            logger.warning("KalshiWS: auth config missing — WebSocket disabled, using REST only")
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="kalshi-ws"
        )
        self._thread.start()
        logger.info("KalshiWS: background thread started")

    def stop(self):
        """Signal the background thread to exit."""
        self._running = False

    def subscribe(self, tickers: List[str]):
        """
        Queue tickers for subscription. Safe to call at any time from any thread.
        Already-subscribed or already-pending tickers are ignored.
        """
        with self._lock:
            new = set(tickers) - self._subscribed - self._pending_subscribe
            if not new:
                return
            self._pending_subscribe.update(new)
        logger.debug(f"KalshiWS: queued {len(new)} tickers for subscription")
        # Wake the asyncio loop so it picks up pending subs without waiting 2s
        if self._loop is not None:
            self._loop.call_soon_threadsafe(lambda: None)

    def get_orderbook(self, ticker: str) -> Optional[dict]:
        """
        Return live orderbook for ticker in REST-compatible format, or None.
        Returns None if: ticker not subscribed, no snapshot yet, or data is stale.
        Format: {"orderbook": {"yes": [[price, qty], ...], "no": [[price, qty], ...]}}
        """
        with self._lock:
            if ticker not in self._orderbooks:
                return None
            age = time.time() - self._ts.get(ticker, 0.0)
            if age > WS_CACHE_MAX_AGE_SECONDS:
                return None
            yes_dict = dict(self._orderbooks[ticker].get("yes", {}))
            no_dict = dict(self._orderbooks[ticker].get("no", {}))

        yes_levels = sorted(
            [[p, q] for p, q in yes_dict.items() if q > 0],
            key=lambda x: -x[0],
        )
        no_levels = sorted(
            [[p, q] for p, q in no_dict.items() if q > 0],
            key=lambda x: -x[0],
        )
        return {"orderbook": {"yes": yes_levels, "no": no_levels}}

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def subscribed_count(self) -> int:
        with self._lock:
            return len(self._subscribed)

    def status(self) -> dict:
        with self._lock:
            return {
                "connected": self._connected,
                "subscribed": len(self._subscribed),
                "pending": len(self._pending_subscribe),
                "cached_tickers": len(self._orderbooks),
            }

    # ── Auth ─────────────────────────────────────────────────────────────────

    def _load_auth(self) -> bool:
        key_path = os.getenv("KALSHI_PRIVATE_KEY_PATH", "")
        self._key_id = os.getenv("KALSHI_API_KEY_ID", "")
        if not key_path or not self._key_id:
            return False
        try:
            from cryptography.hazmat.primitives import serialization
            with open(key_path, "rb") as f:
                self._private_key = serialization.load_pem_private_key(f.read(), password=None)
            return True
        except Exception as e:
            logger.warning(f"KalshiWS: failed to load private key: {e}")
            return False

    def _auth_headers(self) -> dict:
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding as apadding
        ts_ms = str(int(time.time() * 1000))
        msg = f"{ts_ms}GET/trade-api/ws/v2"
        sig = self._private_key.sign(
            msg.encode(),
            apadding.PSS(
                mgf=apadding.MGF1(hashes.SHA256()),
                salt_length=apadding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": self._key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts_ms,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
        }

    # ── Background thread ────────────────────────────────────────────────────

    def _run_loop(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._connect_loop())

    async def _connect_loop(self):
        delay = RECONNECT_DELAY_SECONDS
        while self._running:
            try:
                await self._run_connection()
                delay = RECONNECT_DELAY_SECONDS  # reset on clean disconnect
            except Exception as e:
                logger.warning(f"KalshiWS: connection error — {e}")
            if not self._running:
                break
            self._connected = False
            logger.info(f"KalshiWS: reconnecting in {delay}s")
            await asyncio.sleep(delay)
            delay = min(delay * 2, MAX_RECONNECT_DELAY_SECONDS)

    async def _run_connection(self):
        import websockets

        headers = self._auth_headers()
        logger.info("KalshiWS: connecting to %s", WS_URL)

        async with websockets.connect(
            WS_URL,
            additional_headers=headers,
            ping_interval=20,
            ping_timeout=10,
        ) as ws:
            self._connected = True
            logger.info("KalshiWS: connected")

            # On reconnect, re-queue all previously subscribed tickers
            with self._lock:
                to_resubscribe = list(self._subscribed)
                self._subscribed.clear()
                self._pending_subscribe.update(to_resubscribe)

            while self._running:
                # Flush pending subscriptions
                with self._lock:
                    pending = list(self._pending_subscribe)
                    self._pending_subscribe.clear()

                if pending:
                    self._msg_id += 1
                    await ws.send(json.dumps({
                        "id": self._msg_id,
                        "cmd": "subscribe",
                        "params": {
                            "channels": ["orderbook_delta"],
                            "market_tickers": pending,
                        },
                    }))
                    with self._lock:
                        self._subscribed.update(pending)
                    logger.info(
                        "KalshiWS: subscribed to %d tickers (total=%d)",
                        len(pending),
                        len(self._subscribed),
                    )

                # Wait for next message with a short timeout so we can flush
                # pending subscriptions without a long delay
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                    self._handle_message(raw)
                except asyncio.TimeoutError:
                    continue
                except websockets.ConnectionClosed:
                    logger.warning("KalshiWS: connection closed by server")
                    break

    # ── Message handling ─────────────────────────────────────────────────────

    def _handle_message(self, raw: str):
        try:
            msg = json.loads(raw)
        except Exception:
            return

        msg_type = msg.get("type")
        data = msg.get("msg", {})
        if not isinstance(data, dict):
            return
        ticker = data.get("market_ticker")
        if not ticker:
            return

        if msg_type == "orderbook_snapshot":
            self._apply_snapshot(ticker, data)
        elif msg_type == "orderbook_delta":
            self._apply_delta(ticker, data)

    def _apply_snapshot(self, ticker: str, data: dict):
        yes_dict: Dict[int, int] = {}
        no_dict: Dict[int, int] = {}

        for level in data.get("yes", []):
            price, qty = self._parse_level(level)
            if price is not None and qty is not None and qty > 0:
                yes_dict[price] = qty

        for level in data.get("no", []):
            price, qty = self._parse_level(level)
            if price is not None and qty is not None and qty > 0:
                no_dict[price] = qty

        with self._lock:
            self._orderbooks[ticker] = {"yes": yes_dict, "no": no_dict}
            self._ts[ticker] = time.time()

    def _apply_delta(self, ticker: str, data: dict):
        price = data.get("price")
        delta = data.get("delta")
        side = data.get("side")

        if price is None or delta is None or side not in ("yes", "no"):
            return

        with self._lock:
            if ticker not in self._orderbooks:
                # Delta before snapshot — re-queue for subscription to get snapshot
                self._pending_subscribe.add(ticker)
                return
            ob = self._orderbooks[ticker][side]
            new_qty = ob.get(int(price), 0) + int(delta)
            if new_qty <= 0:
                ob.pop(int(price), None)
            else:
                ob[int(price)] = new_qty
            self._ts[ticker] = time.time()

    @staticmethod
    def _parse_level(level) -> tuple:
        if isinstance(level, (list, tuple)) and len(level) >= 2:
            try:
                return int(level[0]), int(level[1])
            except (ValueError, TypeError):
                return None, None
        if isinstance(level, dict):
            try:
                return int(level.get("price", -1)), int(level.get("quantity", level.get("qty", 0)))
            except (ValueError, TypeError):
                return None, None
        return None, None


# Module-level singleton
ws_manager = KalshiWebSocketManager()
