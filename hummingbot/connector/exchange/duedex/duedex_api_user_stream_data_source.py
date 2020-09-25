#!/usr/bin/env python
import asyncio
import json
import logging
import time
from typing import (
    Any,
    AsyncIterable,
    Dict,
    Optional,
)
import websockets

from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.duedex.duedex_auth import DuedexAuth

DUEDEX_WS_URI = "wss://feed.duedex.com/v1/feed"


class DuedexAPIUserStreamDataSource(UserStreamTrackerDataSource):

    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _user_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._user_logger is None:
            cls._user_logger = logging.getLogger(__name__)

        return cls._user_logger

    def __init__(self, duedex_auth: DuedexAuth):
        self._auth: DuedexAuth = duedex_auth
        self._last_recv_time: float = 0
        super().__init__()

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    async def _inner_messages(self, ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        # Terminate the recv() loop as soon as the next message timed out, so the outer loop can reconnect.
        try:
            while True:
                try:
                    msg: str = await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)
                    self._last_recv_time = time.time()
                    yield msg
                except asyncio.TimeoutError:
                    try:
                        pong_waiter = await ws.ping()
                        await asyncio.wait_for(pong_waiter, timeout=self.PING_TIMEOUT)
                        self._last_recv_time = time.time()
                    except asyncio.TimeoutError:
                        raise
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket ping timed out. Going to reconnect...")
            return
        except websockets.exceptions.ConnectionClosed:
            return
        finally:
            await ws.close()

    async def listen_for_user_stream(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        try:
            while True:
                try:
                    async with websockets.connect(DUEDEX_WS_URI) as ws:
                        ws: websockets.WebSocketClientProtocol = ws
                        # To authenticate, the client first sends a challenge message.
                        req: Dict[str, Any] = {"type": "challenge"}
                        await ws.send(json.dumps(req))
                        async for raw_msg in self._inner_messages(ws):
                            msg: Dict[str, Any] = json.loads(raw_msg)
                            if "type" in msg:
                                if msg["type"] == "challenge":
                                    req = self._auth.get_ws_signature_dict(msg['challenge'])
                                    await ws.send(json.dumps(req))
                                elif msg["type"] == "auth":
                                    self.logger().info("Successfully authenticated")
                                    # Subscribe to Topics
                                    await ws.send(json.dumps({
                                        "type": "subscribe",
                                        "channels": [
                                            {
                                                "name": "margins",
                                            },
                                            {
                                                "name": "orders",
                                            }
                                        ]
                                    }))
                                elif msg["type"] == "subscriptions":
                                    for channel in msg["channels"]:
                                        self.logger().info(f"Success to subscribe {channel}")
                                elif msg["type"] in ["snapshot", "update"]:
                                    output.put_nowait(msg)
                                else:
                                    self.logger().error(f"Unrecognized type received from Duedex websocket: {msg['type']}")
                            else:
                                self.logger().error(f"Unrecognized message received from Duedex websocket: {msg}")
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.logger().error("Unexpected error with WebSocket connection. Retrying after 5 seconds...",
                                        exc_info=True)
                    await asyncio.sleep(5)
        finally:
            pass
