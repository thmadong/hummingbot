import asyncio
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.aster import aster_constants as CONSTANTS, aster_web_utils as web_utils
from hummingbot.connector.exchange.aster.aster_auth import AsterAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.aster.aster_exchange import AsterExchange


class AsterAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Aster user stream: a listen key is created through the signed REST API and the account events are
    pushed on ``wss://sstream.asterdex.com/ws/<listenKey>``.
    """

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: AsterAuth,
                 trading_pairs: List[str],
                 connector: 'AsterExchange',
                 api_factory: WebAssistantsFactory):
        super().__init__()
        self._auth: AsterAuth = auth
        self._trading_pairs = trading_pairs
        self._connector = connector
        self._api_factory = api_factory
        self._current_listen_key: Optional[str] = None
        self._manage_listen_key_task: Optional[asyncio.Task] = None
        self._listen_key_lock = asyncio.Lock()

    async def _get_listen_key(self) -> str:
        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            data = await rest_assistant.execute_request(
                url=web_utils.private_rest_url(CONSTANTS.LISTEN_KEY_PATH_URL),
                method=RESTMethod.POST,
                is_auth_required=True,
                throttler_limit_id=CONSTANTS.LISTEN_KEY_PATH_URL,
            )
            return data["listenKey"]
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error("Error obtaining the listen key.", exc_info=True)
            raise IOError("Could not obtain the Aster listen key.")

    async def _ping_listen_key(self) -> None:
        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            await rest_assistant.execute_request(
                url=web_utils.private_rest_url(CONSTANTS.LISTEN_KEY_PATH_URL),
                params={"listenKey": self._current_listen_key},
                method=RESTMethod.PUT,
                is_auth_required=True,
                throttler_limit_id=CONSTANTS.LISTEN_KEY_PATH_URL,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exception:
            self.logger().warning(f"Failed to refresh the listen key: {exception}")
            self._current_listen_key = None

    async def _manage_listen_key_loop(self):
        while True:
            await self._sleep(CONSTANTS.LISTEN_KEY_KEEPALIVE_TIME_INTERVAL)
            if self._current_listen_key is not None:
                await self._ping_listen_key()

    def _ensure_listen_key_keepalive_task(self):
        if self._manage_listen_key_task is None or self._manage_listen_key_task.done():
            self._manage_listen_key_task = asyncio.create_task(self._manage_listen_key_loop())

    async def _connected_websocket_assistant(self) -> WSAssistant:
        async with self._listen_key_lock:
            self._current_listen_key = await self._get_listen_key()

        self._ensure_listen_key_keepalive_task()

        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(
            ws_url=f"{CONSTANTS.WSS_URL}/{self._current_listen_key}",
            ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        # The listen key in the connection URL already scopes the stream; nothing else to subscribe.
        self.logger().info("Connected to the Aster user data stream...")

    async def _process_event_message(self, event_message: Dict[str, Any], queue):
        if not isinstance(event_message, dict) or len(event_message) == 0:
            return
        queue.put_nowait(event_message)

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]):
        websocket_assistant and await websocket_assistant.disconnect()
