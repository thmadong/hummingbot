import json
import threading
from collections import OrderedDict
from typing import Any, Dict
from urllib.parse import urlencode

from eth_account import Account
from eth_account.messages import encode_typed_data

from hummingbot.connector.exchange.aster import aster_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class AsterAuth(AuthBase):
    """
    Aster "API wallet" authentication.

    Every signed request carries ``nonce`` (current time in microseconds), ``signer`` (the API wallet
    address) and ``signature``. The signature is an EIP-712 signature (domain ``AsterSignTransaction``)
    over the exact URL-encoded parameter string that is sent to the server, produced with the API
    wallet's private key.
    """

    def __init__(
            self,
            user_address: str,
            signer_address: str,
            signer_private_key: str,
            time_provider: TimeSynchronizer):
        self._user_address = user_address
        self._signer_address = signer_address
        self._account = Account.from_key(signer_private_key)
        self.time_provider = time_provider
        self._nonce_lock = threading.Lock()
        self._last_seconds = 0
        self._sequence = 0

    @property
    def user_address(self) -> str:
        return self._user_address

    def _nonce(self) -> str:
        """
        Microsecond nonce. Aster keeps only the last 100 nonces per agent address and rejects repeats,
        so the value is bumped by one microsecond when several requests land in the same second.
        """
        with self._nonce_lock:
            now_seconds = int(self.time_provider.time())
            if now_seconds == self._last_seconds:
                self._sequence += 1
            else:
                self._last_seconds = now_seconds
                self._sequence = 0
            return str(now_seconds * 1_000_000 + self._sequence)

    def _sign(self, message: str) -> str:
        typed_data = {
            "types": CONSTANTS.EIP712_TYPES,
            "primaryType": "Message",
            "domain": CONSTANTS.EIP712_DOMAIN,
            "message": {"msg": message},
        }
        structured_data = encode_typed_data(full_message=typed_data)
        signed = self._account.sign_message(structured_data)
        return signed.signature.hex()

    def _signed_query(self, params: Dict[str, Any]) -> str:
        signed_params = OrderedDict(params or {})
        signed_params["nonce"] = self._nonce()
        signed_params["signer"] = self._signer_address
        query = urlencode(signed_params)
        signature = self._sign(query)
        return f"{query}&signature={signature}"

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Adds the nonce, signer and EIP-712 signature required for authenticated interactions.
        """
        if request.method in (RESTMethod.POST, RESTMethod.PUT, RESTMethod.PATCH):
            data = request.data
            if isinstance(data, (str, bytes)) and len(data) > 0:
                params = json.loads(data)
            else:
                params = {}
            params.update(request.params or {})
            request.params = None
            request.data = self._signed_query(params)

            headers = dict(request.headers or {})
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            request.headers = headers
        else:
            query = self._signed_query(dict(request.params or {}))
            separator = "&" if "?" in request.url else "?"
            request.url = f"{request.url}{separator}{query}"
            request.params = None

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        The user stream is authenticated through the listen key, not through the websocket request.
        """
        return request  # pass-through
