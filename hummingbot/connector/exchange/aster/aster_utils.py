from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True

EXAMPLE_PAIR = "ASTER-USDT"

# Aster spot default tier: 0.02% maker / 0.07% taker
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0002"),
    taker_percent_fee_decimal=Decimal("0.0007"),
    buy_percent_fee_deducted_from_returns=True
)


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Verifies if a trading pair is enabled to operate with based on its exchange information.

    Aster exposes no permission sets, so only the trading status is checked. Symbols starting with
    "TEST" are Aster's internal test markets and are explicitly excluded from trading.
    """
    symbol = str(exchange_info.get("symbol", ""))
    is_trading = exchange_info.get("status") == "TRADING"
    is_internal_test_symbol = symbol.upper().startswith("TEST")
    return is_trading and not is_internal_test_symbol


class AsterConfigMap(BaseConnectorConfigMap):
    connector: str = "aster"
    aster_user_address: str = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Aster login wallet address (user)",
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    aster_signer_address: str = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Aster API wallet (signer) address",
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    aster_signer_private_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Aster API wallet (signer) private key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="aster")


KEYS = AsterConfigMap.model_construct()
