import asyncio
import time
from decimal import Decimal
from typing import List, Optional

import aiohttp
from pydantic import Field

from hummingbot.core.data_type.common import PriceType, TradeType
from hummingbot.strategy_v2.controllers.market_making_controller_base import (
    MarketMakingControllerBase,
    MarketMakingControllerConfigBase,
)
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction

BINANCE_PERP_BOOK_URL = "https://fapi.binance.com/fapi/v1/ticker/bookTicker?symbol={symbol}"
ASTER_PERP_BOOK_URL = "https://fapi.asterdex.com/fapi/v1/ticker/bookTicker?symbol={symbol}"


class PMMPerpAnchoredConfig(MarketMakingControllerConfigBase):
    """
    与 pmm_simple 相同的挂单结构（价差 / 档位额度 / 三重屏障），但参考价改成**外部永续合约的中间价**。

    适用场景：现货盘口稀疏或尚未形成时，用流动性更好的永续合约作为定价锚，
    在现货市场按锚价挂单。当现货偏离锚价时，挂单会自然以更优价格成交，起到收敛价差的作用。
    """

    controller_name: str = "pmm_perp_anchored"
    anchor_symbol: str = Field(
        default="BTWUSDT",
        json_schema_extra={
            "prompt": "Enter the perpetual symbol used as price anchor (e.g. BTWUSDT): ",
            "prompt_on_new": True, "is_updatable": True}
    )
    anchor_refresh_s: float = Field(
        default=2.0,
        json_schema_extra={
            "prompt": "Enter the anchor refresh interval in seconds (e.g. 2): ",
            "prompt_on_new": True, "is_updatable": True}
    )
    anchor_max_age_s: float = Field(
        default=10.0,
        json_schema_extra={
            "prompt": "Enter how old the anchor price may be before quoting stops (seconds, e.g. 10): ",
            "prompt_on_new": True, "is_updatable": True}
    )
    fallback_to_aster_perp: bool = Field(
        default=True,
        json_schema_extra={
            "prompt": "Fall back to the Aster perpetual mid when Binance is unavailable? (True/False): ",
            "prompt_on_new": True, "is_updatable": True}
    )
    max_base_exposure_quote: Decimal = Field(
        default=Decimal("0"), ge=0,
        json_schema_extra={
            "prompt": "Max base-asset exposure in quote units (0 = disabled, e.g. 1100): ",
            "prompt_on_new": True, "is_updatable": True}
    )
    max_basis_pct: Decimal = Field(
        default=Decimal("0"), ge=0,
        json_schema_extra={
            "prompt": "Pause new quotes when |spot/anchor-1| exceeds this (0.15 = 15%, 0 = disabled): ",
            "prompt_on_new": True, "is_updatable": True}
    )


class PMMPerpAnchoredController(MarketMakingControllerBase):
    """
    参考价 = Binance 永续中间价（失败时可选回退到 Aster 永续）。
    锚价不可用或过期时停止创建新挂单，已有 executor 不受影响。
    """

    def __init__(self, config: PMMPerpAnchoredConfig, *args, **kwargs):
        self._session: Optional[aiohttp.ClientSession] = None
        self._anchor_price: Optional[Decimal] = None
        self._anchor_ts: float = 0.0
        self._anchor_source: str = "-"
        self._last_fetch_ts: float = 0.0
        self._last_log_ts: float = 0.0
        self._last_warn_ts: float = 0.0
        self._last_exposure_warn_ts: float = 0.0
        super().__init__(config, *args, **kwargs)

    # ------------------------------------------------------------------ anchor feed

    def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            # trust_env=False：避免容器里遗留的代理变量把行情请求带偏
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=4), trust_env=False)
        return self._session

    async def _book_mid(self, url: str) -> Optional[Decimal]:
        try:
            async with self._ensure_session().get(url) as response:
                if response.status != 200:
                    return None
                data = await response.json()
            bid = Decimal(str(data.get("bidPrice") or "0"))
            ask = Decimal(str(data.get("askPrice") or "0"))
            if bid <= 0 or ask <= 0:
                return None
            return (bid + ask) / 2
        except asyncio.CancelledError:
            raise
        except Exception:
            return None

    async def _refresh_anchor(self, now: float) -> None:
        if now - self._last_fetch_ts < self.config.anchor_refresh_s:
            return
        self._last_fetch_ts = now

        symbol = self.config.anchor_symbol
        price = await self._book_mid(BINANCE_PERP_BOOK_URL.format(symbol=symbol))
        source = "binance_perp"
        if price is None and self.config.fallback_to_aster_perp:
            price = await self._book_mid(ASTER_PERP_BOOK_URL.format(symbol=symbol))
            source = "aster_perp"

        if price is not None:
            self._anchor_price = price
            self._anchor_ts = now
            self._anchor_source = source

    def _spot_mid(self) -> Optional[Decimal]:
        try:
            price = self.market_data_provider.get_price_by_type(
                self.config.connector_name, self.config.trading_pair, PriceType.MidPrice)
            value = Decimal(str(price))
            return value if value > 0 else None
        except Exception:
            return None

    # ------------------------------------------------------------------ controller hooks

    async def update_processed_data(self):
        now = self.market_data_provider.time()
        await self._refresh_anchor(now)

        anchor_ready = (
            self._anchor_price is not None
            and (now - self._anchor_ts) <= self.config.anchor_max_age_s
            and self._anchor_price > 0
        )
        spot_mid = self._spot_mid()
        basis_pct: Optional[Decimal] = None
        if anchor_ready and spot_mid is not None:
            basis_pct = (spot_mid / self._anchor_price - 1) * 100

        self.processed_data = {
            # 基类 get_price_and_amount 会读 reference_price 与 spread_multiplier
            "reference_price": self._anchor_price or Decimal("0"),
            "spread_multiplier": Decimal("1"),
            "anchor_ready": anchor_ready,
            "anchor_source": self._anchor_source,
            "anchor_age_s": round(now - self._anchor_ts, 1) if self._anchor_price is not None else None,
            "spot_mid": spot_mid,
            "basis_pct": basis_pct,
        }

        if anchor_ready and now - self._last_log_ts >= 60:
            self._last_log_ts = now
            exposure_txt = ""
            if self.config.max_base_exposure_quote > 0:
                exposure = self._current_buy_exposure_quote(self._anchor_price)
                exposure_txt = (f" | exposure={exposure:.2f}/"
                                f"{self.config.max_base_exposure_quote:.0f}U")
            self.logger().info(
                f"anchor={self._anchor_price:.6f} ({self._anchor_source}) | "
                f"spot_mid={spot_mid if spot_mid is not None else 'n/a'} | "
                f"basis={f'{basis_pct:+.2f}%' if basis_pct is not None else 'n/a'}"
                + exposure_txt
            )

    def create_actions_proposal(self) -> List[CreateExecutorAction]:
        if not self.processed_data.get("anchor_ready"):
            now = self.market_data_provider.time()
            if now - self._last_warn_ts >= 60:
                self._last_warn_ts = now
                self.logger().warning(
                    "anchor price unavailable or stale — pausing new quotes "
                    f"(source={self._anchor_source}, age={self.processed_data.get('anchor_age_s')}s, "
                    f"max_age={self.config.anchor_max_age_s}s)"
                )
            return []

        if self._basis_guard_blocking():
            return []

        actions = super().create_actions_proposal()
        return self._apply_buy_exposure_cap(actions)

    # ------------------------------------------------------------------ risk guards

    def _basis_guard_blocking(self) -> bool:
        """基差超过阈值时暂停新建挂单：此时本地盘口与锚价脱节，挂单大概率穿价成交。"""
        limit = self.config.max_basis_pct
        basis = self.processed_data.get("basis_pct")
        if limit <= 0 or basis is None:
            return False
        if abs(basis) <= limit * 100:
            return False
        now = self.market_data_provider.time()
        if now - self._last_warn_ts >= 60:
            self._last_warn_ts = now
            self.logger().warning(
                f"basis {basis:+.2f}% exceeds max_basis_pct {limit * 100:.2f}% — pausing new quotes "
                f"(anchor={self.processed_data.get('reference_price')}, spot={self.processed_data.get('spot_mid')})"
            )
        return True

    def _current_buy_exposure_quote(self, price: Decimal) -> Decimal:
        """已持有基础币的市值 + 活跃买单尚未成交的部分。"""
        held = Decimal(str(self.get_current_base_position())) * price
        pending = Decimal("0")
        for executor in self.executors_info:
            if not executor.is_active or executor.side != TradeType.BUY:
                continue
            amount = Decimal(str(executor.config.amount))
            filled_quote = Decimal(str(executor.filled_amount_quote or 0))
            remaining = max(Decimal("0"), amount * price - filled_quote)
            pending += remaining
        return held + pending

    def _apply_buy_exposure_cap(self, actions: List[CreateExecutorAction]) -> List[CreateExecutorAction]:
        """买侧敞口超过上限后不再新增买单；卖单不受限（现货无法做空，卖出只会减少敞口）。"""
        cap = self.config.max_base_exposure_quote
        if cap <= 0:
            return actions
        price = Decimal(str(self.processed_data.get("reference_price") or "0"))
        if price <= 0:
            return actions

        exposure = self._current_buy_exposure_quote(price)
        kept: List[CreateExecutorAction] = []
        for action in actions:
            executor_config = getattr(action, "executor_config", None)
            if executor_config is not None and getattr(executor_config, "side", None) == TradeType.BUY:
                entry_price = Decimal(str(executor_config.entry_price or price))
                notional = Decimal(str(executor_config.amount)) * entry_price
                if exposure + notional > cap:
                    now = self.market_data_provider.time()
                    if now - self._last_exposure_warn_ts >= 60:
                        self._last_exposure_warn_ts = now
                        self.logger().warning(
                            f"base exposure {exposure:.2f} USDT would exceed cap {cap:.2f} USDT — "
                            f"skipping new buy levels"
                        )
                    continue
                exposure += notional
            kept.append(action)
        return kept

    def get_executor_config(self, level_id: str, price: Decimal, amount: Decimal):
        trade_type = self.get_trade_type_from_level_id(level_id)
        return PositionExecutorConfig(
            timestamp=self.market_data_provider.time(),
            level_id=level_id,
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            entry_price=price,
            amount=amount,
            triple_barrier_config=self.config.triple_barrier_config,
            leverage=self.config.leverage,
            side=trade_type,
        )

    def on_stop(self):
        if self._session is not None and not self._session.closed:
            try:
                asyncio.get_event_loop().create_task(self._session.close())
            except RuntimeError:
                pass
