#!/usr/bin/env python3
"""BTW 做市看门狗：账户回撤达到阈值就停策略并发邮件告警。

由 cron 每 5 分钟跑一次。判定与执行都**不需要 keystore 密码**：

- 权益数据来自 `hbot status --json`（运行中的引擎把 balances 写进 status.json，
  读文件即可，不解密任何密钥）；
- 停止用 `hbot stop`（只读 PID 并发 SIGTERM）；
- 发信只用 SMTP 授权码，与策略密钥无关。

判定口径
--------
权益 = USDT 余额 + BTW 余额 × BTW 价格（现货中间价优先，无双边盘口时退化为永续最新价）。
回撤 = (基准权益 - 当前权益) / 基准权益。首次运行时把当时的权益记为基准，
之后可以用 `--baseline` 指定或 `--reset-baseline` 重设（入金/出金后必须重设，
否则入金会被当成盈利、出金会被当成亏损）。

触发后
------
1. 执行 `hbot stop --timeout 60`（优雅停止：撤单，已成交敞口会转为持仓保留）；
2. 发邮件到 `--mail-to`（默认 2194667913@qq.com）；
3. 状态文件记录 triggered，避免每 5 分钟重复发信；回撤回到阈值一半以下自动复位。

SMTP 凭据来源（按优先级）
------------------------
1. 进程环境变量 `HBOT_ALERT_SMTP_HOST/_PORT/_USER/_PASSWORD`
2. `$HBOT_ALERT_ENV_FILE` 指向的文件
3. `~/.config/hbot-alert.env`
4. `/home/linuxuser/thmadong/quant/.env`（复用已有的 QUANT_ALERT_SMTP_* 配置）
只读这几个键，不会把整份 .env 载入环境。

可调参数（阈值 / 收件人）
------------------------
优先级：命令行 > 进程环境变量 > 环境文件 > 内置默认。

| 参数 | 命令行 | 环境变量 | 默认 |
|---|---|---|---|
| 回撤阈值 | `--threshold 0.15` | `HBOT_WATCHDOG_THRESHOLD` | `0.20` |
| 收件人 | `--mail-to a@b.com` | `HBOT_ALERT_MAIL_TO` | `2194667913@qq.com` |

改 `~/.config/hbot-alert.env` 即可，不需要动 cron：

    HBOT_WATCHDOG_THRESHOLD=0.15
    HBOT_ALERT_MAIL_TO=2194667913@qq.com

用法
----
    python3 ops/btw_watchdog.py                 # 正常巡检（cron 用这个）
    python3 ops/btw_watchdog.py --dry-run       # 只判定，不停不发信
    python3 ops/btw_watchdog.py --show-config   # 打印生效的阈值/收件人/SMTP
    python3 ops/btw_watchdog.py --test-email    # 发一封测试邮件后退出
    python3 ops/btw_watchdog.py --reset-baseline
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import smtplib
import ssl
import subprocess
import sys
import time
import urllib.request
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, Optional

STATE_DIR = Path.home() / ".local" / "state" / "hbot-watchdog"
STATE_FILE = STATE_DIR / "state.json"
LOG_FILE = STATE_DIR / "watchdog.jsonl"

SPOT_TICKER_URL = "https://sapi.asterdex.com/api/v3/ticker/bookTicker?symbol=BTWUSDT"
PERP_TICKER_URL = "https://fapi.asterdex.com/fapi/v1/ticker/price?symbol=BTWUSDT"

DEFAULT_MAIL_TO = "2194667913@qq.com"
DEFAULT_THRESHOLD = 0.20
BASE_ASSET = "BTW"
QUOTE_ASSET = "USDT"

# 可调参数。优先级：命令行 > 进程环境变量 > 环境文件 > 内置默认值。
# 环境文件按 ENV_FILES 顺序查找，同一个键以先出现者为准。
ENV_NAMES = {
    "HOST": ("HBOT_ALERT_SMTP_HOST", "QUANT_ALERT_SMTP_HOST"),
    "PORT": ("HBOT_ALERT_SMTP_PORT", "QUANT_ALERT_SMTP_PORT"),
    "USER": ("HBOT_ALERT_SMTP_USER", "QUANT_ALERT_SMTP_USER"),
    "PASSWORD": ("HBOT_ALERT_SMTP_PASSWORD", "QUANT_ALERT_SMTP_PASSWORD"),
    "MAIL_TO": ("HBOT_ALERT_MAIL_TO", "QUANT_ALERT_MAIL_TO"),
    "THRESHOLD": ("HBOT_WATCHDOG_THRESHOLD",),
}
ENV_FILES = [
    Path(os.environ.get("HBOT_ALERT_ENV_FILE", "")) if os.environ.get("HBOT_ALERT_ENV_FILE") else None,
    Path.home() / ".config" / "hbot-alert.env",
    Path("/home/linuxuser/thmadong/quant/.env"),
]


# --------------------------------------------------------------------------- utils

def now_iso() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def log(record: Dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    record.setdefault("ts", now_iso())
    with LOG_FILE.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    print(json.dumps(record, ensure_ascii=False))


def read_state() -> Dict[str, Any]:
    try:
        return json.loads(STATE_FILE.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def write_state(state: Dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, ensure_ascii=False))
    tmp.replace(STATE_FILE)


def hbot_binary() -> str:
    found = shutil.which("hbot")
    if found:
        return found
    candidate = Path.home() / ".local" / "bin" / "hbot"
    return str(candidate)


def run_hbot(args: list[str], timeout: float = 30.0) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    env["PATH"] = f"{Path.home() / '.local' / 'bin'}:/usr/local/bin:/usr/bin:/bin"
    # 强制走 docker 分支：cron 环境里没有 conda 初始化，但显式声明可避免将来装回 conda 后被劫持。
    env.setdefault("HBOT_PREFER", "docker")
    return subprocess.run(
        [hbot_binary()] + args,
        capture_output=True, text=True, timeout=timeout, env=env,
    )


def get_json(url: str, timeout: float = 10.0) -> Optional[Dict[str, Any]]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return json.loads(response.read().decode())
    except Exception:
        return None


def base_price() -> Optional[float]:
    spot = get_json(SPOT_TICKER_URL)
    if spot:
        bid = float(spot.get("bidPrice") or 0)
        ask = float(spot.get("askPrice") or 0)
        if bid > 0 and ask > 0:
            return (bid + ask) / 2
    perp = get_json(PERP_TICKER_URL)
    if perp:
        price = float(perp.get("price") or 0)
        if price > 0:
            return price
    return None


# --------------------------------------------------------------------------- email

def load_config() -> Dict[str, str]:
    """合并出有效配置：进程环境变量优先，其次按顺序扫描环境文件（同一个键先出现者为准）。"""
    config: Dict[str, str] = {}
    for key, env_names in ENV_NAMES.items():
        for name in env_names:
            value = os.environ.get(name)
            if value:
                config[key] = value
                break

    missing = [key for key in ENV_NAMES if key not in config]
    if missing:
        wanted = {name for names in ENV_NAMES.values() for name in names}
        for path in ENV_FILES:
            if not missing:
                break
            if path is None or not path.exists():
                continue
            for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                name, value = line.split("=", 1)
                value = value.strip().strip('"').strip("'")
                if name in wanted and value:
                    for key, names in ENV_NAMES.items():
                        if name in names and key not in config:
                            config[key] = value
            missing = [key for key in ENV_NAMES if key not in config]

    config.setdefault("MAIL_TO", DEFAULT_MAIL_TO)
    config.setdefault("THRESHOLD", str(DEFAULT_THRESHOLD))
    return config


def send_mail(subject: str, body: str, config: Dict[str, str], timeout: float = 20.0) -> bool:
    host = config.get("HOST")
    user = config.get("USER")
    password = config.get("PASSWORD")
    mail_to = config.get("MAIL_TO") or DEFAULT_MAIL_TO
    if not (host and user and password):
        log({"event": "mail_skipped", "reason": "smtp not configured"})
        return False

    try:
        port = int(config.get("PORT") or 465)
    except ValueError:
        port = 465

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = user
    message["To"] = mail_to
    message.set_content(body)

    try:
        if port == 465:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(host, port, timeout=timeout, context=context) as server:
                server.login(user, password)
                server.send_message(message)
        else:
            with smtplib.SMTP(host, port, timeout=timeout) as server:
                server.starttls(context=ssl.create_default_context())
                server.login(user, password)
                server.send_message(message)
        log({"event": "mail_sent", "to": mail_to, "subject": subject})
        return True
    except Exception as exception:  # 发信失败不影响熔断结论，下一轮会重试
        log({"event": "mail_failed", "error": str(exception)[:300]})
        return False


# --------------------------------------------------------------------------- main flow

def parse_json_output(text: str) -> Optional[Dict[str, Any]]:
    """`hbot --json` 输出是缩进过的 JSON，可能带前后噪声，取第一个 `{` 到最后一个 `}`。"""
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        return json.loads(text[start:end + 1])
    except json.JSONDecodeError:
        return None


def read_account() -> Optional[Dict[str, Any]]:
    result = run_hbot(["status", "--json"], timeout=30)
    if result.returncode != 0:
        return None
    status = parse_json_output(result.stdout)
    if status is None:
        return None
    if not status.get("running"):
        return {"running": False}

    balances = (status.get("balances") or {}).get("aster") or {}
    usdt = float(balances.get(QUOTE_ASSET, 0) or 0)
    base = float(balances.get(BASE_ASSET, 0) or 0)
    price = base_price()
    if price is None and base > 0:
        return {"running": True, "error": "no price available"}
    equity = usdt + base * (price or 0)
    return {
        "running": True,
        "pid": status.get("pid"),
        "usdt": usdt,
        "base": base,
        "price": price,
        "equity": equity,
        "other_assets": sorted(a for a, v in balances.items() if a not in (QUOTE_ASSET, BASE_ASSET) and v),
    }


def stop_bot() -> Dict[str, Any]:
    try:
        result = run_hbot(["stop", "--timeout", "60"], timeout=120)
        return {"ok": result.returncode == 0, "exit": result.returncode,
                "stdout": (result.stdout or "").strip()[:400],
                "stderr": (result.stderr or "").strip()[:400]}
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": "hbot stop timed out"}
    except Exception as exception:
        return {"ok": False, "error": str(exception)[:300]}


def build_alert_body(account: Dict[str, Any], baseline: float, threshold: float) -> str:
    drawdown = (baseline - account["equity"]) / baseline if baseline else 0
    return "\n".join([
        "BTW 做市看门狗触发熔断",
        "",
        f"时间:        {now_iso()}",
        f"基准权益:    {baseline:.2f} USDT",
        f"当前权益:    {account['equity']:.2f} USDT",
        f"回撤:        {drawdown * 100:.2f}% (阈值 {threshold * 100:.0f}%)",
        "",
        f"USDT:        {account['usdt']:.2f}",
        f"BTW:         {account['base']:.0f}",
        f"BTW 价格:    {account['price']:.5f}" if account.get("price") else "BTW 价格:    N/A",
        "",
        "已执行: hbot stop（优雅停止，撤单）",
        "",
        "注意: stop 只保证停止报价并撤单；已成交的敞口会转为持仓保留，",
        "     请登录 Aster 确认是否需要手动平仓。",
        "",
        "复核后重新启动: hbot start conf_aster_btw_mm.yml",
        "重新设置基准:   python3 ops/btw_watchdog.py --reset-baseline",
    ])


def main() -> int:
    parser = argparse.ArgumentParser(description="BTW 做市账户回撤看门狗")
    parser.add_argument("--threshold", type=float, default=None,
                        help=f"回撤阈值（0.20 = 20%%）。优先取 $HBOT_WATCHDOG_THRESHOLD，默认 {DEFAULT_THRESHOLD}")
    parser.add_argument("--baseline", type=float, default=None, help="指定基准权益（USDT）")
    parser.add_argument("--mail-to", default=None,
                        help=f"告警收件人。优先取 $HBOT_ALERT_MAIL_TO，默认 {DEFAULT_MAIL_TO}")
    parser.add_argument("--dry-run", action="store_true", help="只判定，不停止机器人也不发信")
    parser.add_argument("--test-email", action="store_true", help="发一封测试邮件后退出")
    parser.add_argument("--reset-baseline", action="store_true", help="用当前权益重设基准")
    parser.add_argument("--show-config", action="store_true", help="打印生效的阈值/收件人/SMTP（密码打码）后退出")
    args = parser.parse_args()

    config = load_config()
    if args.mail_to:
        config["MAIL_TO"] = args.mail_to
    threshold = args.threshold if args.threshold is not None else float(config["THRESHOLD"])

    if args.show_config:
        print(json.dumps({
            "threshold": threshold,
            "mail_to": config.get("MAIL_TO"),
            "threshold_source": ("cli" if args.threshold is not None
                                 else "$HBOT_WATCHDOG_THRESHOLD" if os.environ.get("HBOT_WATCHDOG_THRESHOLD")
                                 else "env file / default"),
            "smtp_host": config.get("HOST"),
            "smtp_port": config.get("PORT"),
            "smtp_user": config.get("USER"),
            "smtp_password": "***" if config.get("PASSWORD") else "(unset)",
            "env_files": [str(p) for p in ENV_FILES if p is not None],
            "state_file": str(STATE_FILE),
        }, indent=2, ensure_ascii=False))
        return 0

    if args.test_email:
        ok = send_mail(
            "[HBOT] 看门狗测试邮件",
            f"这是一封测试邮件，发送时间 {now_iso()}。\n收到即代表告警通道可用。"
            f"\n当前阈值 {threshold * 100:.0f}%，收件人 {config.get('MAIL_TO')}。",
            config,
        )
        return 0 if ok else 1

    state = read_state()
    account = read_account()

    if account is None:
        log({"event": "status_unavailable"})
        return 0
    if not account.get("running"):
        log({"event": "bot_not_running"})
        return 0
    if "error" in account:
        log({"event": "equity_unavailable", "error": account["error"]})
        return 0

    equity = account["equity"]
    baseline = args.baseline or state.get("baseline")
    if args.reset_baseline or not baseline:
        state["baseline"] = equity
        state["baseline_at"] = now_iso()
        state.pop("triggered_at", None)
        write_state(state)
        log({"event": "baseline_set", "baseline": round(equity, 2)})
        return 0

    baseline = float(baseline)
    drawdown = (baseline - equity) / baseline if baseline else 0.0
    already_triggered = bool(state.get("triggered_at"))

    # 回撤回到阈值一半以下时复位，允许下一次触发（避免永久静默）
    if already_triggered and drawdown < threshold / 2:
        state.pop("triggered_at", None)
        state.pop("triggered_equity", None)
        write_state(state)
        log({"event": "rearmed", "drawdown": round(drawdown, 4)})
        already_triggered = False

    summary = {
        "event": "check",
        "equity": round(equity, 2),
        "baseline": round(baseline, 2),
        "drawdown": round(drawdown, 4),
        "threshold": threshold,
        "usdt": round(account["usdt"], 2),
        "base": round(account["base"], 2),
        "price": account.get("price"),
        "other_assets": account.get("other_assets"),
    }

    if drawdown < threshold or already_triggered:
        log(summary)
        return 0

    if args.dry_run:
        log({**summary, "event": "would_trigger", "dry_run": True})
        return 0

    stop_result = stop_bot()
    send_mail(
        f"[HBOT] BTW 做市触发熔断：回撤 {drawdown * 100:.1f}%",
        build_alert_body(account, baseline, threshold),
        config,
    )
    state["triggered_at"] = now_iso()
    state["triggered_equity"] = round(equity, 2)
    state["triggered_drawdown"] = round(drawdown, 4)
    write_state(state)
    log({**summary, "event": "triggered", "stop": stop_result})
    return 0


if __name__ == "__main__":
    sys.exit(main())
