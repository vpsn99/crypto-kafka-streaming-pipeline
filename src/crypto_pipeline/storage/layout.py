from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path


def _utc_dt_from_ms(epoch_ms: int) -> datetime:
    return datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)


def parquet_partition_path(root: str, subdir: str, symbol: str, trade_ts_ms: int) -> Path:
    """
    Partitioning layout:
      <root>/<subdir>/pair=BTCUSDT/trade_date=YYYY-MM-DD/hour=HH/
    based on trade timestamp (UTC).
    """
    dt = _utc_dt_from_ms(epoch_ms=trade_ts_ms)
    trade_date = dt.strftime("%Y-%m-%d")
    hour = dt.strftime("%H")

    return (
        Path(root)
        / subdir
        / f"pair={symbol}"
        / f"trade_date={trade_date}"
        / f"hour={hour}"
    )