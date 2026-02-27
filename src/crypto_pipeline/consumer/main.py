from __future__ import annotations

import os
import time
from collections import defaultdict
from typing import Any

import orjson
import polars as pl
import structlog
from confluent_kafka import Consumer, KafkaException, TopicPartition

from crypto_pipeline.config import load_settings
from crypto_pipeline.logging import setup_logging
from crypto_pipeline.consumer.writer_parquet import ParquetWriter
from crypto_pipeline.storage.layout import parquet_partition_path

log = structlog.get_logger()


def build_consumer(settings) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": os.getenv("KAFKA_CONSUMER_GROUP", "crypto-consumer"),
            "auto.offset.reset": os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
            "enable.auto.commit": False,  # IMPORTANT: commit only after write
            "client.id": "crypto-consumer",
        }
    )


def msg_to_record(msg) -> dict[str, Any]:
    return orjson.loads(msg.value())


def flush_batch(
    writer: ParquetWriter,
    records: list[dict[str, Any]],
    offsets_to_commit: list[TopicPartition],
    consumer: Consumer,
    parquet_root: str,
    parquet_subdir: str,
) -> None:
    if not records:
        return

    df = pl.from_dicts(records)

    # group by partition key so we write each group into correct partition folder
    # For now: symbol + trade_ts determine partition folder
    grouped = defaultdict(list)
    for r in records:
        grouped[(r["symbol"], r["trade_ts"])].append(r)

    written_files = 0
    for (symbol, trade_ts), recs in grouped.items():
        out_dir = parquet_partition_path(parquet_root, parquet_subdir, symbol, int(trade_ts))
        out_df = pl.from_dicts(recs)
        out_path = writer.write(out_df, out_dir)
        written_files += 1
        log.info(
            "parquet_written",
            file=str(out_path),
            rows=len(recs),
            symbol=symbol,
        )

    # commit offsets only after successful writes
    consumer.commit(offsets=offsets_to_commit, asynchronous=False)
    log.info(
        "offsets_committed",
        partitions=[{"topic": tp.topic, "partition": tp.partition, "offset": tp.offset} for tp in offsets_to_commit],
        files=written_files,
        rows=len(records),
    )


def run() -> None:
    settings = load_settings()
    setup_logging(settings.log_level)

    parquet_root = os.getenv("PARQUET_ROOT", "./data/parquet")
    parquet_subdir = os.getenv("PARQUET_TOPIC_SUBDIR", "trades")
    batch_size = int(os.getenv("BATCH_SIZE", "5000"))
    flush_seconds = int(os.getenv("FLUSH_SECONDS", "10"))

    consumer = build_consumer(settings)
    consumer.subscribe([settings.kafka_topic_trades])

    writer = ParquetWriter(parquet_root, parquet_subdir)

    records: list[dict[str, Any]] = []
    offsets_map: dict[tuple[str, int], int] = {}  # (topic, partition) -> last_offset+1
    last_flush = time.time()
    consumed = 0

    log.info(
        "consumer_starting",
        topic=settings.kafka_topic_trades,
        group=os.getenv("KAFKA_CONSUMER_GROUP", "crypto-consumer"),
        parquet_root=parquet_root,
    )

    try:
        while True:
            msg = consumer.poll(1.0)
            now = time.time()

            if msg is None:
                # flush by time
                if records and (now - last_flush >= flush_seconds):
                    offsets_to_commit = [
                        TopicPartition(topic=t, partition=p, offset=o)
                        for (t, p), o in offsets_map.items()
                    ]
                    flush_batch(writer, records, offsets_to_commit, consumer, parquet_root, parquet_subdir)
                    records = []
                    offsets_map = {}
                    last_flush = now
                continue

            if msg.error():
                raise KafkaException(msg.error())

            rec = msg_to_record(msg)
            records.append(rec)
            consumed += 1

            # commit offset should be last processed offset + 1
            offsets_map[(msg.topic(), msg.partition())] = msg.offset() + 1

            if len(records) >= batch_size or (now - last_flush >= flush_seconds):
                offsets_to_commit = [
                    TopicPartition(topic=t, partition=p, offset=o)
                    for (t, p), o in offsets_map.items()
                ]
                flush_batch(writer, records, offsets_to_commit, consumer, parquet_root, parquet_subdir)
                records = []
                offsets_map = {}
                last_flush = now

            if consumed % 2000 == 0:
                log.info("consumer_progress", consumed=consumed)

    finally:
        consumer.close()


if __name__ == "__main__":
    run()