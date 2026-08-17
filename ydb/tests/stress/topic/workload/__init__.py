import concurrent.futures
import logging
import os
import stat
import tempfile
import time
from library.python import resource
import ydb

from ydb.tests.stress.common.common import WorkloadBase
from .command_executor import CommandExecutor
from .config import WorkloadConfig, TestConfig

logger = logging.getLogger("YdbTopicWorkload")


class StartFromOffsetsEventHandler(ydb.TopicReaderEvents.EventHandler):
    def __init__(self, offsets_by_partition):
        self.offsets_by_partition = offsets_by_partition

    def on_partition_get_start_offset(self, event):
        return ydb.TopicReaderEvents.OnPartitionGetStartOffsetResponse(
            start_offset=self.offsets_by_partition[event.partition_id]
        )


class YdbTopicWorkload(WorkloadBase):
    def __init__(self, endpoint, database, duration, consumers, producers, tables_prefix, *, limit_memory_usage=False, config=None,
                 chunk_index=None, chunk_size=None):
        super().__init__(None, tables_prefix, 'topic', None)
        self.endpoint = endpoint
        self.database = database
        self.duration = str(duration)
        self.consumers = str(consumers)
        self.producers = str(producers)
        self.limit_memory_usage = limit_memory_usage
        self.config = config or WorkloadConfig()
        self.stats_window = self.config.STATS_WINDOW
        self.tempdir = None
        self.driver = None
        self._executor = CommandExecutor()
        self._unpack_resource('ydb_cli')
        self.chunk_index = chunk_index
        self.chunk_size = chunk_size

    def __del__(self):
        if self.driver:
            self.driver.stop()
        if self.tempdir:
            self.tempdir.cleanup()

    def _get_driver(self):
        if not self.driver:
            self.driver = ydb.Driver(ydb.DriverConfig(
                endpoint=self.endpoint,
                database=self.database,
            ))
            self.driver.wait(timeout=60)
        return self.driver

    def _unpack_resource(self, name):
        self.tempdir = tempfile.TemporaryDirectory(dir=os.getcwd())
        self.working_dir = os.path.join(self.tempdir.name, "topic_ydb_cli")
        os.makedirs(self.working_dir, exist_ok=True)
        res = resource.find(name)
        path_to_unpack = os.path.join(self.working_dir, name)
        with open(path_to_unpack, "wb") as f:
            f.write(res)

        st = os.stat(path_to_unpack)
        os.chmod(path_to_unpack, st.st_mode | stat.S_IEXEC)
        self.cli_path = path_to_unpack

    def _get_cli_common_args(self) -> list[str]:
        return [
            self.cli_path,
            '--verbose',
            '--endpoint', self.endpoint,
            '--database={}'.format(self.database),
        ]

    def get_command_prefix(self, subcmds: list[str]) -> list[str]:
        return [
            *self._get_cli_common_args(),
            'workload', 'topic'
        ] + subcmds

    def _create_test_topic(self, topic_name, consumers=None, partitions=None,
                           partitions_per_tablet=None,
                           auto_partitioning_stabilization_window=None,
                           auto_partitioning_up_utilization=None,
                           auto_partitioning_max_partitions=None) -> None:
        """Создает тестовый топик."""
        args = ['init']
        if consumers:
            args.extend(['-c', str(consumers)])
        if partitions_per_tablet:
            args.extend(['--partitions-per-tablet', str(partitions_per_tablet)])
        if auto_partitioning_max_partitions:
            args.extend(['--auto-partitioning-max-partitions-count', str(auto_partitioning_max_partitions)])
        args.extend([
            '--topic', topic_name,
            '--auto-partitioning',
            '--auto-partitioning-stabilization-window-seconds',
            auto_partitioning_stabilization_window or self.config.AUTO_PARTITIONING_WINDOW,
            '--auto-partitioning-up-utilization-percent',
            auto_partitioning_up_utilization or self.config.AUTO_PARTITIONING_UTILIZATION,
        ])
        if partitions:
            args.extend(['--partitions', str(partitions)])
        self.cmd_run(self.get_command_prefix(subcmds=args))

    def _configure_topic_retention(self, topic_name, retention_period) -> None:
        """Настраивает период ретеншена для топика."""
        self.cmd_run([
            *self._get_cli_common_args(),
            'topic', 'alter',
            f'--retention-period={retention_period}',
            topic_name,
        ])

    def _add_consumer_to_topic(self, topic_name, consumer_name,
                               availability_period_seconds) -> None:
        """Добавляет консьюмера в топик."""
        self.cmd_run([
            *self._get_cli_common_args(),
            'topic', 'consumer', 'add',
            f'--availability-period={availability_period_seconds}s',
            '--consumer', consumer_name,
            topic_name,
        ])

    def _add_data_holder_consumer_to_topic(self, topic_name) -> None:
        availability_period = int(self.duration) * self.config.AVAILABILITY_PERIOD_NUMERATOR // self.config.AVAILABILITY_PERIOD_DENOMINATOR
        self._add_consumer_to_topic(
            topic_name,
            self.config.DATA_HOLDER_CONSUMER,
            availability_period
        )

    def _run_workload(self, topic_name, duration, byte_rate, producers, consumers,
                      consumer_threads=None,
                      tx_commit_interval=None, use_tx=True, with_config=True,
                      codec="raw", batch_flush_message_count=1,
                      batch_flush_interval="1s", batch_flush_size=None,
                      batch_inner_codec=None, consumer_prefix=None) -> None:
        """Запускает тестовую нагрузку с мониторингом.

        Args:
            topic_name: имя топика
            duration: длительность в секундах
            byte_rate: скорость записи (например, '100M')
            producers: количество продюсеров
            consumers: количество консьюмеров
            tx_commit_interval: интервал коммита транзакций (если use_tx=True). Если None, используется config.TX_COMMIT_INTERVAL
            use_tx: использовать ли транзакции
            with_config: включать ли конфигурационные транзакции
        """
        if tx_commit_interval is None:
            tx_commit_interval = self.config.TX_COMMIT_INTERVAL

        self._executor.set_monitor(hang_timeout=self.config.STATS_HANG_TIMEOUT, window_interval=self.stats_window)

        args = [
            'run', 'full', '-s', str(duration),
            f'--window={self.stats_window}',
            '--byte-rate', byte_rate,
            '-p', str(producers), '-c', str(consumers),
            '--topic', topic_name,
            '--codec', codec,
            '--batch-flush-message-count', str(batch_flush_message_count),
            '--batch-flush-interval', batch_flush_interval,
        ]
        if batch_flush_size:
            args.extend(['--batch-flush-size', batch_flush_size])
        if batch_inner_codec:
            args.extend(['--batch-inner-codec', batch_inner_codec])
        if consumer_threads:
            args.extend(['-t', str(consumer_threads)])
        if consumer_prefix:
            args.extend(['--consumer-prefix', consumer_prefix])
        if use_tx:
            args.extend(['--use-tx', '--tx-commit-interval', tx_commit_interval])
        if self.limit_memory_usage:
            args.extend([
                f'--max-memory-usage-per-consumer={self.config.MEMORY_LIMIT_PER_CONSUMER}',
                f'--max-memory-usage-per-producer={self.config.MEMORY_LIMIT_PER_PRODUCER}',
            ])
        if with_config:
            args.extend([
                '--configure-consumers', self.config.CONFIG_CONSUMERS_COUNT,
                '--describe-topic',
                '--describe-consumer', self.config.DATA_HOLDER_CONSUMER,
            ])
        self.cmd_run_with_monitoring(self.get_command_prefix(subcmds=args))

    def _run_write_workload(self, topic_name, duration, byte_rate, producers,
                            keyed_writes=False, producer_keys_count=None,
                            tx_commit_interval=None, use_tx=True,
                            codec="raw", batch_flush_message_count=1,
                            batch_flush_interval="1s", batch_flush_size=None,
                            batch_inner_codec=None) -> None:
        if tx_commit_interval is None:
            tx_commit_interval = self.config.TX_COMMIT_INTERVAL

        args = [
            'run', 'write', '-s', str(duration),
            f'--window={self.stats_window}',
            '--byte-rate', byte_rate,
            '-t', str(producers),
            '--topic', topic_name,
            '--codec', codec,
            '--batch-flush-message-count', str(batch_flush_message_count),
            '--batch-flush-interval', batch_flush_interval,
        ]
        if batch_flush_size:
            args.extend(['--batch-flush-size', batch_flush_size])
        if batch_inner_codec:
            args.extend(['--batch-inner-codec', batch_inner_codec])
        if use_tx:
            args.extend(['--use-tx', '--tx-commit-interval', tx_commit_interval])
        if keyed_writes:
            args.append('--keyed-writes')
        if producer_keys_count is not None:
            args.extend(['--producer-keys-count', str(producer_keys_count)])
        if self.limit_memory_usage:
            args.extend([
                f'--max-memory-usage-per-producer={self.config.MEMORY_LIMIT_PER_PRODUCER}',
            ])
        self.cmd_run(self.get_command_prefix(subcmds=args))

    def _describe_partition_tree(self, topic_name):
        description = self._get_driver().topic_client.describe_topic(topic_name, include_stats=True)
        parents_by_partition = {
            partition.partition_id: set(partition.parent_partition_ids)
            for partition in description.partitions
        }
        end_offsets = {
            partition.partition_id: partition.partition_stats.partition_end
            for partition in description.partitions
        }
        return parents_by_partition, end_offsets

    def _read_topic_records_from_beginning(self, topic_name, end_offsets, timeout):
        topic_selector = ydb.TopicReaderSelector(
            path=topic_name,
            partitions=list(end_offsets),
        )
        expected_count = sum(end_offsets.values())
        records = []
        deadline = time.time() + timeout
        with self._get_driver().topic_client.reader(
            topic_selector,
            consumer=None,
            event_handler=StartFromOffsetsEventHandler(
                {partition_id: 0 for partition_id in end_offsets}
            ),
        ) as reader:
            while len(records) < expected_count and time.time() < deadline:
                try:
                    batch = reader.receive_batch(max_messages=1000, timeout=1)
                except TimeoutError:
                    continue
                if batch is None:
                    continue
                for message in batch.messages:
                    records.append({
                        "key": message.metadata_items.get("__key"),
                        "partition_id": message.partition_id,
                        "seqno": message.seqno,
                    })

        if len(records) != expected_count:
            raise AssertionError(
                f"Did not read all logical messages from {topic_name}: "
                f"got {len(records)}, expected {expected_count}"
            )
        return records

    def _run_read_workload(self, topic_name, duration, consumers, consumer_threads,
                           consumer_prefix) -> None:
        args = [
            'run', 'read', '-s', str(duration),
            f'--window={self.stats_window}',
            '--topic', topic_name,
            '-c', str(consumers),
            '-t', str(consumer_threads),
            '--consumer-prefix', consumer_prefix,
        ]
        if self.limit_memory_usage:
            args.extend([
                f'--max-memory-usage-per-consumer={self.config.MEMORY_LIMIT_PER_CONSUMER}',
            ])
        self.cmd_run(self.get_command_prefix(subcmds=args))

    def _get_topic_end_offsets(self, topic_name):
        description = self._get_driver().topic_client.describe_topic(topic_name, include_stats=True)
        return {
            partition.partition_id: partition.partition_stats.partition_end
            for partition in description.partitions
        }

    def _write_fixed_messages(self, topic_name, values, producer_id, partition_id=0):
        with self._get_driver().topic_client.writer(
            topic_name,
            producer_id=producer_id,
            partition_id=partition_id,
        ) as writer:
            for value in values:
                writer.write(ydb.TopicWriterMessage(value), timeout=30)

    def _write_fixed_messages_in_transaction(self, topic_name, values, producer_id,
                                             partition_id=0):
        with ydb.QuerySessionPool(self._get_driver()) as session_pool:
            def callee(tx):
                writer = self._get_driver().topic_client.tx_writer(
                    tx,
                    topic_name,
                    producer_id=producer_id,
                    partition_id=partition_id,
                )
                for value in values:
                    writer.write(ydb.TopicWriterMessage(value), timeout=30)
                writer.flush(timeout=30)
                writer.close(flush=False)

            session_pool.retry_tx_sync(callee)

    def _read_topic_from_offsets(self, topic_name, offsets_by_partition, expected_count,
                                 timeout):
        driver = self._get_driver()
        topic_selector = ydb.TopicReaderSelector(
            path=topic_name,
            partitions=list(offsets_by_partition),
        )
        read_count = 0
        deadline = time.time() + timeout
        with driver.topic_client.reader(
            topic_selector,
            consumer=None,
            event_handler=StartFromOffsetsEventHandler(offsets_by_partition),
        ) as reader:
            while read_count < expected_count and time.time() < deadline:
                try:
                    batch = reader.receive_batch(max_messages=1000, timeout=1)
                except TimeoutError:
                    continue
                if batch is None:
                    continue
                read_count += len(batch.messages)

        if read_count != expected_count:
            raise AssertionError(
                f"Did not read all logical messages from {topic_name}: "
                f"got {read_count}, expected {expected_count}, "
                f"offsets_by_partition={offsets_by_partition}"
            )

    def _is_same_or_descendant_partition(self, ancestor, partition, parents_by_partition):
        pending = [partition]
        seen = set()
        while pending:
            current = pending.pop()
            if current == ancestor:
                return True
            if current in seen:
                continue
            seen.add(current)
            pending.extend(parents_by_partition.get(current, set()))
        return False

    def _validate_keyed_records(self, topic_name, records, parents_by_partition):
        records_by_key = {}
        for record in records:
            key = record["key"]
            if key is None:
                raise AssertionError(f"Keyed workload message in {topic_name} has no __key metadata")
            records_by_key.setdefault(key, []).append(record)

        for key, key_records in records_by_key.items():
            key_records.sort(key=lambda record: record["seqno"])
            prev_seqno = None
            prev_partition = None
            for record in key_records:
                seqno = record["seqno"]
                partition = record["partition_id"]
                if prev_seqno is not None and seqno <= prev_seqno:
                    raise AssertionError(
                        f"Non-increasing seqNo for key {key!r} in {topic_name}: "
                        f"got {seqno} after {prev_seqno}"
                    )
                if (
                    prev_partition is not None
                    and partition != prev_partition
                    and not self._is_same_or_descendant_partition(
                        prev_partition,
                        partition,
                        parents_by_partition,
                    )
                ):
                    raise AssertionError(
                        f"Key {key!r} moved from partition {prev_partition} to unrelated "
                        f"partition {partition} in {topic_name}"
                    )
                prev_seqno = seqno
                prev_partition = partition

    def _read_topic_from_beginning(self, topic_name, end_offsets, timeout):
        expected_count = sum(end_offsets.values())
        self._read_topic_from_offsets(
            topic_name,
            {partition_id: 0 for partition_id in end_offsets},
            expected_count,
            timeout,
        )

    def _read_topic_from_mid_offsets(self, topic_name, end_offsets, read_duration):
        for read_offset in range(1, 6):
            offsets_by_partition = {
                partition_id: read_offset
                for partition_id, end_offset in end_offsets.items()
                if end_offset > read_offset
            }
            expected_count = sum(
                end_offsets[partition_id] - read_offset
                for partition_id in offsets_by_partition
            )
            if expected_count == 0:
                continue
            self._read_topic_from_offsets(
                topic_name,
                offsets_by_partition,
                expected_count,
                timeout=read_duration + 30,
            )

    def _read_partition_from_offset(self, topic_name, partition_id, read_offset,
                                    expected_count, timeout):
        driver = self._get_driver()
        topic_selector = ydb.TopicReaderSelector(
            path=topic_name,
            partitions=[partition_id],
        )
        messages = []
        deadline = time.time() + timeout
        with driver.topic_client.reader(
            topic_selector,
            consumer=None,
            event_handler=StartFromOffsetsEventHandler({partition_id: read_offset}),
        ) as reader:
            while len(messages) < expected_count and time.time() < deadline:
                try:
                    batch = reader.receive_batch(max_messages=1000, timeout=1)
                except TimeoutError:
                    continue
                if batch is None:
                    continue
                messages.extend((message.offset, message.data) for message in batch.messages)

        if len(messages) != expected_count:
            raise AssertionError(
                f"Did not read partition {partition_id} from {topic_name} at offset {read_offset}: "
                f"got {len(messages)}, expected {expected_count}"
            )
        return messages

    def _write_and_verify_transactional_mid_blob_probe(self, topic_name, read_duration):
        partition_id = 0
        prefix_count = 1000
        tx_count = 300
        prefix_messages = [
            f"stress-mid-blob-prefix-{i}"
            for i in range(prefix_count)
        ]
        tx_messages = [
            f"stress-mid-blob-transaction-{i}-" + ("x" * 4096)
            for i in range(tx_count)
        ]

        self._write_fixed_messages(
            topic_name,
            prefix_messages,
            producer_id="stress-mid-blob-prefix-producer",
            partition_id=partition_id,
        )
        self._write_fixed_messages_in_transaction(
            topic_name,
            tx_messages,
            producer_id="stress-mid-blob-transaction-producer",
            partition_id=partition_id,
        )

        for delta in (1, 50, 150, tx_count - 1):
            read_offset = prefix_count + delta
            read_result = self._read_partition_from_offset(
                topic_name,
                partition_id,
                read_offset,
                tx_count - delta,
                timeout=read_duration + 30,
            )
            expected_offsets = list(range(read_offset, prefix_count + tx_count))
            actual_offsets = [offset for offset, _ in read_result]
            if actual_offsets != expected_offsets:
                raise AssertionError(
                    f"Wrong offsets from partition {partition_id} in {topic_name}: "
                    f"got {actual_offsets[:10]}..., expected {expected_offsets[:10]}..."
                )
            expected_payloads = [
                message.encode("utf-8")
                for message in tx_messages[delta:]
            ]
            actual_payloads = [data for _, data in read_result]
            if actual_payloads != expected_payloads:
                raise AssertionError(
                    f"Wrong payloads from partition {partition_id} in {topic_name} "
                    f"at offset {read_offset}"
                )

    def _cleanup_test_topic(self, topic_name) -> None:
        """Удаляет тестовый топик."""
        self.cmd_run(self.get_command_prefix(subcmds=['clean', '--topic', topic_name]))

    def cmd_run(self, cmd):
        self._executor.run(cmd)

    def cmd_run_with_monitoring(self, cmd):
        executor = CommandExecutor()
        executor.set_monitor(hang_timeout=self.config.STATS_HANG_TIMEOUT, window_interval=self.stats_window)

        executor.run_with_monitoring(cmd)

    def __one_tablet_but_a_distributed_transaction(self):
        self.run_topic_write_with_tx(TestConfig(
            partitions=10,
            partitions_per_tablet=10,
            producers=20,  # producers=int(self.producers),
            consumers=int(self.consumers),
            consumer_threads=int(self.consumers),
            byte_rate="500K"  # byte_rate=self.config.DEFAULT_BYTE_RATE
        ))

    def __two_tablets_distributed_transaction(self):
        self.run_topic_write_with_tx(TestConfig(
            partitions=10,
            partitions_per_tablet=5,
            producers=20,  # producers=int(self.producers),
            consumers=int(self.consumers),
            consumer_threads=int(self.consumers),
            byte_rate="500K"  # byte_rate=self.config.DEFAULT_BYTE_RATE
        ))

    def __a_wide_transaction_with_multiple_partitions_in_one_tablet(self):
        self.run_topic_write_with_tx(TestConfig(
            partitions=200,
            partitions_per_tablet=10,
            producers=20,
            consumers=int(self.consumers),
            consumer_threads=int(self.consumers),
            byte_rate="10M"  # byte_rate=self.config.DEFAULT_BYTE_RATE
        ))

    def __wide_transaction_one_tablet_contains_one_partition(self):
        self.run_topic_write_with_tx(TestConfig(
            partitions=200,
            partitions_per_tablet=1,
            producers=20,
            consumers=int(self.consumers),
            consumer_threads=int(self.consumers),
            byte_rate="10M"  # byte_rate=self.config.DEFAULT_BYTE_RATE
        ))

    def __immediate_transaction(self):
        self.run_topic_write_with_tx(TestConfig(
            partitions=1,
            partitions_per_tablet=1,
            producers=20,  # producers=int(self.producers),
            consumers=int(self.consumers),
            consumer_threads=int(self.consumers),
            byte_rate="50K"  # byte_rate=self.config.SMALL_BYTE_RATE
        ))

    def __non_transactional_workload(self):
        # Keep wide partition coverage; lower byte_rate only to ease gRPC drain on teardown (#46635).
        self.run_topic_write_without_tx(TestConfig(
            partitions=200,
            partitions_per_tablet=10,
            producers=20,  # producers=int(self.producers),
            consumers=int(self.consumers),
            consumer_threads=int(self.consumers),
            byte_rate="1M"  # byte_rate=self.config.DEFAULT_BYTE_RATE
        ))

    def __keyed_producer_auto_partitioning_workload(self):
        # Write with workload keyed-writes so the CLI uses IProducer, then read the
        # whole auto-partitioned topic and verify per-key ordering and partition lineage.
        topic_name = "workload_keyed_producer_auto_partitioning"
        producer_keys_count = 32
        read_timeout = max(30, int(self.duration))
        retention_seconds = int(self.duration) + read_timeout + 120

        self._create_test_topic(
            topic_name,
            partitions=1,
            partitions_per_tablet=1,
            auto_partitioning_stabilization_window="5",
            auto_partitioning_up_utilization="1",
            auto_partitioning_max_partitions=8,
        )
        try:
            self._configure_topic_retention(topic_name, f"{retention_seconds}s")
            self._run_write_workload(
                topic_name,
                self.duration,
                "1M",
                producers=1,
                keyed_writes=True,
                producer_keys_count=producer_keys_count,
            )

            parents_by_partition, end_offsets = self._describe_partition_tree(topic_name)
            if sum(end_offsets.values()) == 0:
                raise AssertionError(f"No messages were written to {topic_name}")

            records = self._read_topic_records_from_beginning(
                topic_name,
                end_offsets,
                timeout=read_timeout + 30,
            )
            self._validate_keyed_records(topic_name, records, parents_by_partition)
        finally:
            self._cleanup_test_topic(topic_name)

    # Stress kafka-batch writes without transactions while workload readers continuously cut physical
    # batches back into logical topic messages.
    def __batched_non_transactional_workload(self):
        self.run_topic_write_without_tx(TestConfig(
            partitions=10,
            partitions_per_tablet=5,
            producers=20,
            consumers=int(self.consumers),
            consumer_threads=int(self.consumers),
            byte_rate="1M",
            codec="kafka-batch",
            batch_flush_message_count=5,
            batch_flush_interval="1s",
        ))

    # Stress kafka-batch writes inside topic transactions and verify workload readers can keep up
    # with committed physical batches while commits happen periodically.
    def __batched_transactional_workload(self):
        self.run_topic_write_with_tx(TestConfig(
            partitions=10,
            partitions_per_tablet=5,
            producers=20,
            consumers=int(self.consumers),
            consumer_threads=int(self.consumers),
            byte_rate="1M",
            codec="kafka-batch",
            batch_flush_message_count=5,
            batch_flush_interval="1s",
        ))

    def _mixed_transactional_and_batched_phases(self):
        return [
            {
                "name": "raw-ntx",
                "codec": "raw",
                "use_tx": False,
                "batch_flush_message_count": 1,
            },
            {
                "name": "raw-tx",
                "codec": "raw",
                "use_tx": True,
                "batch_flush_message_count": 1,
            },
            {
                "name": "kafka-batch-ntx",
                "codec": "kafka-batch",
                "use_tx": False,
                "batch_flush_message_count": 5,
            },
            {
                "name": "kafka-batch-tx",
                "codec": "kafka-batch",
                "use_tx": True,
                "batch_flush_message_count": 5,
            },
        ]

    # Run plain and kafka-batch writers at the same time, both transactional and non-transactional,
    # so one topic contains a live mix of regular messages, physical batches, committed tx writes,
    # and non-tx writes while independent readers consume the interleaved stream.
    def __mixed_transactional_and_batched_workload(self):
        topic_name = "workload_mixed_tx_ntx_raw_kafka_batch"
        phase_consumers = 4
        availability_period = (
            int(self.duration)
            * self.config.AVAILABILITY_PERIOD_NUMERATOR
            // self.config.AVAILABILITY_PERIOD_DENOMINATOR
        )
        phases = self._mixed_transactional_and_batched_phases()

        self._create_test_topic(
            topic_name,
            partitions=10,
            partitions_per_tablet=5
        )
        try:
            self._configure_topic_retention(topic_name, self.config.RETENTION)
            self._add_data_holder_consumer_to_topic(topic_name)
            for phase in phases:
                consumer_prefix = f"mixed-{phase['name']}-consumer"
                for consumer_idx in range(phase_consumers):
                    self._add_consumer_to_topic(
                        topic_name,
                        f"{consumer_prefix}-{consumer_idx}",
                        availability_period
                    )

            with concurrent.futures.ThreadPoolExecutor(max_workers=len(phases)) as executor:
                futures = [
                    executor.submit(
                        self._run_workload,
                        topic_name,
                        self.duration,
                        "500K",
                        5,
                        phase_consumers,
                        consumer_threads=phase_consumers,
                        use_tx=phase["use_tx"],
                        with_config=False,
                        codec=phase["codec"],
                        batch_flush_message_count=phase["batch_flush_message_count"],
                        batch_flush_interval="1s",
                        consumer_prefix=f"mixed-{phase['name']}-consumer",
                    )
                    for phase in phases
                ]
                for future in futures:
                    future.result()
        finally:
            self._cleanup_test_topic(topic_name)

    # Write the same mixed raw/kafka-batch and tx/non-tx stream first, then drain it with
    # a fresh consumer and verify that every logical message written to the topic is read.
    def __mixed_transactional_and_batched_validated_workload(self):
        topic_name = "workload_mixed_tx_ntx_raw_kafka_batch_validated"
        phases = self._mixed_transactional_and_batched_phases()
        read_duration = max(30, int(self.duration))
        retention_seconds = int(self.duration) + read_duration + 120

        self._create_test_topic(
            topic_name,
            partitions=10,
            partitions_per_tablet=5
        )
        try:
            self._configure_topic_retention(topic_name, f"{retention_seconds}s")
            self._write_and_verify_transactional_mid_blob_probe(topic_name, read_duration)

            with concurrent.futures.ThreadPoolExecutor(max_workers=len(phases)) as executor:
                futures = [
                    executor.submit(
                        self._run_write_workload,
                        topic_name,
                        self.duration,
                        "500K",
                        5,
                        use_tx=phase["use_tx"],
                        codec=phase["codec"],
                        batch_flush_message_count=phase["batch_flush_message_count"],
                        batch_flush_interval="1s",
                    )
                    for phase in phases
                ]
                for future in futures:
                    future.result()

            end_offsets = self._get_topic_end_offsets(topic_name)
            expected_count = sum(end_offsets.values())
            if expected_count == 0:
                raise AssertionError(f"No messages were written to {topic_name}")

            self._read_topic_from_beginning(
                topic_name,
                end_offsets,
                timeout=read_duration + 30,
            )
            self._read_topic_from_mid_offsets(topic_name, end_offsets, read_duration)
        finally:
            self._cleanup_test_topic(topic_name)

    @property
    def workload_topic_name(self) -> str:
        return f'{self.table_prefix}'

    def __loop(self):
        # Создаем тестовый топик
        self._create_test_topic(
            self.workload_topic_name,
            self.consumers,
            self.producers
        )

        # Настраиваем тестовый топик
        self._configure_topic_retention(self.workload_topic_name, self.config.RETENTION)
        self._add_data_holder_consumer_to_topic(self.workload_topic_name)

        # Запускаем тестовую нагрузку
        self._run_workload(
            self.workload_topic_name,
            self.duration,
            # DEFAULT_BYTE_RATE (100M) overloads a single CI node
            self.config.SMALL_BYTE_RATE,
            self.producers,
            self.consumers,
            with_config=True
        )

        # Удаляем тестовый топик
        self._cleanup_test_topic(self.workload_topic_name)

    def run_topic_write_with_tx(self, test_config: TestConfig):
        topic_name = f'workload_topic{self._test_config_suffix(test_config)}'

        # Создаем тестовый топик
        self._create_test_topic(
            topic_name,
            test_config.consumers,
            test_config.partitions,
            test_config.partitions_per_tablet
        )

        # Настраиваем тестовый топик
        self._configure_topic_retention(topic_name, self.config.RETENTION)
        self._add_data_holder_consumer_to_topic(topic_name)

        # Запускаем тестовую нагрузку
        self._run_workload(
            topic_name,
            self.duration,
            test_config.byte_rate,
            test_config.producers,
            test_config.consumers,
            consumer_threads=test_config.consumer_threads,
            use_tx=True,
            with_config=True,
            codec=test_config.codec,
            batch_flush_message_count=test_config.batch_flush_message_count,
            batch_flush_interval=test_config.batch_flush_interval,
            batch_flush_size=test_config.batch_flush_size,
            batch_inner_codec=test_config.batch_inner_codec
        )

        # Удаляем тестовый топик
        self._cleanup_test_topic(topic_name)

    def run_topic_write_without_tx(self, test_config: TestConfig):
        topic_name = f'workload_ntx{self._test_config_suffix(test_config)}'

        # Создаем тестовый топик
        self._create_test_topic(
            topic_name,
            test_config.consumers,
            test_config.partitions,
            test_config.partitions_per_tablet
        )

        # Настраиваем тестовый топик
        self._configure_topic_retention(topic_name, self.config.RETENTION)
        self._add_data_holder_consumer_to_topic(topic_name)

        # Запускаем тестовую нагрузку без транзакций
        self._run_workload(
            topic_name,
            self.duration,
            test_config.byte_rate,
            test_config.producers,
            test_config.consumers,
            consumer_threads=test_config.consumer_threads,
            use_tx=False,
            with_config=True,
            codec=test_config.codec,
            batch_flush_message_count=test_config.batch_flush_message_count,
            batch_flush_interval=test_config.batch_flush_interval,
            batch_flush_size=test_config.batch_flush_size,
            batch_inner_codec=test_config.batch_inner_codec
        )

        # Удаляем тестовый топик
        self._cleanup_test_topic(topic_name)

    def get_workload_thread_funcs(self):
        tests = [
            self.__loop,
            self.__one_tablet_but_a_distributed_transaction,
            self.__two_tablets_distributed_transaction,
            self.__a_wide_transaction_with_multiple_partitions_in_one_tablet,
            self.__wide_transaction_one_tablet_contains_one_partition,
            self.__immediate_transaction,
            self.__non_transactional_workload,
            self.__keyed_producer_auto_partitioning_workload,
            self.__batched_non_transactional_workload,
            self.__batched_transactional_workload,
            self.__mixed_transactional_and_batched_workload,
            self.__mixed_transactional_and_batched_validated_workload,
        ]
        if (self.chunk_index is None) or (self.chunk_size is None):
            return tests
        chunk = tests[self.chunk_index * self.chunk_size:(self.chunk_index + 1) * self.chunk_size]

        # One callable so WorkloadBase starts a single worker; run chunk
        # scenarios one by one (parallel topic stresses overload CI, #46635).
        def run_chunk():
            for f in chunk:
                f()

        return [run_chunk]

    def _test_config_suffix(self, test_config: TestConfig) -> str:
        suffix = f'_pr{test_config.producers}_p{test_config.partitions}_pq{test_config.partitions_per_tablet}'
        if test_config.codec != "raw" or test_config.batch_flush_message_count != 1:
            codec = test_config.codec.replace("-", "_")
            suffix += f'_{codec}_b{test_config.batch_flush_message_count}'
        return suffix
