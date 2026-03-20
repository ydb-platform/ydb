import logging
import tempfile
import os
import stat
from library.python import resource

from ydb.tests.stress.common.common import WorkloadBase
from .command_executor import CommandExecutor
from .config import WorkloadConfig, TestConfig

logger = logging.getLogger("YdbTopicWorkload")


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
        self._executor = CommandExecutor()
        self._unpack_resource('ydb_cli')
        self.chunk_index = chunk_index
        self.chunk_size = chunk_size

    def __del__(self):
        if self.tempdir:
            self.tempdir.cleanup()

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
                           partitions_per_tablet=None) -> None:
        """Создает тестовый топик."""
        args = ['init']
        if consumers:
            args.extend(['-c', str(consumers)])
        if partitions_per_tablet:
            args.extend(['--partitions-per-tablet', str(partitions_per_tablet)])
        args.extend([
            '--topic', topic_name,
            '--auto-partitioning',
            '--auto-partitioning-stabilization-window-seconds', self.config.AUTO_PARTITIONING_WINDOW,
            '--auto-partitioning-up-utilization-percent', self.config.AUTO_PARTITIONING_UTILIZATION,
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

    def _run_workload(self, topic_name, duration, byte_rate, producers, consumers,
                      consumer_threads=None,
                      tx_commit_interval=None, use_tx=True, with_config=True) -> None:
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
        ]
        if consumer_threads:
            args.extend(['-t', str(consumer_threads)])
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
        self.run_topic_write_without_tx(TestConfig(
            partitions=200,
            partitions_per_tablet=10,
            producers=20,  # producers=int(self.producers),
            consumers=int(self.consumers),
            consumer_threads=int(self.consumers),
            byte_rate="10M"  # byte_rate=self.config.DEFAULT_BYTE_RATE
        ))

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
        availability_period = int(self.duration) * self.config.AVAILABILITY_PERIOD_NUMERATOR // self.config.AVAILABILITY_PERIOD_DENOMINATOR
        self._add_consumer_to_topic(
            self.workload_topic_name,
            self.config.DATA_HOLDER_CONSUMER,
            availability_period
        )

        # Запускаем тестовую нагрузку
        self._run_workload(
            self.workload_topic_name,
            self.duration,
            self.config.DEFAULT_BYTE_RATE,
            self.producers,
            self.consumers,
            with_config=True
        )

        # Удаляем тестовый топик
        self._cleanup_test_topic(self.workload_topic_name)

    def run_topic_write_with_tx(self, test_config: TestConfig):
        topic_name = f'workload_topic_pr{test_config.producers}_p{test_config.partitions}_pq{test_config.partitions_per_tablet}'

        # Создаем тестовый топик
        self._create_test_topic(
            topic_name,
            test_config.consumers,
            test_config.partitions,
            test_config.partitions_per_tablet
        )

        # Настраиваем тестовый топик
        self._configure_topic_retention(topic_name, self.config.RETENTION)

        # Запускаем тестовую нагрузку
        self._run_workload(
            topic_name,
            self.duration,
            test_config.byte_rate,
            test_config.producers,
            test_config.consumers,
            consumer_threads=test_config.consumer_threads,
            use_tx=True,
            with_config=True
        )

        # Удаляем тестовый топик
        self._cleanup_test_topic(topic_name)

    def run_topic_write_without_tx(self, test_config: TestConfig):
        topic_name = f'workload_ntx_pr{test_config.producers}_p{test_config.partitions}_pq{test_config.partitions_per_tablet}'

        # Создаем тестовый топик
        self._create_test_topic(
            topic_name,
            test_config.consumers,
            test_config.partitions,
            test_config.partitions_per_tablet
        )

        # Настраиваем тестовый топик
        self._configure_topic_retention(topic_name, self.config.RETENTION)

        # Запускаем тестовую нагрузку без транзакций
        self._run_workload(
            topic_name,
            self.duration,
            test_config.byte_rate,
            test_config.producers,
            test_config.consumers,
            consumer_threads=test_config.consumer_threads,
            use_tx=False,
            with_config=True
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
        ]
        if (self.chunk_index is None) or (self.chunk_size is None):
            return tests
        return tests[self.chunk_index * self.chunk_size:(self.chunk_index + 1) * self.chunk_size]
