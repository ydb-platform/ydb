import logging
import subprocess
import tempfile
import os
import stat
import time
from library.python import resource


from ydb.tests.stress.common.common import WorkloadBase

logger = logging.getLogger("YdbTopicWorkload")


class YdbTopicWorkload(WorkloadBase):
    def __init__(self, endpoint, database, duration, consumers, producers, tables_prefix, *, limit_memory_usage=False):
        super().__init__(None, tables_prefix, 'topic', None)
        self.endpoint = endpoint
        self.database = database
        self.duration = str(duration)
        self.consumers = str(consumers)
        self.producers = str(producers)
        self.limit_memory_usage = limit_memory_usage
        self.tempdir = None
        self._unpack_resource('ydb_cli')

    def __del__(self):
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

    @property
    def workload_topic_name(self) -> str:
        return f'{self.table_prefix}'

    def get_command_prefix(self, subcmds: list[str]) -> list[str]:
        return [
            *self._get_cli_common_args(),
            'workload', 'topic'
        ] + subcmds + ['--topic', self.workload_topic_name]

    def cmd_run(self, cmd):
        logger.debug(f"Running cmd {cmd}")
        print(f"Running cmd {cmd} at {time.time()}")
        subprocess.run(cmd, check=True, text=True)
        print(f"End at {time.time()}")

    def __loop(self):
        # init
        self.cmd_run(
            self.get_command_prefix(subcmds=['init', '-c', self.consumers, '-p', self.producers])
        )
        # adjust
        self.cmd_run([
            *self._get_cli_common_args(),
            'topic', 'alter',
            '--retention-period=2s',
            self.workload_topic_name,
        ])
        self.cmd_run([
            *self._get_cli_common_args(),
            'topic', 'consumer', 'add',
            f'--availability-period={int(self.duration) * 9 // 10}s',
            '--consumer', 'data_holder',
            self.workload_topic_name,
        ])
        # run
        run_cmd_args = ['run', 'full', '-s', self.duration, '--byte-rate', '100M', '--use-tx', '--tx-commit-interval', '2000', '-p', self.producers, '-c', self.consumers]
        if self.limit_memory_usage:
            run_cmd_args.extend([
                '--max-memory-usage-per-consumer=2M',
                '--max-memory-usage-per-producer=2M',
            ])
        self.cmd_run(
            self.get_command_prefix(subcmds=run_cmd_args)
        )
        # clean
        self.cmd_run(
            self.get_command_prefix(subcmds=['clean'])
        )

    def get_workload_thread_funcs(self):
        r = [self.__loop]
        return r
