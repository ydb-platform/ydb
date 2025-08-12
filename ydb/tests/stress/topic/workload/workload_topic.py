import logging
import subprocess
import tempfile
import os
import stat
from library.python import resource


from ydb.tests.stress.common.common import WorkloadBase

logger = logging.getLogger("YdbTopicWorkload")


class YdbTopicWorkload(WorkloadBase):
    def __init__(self, endpoint, database, duration, consumers, producers, tables_prefix):
        super().__init__(None, tables_prefix, 'topic', None)
        self.endpoint = endpoint
        self.database = database
        self.duration = str(duration)
        self.consumers = str(consumers)
        self.producers = str(producers)
        self.tempdir = None
        self._unpack_resource('ydb_cli')

    def __del__(self):
        self.tempdir.cleanup()

    def _unpack_resource(self, name):
        self.tempdir = tempfile.TemporaryDirectory(dir=os.getcwd())
        self.working_dir = os.path.join(self.tempdir, "ydb_cli")
        os.makedirs(self.working_dir, exist_ok=True)
        res = resource.find(name)
        path_to_unpack = os.path.join(self.working_dir, name)
        with open(path_to_unpack, "wb") as f:
            f.write(res)

        st = os.stat(path_to_unpack)
        os.chmod(path_to_unpack, st.st_mode | stat.S_IEXEC)
        self.cli_path = path_to_unpack

    def get_command_prefix(self, subcmds: list[str]) -> list[str]:
        return [
            self.cli_path,
            '--verbose',
            '--endpoint', self.endpoint,
            '--database={}'.format(self.database),
            'workload', 'topic'
        ] + subcmds + ['--topic', f'{self.table_prefix}']

    def cmd_run(self, cmd):
        logger.debug(f"Running cmd {cmd}")
        subprocess.run(cmd, check=True, text=True)

    def __loop(self):
        # init
        self.cmd_run(
            self.get_command_prefix(subcmds=['init', '-c', self.consumers, '-p', self.producers])
        )
        # run
        self.cmd_run(
            self.get_command_prefix(subcmds=['run', 'full', '-s', self.duration, '--byte-rate', '100M', '--use-tx', '--tx-commit-interval', '2000', '-p', self.producers, '-c', self.consumers])
        )
        # clean
        self.cmd_run(
            self.get_command_prefix(subcmds=['clean'])
        )

    def get_workload_thread_funcs(self):
        r = [self.__loop]
        return r
