import logging
import subprocess
import tempfile
import os
import stat
import time
from library.python import resource


from ydb.tests.stress.common.common import WorkloadBase

logger = logging.getLogger("YdbKvWorkload")


class YdbKvWorkload(WorkloadBase):
    def __init__(self, endpoint, database, duration, store_type, tables_prefix):
        super().__init__(None, tables_prefix, 'kv', None)
        self.endpoint = endpoint
        self.database = database
        self.duration = str(duration)
        self.store_type = store_type
        self.tempdir = None
        self._unpack_resource('ydb_cli')

    def __del__(self):
        self.tempdir.cleanup()

    def _unpack_resource(self, name):
        self.tempdir = tempfile.TemporaryDirectory(dir=os.getcwd())
        self.working_dir = os.path.join(self.tempdir.name, "kv_ydb_cli")
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
            'workload', 'kv'
        ] + subcmds + ['--path', f'{self.table_prefix}_{self.store_type}']

    def cmd_run(self, cmd):
        logger.debug(f"Running cmd {cmd}")
        print(f"Running cmd {cmd} at {time.time()}")
        subprocess.run(cmd, check=True, text=True)
        print(f"End at {time.time()}")

    def __loop(self):
        # init
        self.cmd_run(
            self.get_command_prefix(subcmds=['init',
                                             "--min-partitions", "1",
                                             "--partition-size", "10",
                                             "--auto-partition", "0",
                                             "--init-upserts", "0",
                                             "--cols", "5",
                                             "--int-cols", "2",
                                             "--key-cols", "3"])
        )
        # run
        self.cmd_run(
            self.get_command_prefix(subcmds=["run", "mixed",
                                             "--seconds", self.duration,
                                             "--threads", "10",
                                             "--cols", "5",
                                             "--len", "200",
                                             "--int-cols", "2",
                                             "--key-cols", "3"])
        )
        # clean
        self.cmd_run(
            self.get_command_prefix(subcmds=['clean'])
        )

    def get_workload_thread_funcs(self):
        r = [self.__loop]
        return r
