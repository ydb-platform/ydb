import logging
import subprocess
import tempfile
import os
import stat
import time
from library.python import resource

from ydb.tests.stress.common.common import WorkloadBase

logger = logging.getLogger("YdbTpccWorkload")


class YdbTpccWorkload(WorkloadBase):
    def __init__(self, endpoint, database, duration, warehouses, tables_prefix, tx_mode=None):
        super().__init__(None, tables_prefix, 'tpcc', None)
        self.endpoint = endpoint
        self.database = database
        self.duration = str(duration)
        self.warehouses = str(warehouses)
        self.tx_mode = tx_mode
        self.tempdir = None
        self._unpack_resource('ydb_cli')

    def __del__(self):
        self.tempdir.cleanup()

    def _unpack_resource(self, name):
        self.tempdir = tempfile.TemporaryDirectory(dir=os.getcwd())
        self.working_dir = os.path.join(self.tempdir.name, "tpcc_ydb_cli")
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
            '--endpoint', self.endpoint,
            f'--database={self.database}',
            'workload', 'tpcc',
            '-p', self.table_prefix,
        ] + subcmds

    def cmd_run(self, cmd):
        logger.debug(f"Running cmd {cmd}")
        print(f"Running cmd {cmd} at {time.time()}")
        subprocess.run(cmd, check=True, text=True)
        print(f"End at {time.time()}")

    def import_data(self):
        self.cmd_run(
            self.get_command_prefix(['init', '--warehouses', self.warehouses])
        )
        self.cmd_run(
            self.get_command_prefix(['import', '--no-tui', '--warehouses', self.warehouses])
        )

    def clean_data(self):
        self.cmd_run(
            self.get_command_prefix(['clean'])
        )

    def __loop(self):
        cmd = ['run', '--no-tui', '--format', 'Json',
               '--warmup', '30s',
               '--time', f'{self.duration}s',
               '--warehouses', self.warehouses]
        if self.tx_mode == 'mixed':
            cmd += ['--tx-mode', 'mixed',
                    '--tx-mode-weight-serializable', '30',
                    '--tx-mode-weight-snapshot', '70']
        elif self.tx_mode is not None:
            cmd += ['--tx-mode', self.tx_mode]
        self.cmd_run(self.get_command_prefix(cmd))

    def get_workload_thread_funcs(self):
        return [self.__loop]
