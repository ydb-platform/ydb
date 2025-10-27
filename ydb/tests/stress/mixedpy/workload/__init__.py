from concurrent.futures import ThreadPoolExecutor
import logging
import subprocess
import tempfile
import os
import stat
from library.python import resource


from ydb.tests.stress.common.common import WorkloadBase

logger = logging.getLogger("YdbMixedWorkload")


class YdbMixedWorkload(WorkloadBase):
    def __init__(self, endpoint, database, duration, store_type, tables_prefix):
        super().__init__(None, tables_prefix, 'mixed', None)
        self.store_type = store_type
        self.endpoint = endpoint
        self.database = database
        self.duration = str(duration // 3)
        self.tempdir = None
        self._unpack_resource('ydb_cli')

    def __del__(self):
        self.tempdir.cleanup()

    def _unpack_resource(self, name):
        self.tempdir = tempfile.TemporaryDirectory(dir=os.getcwd())
        self.working_dir = os.path.join(self.tempdir.name, "mixed_ydb_cli")
        os.makedirs(self.working_dir, exist_ok=True)
        res = resource.find(name)
        path_to_unpack = os.path.join(self.working_dir, name)
        with open(path_to_unpack, "wb") as f:
            f.write(res)

        st = os.stat(path_to_unpack)
        os.chmod(path_to_unpack, st.st_mode | stat.S_IEXEC)
        self.cli_path = path_to_unpack

    def cmd_run(self, cmd):
        logger.debug(f"Running cmd {cmd}")
        subprocess.run(cmd, check=True, text=True)

    def get_command_prefix(self, subcmds: list[str]) -> list[str]:
        return [
            self.cli_path,
            '--verbose',
            '--endpoint', self.endpoint,
            '--database={}'.format(self.database),
            'workload', 'mixed'
        ] + subcmds + [
            '--path', f'{self.table_prefix}_{self.store_type}'
        ]

    @classmethod
    def get_cols_count_command_params(cls) -> list[str]:
        return [
            '--int-cols', '5',
            '--str-cols', '5',
        ]

    def get_bulk_upsert_cmd(self):
        return self.get_command_prefix(subcmds=['run', 'bulk_upsert'] + self.get_cols_count_command_params()) + [
            '-s', self.duration,
            '-t', '40',
            '--rows', '1000',
            '--window', '20',
            '--len', '1000',
        ]

    def get_select_cmd(self):
        return self.get_command_prefix(subcmds=['run', 'select']) + [
            '-s', self.duration,
            '-t', '5',
            '--rows', '100',
        ]

    def get_update_cmd(self):
        return self.get_command_prefix(subcmds=['run', 'upsert'] + self.get_cols_count_command_params()) + [
            '-s', self.duration,
            '-t', '5',
            '--rows', '100',
            '--len', '1000',
        ]

    def __loop(self):
        upload_commands = [self.get_bulk_upsert_cmd()]
        upload_select_commands = [self.get_bulk_upsert_cmd(), self.get_select_cmd()]
        upload_select_update_commands = [self.get_bulk_upsert_cmd(), self.get_select_cmd(), self.get_update_cmd()]

        # init

        self.cmd_run(
            self.get_command_prefix(subcmds=['clean'])
        )

        self.cmd_run(
            self.get_command_prefix(subcmds=['init']) + self.get_cols_count_command_params() + [
                '--store', self.store_type,
            ]
        )

        with ThreadPoolExecutor() as executor:
            for command in upload_commands:
                executor.submit(
                    self.cmd_run,
                    command
                )

        with ThreadPoolExecutor() as executor:
            for command in upload_select_commands:
                executor.submit(
                    self.cmd_run,
                    command
                )

        with ThreadPoolExecutor() as executor:
            for command in upload_select_update_commands:
                executor.submit(
                    self.cmd_run,
                    command
                )

        self.cmd_run(
            self.get_command_prefix(subcmds=['clean'])
        )

    def get_workload_thread_funcs(self):
        r = [self.__loop]
        return r
