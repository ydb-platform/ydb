from concurrent.futures import ThreadPoolExecutor
import logging
import subprocess
import tempfile
import os
import stat
from library.python import resource


from ydb.tests.stress.common.common import WorkloadBase

logger = logging.getLogger("YdbLogWorkload")


class YdbLogWorkload(WorkloadBase):
    def __init__(self, endpoint, database, store_type, tables_prefix):
        super().__init__(None, tables_prefix, 'log', None)
        self.store_type = store_type
        self.endpoint = endpoint
        self.database = database
        self._unpack_resource('ydb_cli')

    def _unpack_resource(self, name):
        self.working_dir = os.path.join(tempfile.gettempdir(), "ydb_cli")
        os.makedirs(self.working_dir, exist_ok=True)
        res = resource.find(name)
        path_to_unpack = os.path.join(self.working_dir, name)
        with open(path_to_unpack, "wb") as f:
            f.write(res)

        st = os.stat(path_to_unpack)
        os.chmod(path_to_unpack, st.st_mode | stat.S_IEXEC)
        self.cli_path = path_to_unpack

    def get_command_prefix(self, subcmds: list[str], path: str) -> list[str]:
        return [
            self.cli_path,
            '--verbose',
            '--endpoint', self.endpoint,
            '--database={}'.format(self.database),
            'workload', 'log'
        ] + subcmds + [
            '--path', path
        ]

    def cmd_run(self, cmd):
        logger.debug(f"Running cmd {cmd}")
        subprocess.run(cmd, check=True, text=True)

    @classmethod
    def get_insert_command_params(cls) -> list[str]:
        return [
            '--int-cols', '2',
            '--str-cols', '5',
            '--key-cols', '4',
            '--len', '200',
        ]

    def __loop(self):
        upload_commands = [
            # import command
            self.get_command_prefix(subcmds=['import', '--bulk-size', '1000', '-t', '1', 'generator'], path=self.store_type) + self.get_insert_command_params() + ['--rows', '100000'],
            # bulk upsert workload
            self.get_command_prefix(subcmds=['run', 'bulk_upsert'], path=self.store_type) + self.get_insert_command_params() + ['--seconds', '10', '--threads', '10'],

            # upsert workload
            self.get_command_prefix(subcmds=['run', 'upsert'], path=self.store_type) + self.get_insert_command_params() + ['--seconds', '10', '--threads', '10'],

            # insert workload
            self.get_command_prefix(subcmds=['run', 'insert'], path=self.store_type) + self.get_insert_command_params() + ['--seconds', '10', '--threads', '10'],
        ]

        # init

        self.cmd_run(
            self.get_command_prefix(subcmds=['init'], path=self.store_type) + self.get_insert_command_params() + [
                '--store', self.store_type,
                '--min-partitions', '100',
                '--partition-size', '10',
                '--auto-partition', '0',
            ],
        )
        with ThreadPoolExecutor() as executor:
            executor.submit(
                self.cmd_run,
                self.get_command_prefix(subcmds=['run', 'select'], path=self.store_type) + [
                    '--client-timeout', '10000',
                    '--threads', '10',
                    '--seconds', str(10 * len(upload_commands)),
                ]
            )

            for command in upload_commands:
                self.cmd_run(command)

    def get_workload_thread_funcs(self):
        r = [self.__loop]
        return r
