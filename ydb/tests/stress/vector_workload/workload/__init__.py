import logging
import os
import stat
import subprocess
import tempfile
import time

from library.python import resource
from ydb.tests.stress.common.common import WorkloadBase

logger = logging.getLogger("YdbVectorWorkload")


class YdbVectorWorkload(WorkloadBase):
    def __init__(self, endpoint, database, duration):
        super().__init__(None, '', 'vector_workload', None)
        self.endpoint = endpoint
        self.database = database
        self.duration = str(duration)
        self.tempdir = None
        self._unpack_resource('ydb_cli')

    def __del__(self):
        self.tempdir.cleanup()

    def _unpack_resource(self, name):
        self.tempdir = tempfile.TemporaryDirectory(dir=os.getcwd())
        self.working_dir = os.path.join(self.tempdir.name, "vector_ydb_cli")
        os.makedirs(self.working_dir, exist_ok=True)
        res = resource.find(name)
        path_to_unpack = os.path.join(self.working_dir, name)
        with open(path_to_unpack, "wb") as f:
            f.write(res)

        st = os.stat(path_to_unpack)
        os.chmod(path_to_unpack, st.st_mode | stat.S_IEXEC)
        self.cli_path = path_to_unpack

    def get_command_prefix(self, subcmds):
        return [
            self.cli_path,
            '--verbose',
            '--endpoint', self.endpoint,
            '--database={}'.format(self.database),
            'workload', 'vector'
        ] + subcmds

    def cmd_run(self, cmd):
        logger.debug(f"Running cmd {cmd}")
        print(f"Running cmd {cmd} at {time.time()}")
        subprocess.run(cmd, check=True, text=True)
        print(f"End at {time.time()}")

    def __loop(self):
        # init
        self.cmd_run(
            self.get_command_prefix(subcmds=['init', '--clear'])
        )
        # import generator
        self.cmd_run(
            self.get_command_prefix(subcmds=[
                'import', 'generator',
                '--rows', '10000',
                '--distance', 'cosine',
            ])
        )
        # wait for table statistics to be calculated
        print("Waiting for table statistics to be calculated...")
        time.sleep(30)
        # run select with recall measurement
        self.cmd_run(
            self.get_command_prefix(subcmds=[
                'run', 'select',
                '--seconds', self.duration,
                '--threads', '10',
                '--recall',
            ])
        )
        # clean
        self.cmd_run(
            self.get_command_prefix(subcmds=['clean'])
        )

    def get_workload_thread_funcs(self):
        return [self.__loop]
