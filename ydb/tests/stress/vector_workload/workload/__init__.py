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
    def __init__(self, endpoint, database, duration, mode="standalone", data_dir=None):
        super().__init__(None, '', 'vector_workload', None)
        self.endpoint = endpoint
        self.database = database
        self.duration = str(duration)
        self.mode = mode
        self.data_dir = data_dir
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

    def get_tools_prefix(self, subcmds):
        return [
            self.cli_path,
            '--verbose',
            '--endpoint', self.endpoint,
            '--database={}'.format(self.database),
            'tools',
        ] + subcmds

    def cmd_run(self, cmd):
        logger.debug(f"Running cmd {cmd}")
        print(f"Running cmd {cmd} at {time.time()}")
        subprocess.run(cmd, check=True, text=True)
        print(f"End at {time.time()}")

    def __loop_standalone(self):
        """Original mode: generate data, build index, run select, clean."""
        self.cmd_run(
            self.get_command_prefix(subcmds=['init', '--clear'])
        )
        self.cmd_run(
            self.get_command_prefix(subcmds=[
                'import', 'generator',
                '--rows', '10000',
                '--distance', 'cosine',
            ])
        )
        print("Waiting for table statistics to be calculated...")
        time.sleep(30)
        self.cmd_run(
            self.get_command_prefix(subcmds=[
                'run', 'select',
                '--seconds', self.duration,
                '--threads', '10',
                '--recall',
            ])
        )
        self.cmd_run(
            self.get_command_prefix(subcmds=['clean'])
        )

    def __loop_generate(self):
        """Generate mode: generate data, dump to files, build index, run select, clean."""
        self.cmd_run(
            self.get_command_prefix(subcmds=['init', '--clear'])
        )
        self.cmd_run(
            self.get_command_prefix(subcmds=[
                'import', 'generator',
                '--rows', '10000',
                '--distance', 'cosine',
                '--index-type', 'None',
            ])
        )
        # Dump table data to data_dir for later use by load mode
        print(f"Dumping table data to {self.data_dir}")
        if os.path.exists(self.data_dir):
            import shutil
            shutil.rmtree(self.data_dir)
        self.cmd_run(
            self.get_tools_prefix(subcmds=[
                'dump',
                '-p', self.database + '/vector_index_workload',
                '-o', self.data_dir,
            ])
        )
        # Build vector index
        self.cmd_run(
            self.get_command_prefix(subcmds=[
                'build-index',
                '--distance', 'cosine',
            ])
        )
        print("Waiting for table statistics to be calculated...")
        time.sleep(30)
        self.cmd_run(
            self.get_command_prefix(subcmds=[
                'run', 'select',
                '--seconds', self.duration,
                '--threads', '10',
                '--recall',
            ])
        )
        self.cmd_run(
            self.get_command_prefix(subcmds=['clean'])
        )

    def __loop_load(self):
        """Load mode: restore data from dump, build index, run select, clean."""
        # Restore table and data from dump
        print(f"Restoring table data from {self.data_dir}")
        self.cmd_run(
            self.get_tools_prefix(subcmds=[
                'restore',
                '-p', self.database,
                '-i', self.data_dir,
                '--restore-indexes', '0',
            ])
        )
        # Build vector index
        self.cmd_run(
            self.get_command_prefix(subcmds=[
                'build-index',
                '--distance', 'cosine',
            ])
        )
        print("Waiting for table statistics to be calculated...")
        time.sleep(30)
        self.cmd_run(
            self.get_command_prefix(subcmds=[
                'run', 'select',
                '--seconds', self.duration,
                '--threads', '10',
                '--recall',
            ])
        )
        self.cmd_run(
            self.get_command_prefix(subcmds=['clean'])
        )

    def get_workload_thread_funcs(self):
        if self.mode == "generate":
            return [self.__loop_generate]
        elif self.mode == "load":
            return [self.__loop_load]
        else:
            return [self.__loop_standalone]
