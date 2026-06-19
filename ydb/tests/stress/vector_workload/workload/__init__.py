import logging
import os
import shutil
import stat
import subprocess
import tempfile
import time

from library.python import resource
from ydb.tests.stress.common.common import WorkloadBase

logger = logging.getLogger("YdbVectorWorkload")

QUERY_TABLE_NAME = "vector_query_table"


class YdbVectorWorkload(WorkloadBase):
    def __init__(self, endpoint, database, duration, mode="standalone", data_dir=None, targets=100, warmup=0):
        super().__init__(None, '', 'vector_workload', None)
        self.endpoint = endpoint
        self.database = database
        self.duration = str(duration)
        self.mode = mode
        self.data_dir = data_dir
        self.targets = str(targets)
        self.warmup = str(warmup)
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

    def get_cli_prefix(self):
        return [
            self.cli_path,
            '--verbose',
            '--endpoint', self.endpoint,
            '--database={}'.format(self.database),
        ]

    def get_command_prefix(self, subcmds):
        return self.get_cli_prefix() + ['workload', 'vector'] + subcmds

    def get_tools_prefix(self, subcmds):
        return self.get_cli_prefix() + ['tools'] + subcmds

    def cmd_run(self, cmd):
        logger.debug(f"Running cmd {cmd}")
        print(f"Running cmd {cmd} at {time.time()}")
        subprocess.run(cmd, check=True, text=True)
        print(f"End at {time.time()}")

    def cmd_run_with_retry(self, cmd, retries=5, delay=30):
        """Run command with retries and delay between attempts."""
        for attempt in range(1, retries + 1):
            try:
                self.cmd_run(cmd)
                return
            except subprocess.CalledProcessError:
                if attempt == retries:
                    raise
                print(f"Attempt {attempt}/{retries} failed, retrying in {delay}s...")
                time.sleep(delay)

    def _create_query_table(self):
        """Create a query table with N rows from the main table."""
        print(f"Creating query table {QUERY_TABLE_NAME} with {self.targets} rows...")
        create_sql = (
            f"CREATE TABLE `{QUERY_TABLE_NAME}` "
            f"(id Uint64 NOT NULL, embedding String, PRIMARY KEY(id));"
        )
        self.cmd_run(self.get_cli_prefix() + ['yql', '-s', create_sql])

        populate_sql = (
            f"UPSERT INTO `{QUERY_TABLE_NAME}` "
            f"SELECT id, embedding FROM `vector_index_workload` "
            f"ORDER BY id LIMIT {self.targets};"
        )
        self.cmd_run(self.get_cli_prefix() + ['yql', '-s', populate_sql])

    def _dump_query_table(self):
        """Dump query table to data_dir for reuse by load mode."""
        print(f"Dumping query table to {self.data_dir}")
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
        self.cmd_run(
            self.get_tools_prefix(subcmds=[
                'dump',
                '-p', self.database + '/' + QUERY_TABLE_NAME,
                '-o', self.data_dir,
            ])
        )

    def _restore_query_table(self):
        """Restore query table from data_dir."""
        print(f"Restoring query table from {self.data_dir}")
        self.cmd_run(
            self.get_tools_prefix(subcmds=[
                'restore',
                '-p', self.database,
                '-i', self.data_dir,
            ])
        )

    def _import_data(self):
        """Generate data using deterministic seed, build index, wait for stats."""
        self.cmd_run(
            self.get_command_prefix(subcmds=['init', '--clear'])
        )
        # Import data without building index
        self.cmd_run(
            self.get_command_prefix(subcmds=[
                'import', 'generator',
                '--rows', '10000',
                '--distance', 'cosine',
                '--index-type', 'None',
            ])
        )
        # Build index explicitly and wait for completion
        self.cmd_run(
            self.get_command_prefix(subcmds=[
                'build-index',
                '--distance', 'cosine',
            ])
        )
        # Wait for table statistics to be calculated after index build
        print("Waiting for table statistics to be calculated...")
        time.sleep(30)

    def _get_select_subcmds(self, seconds):
        subcmds = [
            'run', 'select',
            '--seconds', str(seconds),
            '--threads', '10',
            '--targets', self.targets,
        ]
        if self.mode in ('generate', 'load'):
            subcmds.extend(['--query-table', QUERY_TABLE_NAME])
        return subcmds

    def _run_select(self):
        """Optionally warmup, then run select workload."""
        if int(self.warmup) > 0:
            print(f"Warmup run for {self.warmup}s...")
            self.cmd_run_with_retry(
                self.get_command_prefix(subcmds=self._get_select_subcmds(self.warmup))
            )
        self.cmd_run_with_retry(
            self.get_command_prefix(subcmds=self._get_select_subcmds(self.duration))
        )

    def __loop_standalone(self):
        """Original mode: generate data, build index, run select, clean."""
        self._import_data()
        self._run_select()
        self.cmd_run(
            self.get_command_prefix(subcmds=['clean'])
        )

    def __loop_generate(self):
        """Generate mode: same import as load, but also create and dump query table."""
        self._import_data()
        self._create_query_table()
        self._dump_query_table()
        self._run_select()
        self.cmd_run(
            self.get_command_prefix(subcmds=['clean'])
        )

    def __loop_load(self):
        """Load mode: same import as generate, restore query table from dump."""
        self._import_data()
        self._restore_query_table()
        self._run_select()
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
