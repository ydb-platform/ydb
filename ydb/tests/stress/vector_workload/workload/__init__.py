import json
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
    def __init__(self, endpoint, database, duration, mode="standalone", data_dir=None, targets=1000, warmup=0, rows=10000, threads=10, clusters=None, levels=None, s3_endpoint=None, s3_bucket=None, s3_source=None, s3_destination=None, s3_query_source=None, s3_query_destination=None):
        super().__init__(None, '', 'vector_workload', None)
        self.endpoint = endpoint
        self.database = database
        self.duration = str(duration)
        self.mode = mode
        self.data_dir = data_dir
        self.targets = str(targets)
        self.warmup = str(warmup)
        self.rows = str(rows)
        self.threads = str(threads)
        self.clusters = str(clusters) if clusters is not None else None
        self.levels = str(levels) if levels is not None else None
        self.s3_endpoint = s3_endpoint
        self.s3_bucket = s3_bucket
        self.s3_source = s3_source
        self.s3_destination = s3_destination
        self.s3_query_source = s3_query_source
        self.s3_query_destination = s3_query_destination
        # Table the workload operates on. In s3 mode the data is imported to a
        # user-provided destination path, otherwise the default workload table.
        self.table_name = "vector_index_workload"
        self.query_table_name = QUERY_TABLE_NAME
        if self.mode == "s3":
            if self.s3_destination:
                self.table_name = self._rel_to_database(self.s3_destination)
            if self.s3_query_source:
                query_dest = self.s3_query_destination or f"{self.database}/{QUERY_TABLE_NAME}"
                self.query_table_name = self._rel_to_database(query_dest)
        self.tempdir = None
        self._unpack_resource('ydb_cli')

    def _rel_to_database(self, path):
        """Convert an absolute database path to a name relative to the database."""
        prefix = self.database.rstrip('/') + '/'
        if path.startswith(prefix):
            return path[len(prefix):]
        return path.lstrip('/')

    def __del__(self):
        if self.tempdir is not None:
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

    def _wait_for_table_stats(self, timeout=90, interval=3):
        """Poll until the table's row-count statistic is non-zero.

        After an index build the row-count estimate is computed asynchronously.
        The vector workload's Init() reads it via DescribeTable().GetTableRows()
        and aborts ("statistics is not calculated yet") if it starts too early.
        We poll the same underlying datashard stat through the .sys/partition_stats
        system view and return as soon as it lands — usually far quicker than a
        fixed sleep. This is best-effort: if the view is not queryable we fall
        back to a short fixed wait, and on timeout we proceed anyway (the select's
        own cmd_run_with_retry is the backstop)."""
        full_path = f"{self.database.rstrip('/')}/{self.table_name}"
        query = (
            "SELECT COALESCE(SUM(RowCount), 0u) AS rows "
            f"FROM `.sys/partition_stats` WHERE Path = '{full_path}';"
        )
        cmd = self.get_cli_prefix() + ['yql', '-s', query, '--format', 'json-unicode']
        print(f"Waiting for table statistics on {full_path}...")
        deadline = time.time() + timeout
        query_ok = False
        while time.time() < deadline:
            rows = None
            try:
                proc = subprocess.run(cmd, check=True, text=True, capture_output=True)
                query_ok = True
                for line in proc.stdout.splitlines():
                    line = line.strip()
                    if line:
                        val = json.loads(line).get('rows')
                        if val is not None:
                            rows = int(val)
            except (subprocess.CalledProcessError, ValueError) as e:
                if not query_ok:
                    # System view not queryable here: don't spin, fall back to a
                    # brief fixed wait and let the select's retry cover the rest.
                    print(f"Cannot query table statistics ({e}); falling back to fixed wait")
                    time.sleep(30)
                    return
                print("Stats poll query failed, retrying...")
            if rows:
                print(f"Table statistics ready: {full_path} has ~{rows} rows")
                return
            time.sleep(interval)
        print(f"Timed out after {timeout}s waiting for statistics on {full_path}; proceeding")

    def _build_index_subcmds(self):
        """Subcommands to build the index on the workload table."""
        subcmds = ['build-index', '--distance', 'cosine', '--table', self.table_name]
        if self.mode == "s3":
            # An imported dataset has a fixed, externally-defined vector dimension
            # and type. Pass 0 so the CLI omits them from the DDL and the server
            # autodetects both from the data.
            subcmds += ['--vector-dimension', '0']
        if self.clusters is not None:
            subcmds += ['--kmeans-tree-clusters', self.clusters]
        if self.levels is not None:
            subcmds += ['--kmeans-tree-levels', self.levels]
        return subcmds

    def _create_query_table(self):
        """Create a query table with N rows from the main table."""
        print(f"Creating query table {self.query_table_name} with {self.targets} rows...")
        create_sql = (
            f"CREATE TABLE `{self.query_table_name}` "
            f"(id Uint64 NOT NULL, embedding String, PRIMARY KEY(id));"
        )
        self.cmd_run(self.get_cli_prefix() + ['yql', '-s', create_sql])

        populate_sql = (
            f"UPSERT INTO `{self.query_table_name}` "
            f"SELECT id, embedding FROM `{self.table_name}` "
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
                '-p', self.database + '/' + self.query_table_name,
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
                '--rows', self.rows,
                '--distance', 'cosine',
                '--index-type', 'None',
            ])
        )
        # Build index explicitly and wait for completion
        self.cmd_run_with_retry(
            self.get_command_prefix(subcmds=self._build_index_subcmds())
        )
        # Wait until table statistics (row-count estimate) are computed after the
        # index build; the select workload aborts if it starts before they land.
        self._wait_for_table_stats()

    def _get_select_subcmds(self, seconds):
        subcmds = [
            'run', 'select',
            '--seconds', str(seconds),
            '--threads', self.threads,
            '--targets', self.targets,
            '--table', self.table_name,
        ]
        if self.mode in ('generate', 'load', 's3'):
            subcmds.extend(['--query-table', self.query_table_name])
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

    def _import_s3_data(self):
        """Import data from S3 using ydb import s3, optionally including the queries table."""
        item_main = f"Source={self.s3_source},Destination={self.s3_destination}"
        cmd = self.get_cli_prefix() + [
            "import", "s3",
            "--s3-endpoint", self.s3_endpoint,
            "--bucket", self.s3_bucket,
            "--item", item_main,
        ]
        if self.s3_query_source:
            query_dest = self.s3_query_destination or f"{self.database}/{QUERY_TABLE_NAME}"
            item_query = f"Source={self.s3_query_source},Destination={query_dest}"
            cmd += ["--item", item_query]
            print(f"Importing queries table from S3: {self.s3_query_source} -> {query_dest}")
        self.cmd_run(cmd)
        # Build index explicitly and wait for completion
        self.cmd_run_with_retry(
            self.get_command_prefix(subcmds=self._build_index_subcmds())
        )
        # Wait until table statistics (row-count estimate) are computed after the
        # index build; the select workload aborts if it starts before they land.
        self._wait_for_table_stats()

    def __loop_s3(self):
        """S3 mode: import data from S3, optionally import queries table, build index, run select, clean."""
        self._import_s3_data()
        if not self.s3_query_source:
            self._create_query_table()
        self._run_select()
        self.cmd_run(
            self.get_command_prefix(subcmds=['clean'])
        )

    def get_workload_thread_funcs(self):
        if self.mode == "generate":
            return [self.__loop_generate]
        elif self.mode == "load":
            return [self.__loop_load]
        elif self.mode == "s3":
            return [self.__loop_s3]
        else:
            return [self.__loop_standalone]
