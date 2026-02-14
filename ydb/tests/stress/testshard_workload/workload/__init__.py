import logging
import subprocess
import tempfile
import os
import stat
import time
from library.python import resource


from ydb.tests.stress.common.common import WorkloadBase

logger = logging.getLogger("YdbTestShardWorkload")

DEFAULT_CONFIG = """
workload:
  sizes:
    - {weight: 9, min: 128, max: 2048, inline: false}
    - {weight: 2, min: 1, max: 128, inline: true}
    - {weight: 1, min: 524288, max: 8388608, inline: false}
  write:
    - frequency: 10.0
      max_interval_ms: 1000
  restart:
    - frequency: 0.01
      max_interval_ms: 120000
  patch_fraction_ppm: 100000

limits:
  data:
    min: 750000000
    max: 1000000000
  concurrency:
    writes: 3
    reads: 1

timing:
  delay_start: 5
  reset_on_full: true
  stall_counter: 1000
tracing:
  put_fraction_ppm: 1000
  verbosity: 15

validation:
  server: "__TSSERVER_HOST__:__TSSERVER_PORT__"
  after_bytes: 5000000000
"""


class YdbTestShardWorkload(WorkloadBase):
    def __init__(self, endpoint, database, duration, owner_idx, count, config_path=None, channels=None, tsserver_port=35000, tsserver_host='localhost'):
        self.tsserver_process = None
        self.tempdir = None
        super().__init__(None, '', 'testshard', None)
        self.endpoint = endpoint
        self.database = database
        self.duration = int(duration)
        self.owner_idx = str(owner_idx)
        self.count = str(count)
        self.custom_config_path = config_path
        self.channels = channels
        self.tsserver_port = tsserver_port
        self.tsserver_host = tsserver_host
        self._unpack_resource('ydb_cli')
        self._unpack_resource('tsserver')
        self._prepare_config()

    def __del__(self):
        self._stop_tsserver()
        if self.tempdir:
            self.tempdir.cleanup()

    def _unpack_resource(self, name):
        if self.tempdir is None:
            self.tempdir = tempfile.TemporaryDirectory(dir=os.getcwd())
            self.working_dir = os.path.join(self.tempdir.name, "testshard_ydb_cli")
            os.makedirs(self.working_dir, exist_ok=True)

        res = resource.find(name)
        path_to_unpack = os.path.join(self.working_dir, name)
        with open(path_to_unpack, "wb") as f:
            f.write(res)

        st = os.stat(path_to_unpack)
        os.chmod(path_to_unpack, st.st_mode | stat.S_IEXEC)

        if name == 'ydb_cli':
            self.cli_path = path_to_unpack
        elif name == 'tsserver':
            self.tsserver_path = path_to_unpack

    def _prepare_config(self):
        if self.custom_config_path and os.path.exists(self.custom_config_path):
            self.config_path = self.custom_config_path
            logger.info(f"Using custom config: {self.config_path}")
            return

        if self.tempdir is None:
            self.tempdir = tempfile.TemporaryDirectory(dir=os.getcwd())
            self.working_dir = os.path.join(self.tempdir.name, "testshard_ydb_cli")
            os.makedirs(self.working_dir, exist_ok=True)

        self.config_path = os.path.join(self.working_dir, "default_config.yaml")
        config_content = DEFAULT_CONFIG \
            .replace("__TSSERVER_PORT__", str(self.tsserver_port)) \
            .replace("__TSSERVER_HOST__", str(self.tsserver_host))
        with open(self.config_path, "w") as f:
            f.write(config_content)
        logger.info(f"Created default config: {self.config_path} with tsserver port: {self.tsserver_port}")

    def _get_cli_common_args(self) -> list[str]:
        return [
            self.cli_path,
            '--verbose',
            '--endpoint', self.endpoint,
            '--database={}'.format(self.database),
        ]

    def cmd_run(self, cmd):
        logger.info(f"Executing: {' '.join(cmd)}")
        subprocess.run(cmd, check=True, text=True)

    def _start_tsserver(self):
        if self.tsserver_process is not None:
            logger.warning("tsserver already running")
            return

        logger.info(f"Starting tsserver on port {self.tsserver_port}, path: {self.tsserver_path}")
        self.tsserver_process = subprocess.Popen(
            [self.tsserver_path, str(self.tsserver_port)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        time.sleep(1)

        if self.tsserver_process.poll() is not None:
            returncode = self.tsserver_process.returncode
            raise RuntimeError(f"Failed to start tsserver, exit code: {returncode}")

        logger.info(f"tsserver started successfully (PID: {self.tsserver_process.pid}) on port {self.tsserver_port}")

    def _stop_tsserver(self):
        if self.tsserver_process is None:
            return

        logger.info(f"Stopping tsserver (PID: {self.tsserver_process.pid})")
        self.tsserver_process.terminate()
        try:
            self.tsserver_process.wait(timeout=5)
            logger.info("tsserver stopped successfully")
        except subprocess.TimeoutExpired:
            logger.warning("tsserver did not terminate, killing")
            self.tsserver_process.kill()
            self.tsserver_process.wait()

        self.tsserver_process = None

    def __loop(self):
        self._start_tsserver()

        try:
            cmd = [
                *self._get_cli_common_args(),
                'workload', 'testshard', 'init',
                '--owner-idx', self.owner_idx,
                '--count', self.count,
                '--config-file', self.config_path,
            ]
            if self.channels:
                cmd.extend(['--channels', ','.join(self.channels)])
            self.cmd_run(cmd)

            logger.info(f"Running testshard workload for {self.duration} seconds...")
            time.sleep(self.duration)

            self.cmd_run([
                *self._get_cli_common_args(),
                'workload', 'testshard', 'clean',
                '--owner-idx', self.owner_idx,
                '--count', self.count,
            ])
        finally:
            self._stop_tsserver()

    def get_workload_thread_funcs(self):
        return [self.__loop]
