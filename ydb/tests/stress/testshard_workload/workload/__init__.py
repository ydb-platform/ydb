import json
import logging
import subprocess
import tempfile
import os
import stat
import time
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from typing import Optional
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


@dataclass
class OperationCounters:
    ok: int = 0
    fail: int = 0

    @property
    def total(self) -> int:
        return self.ok + self.fail

    @property
    def success_rate(self) -> float:
        return self.ok / self.total if self.total > 0 else 1.0

    def __add__(self, other: 'OperationCounters') -> 'OperationCounters':
        return OperationCounters(
            ok=self.ok + other.ok,
            fail=self.fail + other.fail
        )


@dataclass
class TestShardStats:
    tablet_id: int
    write: OperationCounters = field(default_factory=OperationCounters)
    patch: OperationCounters = field(default_factory=OperationCounters)
    delete: OperationCounters = field(default_factory=OperationCounters)
    read: OperationCounters = field(default_factory=OperationCounters)
    overall_success_rate: float = 1.0
    error: Optional[str] = None


class TestShardStatsCollector:
    def __init__(self, monitoring_url: str):
        self.monitoring_url = monitoring_url

    def fetch_tablet_stats(self, tablet_id: int, timeout: int = 5) -> TestShardStats:
        url = f"{self.monitoring_url}/tablets/app?TabletID={tablet_id}&json=1"
        stats = TestShardStats(tablet_id=tablet_id)

        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=timeout) as response:
                data = json.loads(response.read().decode('utf-8'))

            def parse_counters(name: str) -> OperationCounters:
                counters_data = data.get(name, {})
                return OperationCounters(
                    ok=counters_data.get('ok', 0),
                    fail=counters_data.get('fail', 0)
                )

            stats.write = parse_counters('writeCounters')
            stats.patch = parse_counters('patchCounters')
            stats.delete = parse_counters('deleteCounters')
            stats.read = parse_counters('readCounters')
            stats.overall_success_rate = data.get('overallSuccessRate', 1.0)

        except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as e:
            stats.error = str(e)
            logger.debug(f"Failed to fetch stats for tablet {tablet_id}: {e}")

        return stats

    def aggregate_stats(self, stats_list: list[TestShardStats]) -> dict:
        total_write = OperationCounters()
        total_patch = OperationCounters()
        total_delete = OperationCounters()
        total_read = OperationCounters()

        successful_tablets = 0
        failed_tablets = 0

        for stats in stats_list:
            if stats.error:
                failed_tablets += 1
                continue

            successful_tablets += 1
            total_write += stats.write
            total_patch += stats.patch
            total_delete += stats.delete
            total_read += stats.read

        total_overall = total_write + total_patch + total_delete + total_read

        return {
            'tablets': {
                'total': len(stats_list),
                'successful': successful_tablets,
                'failed': failed_tablets,
            },
            'write': total_write,
            'patch': total_patch,
            'delete': total_delete,
            'read': total_read,
            'overall': total_overall,
        }

    def print_stats(self, aggregated: dict):
        tablets = aggregated['tablets']
        logger.info(f"Tablets: {tablets['successful']}/{tablets['total']} responding")
        if tablets['failed'] > 0:
            logger.warning(f"{tablets['failed']} tablets failed to respond")

        def format_counters(name: str, data: OperationCounters) -> str:
            rate_pct = data.success_rate * 100
            return f"{name:10s}: {data.ok:>10d} ok / {data.fail:>10d} fail / {data.total:>10d} total = {rate_pct:>7.3f}%"

        logger.info(format_counters("Write", aggregated['write']))
        logger.info(format_counters("Patch", aggregated['patch']))
        logger.info(format_counters("Delete", aggregated['delete']))
        logger.info(format_counters("Read", aggregated['read']))
        logger.info("-" * 70)
        logger.info(format_counters("Overall", aggregated['overall']))


class YdbTestShardWorkload(WorkloadBase):
    def __init__(self, endpoint, database, duration, owner_idx, count, config_path=None, channels=None, tsserver_host='localhost',
                 tsserver_port=35000, stats_interval=10, monitoring_port=8765):
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
        self.stats_interval = stats_interval
        self.monitoring_port = monitoring_port
        self.tablet_ids = []
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

    def _get_monitoring_url(self) -> str:
        host = self.endpoint.replace('grpc://', '').replace('grpcs://', '').split(':')[0]
        return f"http://{host}:{self.monitoring_port}"

    def cmd_run(self, cmd) -> str:
        logger.info(f"Executing: {' '.join(cmd)}")
        result = subprocess.run(cmd, check=True, text=True, capture_output=True)
        return result.stdout

    def _parse_tablet_ids(self, output: str) -> list[int]:
        tablet_ids = []
        for line in output.split('\n'):
            if 'Tablet IDs:' in line:
                ids_part = line.split('Tablet IDs:')[1].strip()
                for id_str in ids_part.split(','):
                    id_str = id_str.strip()
                    if id_str:
                        try:
                            tablet_ids.append(int(id_str))
                        except ValueError:
                            logger.warning(f"Failed to parse tablet ID: {id_str}")
        return tablet_ids

    def _fetch_and_print_stats(self):
        if not self.tablet_ids:
            logger.warning("No tablet IDs available for stats collection")
            return

        monitoring_url = self._get_monitoring_url()
        collector = TestShardStatsCollector(monitoring_url)
        stats_list = []

        for tablet_id in self.tablet_ids:
            stats = collector.fetch_tablet_stats(tablet_id)
            stats_list.append(stats)
            if stats.error:
                logger.debug(f"Failed to fetch stats for tablet {tablet_id}: {stats.error}")

        aggregated = collector.aggregate_stats(stats_list)
        collector.print_stats(aggregated)

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

            output = self.cmd_run(cmd)
            self.tablet_ids = self._parse_tablet_ids(output)
            logger.info(f"Created tablets: {self.tablet_ids}")

            logger.info(f"Running testshard workload for {self.duration} seconds...")
            logger.info(f"Stats will be printed every {self.stats_interval} seconds")

            elapsed = 0
            while elapsed < self.duration:
                sleep_time = min(self.stats_interval, self.duration - elapsed)
                time.sleep(sleep_time)
                elapsed += sleep_time

                logger.info(f"=== Stats at {elapsed}s / {self.duration}s ===")
                self._fetch_and_print_stats()

            logger.info("=== Final statistics ===")
            self._fetch_and_print_stats()

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
