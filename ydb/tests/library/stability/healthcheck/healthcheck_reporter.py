import json
import logging
import os
import signal
import subprocess
import threading
import time
import traceback
import requests
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
from ydb.tests.library.stability.utils.utils import unpack_resource


# The nemesis orchestrator pins the root logger at WARNING
# (ydb/tests/stability/nemesis/__main__.py), so bare logging.info()/logging.debug()
# calls from this module are dropped. Own logger at DEBUG + propagation to the root
# handler (installed at NOTSET by basicConfig) makes those records visible, and names
# them so healthcheck output is greppable.
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class HealthCheckReporter():
    # Hard cap for terminating in-flight healthcheck subprocesses on stop.
    _KILL_GRACE_SECONDS = 2
    _OUTPUT_LOG_LIMIT = 2000

    def __init__(self, hosts: list[str], store_results: bool = False):
        self.stop = False
        self.healthcheck_thread: threading.Thread = None
        self.ydb_path = os.path.join(os.getcwd(), 'ydb_cli_hc')
        self.hosts = hosts
        self.hc_request_timeout_seconds = 45
        self.hc_period_seconds = 15
        self.store_results = store_results
        self.last_results = {}
        # Monotonic start of the last stored tick (not end): a ~50s tick must not look post-fault.
        self.last_update: float | None = None
        # Interruptible sleep between ticks.
        self._stop_event = threading.Event()
        # Track live subprocesses so stop_healthchecks() can kill them.
        self._active_procs_lock = threading.Lock()
        self._active_procs: set[subprocess.Popen] = set()
        self._executor: ThreadPoolExecutor | None = None
        unpack_resource('ydb_cli', self.ydb_path)
        # Deliberately no `ydb_cli version` probe: that command leaves NeedToCheckForUpdate
        # set, which can make the CLI reach out to StorageUrl over the network
        # (ydb/apps/ydb/commands/ydb_root.cpp:91), and __init__ runs during app init on the
        # first request. Size is enough to spot a stale/unexpected binary, and a CLI that
        # cannot parse our arguments now reports that on stderr anyway.
        try:
            cli_size = os.path.getsize(self.ydb_path)
        except OSError as e:
            cli_size = f'<unavailable: {e}>'
        logger.info(
            "healthcheck starting: cli=%s (%s bytes), hosts=%d, period=%ss, timeout=%ss",
            self.ydb_path, cli_size, len(self.hosts),
            self.hc_period_seconds, self.hc_request_timeout_seconds,
        )

    @classmethod
    def __shorten(cls, text):
        if not text:
            return ''
        text = text.strip()
        if len(text) <= cls._OUTPUT_LOG_LIMIT:
            return text
        return f'{text[:cls._OUTPUT_LOG_LIMIT]}...<truncated, {len(text)} chars total>'

    def start_healthchecks(self):
        self.stop = False
        self._stop_event.clear()
        max_workers = min(len(self.hosts), 10) if self.hosts else 1
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self.healthcheck_thread = threading.Thread(target=self.__execute_healthcheck_thr, args=())
        self.healthcheck_thread.start()

    def stop_healthchecks(self):
        self.stop = True
        self._stop_event.set()

        # Kill in-flight ydb_cli_hc subprocesses so subprocess.run-equivalents return fast.
        self.__terminate_active_procs()

        # Cancel queued tasks and let running ones finish (they will exit fast because procs are dead).
        if self._executor is not None:
            self._executor.shutdown(wait=False, cancel_futures=True)

        if self.healthcheck_thread and self.healthcheck_thread.is_alive():
            self.healthcheck_thread.join(self._KILL_GRACE_SECONDS + 5)
            if self.healthcheck_thread.is_alive():
                logger.warning("Healthcheck thread did not stop gracefully, it may be hanging in a blocking operation")

    def __terminate_active_procs(self):
        with self._active_procs_lock:
            procs = list(self._active_procs)

        for proc in procs:
            try:
                # Send SIGTERM to the whole process group started via start_new_session=True.
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except (ProcessLookupError, PermissionError, OSError):
                pass

        deadline = time.monotonic() + self._KILL_GRACE_SECONDS
        for proc in procs:
            remaining = max(0.0, deadline - time.monotonic())
            try:
                proc.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except (ProcessLookupError, PermissionError, OSError):
                    pass
                try:
                    proc.wait(timeout=1.0)
                except subprocess.TimeoutExpired:
                    logger.warning("ydb_cli_hc pid=%s did not exit after SIGKILL", proc.pid)

    def __execute_healthcheck_thr(self):
        while not self.stop:
            try:
                tick_started_at = time.monotonic()
                results = self.__execute_healthcheck()
                if self.store_results:
                    self.last_results = results
                    self.last_update = tick_started_at
                self.__publish_healthcheck_results(results)
                self.__log_tick_summary(results, time.monotonic() - tick_started_at)
            except Exception:
                logger.error(f"Error in healthcheck thread: {traceback.format_exc()}")
            # Interruptible sleep: wakes up immediately on stop_healthchecks().
            if self._stop_event.wait(timeout=self.hc_period_seconds):
                break

    def __log_tick_summary(self, results, elapsed):
        # Without this the happy path is completely silent, so a healthy cluster and a
        # dead healthcheck thread look identical in the log.
        if not results:
            logger.info("healthcheck tick: no hosts checked in %.1fs", elapsed)
            return
        by_status = Counter(
            result.get('self_check_result', 'UNSPECIFIED') for result in results.values()
        )
        not_good = sorted(
            host for host, result in results.items()
            if result.get('self_check_result', 'UNSPECIFIED') != 'GOOD'
        )
        summary = ', '.join(f'{status}={count}' for status, count in sorted(by_status.items()))
        if not_good:
            logger.info(
                "healthcheck tick: %d hosts in %.1fs — %s; not GOOD: %s",
                len(results), elapsed, summary, ', '.join(not_good),
            )
        else:
            logger.info("healthcheck tick: %d hosts in %.1fs — %s", len(results), elapsed, summary)

    def __run_one_healthcheck(self, host):
        if self.stop:
            return host, {'self_check_result': 'HC_REQUEST_ERROR'}

        cmd = [
            self.ydb_path,
            '--endpoint', f'grpc://{host}:2135',
            'monitoring', 'healthcheck',
            '--format', 'json',
        ]
        proc = None
        started_at = time.monotonic()
        try:
            proc = subprocess.Popen(
                cmd,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,  # own process group => killpg in stop_healthchecks()
            )
            with self._active_procs_lock:
                self._active_procs.add(proc)

            try:
                stdout, stderr = proc.communicate(timeout=self.hc_request_timeout_seconds)
            except subprocess.TimeoutExpired:
                # Treat timeout the same as the previous subprocess.run(timeout=...) behaviour.
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except (ProcessLookupError, PermissionError, OSError):
                    pass
                stdout, stderr = proc.communicate(timeout=1)
                logger.error(
                    "Healthcheck for %s timed out after %ss. stdout: %r, stderr: %r",
                    host, self.hc_request_timeout_seconds,
                    self.__shorten(stdout), self.__shorten(stderr),
                )
                return host, {'self_check_result': 'HC_REQUEST_ERROR'}

            if proc.returncode != 0:
                if self.stop:
                    # We SIGTERM'd it ourselves in stop_healthchecks(); not a cluster problem.
                    logger.debug("Healthcheck for %s killed on shutdown (rc=%s)", host, proc.returncode)
                    return host, {'self_check_result': 'HC_REQUEST_ERROR'}
                # The CLI reports the actual reason (transport error, auth, misuse, ...)
                # on stderr; without it a bare exit code says nothing.
                logger.error(
                    "Healthcheck for %s failed: rc=%s after %.1fs, cmd=%s, stderr: %r, stdout: %r",
                    host, proc.returncode, time.monotonic() - started_at, ' '.join(cmd),
                    self.__shorten(stderr), self.__shorten(stdout),
                )
                return host, {'self_check_result': 'HC_REQUEST_ERROR'}

            try:
                return host, json.loads(stdout)
            except json.JSONDecodeError as e:
                logger.error(
                    "Healthcheck for %s returned rc=0 but unparseable output (%s). stdout: %r, stderr: %r",
                    host, e, self.__shorten(stdout), self.__shorten(stderr),
                )
                return host, {'self_check_result': 'HC_REQUEST_ERROR'}
        except Exception:
            logger.error(f"Unexpected error during healthcheck for {host}: {traceback.format_exc()}")
            return host, {'self_check_result': 'HC_REQUEST_ERROR'}
        finally:
            if proc is not None:
                with self._active_procs_lock:
                    self._active_procs.discard(proc)

    def __execute_healthcheck(self):
        results = {}
        if self._executor is None or not self.hosts:
            return results

        future_to_host = {
            self._executor.submit(self.__run_one_healthcheck, host): host
            for host in self.hosts
        }

        # Use as_completed with a single overall timeout so total latency stays
        # close to the slowest host instead of growing with len(hosts) * timeout.
        overall_timeout = self.hc_request_timeout_seconds + 5
        try:
            for future in as_completed(future_to_host, timeout=overall_timeout):
                host = future_to_host[future]
                if self.stop:
                    future.cancel()
                    results[host] = {'self_check_result': 'HC_RESULT_ERROR'}
                    continue
                try:
                    got_host, result = future.result()
                    results[got_host] = result
                except Exception:
                    logger.error(f"Failed to retrieve result for healthcheck on {host}: {traceback.format_exc()}")
                    results[host] = {'self_check_result': 'HC_RESULT_ERROR'}
        except FuturesTimeoutError:
            # Any futures that did not finish within the overall timeout: mark as error and try to cancel.
            for future, host in future_to_host.items():
                if host in results:
                    continue
                future.cancel()
                results[host] = {'self_check_result': 'HC_RESULT_ERROR'}
        return results

    def __publish_healthcheck_results(self, results):
        for host, host_result in results.items():
            target_url = f"http://{host}:3124/write"
            self_check_result = host_result.get('self_check_result', 'UNSPECIFIED')
            host_metric = {
                "labels": {
                    "sensor": "test_metric",
                    "name": 'ydb_healthcheck_status',
                    "self_check_result": self_check_result,
                },
                "value": 1
            }
            payload = {
                "metrics": [host_metric]
            }
            headers = {'Content-Type': 'application/json'}
            try:
                requests.post(target_url, json=payload, headers=headers, timeout=(5, 5))
            except Exception as e:
                logger.error(f"Failed to publish healthcheck results for {host}: {e}")
