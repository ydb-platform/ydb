import json
import logging
import os
import signal
import subprocess
import threading
import time
import traceback
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
from ydb.tests.library.stability.utils.utils import unpack_resource


class HealthCheckReporter():
    # Hard cap for terminating in-flight healthcheck subprocesses on stop.
    _KILL_GRACE_SECONDS = 2

    def __init__(self, hosts: list[str], store_results: bool = False):
        self.stop = False
        self.healthcheck_thread: threading.Thread = None
        self.ydb_path = os.path.join(os.getcwd(), 'ydb_cli_hc')
        self.hosts = hosts
        self.hc_request_timeout_seconds = 45
        self.hc_period_seconds = 15
        self.store_results = store_results
        self.last_results = {}
        # Interruptible sleep between ticks.
        self._stop_event = threading.Event()
        # Track live subprocesses so stop_healthchecks() can kill them.
        self._active_procs_lock = threading.Lock()
        self._active_procs: set[subprocess.Popen] = set()
        self._executor: ThreadPoolExecutor | None = None
        unpack_resource('ydb_cli', self.ydb_path)

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
                logging.warning("Healthcheck thread did not stop gracefully, it may be hanging in a blocking operation")

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
                    logging.warning("ydb_cli_hc pid=%s did not exit after SIGKILL", proc.pid)

    def __execute_healthcheck_thr(self):
        while not self.stop:
            try:
                results = self.__execute_healthcheck()
                if self.store_results:
                    self.last_results = results
                self.__publish_healthcheck_results(results)
            except Exception as e:
                logging.error(f"Error in healthcheck thread: {e}")
            # Interruptible sleep: wakes up immediately on stop_healthchecks().
            if self._stop_event.wait(timeout=self.hc_period_seconds):
                break

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
                stdout, _ = proc.communicate(timeout=self.hc_request_timeout_seconds)
            except subprocess.TimeoutExpired:
                # Treat timeout the same as the previous subprocess.run(timeout=...) behaviour.
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except (ProcessLookupError, PermissionError, OSError):
                    pass
                proc.communicate(timeout=1)
                raise

            if proc.returncode != 0:
                raise subprocess.CalledProcessError(proc.returncode, cmd)
            return host, json.loads(stdout)
        except Exception:
            logging.error(f"Unexpected error during healthcheck for {host}: {traceback.format_exc()}")
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
                    logging.error(f"Failed to retrieve result for healthcheck on {host}: {traceback.format_exc()}")
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
            host_metric = {
                "labels": {
                    "sensor": "test_metric",
                    "name": 'ydb_healthcheck_status',
                    "self_check_result": host_result['self_check_result'],
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
                logging.error(f"Failed to publish healthcheck results for {host}: {e}")
