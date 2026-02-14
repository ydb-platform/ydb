import json
import logging
import subprocess
import threading
import time
import traceback
import requests
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from ydb.tests.library.stability.utils.utils import unpack_resource


class HealthCheckReporter():
    def __init__(self, hosts: list[str]):
        self.stop = False
        self.healthcheck_thread: threading.Thread = None
        self.ydb_path = os.path.join(os.getcwd(), 'ydb_cli_hc')
        self.hosts = hosts
        self.hc_request_timeout_seconds = 45
        self.hc_period_seconds = 15
        unpack_resource('ydb_cli', self.ydb_path)

    def start_healthchecks(self):
        self.healthcheck_thread = threading.Thread(target=self.__execute_healthcheck_thr, args=())
        self.healthcheck_thread.start()

    def stop_healthchecks(self):
        self.stop = True
        if self.healthcheck_thread and self.healthcheck_thread.is_alive():
            self.healthcheck_thread.join(self.hc_request_timeout_seconds)
            if self.healthcheck_thread.is_alive():
                logging.warning("Healthcheck thread did not stop gracefully, it may be hanging in a blocking operation")

    def __execute_healthcheck_thr(self):
        while self.stop is False:
            try:
                self.__publish_healthcheck_results(self.__execute_healthcheck())
            except Exception as e:
                logging.error(f"Error in healthcheck thread: {e}")
            time.sleep(self.hc_period_seconds)

    def __execute_healthcheck(self):
        results = {}

        def run_healthcheck_for_host(host):
            try:
                cmd = [f'{self.ydb_path}', '--endpoint', f'grpc://{host}:2135', 'monitoring', 'healthcheck', '--format', 'json']
                result = subprocess.run(cmd, check=True, text=True, capture_output=True, timeout=self.hc_request_timeout_seconds)
                return host, json.loads(result.stdout)
            except Exception:
                logging.error(f"Unexpected error during healthcheck for {host}: {traceback.format_exc()}")
                return host, {
                    'self_check_result': 'HC_REQUEST_ERROR'
                }

        max_workers = min(len(self.hosts), 10)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_host = {executor.submit(run_healthcheck_for_host, host): host for host in self.hosts}

            for future in as_completed(future_to_host):
                try:
                    host, result = future.result(timeout=self.hc_request_timeout_seconds)
                    results[host] = result
                except Exception:
                    host = future_to_host[future]
                    logging.error(f"Failed to retrieve result for healthcheck on {host}: {traceback.format_exc()}")
                    results[host] = {
                        'self_check_result': 'HC_RESULT_ERROR'
                    }

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
