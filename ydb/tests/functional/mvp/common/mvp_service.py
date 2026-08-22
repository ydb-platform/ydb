import os

import requests
import yatest.common

from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.library.harness.daemon import Daemon


class MvpHttpService:
    def __init__(self, binary_path, config_path, http_port, service_name):
        self.http_port = http_port
        self.service_name = service_name
        self._daemon = Daemon(
            command=[
                binary_path,
                "--config",
                config_path,
                "--http-port",
                str(http_port),
                "--stderr",
            ],
            cwd=yatest.common.output_path(),
            timeout=30,
            stdout_file=os.path.join(yatest.common.output_path(), f"{service_name}.stdout"),
            stderr_file=os.path.join(yatest.common.output_path(), f"{service_name}.stderr"),
            stderr_on_error_lines=100,
        )

    @property
    def endpoint(self):
        return f"http://localhost:{self.http_port}"

    @property
    def stdout_file_name(self):
        return self._daemon.stdout_file_name

    @property
    def stderr_file_name(self):
        return self._daemon.stderr_file_name

    def is_ready(self):
        try:
            return requests.get(f"{self.endpoint}/ping", timeout=1).status_code == 200
        except requests.RequestException:
            return False

    def start(self):
        self._daemon.start()
        ready = wait_for(
            self.is_ready,
            timeout_seconds=30,
            step_seconds=0.5,
        )
        if not ready and self._daemon.is_alive():
            self._daemon.stop()
        assert ready, f"{self.service_name} did not become ready on /ping"
        return self

    def stop(self):
        self._daemon.stop()
