from datetime import datetime
import json
import os
import shutil
import socket
import subprocess
import yaml
from typing import Dict, Any, Sequence

import yatest.common

from ydb.library.yql.providers.generic.connector.tests.utils.log import make_logger

LOGGER = make_logger(__name__)


class DockerComposeHelper:
    docker_bin_path: os.PathLike
    docker_compose_bin_path: os.PathLike

    docker_compose_yml_path: os.PathLike
    docker_compose_yml_data: Dict[str, Any]

    def __init__(self, docker_compose_yml_path: os.PathLike):
        self.docker_bin_path = shutil.which('docker')

        self.docker_compose_bin_path = None
        try:
            self.docker_compose_bin_path = yatest.common.build_path('library/recipes/docker_compose/bin/docker-compose')
        except Exception as e:
            LOGGER.warn(f"Exception while determining docker-compose path: {e}")

            self.docker_compose_bin_path = shutil.which('docker-compose')
        finally:
            if self.docker_compose_bin_path is None:
                raise ValueError("no `docker-compose` installed on the host")

        self.docker_compose_yml_path = docker_compose_yml_path

        with open(self.docker_compose_yml_path) as f:
            self.docker_compose_yml_data = yaml.load(f)

    def get_external_port(self, service_name: str, internal_port: int) -> int:
        cmd = [
            self.docker_compose_bin_path,
            '-f',
            self.docker_compose_yml_path,
            'port',
            service_name,
            str(internal_port),
        ]
        try:
            out = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            external_port = int(out.split(b':')[1])
            return external_port
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"docker-compose error: {e.output} (code {e.returncode})")

    @staticmethod
    def __is_valid_ipv4_address(address: str) -> bool:
        try:
            socket.inet_pton(socket.AF_INET, address)
        except AttributeError as e1:  # no inet_pton here, sorry
            LOGGER.warn(f"validate '{address}' with inet_pton error: {e1}")
            try:
                socket.inet_aton(address)
            except socket.error as e2:
                LOGGER.error(f"validate '{address}' with inet_aton error: {e2}")
                return False
            return address.count('.') == 3
        except socket.error as e3:  # not a valid address
            LOGGER.error(f"validate '{address}' with inet_pton error: {e3}")
            return False

        return True

    @staticmethod
    def __is_valid_ipv6_address(address: str) -> bool:
        try:
            socket.inet_pton(socket.AF_INET6, address)
        except socket.error:  # not a valid address
            return False
        return True

    @staticmethod
    def __is_valid_ip_address(address: str) -> bool:
        return DockerComposeHelper.__is_valid_ipv4_address(address) or DockerComposeHelper.__is_valid_ipv6_address(
            address
        )

    def get_internal_ip(self, service_name: str) -> str:
        container_name = self.docker_compose_yml_data['services'][service_name]['container_name']
        cmd = [
            self.docker_bin_path,
            "inspect",
            "-f",
            "'{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}'",
            container_name,
        ]
        try:
            out = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('utf8').strip().strip("'")

            if not DockerComposeHelper.__is_valid_ip_address(out):
                raise ValueError(f"IP determined for container '{container_name}' is invalid: '{out}'")

            return out
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"docker-compose error: {e.output} (code {e.returncode})")

    def get_container_name(self, service_name: str) -> str:
        return self.docker_compose_yml_data['services'][service_name]['container_name']

    def list_ydb_tables(self) -> Sequence[str]:
        cmd = [
            self.docker_bin_path,
            'exec',
            self.docker_compose_yml_data["services"]["ydb"]["container_name"],
            '/ydb',
            '--endpoint=grpc://localhost:2136',
            '--database=/local',
            'scheme',
            'ls',
            '--format=json',
        ]

        LOGGER.debug("calling command: " + " ".join(cmd))

        # let tables initialize
        # TODO maybe try except where timeout (quick check: to get it set sleep to zero and review error log for ../datasource/ydb -F *optional*)
        # time.sleep(15)

        # This should be enough for database to initialize
        #   makes CalledProcessError if database did not initialize it`s first tables before check
        passed = False
        err = None
        start = datetime.now()

        timeout = 15
        while (datetime.now() - start).total_seconds() < timeout and not passed:
            try:
                out = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('utf8')
                passed = True
            except subprocess.CalledProcessError as e:
                err = RuntimeError(f"docker-compose error: {e.output} (code {e.returncode})")

        if not passed:
            if err is not None:
                raise err
            else:
                raise RuntimeError("docker-compose error: timed out to check cmd output")

        data = json.loads(out)

        result = []
        for item in data:
            if item['type'] == 'table':
                result.append(item['path'])

        return result

    def list_mysql_tables(self) -> Sequence[str]:
        params = self.docker_compose_yml_data["services"]["mysql"]
        password = params["environment"]["MYSQL_ROOT_PASSWORD"]
        db = params["environment"]["MYSQL_DATABASE"]
        cmd = [
            self.docker_bin_path,
            'exec',
            params["container_name"],
            'mysql',
            f'--password={password}',
            db,
            '-e',
            f'SELECT table_name FROM information_schema.tables WHERE table_schema = "{db}"',
        ]

        LOGGER.debug("calling command: " + " ".join(cmd))

        out = None

        try:
            out = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('utf8')
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"docker cmd failed: {e.output} (code {e.returncode})")
        else:
            return out.splitlines()[2:]
