import os
import subprocess
import shutil
import yaml
import socket
from typing import Dict, Any

import yatest.common

from ydb.library.yql.providers.generic.connector.tests.utils.log import make_logger

LOGGER = make_logger(__name__)


class EndpointDeterminer:
    docker_bin_path: os.PathLike
    docker_compose_bin_path: os.PathLike

    docker_compose_yml_path: os.PathLike
    docker_compose_yml_data: Dict[str, Any]

    def __init__(self, docker_compose_yml_path: os.PathLike):
        self.docker_bin_path = shutil.which('docker')
        self.docker_compose_bin_path = yatest.common.build_path('library/recipes/docker_compose/bin/docker-compose')
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
        return EndpointDeterminer.__is_valid_ipv4_address(address) or EndpointDeterminer.__is_valid_ipv6_address(
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

            if not EndpointDeterminer.__is_valid_ip_address(out):
                raise ValueError(f"IP determined for container '{container_name}' is invalid: '{out}'")

            return out
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"docker-compose error: {e.output} (code {e.returncode})")
