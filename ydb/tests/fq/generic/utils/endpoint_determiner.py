import os
import subprocess

import yatest.common


class EndpointDeterminer:
    docker_compose_bin: os.PathLike
    docker_compose_yml: os.PathLike

    def __init__(self, docker_compose_yml: os.PathLike):
        self.docker_compose_bin = yatest.common.build_path(
            "library/recipes/docker_compose/bin/docker-compose"
        )
        self.docker_compose_yml = docker_compose_yml

    def get_port(self, service_name: str, internal_port: int) -> int:
        cmd = [
            self.docker_compose_bin,
            "-f",
            self.docker_compose_yml,
            "port",
            service_name,
            str(internal_port),
        ]
        try:
            out = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            external_port = int(out.split(b":")[1])
            return external_port
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"docker-compose error: {e.output} (code {e.returncode})"
            )
