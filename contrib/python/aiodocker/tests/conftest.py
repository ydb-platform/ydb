from __future__ import annotations

import asyncio
import os
import secrets
import subprocess
import sys
import time
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any, Callable

import pytest
from packaging.version import parse as parse_version

from aiodocker.containers import DockerContainer
from aiodocker.docker import Docker
from aiodocker.exceptions import DockerError
from aiodocker.types import AsyncContainerFactory


@pytest.fixture(scope="session")
async def random_name():
    random_image_name = "aiodocker-" + secrets.token_hex(4)
    yield random_image_name

    docker = Docker()
    try:
        image = await docker.images.inspect(random_image_name)
        print(f"Deleting image: {random_image_name} ({image['Id']})")
        await docker.images.delete(random_image_name, force=True)
    except DockerError as e:
        if e.status == 404:
            pass
        else:
            raise
    finally:
        await docker.close()


@pytest.fixture(scope="module")
def plain_registry() -> Iterator[TempContainer]:
    with TempContainer(
        "registry:2",
        name=f"aiodocker-test-registry-plain-{secrets.token_hex(4)}",
        ports=[5000],
        _wait_strategy=HttpWaitStrategy(5000).for_status_code(200),
    ) as plain_registry:
        yield plain_registry


@pytest.fixture(scope="module")
def secure_registry() -> Iterator[TempContainer]:
    with TempContainer(
        "registry:2",
        name=f"aiodocker-test-registry-secure-{secrets.token_hex(4)}",
        ports=[5001],
        volumes=[(f"{os.getcwd()}/tests/certs", "/certs", "ro")],
        env={
            "REGISTRY_AUTH": "htpasswd",
            "REGISTRY_AUTH_HTPASSWD_REALM": "Registry Realm",
            "REGISTRY_AUTH_HTPASSWD_PATH": "/certs/htpasswd",
            "REGISTRY_HTTP_ADDR": "0.0.0.0:5001",
            "REGISTRY_HTTP_TLS_CERTIFICATE": "/certs/registry.crt",
            "REGISTRY_HTTP_TLS_KEY": "/certs/registry.key",
        },
        _wait_strategy=(
            HttpWaitStrategy(5001).using_tls(insecure=True).for_status_code(200)
        ),
    ) as secure_registry:
        yield secure_registry


@pytest.fixture(scope="session")
def image_name() -> str:
    if sys.platform == "win32":
        return "python:3.13"
    return "python:3.13-alpine"


@pytest.fixture(scope="session")
def image_name_updated() -> str:
    if sys.platform == "win32":
        return "python:3.14"
    return "python:3.14-alpine"


@pytest.fixture(scope="session")
async def testing_images(image_name: str, image_name_updated: str) -> list[str]:
    images = [image_name, image_name_updated]
    for ref in images:
        proc = await asyncio.create_subprocess_exec("docker", "pull", ref)
        await proc.wait()
    return images


@pytest.fixture
async def docker(testing_images):
    # Create a new Docker client with the default configuration.
    docker = Docker()
    try:
        yield docker
    finally:
        await docker.close()


@pytest.fixture
async def requires_api_version(
    docker: Docker,
) -> AsyncIterator[Callable[[str, str], None]]:
    # Update version info from auto to the real value
    await docker.version()

    def check(version: str, reason: str) -> None:
        if parse_version(docker.api_version[1:]) < parse_version(version[1:]):
            pytest.skip(reason)

    yield check


@pytest.fixture(scope="module")
def dind_docker_host() -> Iterator[str]:
    """Start a Docker-in-Docker daemon for swarm/service tests."""
    if sys.platform == "win32":
        pytest.skip("swarm commands dont work on Windows")

    compose_file = Path(__file__).parent / "docker-compose.dind.yml"
    project_name = f"aiodocker-dind-{secrets.token_hex(4)}"

    # Start the DinD container
    subprocess.run(
        ["docker", "compose", "-f", str(compose_file), "-p", project_name, "up", "-d"],
        check=True,
        capture_output=True,
    )

    # Get the container ID
    result = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(compose_file),
            "-p",
            project_name,
            "ps",
            "-q",
            "docker-dind",
        ],
        check=True,
        capture_output=True,
    )
    container_id = result.stdout.decode().strip()

    # Wait for Docker daemon to be ready
    for _ in range(30):
        result = subprocess.run(
            ["docker", "exec", container_id, "docker", "info"],
            capture_output=True,
        )
        if result.returncode == 0:
            break
        time.sleep(1)
    else:
        # Cleanup on failure
        subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(compose_file),
                "-p",
                project_name,
                "down",
                "-v",
            ],
            check=False,
        )
        pytest.fail("Docker-in-Docker daemon failed to start in time")

    # Get the mapped port for the DinD daemon
    result = subprocess.run(
        ["docker", "port", container_id, "2375"],
        check=True,
        capture_output=True,
    )
    # Take only the first line (IPv4) and clean it up
    dind_host = (
        result.stdout.decode().strip().split("\n")[0].replace("0.0.0.0", "localhost")
    )
    docker_host = f"tcp://{dind_host}"

    try:
        yield docker_host
    finally:
        # Cleanup: stop and remove the DinD container and volumes
        subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(compose_file),
                "-p",
                project_name,
                "down",
                "-v",
            ],
            check=True,
            capture_output=True,
        )


@pytest.fixture
async def swarm(docker, dind_docker_host, testing_images):
    """Initialize swarm in the DinD environment for service/swarm tests."""
    if sys.platform == "win32":
        pytest.skip("swarm commands dont work on Windows")

    # Create a new Docker client connected to DinD
    dind_docker = Docker(url=dind_docker_host)

    try:
        # Pull necessary images into DinD
        for image in testing_images:
            # Use a pipe via shell to transfer images
            transfer_cmd = f"docker save {image} | docker -H {dind_docker_host} load"
            proc = await asyncio.create_subprocess_shell(
                transfer_cmd,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await proc.communicate()
            if proc.returncode != 0:
                raise RuntimeError(
                    f"Failed to transfer image {image}: {stderr.decode()}"
                )

        # Initialize swarm in DinD
        assert await dind_docker.swarm.init()
        yield dind_docker
    finally:
        # Leave swarm and close the DinD client
        try:
            await dind_docker.swarm.leave(force=True)
        except Exception:
            pass  # Best effort cleanup
        await dind_docker.close()


@pytest.fixture
async def make_container(
    docker: Docker,
) -> AsyncIterator[AsyncContainerFactory]:
    container: DockerContainer | None = None

    async def _spawn(
        config: dict[str, Any],
        name: str,
    ) -> DockerContainer:
        nonlocal container
        container = await docker.containers.create_or_replace(config=config, name=name)
        assert container is not None
        await container.start()
        return container

    try:
        yield _spawn
    finally:
        if container is not None:
            await container.delete(force=True)


@pytest.fixture
async def shell_container(
    docker: Docker,
    make_container: AsyncContainerFactory,
    image_name: str,
) -> DockerContainer:
    config = {
        "Cmd": ["python"],
        "Image": image_name,
        "AttachStdin": False,
        "AttachStdout": False,
        "AttachStderr": False,
        "Tty": True,
        "OpenStdin": True,
    }
    return await make_container(config, "aiodocker-testing-shell")
