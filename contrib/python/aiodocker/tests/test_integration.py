from __future__ import annotations

import asyncio
import datetime
import io
import os
import pathlib
import ssl
import sys
import tarfile
import time
from typing import Any, List

import aiohttp
import pytest
from async_timeout import timeout

import aiodocker
from aiodocker.containers import DockerContainer
from aiodocker.docker import Docker
from aiodocker.execs import Stream
from aiodocker.types import AsyncContainerFactory


async def expect_prompt(stream: Stream) -> bytes:
    try:
        inp = []
        ret: List[bytes] = []
        async with timeout(3):
            while not ret or not ret[-1].endswith(b">>>"):
                msg = await stream.read_out()
                if msg is None:
                    break
                inp.append(msg.data)
                assert msg.stream == 1
                lines = [line.strip() for line in msg.data.splitlines()]
                lines = [line if b"\x1b[K" not in line else b"" for line in lines]
                lines = [line for line in lines if line]
                ret.extend(lines)
            return b"\n".join(ret)
    except asyncio.TimeoutError:
        raise AssertionError(f"[Timeout] {ret} {inp}")


def skip_windows() -> None:
    if sys.platform == "win32":
        # replaced xfail with skip for sake of tests speed
        pytest.skip("image operation fails on Windows")


@pytest.mark.asyncio
async def test_autodetect_host() -> None:
    docker = Docker()
    if "DOCKER_HOST" in os.environ:
        if (
            os.environ["DOCKER_HOST"].startswith("http://")
            or os.environ["DOCKER_HOST"].startswith("https://")
            or os.environ["DOCKER_HOST"].startswith("tcp://")
        ):
            assert docker.docker_host == os.environ["DOCKER_HOST"]
        else:
            assert docker.docker_host in ["unix://localhost", "npipe://localhost"]
    else:
        # assuming that docker daemon is installed locally.
        assert docker.docker_host is not None
    await docker.close()


@pytest.mark.asyncio
async def test_ssl_context(monkeypatch: pytest.MonkeyPatch) -> None:
    cert_dir = pathlib.Path(__file__).parent / "certs"
    monkeypatch.setenv("DOCKER_HOST", "tcp://127.0.0.1:3456")
    monkeypatch.setenv("DOCKER_TLS_VERIFY", "1")
    monkeypatch.setenv("DOCKER_CERT_PATH", str(cert_dir))
    docker = Docker()
    try:
        assert isinstance(docker.connector, aiohttp.TCPConnector)
        assert docker.connector._ssl
    finally:
        await docker.close()

    ssl_ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
    ssl_ctx.load_verify_locations(cafile=str(cert_dir / "ca.pem"))
    ssl_ctx.load_cert_chain(
        certfile=str(cert_dir / "cert.pem"), keyfile=str(cert_dir / "key.pem")
    )
    docker = Docker(ssl_context=ssl_ctx)
    try:
        assert isinstance(docker.connector, aiohttp.TCPConnector)
        assert docker.connector._ssl
    finally:
        await docker.close()

    with pytest.raises(TypeError):
        docker = Docker(ssl_context="bad ssl context")  # type: ignore


@pytest.mark.skipif(
    sys.platform == "win32", reason="Unix sockets are not supported on Windows"
)
@pytest.mark.asyncio
async def test_connect_invalid_unix_socket() -> None:
    docker = Docker("unix:///var/run/does-not-exist-docker.sock")
    assert isinstance(docker.connector, aiohttp.UnixConnector)
    with pytest.raises(aiodocker.DockerError):
        await docker.containers.list()
    await docker.close()


@pytest.mark.skipif(
    sys.platform == "win32", reason="Unix sockets are not supported on Windows"
)
@pytest.mark.asyncio
async def test_connect_envvar(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DOCKER_HOST", "unix:///var/run/does-not-exist-docker.sock")
    docker = Docker()
    assert isinstance(docker.connector, aiohttp.UnixConnector)
    assert docker.docker_host == "unix://localhost"
    with pytest.raises(aiodocker.DockerError):
        await docker.containers.list()
    await docker.close()

    monkeypatch.setenv("DOCKER_HOST", "http://localhost:9999")
    docker = Docker()
    assert isinstance(docker.connector, aiohttp.TCPConnector)
    assert docker.docker_host == "http://localhost:9999"
    with pytest.raises(aiodocker.DockerError):
        await docker.containers.list()
    await docker.close()


@pytest.mark.asyncio
async def test_connect_with_connector() -> None:
    connector = aiohttp.BaseConnector()
    docker = Docker(connector=connector)
    assert docker.connector == connector
    await docker.close()


@pytest.mark.asyncio
async def test_container_lifecycles(docker: Docker, image_name: str) -> None:
    containers = await docker.containers.list(all=True)
    orig_count = len(containers)

    config: dict[str, Any] = {
        "Cmd": ["python"],
        "Image": image_name,
        "AttachStdin": False,
        "AttachStdout": False,
        "AttachStderr": False,
        "Tty": False,
        "OpenStdin": False,
    }

    my_containers = []
    for i in range(3):
        container = await docker.containers.create(config=config)
        assert container
        my_containers.append(container)

    containers = await docker.containers.list(all=True)
    assert len(containers) == orig_count + 3

    f_container = containers[0]
    await f_container.start()
    await f_container.show()

    for container in my_containers:
        await container.delete(force=True)

    containers = await docker.containers.list(all=True)
    assert len(containers) == orig_count


@pytest.mark.asyncio
@pytest.mark.skipif(
    sys.platform in ["darwin", "win32"],
    reason="Docker for Mac and Windows has a bug with websocket",
)
async def test_stdio_stdin(
    docker: Docker,
    testing_images: List[str],
    shell_container: DockerContainer,
) -> None:
    # echo of the input.
    ws = await shell_container.websocket(stdin=True, stdout=True, stream=True)
    await ws.send_str("print('hello world\\n')\n")
    output_bytes = b""
    found = False
    try:
        # collect the websocket outputs for at most 2 secs until we see the
        # output.
        async with timeout(2):
            while True:
                output_bytes += await ws.receive_bytes()
                if b"print('hello world\\n')" in output_bytes:
                    found = True
                    break
    except asyncio.TimeoutError:
        pass
    await ws.close()
    if not found:
        found = b"print('hello world\\n')" in output_bytes
    assert found, output_bytes

    # cross-check with container logs.
    log: List[str] = []
    found = False
    output_str = ""

    try:
        # collect the logs for at most 2 secs until we see the output.
        async with timeout(2):
            async for s in shell_container.log(stdout=True, follow=True):
                log.append(s)
                if "hello world\r\n" in s:
                    found = True
                    break
    except asyncio.TimeoutError:
        pass
    if not found:
        output_str = "".join(log)
        output_str.strip()
        found = "hello world" in output_str.split("\r\n")
    assert found, output_str


@pytest.mark.asyncio
@pytest.mark.parametrize("stderr", [True, False], ids=lambda x: f"stderr={x}")
async def test_attach_nontty(
    image_name: str,
    make_container: AsyncContainerFactory,
    stderr: bool,
) -> None:
    if stderr:
        cmd = [
            "python",
            "-c",
            "import sys, time; time.sleep(3); print('Hello', file=sys.stderr)",
        ]
    else:
        cmd = ["python", "-c", "import time; time.sleep(3); print('Hello')"]
    config: dict[str, Any] = {
        "Cmd": cmd,
        "Image": image_name,
        "AttachStdin": False,
        "AttachStdout": False,
        "AttachStderr": False,
        "Tty": False,
        "OpenStdin": False,
        "StdinOnce": False,
    }
    container = await make_container(config, name="aiodocker-testing-attach-nontty")

    async with container.attach(stdin=False, stdout=True, stderr=True) as stream:
        msg = await stream.read_out()
        assert msg is not None
        fileno, data = msg
        assert fileno == 2 if stderr else 1
        assert data.strip() == b"Hello"


@pytest.mark.asyncio
async def test_attach_nontty_wait_for_exit(
    image_name: str,
    make_container: AsyncContainerFactory,
) -> None:
    cmd = ["python", "-c", "import time; time.sleep(3); print('Hello')"]
    config: dict[str, Any] = {
        "Cmd": cmd,
        "Image": image_name,
        "AttachStdin": False,
        "AttachStdout": False,
        "AttachStderr": False,
        "Tty": False,
        "OpenStdin": False,
        "StdinOnce": False,
    }
    container: DockerContainer = await make_container(
        config,
        name="aiodocker-testing-attach-nontty-wait-for-exit",
    )

    async with container.attach(stdin=False, stdout=True, stderr=True):
        await asyncio.sleep(10)


@pytest.mark.asyncio
@pytest.mark.xfail(reason="Failing since Oct 8th 2024 for unknown reasons")
async def test_attach_tty(
    image_name: str,
    make_container: AsyncContainerFactory,
) -> None:
    skip_windows()
    config: dict[str, Any] = {
        "Cmd": ["python", "-q"],
        "Image": image_name,
        "AttachStdin": True,
        "AttachStdout": True,
        "AttachStderr": True,
        "Tty": True,
        "OpenStdin": True,
        "StdinOnce": False,
    }
    container: DockerContainer = await make_container(
        config, name="aiodocker-testing-attach-tty"
    )

    async with container.attach(stdin=True, stdout=True, stderr=True) as stream:
        await container.resize(w=80, h=25)

        assert await expect_prompt(stream) == b">>>"

        await stream.write_in(b"import sys\n")
        assert await expect_prompt(stream) == b"import sys\n>>>"

        await stream.write_in(b"print('stdout')\n")
        assert await expect_prompt(stream) == b"print('stdout')\nstdout\n>>>"

        await stream.write_in(b"print('stderr', file=sys.stderr)\n")
        assert (
            await expect_prompt(stream)
            == b"print('stderr', file=sys.stderr)\nstderr\n>>>"
        )

        await stream.write_in(b"exit()\n")


@pytest.mark.asyncio
async def test_wait_timeout(
    shell_container: DockerContainer,
) -> None:
    t1 = datetime.datetime.now()
    with pytest.raises(asyncio.TimeoutError):
        await shell_container.wait(timeout=0.5)
    t2 = datetime.datetime.now()
    delta = t2 - t1
    assert delta.total_seconds() < 5


@pytest.mark.asyncio
async def test_put_archive(docker: Docker, image_name: str) -> None:
    skip_windows()

    config: dict[str, Any] = {
        "Cmd": ["python", "-c", "print(open('tmp/bar/foo.txt').read())"],
        "Image": image_name,
        "AttachStdin": False,
        "AttachStdout": False,
        "AttachStderr": False,
        "Tty": False,
        "OpenStdin": False,
    }

    file_data = b"hello world"
    file_like_object = io.BytesIO()
    tar = tarfile.open(fileobj=file_like_object, mode="w")

    info = tarfile.TarInfo(name="bar")
    info.type = tarfile.DIRTYPE
    info.mode = 0o755
    info.mtime = int(time.time())
    tar.addfile(tarinfo=info)

    tarinfo = tarfile.TarInfo(name="bar/foo.txt")
    tarinfo.size = len(file_data)
    tar.addfile(tarinfo, io.BytesIO(file_data))
    tar.list()
    tar.close()

    container = await docker.containers.create_or_replace(
        config=config, name="aiodocker-testing-archive"
    )
    await container.put_archive(path="tmp", data=file_like_object.getvalue())
    await container.start()
    await container.wait(timeout=5)

    output = await container.log(stdout=True, stderr=True)
    assert output[0] == "hello world\n", output
    await container.delete(force=True)


@pytest.mark.asyncio
async def test_get_archive(
    image_name: str,
    make_container: AsyncContainerFactory,
) -> None:
    skip_windows()

    config: dict[str, Any] = {
        "Cmd": [
            "python",
            "-c",
            "with open('tmp/foo.txt', 'w') as f: f.write('test\\n')",
        ],
        "Image": image_name,
        "AttachStdin": False,
        "AttachStdout": False,
        "AttachStderr": False,
        "Tty": True,
        "OpenStdin": False,
    }

    container = await make_container(config, name="aiodocker-testing-get-archive")
    await container.start()
    await asyncio.sleep(1)
    tar_archive = await container.get_archive("tmp/foo.txt")

    assert tar_archive is not None
    assert len(tar_archive.getmembers()) == 1
    foo_file = tar_archive.extractfile("foo.txt")
    assert foo_file is not None
    assert foo_file.read() == b"test\n"


@pytest.mark.asyncio
@pytest.mark.skipif(
    sys.platform == "win32", reason="Port is not exposed on Windows by some reason"
)
async def test_port(
    docker: Docker,
    image_name: str,
    make_container: AsyncContainerFactory,
) -> None:
    config: dict[str, Any] = {
        "Cmd": [
            "python",
            "-c",
            "import socket\n"
            "s=socket.socket()\n"
            "s.bind(('0.0.0.0', 5678))\n"
            "s.listen()\n"
            "while True: s.accept()",
        ],
        "Image": image_name,
        "ExposedPorts": {"5678/tcp": {}},
        "HostConfig": {
            "PublishAllPorts": True,
        },
    }
    container = await make_container(config, name="aiodocker-testing-port")
    port = await container.port(5678)
    assert port is not None


@pytest.mark.asyncio
async def test_events(docker: Docker, image_name: str) -> None:
    # Сheck the stop procedure
    docker.events.subscribe()
    await docker.events.stop()

    subscriber = docker.events.subscribe()

    # Do some stuffs to generate events.
    config: dict[str, Any] = {"Cmd": ["python"], "Image": image_name}
    container = await docker.containers.create_or_replace(
        config=config, name="aiodocker-testing-temp"
    )
    await container.start()
    await container.delete(force=True)

    events_occurred = []
    while True:
        try:
            async with timeout(0.2):
                event = await subscriber.get()
            if event["Actor"]["ID"] == container._id:
                events_occurred.append(event["Action"])
        except asyncio.TimeoutError:
            # no more events
            break
        except asyncio.CancelledError:
            break

    # 'kill' event may be omitted
    assert events_occurred == [
        "create",
        "start",
        "kill",
        "die",
        "destroy",
    ] or events_occurred == ["create", "start", "die", "destroy"]

    await docker.events.stop()
