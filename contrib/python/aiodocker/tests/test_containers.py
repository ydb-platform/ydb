import asyncio
import secrets
import sys
from contextlib import suppress

import pytest

from aiodocker.containers import DockerContainer
from aiodocker.docker import Docker
from aiodocker.exceptions import DockerContainerError, DockerError


async def _validate_hello(container: DockerContainer) -> None:
    try:
        await container.start()
        response = await container.wait()
        assert response["StatusCode"] == 0
        await asyncio.sleep(5)  # wait for output in case of slow test container
        logs = await container.log(stdout=True)
        assert "hello\n" in logs

        with pytest.raises(TypeError):
            await container.log()
    finally:
        await container.delete(force=True)


@pytest.mark.asyncio
async def test_run_existing_container(docker: Docker, image_name: str) -> None:
    await docker.pull(image_name)
    container = await docker.containers.run(
        config={
            "Cmd": ["-c", "print('hello')"],
            "Entrypoint": ["python"],
            "Image": image_name,
        }
    )

    await _validate_hello(container)


@pytest.mark.asyncio
async def test_run_container_with_missing_image(
    docker: Docker, image_name: str
) -> None:
    try:
        await docker.images.delete(image_name)
    except DockerError as e:
        if e.status == 404:
            pass  # already missing, pass
        elif e.status == 409:
            await docker.images.delete(image_name, force=True)
        else:
            raise

    # should automatically pull the image
    container = await docker.containers.run(
        config={
            "Cmd": ["-c", "print('hello')"],
            "Entrypoint": ["python"],
            "Image": image_name,
        }
    )

    await _validate_hello(container)


@pytest.mark.asyncio
async def test_run_failing_start_container(docker: Docker, image_name: str) -> None:
    try:
        await docker.images.delete(image_name)
    except DockerError as e:
        if e.status == 404:
            pass  # already missing, pass
        elif e.status == 409:
            await docker.images.delete(image_name, force=True)
        else:
            raise

    with pytest.raises(DockerContainerError) as e_info:
        await docker.containers.run(
            config={
                # we want to raise an error
                # `executable file not found`
                "Cmd": ["pytohon", "-cprint('hello')"],
                "Image": image_name,
            }
        )

    assert e_info.value.container_id
    # This container is created but not started!
    # We should delete it afterwards.
    cid = e_info.value.container_id
    container = docker.containers.container(cid)
    await container.delete()


@pytest.mark.asyncio
async def test_restart(docker: Docker, image_name: str) -> None:
    # sleep for 10 min to emulate hanging container
    container = await docker.containers.run(
        config={
            "Cmd": ["python", "-c", "import time;time.sleep(600)"],
            "Image": image_name,
        }
    )
    try:
        details = await container.show()
        assert details["State"]["Running"]
        startTime = details["State"]["StartedAt"]
        await container.restart(timeout=1)
        await asyncio.sleep(3)
        details = await container.show()
        assert details["State"]["Running"]
        restartTime = details["State"]["StartedAt"]

        assert restartTime > startTime

        await container.stop()
    finally:
        await container.delete(force=True)


@pytest.mark.asyncio
async def test_container_stats_list(docker: Docker, image_name: str) -> None:
    container = await docker.containers.run(
        config={
            "Cmd": ["-c", "print('hello')"],
            "Entrypoint": ["python"],
            "Image": image_name,
        }
    )

    try:
        await container.start()
        response = await container.wait()
        assert response["StatusCode"] == 0
        stats = await container.stats(stream=False)
        assert "cpu_stats" in stats[0]
    finally:
        await container.delete(force=True)


@pytest.mark.asyncio
async def test_container_stats_stream(docker: Docker, image_name: str) -> None:
    container = await docker.containers.run(
        config={
            "Cmd": ["-c", "print('hello')"],
            "Entrypoint": ["python"],
            "Image": image_name,
        }
    )

    try:
        await container.start()
        response = await container.wait()
        assert response["StatusCode"] == 0
        count = 0
        async for stat in container.stats():
            assert "cpu_stats" in stat
            count += 1
            if count > 3:
                break
    finally:
        await container.delete(force=True)


@pytest.mark.asyncio
async def test_resize(shell_container: DockerContainer) -> None:
    await shell_container.resize(w=120, h=10)


@pytest.mark.skipif(
    sys.platform == "win32", reason="Commit unpaused containers doesn't work on Windows"
)
@pytest.mark.asyncio
async def test_commit(
    docker: Docker,
    image_name: str,
    shell_container: DockerContainer,
) -> None:
    # "Container" key was removed in v1.45.
    # "ContainerConfig" is not present, although this information is now present in "Config"
    # These changes have been verified against v1.45.
    #
    # "Image" key was removed in v1.50.
    # "RepoTags" key was added in v1.21.
    repo_name = f"aiodocker-commit-{secrets.token_hex(8)}"
    tag = "latest"
    ret = await shell_container.commit(repository=repo_name, tag=tag)
    img_id = ret["Id"]
    img = await docker.images.inspect(img_id)
    try:
        assert f"{repo_name}:{tag}" in img["RepoTags"]
        python_img = await docker.images.inspect(image_name)
        python_id = python_img["Id"]
        assert "Parent" in img
        assert img["Parent"] == python_id
    finally:
        await docker.images.delete(img_id)


@pytest.mark.skipif(
    sys.platform == "win32", reason="Commit unpaused containers doesn't work on Windows"
)
@pytest.mark.asyncio
async def test_commit_with_changes(
    docker: Docker,
    image_name: str,
    shell_container: DockerContainer,
) -> None:
    ret = await shell_container.commit(changes=["EXPOSE 8000", 'CMD ["py"]'])
    img_id = ret["Id"]
    img = await docker.images.inspect(img_id)
    assert "8000/tcp" in img["Config"]["ExposedPorts"]
    assert img["Config"]["Cmd"] == ["py"]
    await docker.images.delete(img_id)


@pytest.mark.skipif(sys.platform == "win32", reason="Pause doesn't work on Windows")
@pytest.mark.asyncio
async def test_pause_unpause(shell_container: DockerContainer) -> None:
    await shell_container.pause()
    container_info = await shell_container.show()
    assert "State" in container_info
    state = container_info["State"]
    assert "ExitCode" in state
    assert state["ExitCode"] == 0
    assert "Running" in state
    assert state["Running"] is True
    assert "Paused" in state
    assert state["Paused"] is True

    await shell_container.unpause()
    container_info = await shell_container.show()
    assert "State" in container_info
    state = container_info["State"]
    assert "ExitCode" in state
    assert state["ExitCode"] == 0
    assert "Running" in state
    assert state["Running"] is True
    assert "Paused" in state
    assert state["Paused"] is False


@pytest.mark.asyncio
async def test_capture_log_oneshot(docker: Docker, image_name: str) -> None:
    container = await docker.containers.run(
        config={
            "Cmd": [
                "python",
                "-c",
                "import time;time.sleep(0.2);print(1);time.sleep(0.2);print(2)",
            ],
            "Image": image_name,
        }
    )
    try:
        await asyncio.sleep(1)
        log = await container.log(
            stdout=True,
            stderr=True,
            follow=False,
        )
        assert ["1\n", "2\n"] == log
    finally:
        await container.delete(force=True)


@pytest.mark.asyncio
async def test_capture_log_stream(docker: Docker, image_name: str) -> None:
    container = await docker.containers.run(
        config={
            "Cmd": [
                "python",
                "-c",
                "import time;time.sleep(0.2);print(1);time.sleep(0.2);print(2)",
            ],
            "Image": image_name,
        }
    )
    try:
        log_gen = container.log(
            stdout=True,
            stderr=True,
            follow=True,
        )
        log = []
        async for line in log_gen:
            log.append(line)
        assert ["1\n", "2\n"] == log
    finally:
        await container.delete(force=True)


@pytest.mark.asyncio
async def test_cancel_log(docker: Docker) -> None:
    container = docker.containers.container("invalid_container_id")

    with pytest.raises(DockerError):
        await container.log(
            stdout=True,
            stderr=True,
        )


@pytest.mark.asyncio
async def test_stop_with_graceful_timeout(docker: Docker, image_name: str) -> None:
    """Test that container is successfully stopped with server-side graceful timeout.

    This test verifies:
    1. A container with a script that delays handling SIGTERM
    2. The stop() call with t=2 (server-side timeout) succeeds immediately
    3. The container is forcefully killed after the graceful period expires
    4. HTTP timeout doesn't interfere with server-side stop timeout
    """
    # Create a container with a script that traps SIGTERM and delays shutdown
    # The script will try to sleep for 30 seconds when it receives SIGTERM,
    # which is much longer than our stop timeout
    container = await docker.containers.create(
        config={
            "Cmd": [
                "sh",
                "-c",
                # Trap SIGTERM to delay shutdown, sleep in foreground to keep container running
                "trap 'echo Caught SIGTERM, sleeping...; sleep 999; echo Done sleeping' TERM; "
                "echo Container started; sleep 999",
            ],
            "Image": image_name,
        }
    )

    try:
        await container.start()
        await asyncio.sleep(0.5)  # Let container start up

        # Verify container is running
        info = await container.show()
        assert info["State"]["Running"]

        # Stop with server-side timeout of 2 seconds and HTTP timeout of 5 seconds
        # The container will be forcefully killed (SIGKILL) after 2 seconds
        # because it won't finish handling SIGTERM within that time
        await container.stop(t=2, timeout=5.0)

        # Verify container is stopped
        info = await container.show()
        assert not info["State"]["Running"]
        assert info["State"]["Status"] in ["exited", "dead"]

    finally:
        try:
            await container.delete(force=True)
        except DockerError:
            pass  # Already deleted


@pytest.mark.asyncio
async def test_stop_with_short_http_timeout(docker: Docker, image_name: str) -> None:
    """Test that container is eventually stopped even when HTTP request times out.

    This verifies that when HTTP timeout < server-side stop timeout, the HTTP
    request may time out, but the Docker daemon still completes the stop operation
    and the container is eventually stopped.
    """
    # Create a container that will take time to stop
    container = await docker.containers.create(
        config={
            "Cmd": [
                "sh",
                "-c",
                "trap 'echo Caught SIGTERM; sleep 999' TERM; sleep 999",
            ],
            "Image": image_name,
        }
    )

    try:
        await container.start()
        await asyncio.sleep(0.5)

        # Try to stop with t=3 seconds (server-side) and very short HTTP timeout
        # The HTTP request will time out because Docker stop API waits for the
        # container to actually stop (or for the graceful timeout to expire)
        try:
            await container.stop(t=3, timeout=0.5)
        except asyncio.TimeoutError:
            # Expected: HTTP request times out because timeout (0.5s) < t (3s)
            pass

        # Wait for the server-side stop timeout to complete
        await asyncio.sleep(4)

        # Verify container is stopped despite the HTTP timeout
        # The daemon should have completed the stop operation
        info = await container.show()
        assert not info["State"]["Running"]

    finally:
        try:
            await container.delete(force=True)
        except DockerError:
            pass


@pytest.mark.asyncio
async def test_delete_running_container_with_force(
    docker: Docker, image_name: str
) -> None:
    """Test that delete(force=True) successfully kills and removes a running container.

    This test verifies:
    1. A running container can be forcefully deleted
    2. The delete operation works with a reasonable HTTP timeout
    3. The container is completely removed after the operation
    """
    # Create a container with a script that ignores SIGTERM
    container = await docker.containers.create(
        config={
            "Cmd": [
                "sh",
                "-c",
                "trap 'echo Caught SIGTERM; sleep 999' TERM; sleep 999",
            ],
            "Image": image_name,
        }
    )

    container_id = container.id

    try:
        await container.start()
        await asyncio.sleep(0.5)

        # Verify container is running
        info = await container.show()
        assert info["State"]["Running"]

        # Delete with force=True and reasonable HTTP timeout
        # This will forcefully kill (SIGKILL) and remove the container
        await container.delete(force=True, timeout=5.0)

        # Wait a bit for the deletion to complete
        await asyncio.sleep(1)

        # Verify container no longer exists
        with pytest.raises(DockerError) as exc_info:
            await docker.containers.get(container_id)
        assert exc_info.value.status == 404

    except DockerError as e:
        # If container was already deleted, that's fine
        if e.status != 404:
            raise


@pytest.mark.asyncio
async def test_prune_containers(
    docker: Docker, random_name: str, image_name: str
) -> None:
    """Test that prune with filters removes only the stopped container that matches the filter."""
    # Create two stopped containers
    container_without_label: DockerContainer = await docker.containers.create(
        {"Image": image_name}, name=f"{random_name}_no_label"
    )
    container_with_label: DockerContainer | None = None
    try:
        container_with_label = await docker.containers.create(
            {"Image": image_name, "Labels": {"test": ""}},
            name=f"{random_name}_with_label",
        )

        # Prune stopped containers with label "test"
        result = await docker.containers.prune(filters={"label": "test"})

        # Verify the response structure
        assert isinstance(result, dict)
        assert "ContainersDeleted" in result
        assert "SpaceReclaimed" in result
        assert result["ContainersDeleted"] == [container_with_label.id]
        assert isinstance(result["SpaceReclaimed"], int)

        # Test that the container without the label still exists
        assert await container_without_label.show()

    finally:
        with suppress(DockerError):
            await container_without_label.delete()
        if container_with_label:
            with suppress(DockerError):
                await container_with_label.delete()


@pytest.mark.asyncio
async def test_prune_containers_nothing_to_remove(docker: Docker) -> None:
    """Test a container prune with nothing to remove."""
    result = await docker.containers.prune()

    # Verify the response structure
    assert isinstance(result, dict)
    assert "ContainersDeleted" in result
    assert "SpaceReclaimed" in result
    assert result["ContainersDeleted"] is None
    assert isinstance(result["SpaceReclaimed"], int)
