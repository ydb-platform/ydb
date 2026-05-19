import os
import shutil
import tempfile
import time

import docker
import pytest
import yatest


HEALTH_TIMEOUT_SECONDS = 120
POLL_INTERVAL_SECONDS = 2


def _prepare_context() -> str:
    ctx = tempfile.mkdtemp(prefix="ydb-docker-test-")
    docker_src = yatest.common.source_path(".github/docker")
    shutil.copytree(docker_src, os.path.join(ctx, "main/.github/docker"))
    prebuilt = os.path.join(ctx, "prebuilt")
    os.makedirs(prebuilt)
    shutil.copy(
        yatest.common.binary_path("ydb/apps/ydbd/ydbd"),
        os.path.join(prebuilt, "ydbd"),
    )
    shutil.copy(
        yatest.common.binary_path("ydb/apps/ydb/ydb"),
        os.path.join(prebuilt, "ydb"),
    )
    shutil.copy(
        yatest.common.binary_path("ydb/public/tools/local_ydb/local_ydb"),
        os.path.join(prebuilt, "local_ydb"),
    )
    return ctx


def test_docker_image_starts_healthy():
    # Intentionally no pytest.skip when docker is unavailable: the test must
    # fail loudly, otherwise CI silently loses coverage of nightly publish.
    client = docker.from_env()
    client.ping()

    image_tag = f"ydb-local-test:{os.getpid()}"
    container_name = f"local-ydb-test-{os.getpid()}"
    ctx = _prepare_context()
    container = None

    try:
        image, _ = client.images.build(
            path=ctx,
            dockerfile="main/.github/docker/Dockerfile",
            tag=image_tag,
            buildargs={"BUILD_MODE": "prebuilt"},
            rm=True,
        )

        container = client.containers.run(
            image=image.id,
            name=container_name,
            detach=True,
        )

        deadline = time.monotonic() + HEALTH_TIMEOUT_SECONDS
        status, state = "starting", "running"
        while time.monotonic() < deadline:
            container.reload()
            state = container.attrs["State"]["Status"]
            status = container.attrs["State"].get("Health", {}).get("Status", "none")
            if state == "exited":
                pytest.fail(
                    f"container exited early. logs:\n"
                    f"{container.logs().decode(errors='replace')}"
                )
            if status == "healthy":
                return
            time.sleep(POLL_INTERVAL_SECONDS)

        pytest.fail(
            f"not healthy in {HEALTH_TIMEOUT_SECONDS}s "
            f"(status={status}, state={state}). logs:\n"
            f"{container.logs().decode(errors='replace')}"
        )
    finally:
        if container is not None:
            try:
                container.remove(force=True)
            except docker.errors.DockerException:
                pass
        try:
            client.images.remove(image_tag, force=True)
        except docker.errors.DockerException:
            pass
        shutil.rmtree(ctx, ignore_errors=True)
