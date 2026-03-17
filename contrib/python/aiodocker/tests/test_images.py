from __future__ import annotations

import os
import secrets
import sys
import tarfile
import tempfile
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from io import BytesIO
from pathlib import Path
from typing import Callable

import pytest
from aiohttp import web

from aiodocker import utils
from aiodocker.docker import Docker
from aiodocker.exceptions import DockerError


def skip_windows() -> None:
    if sys.platform == "win32":
        # replaced xfail with skip for sake of tests speed
        pytest.skip("image operation fails on Windows")


@pytest.mark.asyncio
async def test_build_from_remote_file(
    docker: Docker,
    random_name: str,
    requires_api_version: Callable[[str, str], None],
) -> None:
    skip_windows()

    requires_api_version(
        "v1.28",
        "TODO: test disabled because it fails on "
        "API version 1.27, this should be fixed",
    )

    remote = (
        "https://raw.githubusercontent.com/aio-libs/"
        "aiodocker/main/tests/docker/Dockerfile"
    )

    tag = f"{random_name}:1.0"
    await docker.images.build(tag=tag, remote=remote, stream=False)
    try:
        image_info = await docker.images.inspect(tag)
        assert image_info is not None and image_info.get("Id")
        print(image_info)
    finally:
        await docker.images.delete(tag)


@pytest.mark.asyncio
async def test_build_from_remote_tar(docker: Docker, random_name: str) -> None:
    skip_windows()

    @asynccontextmanager
    async def serve_docker_context() -> AsyncIterator[str]:
        # Create a temporary tar file from tests/docker/tar directory
        temp_tar = tempfile.NamedTemporaryFile(mode="wb", suffix=".tar", delete=False)
        tar_path = temp_tar.name
        temp_tar.close()
        try:
            test_dir = Path(__file__).parent / "docker" / "tar"
            with tarfile.open(tar_path, "w") as tar:
                for item in test_dir.iterdir():
                    tar.add(item, arcname=item.name)

            async def serve_tar(_: web.Request) -> web.StreamResponse:
                return web.FileResponse(tar_path)

            app = web.Application()
            app.router.add_get("/docker_context.tar", serve_tar)
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, "127.0.0.1", 0)
            await site.start()
            assert site._server is not None
            port = site._server.sockets[0].getsockname()[1]  # type: ignore
            server_url = f"http://127.0.0.1:{port}/docker_context.tar"
            try:
                yield server_url
            finally:
                await runner.cleanup()
        finally:
            # Clean up the temporary tar file
            if os.path.exists(tar_path):
                os.unlink(tar_path)

    async with serve_docker_context() as remote:
        tag = f"{random_name}:1.0"
        print("---- downloading and building ----")
        output = await docker.images.build(tag=tag, remote=remote, stream=False)
        print("---- build done ----")
        print(output)
        print("----------")
        try:
            image_info = await docker.images.inspect(tag)
            assert image_info is not None and image_info.get("Id")
            print(image_info)
        finally:
            await docker.images.delete(tag)


@pytest.mark.asyncio
async def test_history(docker: Docker, image_name: str) -> None:
    history = await docker.images.history(name=image_name)
    assert history


@pytest.mark.asyncio
async def test_list_images(docker: Docker, image_name: str) -> None:
    images = await docker.images.list(filter=image_name)
    assert len(images) >= 1


@pytest.mark.asyncio
async def test_tag_image(docker: Docker, random_name: str, image_name: str) -> None:
    repository = random_name
    await docker.images.tag(name=image_name, repo=repository, tag="1.0")
    await docker.images.tag(name=image_name, repo=repository, tag="2.0")
    try:
        image = await docker.images.inspect(image_name)
        assert len([x for x in image["RepoTags"] if x.startswith(repository)]) == 2
    finally:
        await docker.images.delete(f"{repository}:1.0")
        await docker.images.delete(f"{repository}:2.0")


@pytest.mark.asyncio
async def test_push_image(
    docker: Docker,
    plain_registry: TempContainer,
    image_name: str,
) -> None:
    registry_addr = f"{plain_registry.get_container_host_ip()}:{plain_registry.get_exposed_port(5000)}"
    repository = f"{registry_addr}/image-{secrets.token_hex(4)}"
    await docker.images.tag(name=image_name, repo=repository)  # tag is set to "latest"
    try:
        await docker.images.push(name=repository)
    finally:
        await docker.images.delete(f"{repository}:latest")


@pytest.mark.asyncio
async def test_push_image_stream(
    docker: Docker,
    plain_registry: TempContainer,
    image_name: str,
) -> None:
    registry_addr = f"{plain_registry.get_container_host_ip()}:{plain_registry.get_exposed_port(5000)}"
    repository = f"{registry_addr}/image-{secrets.token_hex(4)}"
    await docker.images.tag(name=image_name, repo=repository)
    try:
        async for item in docker.images.push(name=repository, stream=True):
            pass
    finally:
        await docker.images.delete(f"{repository}:latest")


@pytest.mark.asyncio
async def test_delete_image(
    docker: Docker,
    plain_registry: TempContainer,
    image_name: str,
) -> None:
    registry_addr = f"{plain_registry.get_container_host_ip()}:{plain_registry.get_exposed_port(5000)}"
    repository = f"{registry_addr}/image-{secrets.token_hex(4)}"
    await docker.images.tag(name=image_name, repo=repository)
    try:
        assert await docker.images.inspect(repository)
    finally:
        await docker.images.delete(name=repository)


@pytest.mark.asyncio
async def test_not_existing_image(docker: Docker, random_name: str) -> None:
    name = f"xxxx-{random_name}:latest"
    with pytest.raises(DockerError) as excinfo:
        await docker.images.inspect(name=name)
    assert excinfo.value.status == 404


@pytest.mark.asyncio
async def test_pull_image(docker: Docker, image_name: str) -> None:
    image = await docker.images.inspect(name=image_name)
    assert image

    with pytest.warns(DeprecationWarning):
        image = await docker.images.get(name=image_name)
        assert "Architecture" in image


@pytest.mark.asyncio
async def test_pull_image_stream(docker: Docker, image_name: str) -> None:
    image = await docker.images.inspect(name=image_name)
    assert image

    async for item in docker.images.pull(image_name, stream=True):
        pass


@pytest.mark.asyncio
async def test_build_from_tar(
    docker: Docker, random_name: str, image_name: str
) -> None:
    name = f"{random_name}:latest"
    dockerfile = f"""
    # Shared Volume
    FROM {image_name}
    """
    f = BytesIO(dockerfile.encode("utf-8"))
    tar_obj = utils.mktar_from_dockerfile(f)
    await docker.images.build(fileobj=tar_obj, encoding="gzip", tag=name)
    tar_obj.close()
    try:
        image = await docker.images.inspect(name=name)
        assert image
    finally:
        await docker.images.delete(name=name)


@pytest.mark.asyncio
async def test_build_from_tar_stream(
    docker: Docker, random_name: str, image_name: str
) -> None:
    name = f"{random_name}:latest"
    dockerfile = f"""
    # Shared Volume
    FROM {image_name}
    """
    f = BytesIO(dockerfile.encode("utf-8"))
    tar_obj = utils.mktar_from_dockerfile(f)
    async for item in docker.images.build(
        fileobj=tar_obj, encoding="gzip", tag=name, stream=True
    ):
        pass
    tar_obj.close()
    try:
        image = await docker.images.inspect(name=name)
        assert image
    finally:
        await docker.images.delete(name=name)


@pytest.mark.asyncio
async def test_export_image(docker: Docker, image_name: str) -> None:
    name = image_name
    async with docker.images.export_image(name=name) as exported_image:
        assert exported_image
        async for chunk in exported_image.iter_chunks():
            pass


@pytest.mark.asyncio
async def test_import_image(docker: Docker) -> None:
    skip_windows()

    async def file_sender(file_name: str) -> AsyncIterator[bytes]:
        with open(file_name, "rb") as f:
            chunk = f.read(2**16)
            while chunk:
                yield chunk
                chunk = f.read(2**16)

    dir = os.path.dirname(__file__)
    hello_world = os.path.join(dir, "docker/google-containers-pause.tar")
    # FIXME: improve annotation for chunked data generator
    response = await docker.images.import_image(data=file_sender(hello_world))  # type: ignore
    for item in response:
        assert "error" not in item

    repository = "gcr.io/google-containers/pause"

    tags = ["1.0", "go", "latest", "test", "test2"]
    try:
        for tag in tags:
            name = f"{repository}:{tag}"
            image = await docker.images.inspect(name=name)
            assert image
    finally:
        # Cleanup imported images
        for tag in tags:
            name = f"{repository}:{tag}"
            try:
                await docker.images.delete(name=name)
            except DockerError:
                pass  # Image might already be deleted


@pytest.mark.asyncio
async def test_pups_image_auth(
    docker: Docker,
    secure_registry: TempContainer,
    image_name: str,
) -> None:
    skip_windows()

    name = image_name
    await docker.images.pull(from_image=name)

    registry_addr = f"{secure_registry.get_container_host_ip()}:{secure_registry.get_exposed_port(5001)}"
    repository = f"{registry_addr}/image:latest"
    image, tag = repository.rsplit(":", 1)
    registry_addr, image_name = image.split("/", 1)
    await docker.images.tag(name=name, repo=image, tag=tag)

    auth_config = {"username": "testuser", "password": "testpassword"}

    await docker.images.push(name=repository, tag=tag, auth=auth_config)

    await docker.images.delete(name=repository)
    await docker.images.pull(repository, auth={"auth": "dGVzdHVzZXI6dGVzdHBhc3N3b3Jk"})

    await docker.images.inspect(repository)
    await docker.images.delete(name=repository)

    # Now compose_auth_header automatically parse and rebuild
    # the encoded value if required.
    await docker.pull(repository, auth="dGVzdHVzZXI6dGVzdHBhc3N3b3Jk")
    with pytest.raises(ValueError):
        # The repository arg must include the registry address.
        await docker.pull("image:latest", auth={"auth": "dGVzdHVzZXI6dGVzdHBhc3N3b3Jk"})
    await docker.pull(repository, auth={"auth": "dGVzdHVzZXI6dGVzdHBhc3N3b3Jk"})
    await docker.images.inspect(repository)


@pytest.mark.asyncio
async def test_build_image_invalid_platform(docker: Docker, image_name: str) -> None:
    dockerfile = f"""
        FROM {image_name}
        """
    f = BytesIO(dockerfile.encode("utf-8"))
    tar_obj = utils.mktar_from_dockerfile(f)
    with pytest.raises(DockerError) as excinfo:
        async for item in docker.images.build(
            fileobj=tar_obj, encoding="gzip", stream=True, platform="foo"
        ):
            pass
    tar_obj.close()
    assert excinfo.value.status == 400
    assert (
        "unknown operating system or architecture: invalid argument"
        in excinfo.exconly()
    )


@pytest.mark.asyncio
async def test_pull_image_invalid_platform(docker: Docker, image_name: str) -> None:
    with pytest.raises(DockerError) as excinfo:
        await docker.images.pull("hello-world", platform="foo")

    assert excinfo.value.status == 400
    assert (
        "unknown operating system or architecture: invalid argument"
        in excinfo.exconly()
    )


@pytest.mark.asyncio
async def test_prune_images(docker: Docker, random_name: str, image_name: str) -> None:
    # Build an image to create a dangling image
    name = f"{random_name}:latest"
    dockerfile = f"""
    FROM {image_name}
    """
    f = BytesIO(dockerfile.encode("utf-8"))
    tar_obj = utils.mktar_from_dockerfile(f)
    await docker.images.build(fileobj=tar_obj, encoding="gzip", tag=name)
    tar_obj.close()

    # Remove the tag to create a dangling image
    await docker.images.delete(name=name)

    # Prune dangling images
    result = await docker.images.prune(filters={"dangling": ["true"]})

    # Verify the response structure
    assert isinstance(result, dict)
    assert "ImagesDeleted" in result
    assert "SpaceReclaimed" in result
    assert isinstance(result["ImagesDeleted"], list)
    assert isinstance(result["SpaceReclaimed"], int)


@pytest.mark.asyncio
async def test_prune_images_without_filters(docker: Docker) -> None:
    # Prune without filters
    result = await docker.images.prune()

    # Verify the response structure
    assert isinstance(result, dict)
    assert "ImagesDeleted" in result
    assert "SpaceReclaimed" in result
    assert result["ImagesDeleted"] is None
    assert isinstance(result["SpaceReclaimed"], int)


@pytest.mark.asyncio
async def test_prune_builds_with_options(docker: Docker, random_name: str) -> None:
    """Test builds prune with options set."""
    result = await docker.images.prune_builds(
        reserved_space=0,
        max_used_space=10**9,
        min_free_space=0,
        all_builds=True,
        filters={"id": random_name},
    )

    # Verify the response structure
    assert isinstance(result, dict)
    assert "CachesDeleted" in result
    assert "SpaceReclaimed" in result
    assert result["CachesDeleted"] is None
    assert isinstance(result["SpaceReclaimed"], int)


@pytest.mark.asyncio
async def test_prune_builds_default_options(docker: Docker) -> None:
    """Test builds prune using default options."""
    result = await docker.images.prune_builds()

    # Verify the response structure
    assert isinstance(result, dict)
    assert "CachesDeleted" in result
    assert "SpaceReclaimed" in result
    assert result["CachesDeleted"] is None
    assert isinstance(result["SpaceReclaimed"], int)
