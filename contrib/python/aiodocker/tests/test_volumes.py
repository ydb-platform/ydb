from contextlib import suppress

import pytest

from aiodocker.docker import Docker
from aiodocker.exceptions import DockerError
from aiodocker.volumes import DockerVolume


@pytest.mark.asyncio
async def test_create_search_get_delete(docker: Docker) -> None:
    name = "aiodocker-test-volume-two"
    await docker.volumes.create({
        "Name": name,
        "Labels": {"some": "label"},
        "Driver": "local",
    })
    volumes_response = await docker.volumes.list(filters={"label": "some=label"})
    volumes = volumes_response["Volumes"]
    assert len(volumes) == 1
    volume_data = volumes[0]
    volume = await docker.volumes.get(volume_data["Name"])
    await volume.delete()
    with pytest.raises(DockerError):
        await docker.volumes.get(name)


@pytest.mark.asyncio
@pytest.mark.parametrize("force_delete", [True, False])
async def test_create_show_delete_volume(docker: Docker, force_delete: bool) -> None:
    name = "aiodocker-test-volume"
    volume = await docker.volumes.create({
        "Name": name,
        "Labels": {},
        "Driver": "local",
    })
    assert volume
    data = await volume.show()
    assert data
    await volume.delete(force_delete)
    with pytest.raises(DockerError):
        await docker.volumes.get(name)


@pytest.mark.asyncio
async def test_prune_volumes(docker: Docker) -> None:
    """Test that prune with filters removes only the volume that matches the filter."""
    # Create two volumes
    volume_without_label = await docker.volumes.create({
        "Labels": {},
        "Driver": "local",
    })
    volume_with_label: DockerVolume | None = None
    try:
        volume_with_label = await docker.volumes.create({
            "Labels": {"test": ""},
            "Driver": "local",
        })

        # Prune volumes with label "test"
        result = await docker.volumes.prune(filters={"label": "test"})

        # Verify the response structure
        assert isinstance(result, dict)
        assert "VolumesDeleted" in result
        assert "SpaceReclaimed" in result
        assert result["VolumesDeleted"] == [volume_with_label.name]
        assert isinstance(result["SpaceReclaimed"], int)

        # Test that the volume without the label still exists
        assert await volume_without_label.show()

    finally:
        with suppress(DockerError):
            await volume_without_label.delete()
        if volume_with_label:
            with suppress(DockerError):
                await volume_with_label.delete()


@pytest.mark.asyncio
async def test_prune_volumes_nothing_to_remove(docker: Docker) -> None:
    """Test a volumes prune with nothing to remove."""
    result = await docker.volumes.prune()

    # Verify the response structure
    assert isinstance(result, dict)
    assert "VolumesDeleted" in result
    assert "SpaceReclaimed" in result
    assert result["VolumesDeleted"] == []
    assert isinstance(result["SpaceReclaimed"], int)
