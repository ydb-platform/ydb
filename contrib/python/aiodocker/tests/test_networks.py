from __future__ import annotations

from contextlib import suppress

import pytest

from aiodocker.docker import Docker
from aiodocker.exceptions import DockerError
from aiodocker.networks import DockerNetwork


@pytest.mark.asyncio
async def test_list_networks(docker: Docker) -> None:
    data = await docker.networks.list()
    networks = {net["Name"]: net for net in data}
    assert "none" in networks
    assert networks["none"]["Driver"] == "null"


@pytest.mark.asyncio
async def test_list_networks_with_filter(docker: Docker) -> None:
    network = await docker.networks.create({
        "Name": "test-net-filter",
        "Labels": {"some": "label"},
    })
    try:
        networks = await docker.networks.list(filters={"label": "some=label"})
        assert len(networks) == 1
    finally:
        if network:
            deleted = await network.delete()
            assert deleted is True


@pytest.mark.asyncio
async def test_networks(docker: Docker) -> None:
    network = await docker.networks.create({"Name": "test-net"})
    net_find = await docker.networks.get("test-net")
    assert (await net_find.show())["Name"] == "test-net"
    assert isinstance(network, DockerNetwork)
    data = await network.show()
    assert data["Name"] == "test-net"
    container = None
    try:
        container = await docker.containers.create({"Image": "python"}, name="test-net")
        await network.connect({"Container": "test-net"})
        await network.disconnect({"Container": "test-net"})
    finally:
        if container is not None:
            await container.delete()
        network_delete_result = await network.delete()
        assert network_delete_result is True


@pytest.mark.asyncio
async def test_network_delete_error(docker: Docker) -> None:
    network = await docker.networks.create({"Name": "test-delete-net"})
    assert await network.delete() is True
    with pytest.raises(DockerError):
        await network.delete()


@pytest.mark.asyncio
async def test_prune_networks(docker: Docker, random_name: str) -> None:
    """Test that prune with filters removes only the unused network that matches the filter."""
    # Create two networks
    network_without_label: DockerNetwork = await docker.networks.create({
        "Name": f"{random_name}_no_label"
    })
    network_with_label: DockerNetwork | None = None
    try:
        network_name = f"{random_name}_label"
        network_with_label = await docker.networks.create({
            "Name": network_name,
            "Labels": {"test": ""},
        })

        # Prune unused networks with label "test"
        result = await docker.networks.prune(filters={"label": "test"})

        # Verify the response structure
        assert isinstance(result, dict)
        assert "NetworksDeleted" in result
        assert result["NetworksDeleted"] == [network_name]

        # Test that the network without the label still exists
        assert await network_without_label.show()

    finally:
        with suppress(DockerError):
            await network_without_label.delete()
        if network_with_label:
            with suppress(DockerError):
                await network_with_label.delete()


@pytest.mark.asyncio
async def test_prune_networks_nothing_to_remove(docker: Docker) -> None:
    """Test a network prune with nothing to remove."""
    result = await docker.networks.prune()

    # Verify the response structure
    assert isinstance(result, dict)
    assert "NetworksDeleted" in result
    assert result["NetworksDeleted"] is None
