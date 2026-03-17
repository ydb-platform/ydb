import pytest

from aiodocker.exceptions import DockerError


@pytest.mark.asyncio
async def test_nodes_list(swarm):
    swarm_nodes = await swarm.nodes.list()
    assert len(swarm_nodes) == 1


@pytest.mark.asyncio
async def test_nodes_list_with_filter(swarm):
    filters = {"role": "manager"}
    filtered_nodes = await swarm.nodes.list(filters=filters)
    assert len(filtered_nodes) == 1

    filters = {"role": "worker"}
    filtered_nodes = await swarm.nodes.list(filters=filters)
    assert len(filtered_nodes) == 0


@pytest.mark.asyncio
async def test_node_inspect(swarm):
    swarm_nodes = await swarm.nodes.list()
    node_id = swarm_nodes[0]["ID"]
    hostname = swarm_nodes[0]["Description"]["Hostname"]

    node = await swarm.nodes.inspect(node_id=hostname)
    assert node_id in node["ID"]

    node = await swarm.nodes.inspect(node_id=node_id)
    assert hostname in node["Description"]["Hostname"]


@pytest.mark.asyncio
async def test_node_remove(swarm):
    swarm_nodes = await swarm.nodes.list()
    node_id = swarm_nodes[0]["ID"]

    with pytest.raises(DockerError) as err_info:
        await swarm.nodes.remove(node_id=node_id)

    assert "is a cluster manager and is a member" in str(err_info.value)


@pytest.mark.asyncio
async def test_node_update(swarm):
    swarm_nodes = await swarm.nodes.list()
    node_id, version = swarm_nodes[0]["ID"], swarm_nodes[0]["Version"]["Index"]

    spec = {
        "Availability": "active",
        "Name": "special-node",
        "Role": "manager",
        "Labels": {"new_label": "true"},
    }

    await swarm.nodes.update(node_id=node_id, version=version, spec=spec)
    node = await swarm.nodes.inspect(node_id=node_id)
    assert node["Spec"] == spec
