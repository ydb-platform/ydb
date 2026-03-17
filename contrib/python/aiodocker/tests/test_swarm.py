import sys

import pytest

from aiodocker.exceptions import DockerError


@pytest.mark.asyncio
async def test_swarm_inspect(swarm):
    swarm_info = await swarm.swarm.inspect()
    assert "ID" in swarm_info
    assert "Spec" in swarm_info


@pytest.mark.asyncio
async def test_swarm_failing_joining(swarm):
    swarm_info = await swarm.swarm.inspect()
    system_info = await swarm.system.info()
    swarm_addr = [system_info["Swarm"]["RemoteManagers"][-1]["Addr"]]
    token = swarm_info["JoinTokens"]["Worker"]
    with pytest.raises(DockerError):
        await swarm.swarm.join(join_token=token, remote_addrs=swarm_addr)


@pytest.mark.asyncio
async def test_swarm_init(dind_docker_host, testing_images):
    if sys.platform == "win32":
        pytest.skip("swarm commands dont work on Windows")

    # Create a separate DinD client for this test
    from aiodocker.docker import Docker

    docker = Docker(url=dind_docker_host)
    try:
        default_addr_pool = ["10.0.0.0/8"]
        data_path_port = 4789
        subnet_size = 24
        assert await docker.swarm.init(
            advertise_addr="127.0.0.1:2377",
            data_path_port=data_path_port,
            default_addr_pool=default_addr_pool,
            subnet_size=subnet_size,
            force_new_cluster=True,
        )
        swarm_inspect = await docker.swarm.inspect()
        assert swarm_inspect["DefaultAddrPool"] == default_addr_pool
        assert swarm_inspect["DataPathPort"] == data_path_port
        assert swarm_inspect["SubnetSize"] == subnet_size
        await docker.swarm.leave(force=True)
    finally:
        await docker.close()
