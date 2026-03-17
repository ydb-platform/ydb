import secrets

import pytest

from aiodocker.exceptions import DockerError


@pytest.fixture
async def tmp_config(swarm):
    random_name = f"aiodocker-{secrets.token_hex(4)}"
    config = await swarm.configs.create(name="config-" + random_name, data=random_name)
    yield config["ID"]
    await swarm.configs.delete(config["ID"])


@pytest.mark.asyncio
async def test_config_list_with_filter(swarm, tmp_config):
    docker_config = await swarm.configs.inspect(config_id=tmp_config)
    name = docker_config["Spec"]["Name"]
    filters = {"name": name}
    filtered_list = await swarm.configs.list(filters=filters)
    assert len(filtered_list) == 1


@pytest.mark.asyncio
async def test_config_update(swarm, tmp_config):
    config = await swarm.configs.inspect(config_id=tmp_config)
    config_id = config["ID"]

    config = await swarm.configs.inspect(config_id)
    current_labels = config["Spec"]["Labels"]
    assert current_labels == {}
    version = config["Version"]["Index"]

    # update the config labels
    await swarm.configs.update(
        config_id=config_id, version=version, labels={"label1": "value1"}
    )

    config = await swarm.configs.inspect(config_id)
    current_labels = config["Spec"]["Labels"]
    version = config["Version"]["Index"]
    assert current_labels == {"label1": "value1"}


@pytest.mark.asyncio
async def test_config_labels(swarm, tmp_config):
    config = await swarm.configs.inspect(config_id=tmp_config)
    config_id1 = config["ID"]
    config = await swarm.configs.inspect(config_id1)
    version = config["Version"]["Index"]

    await swarm.configs.update(
        config_id=config_id1, version=version, labels={"label1": "value1"}
    )

    # create a config with labels
    name = "test_config2"
    config = await swarm.configs.create(
        name=name, data="test config2", labels={"label2": "value2"}
    )
    config_id2 = config["ID"]
    config = await swarm.configs.inspect(config_id2)
    version = config["Version"]["Index"]
    current_labels = config["Spec"]["Labels"]
    assert current_labels == {"label2": "value2"}

    # search config based on labels
    filters = {"label": "label1=value1"}
    filtered_list = await swarm.configs.list(filters=filters)
    assert len(filtered_list) == 1

    await swarm.configs.update(
        config_id=config_id2, version=version, labels={"label1": "value1"}
    )
    config = await swarm.configs.inspect(config_id2)
    version = config["Version"]["Index"]
    current_labels = config["Spec"]["Labels"]
    assert current_labels == {"label1": "value1"}

    filters = {"label": "label1=value1"}
    filtered_list = await swarm.configs.list(filters=filters)
    assert len(filtered_list) == 2

    await swarm.configs.delete(config_id2)


@pytest.mark.asyncio
async def test_config_update_error(swarm, tmp_config):
    config = await swarm.configs.inspect(config_id=tmp_config)
    config_id = config["ID"]

    # await asyncio.sleep(1)
    config = await swarm.configs.inspect(config_id)
    version = config["Version"]["Index"]

    with pytest.raises(DockerError) as error:
        await swarm.configs.update(
            config_id=config_id, version=version, name="new name"
        )
    assert (
        error.value.message == "rpc error: "
        "code = InvalidArgument "
        "desc = only updates to Labels are allowed"
    )


@pytest.mark.asyncio
async def test_config_create_nodata_error(swarm):
    name = "test_config-create_nodata_error"
    with pytest.raises(TypeError):
        await swarm.configs.create(name=name)


@pytest.mark.asyncio
async def test_config_create_b64_error(swarm):
    name = "test_config-create_b64_error"
    not_b64 = "I'm not base64 encoded"
    with pytest.raises(DockerError) as error:
        await swarm.configs.create(name=name, data=not_b64, b64=True)
    assert "illegal base64 data at input byte 1" in error.value.message


@pytest.mark.asyncio
async def test_config_create_duplicated_error(swarm, tmp_config):
    config = await swarm.configs.inspect(config_id=tmp_config)
    name = config["Spec"]["Name"]
    with pytest.raises(DockerError) as error:
        await swarm.configs.create(name=name, data="test config")
    assert (
        error.value.message == "rpc error: "
        "code = AlreadyExists "
        "desc = config " + name + " already exists"
    )
