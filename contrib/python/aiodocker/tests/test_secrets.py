import pytest

from aiodocker.exceptions import DockerError


@pytest.fixture
async def tmp_secret(swarm, random_name):
    secret = await swarm.secrets.create(name="secret-" + random_name, data=random_name)
    yield secret["ID"]
    await swarm.secrets.delete(secret["ID"])


@pytest.mark.asyncio
async def test_secret_list_with_filter(swarm, tmp_secret):
    docker_secret = await swarm.secrets.inspect(secret_id=tmp_secret)
    name = docker_secret["Spec"]["Name"]
    filters = {"name": name}
    filtered_list = await swarm.secrets.list(filters=filters)
    assert len(filtered_list) == 1


@pytest.mark.asyncio
async def test_secret_update(swarm, tmp_secret):
    secret = await swarm.secrets.inspect(secret_id=tmp_secret)
    secret_id = secret["ID"]

    secret = await swarm.secrets.inspect(secret_id)
    current_labels = secret["Spec"]["Labels"]
    assert current_labels == {}
    version = secret["Version"]["Index"]

    # update the secret labels
    await swarm.secrets.update(
        secret_id=secret_id, version=version, labels={"label1": "value1"}
    )

    secret = await swarm.secrets.inspect(secret_id)
    current_labels = secret["Spec"]["Labels"]
    version = secret["Version"]["Index"]
    assert current_labels == {"label1": "value1"}


@pytest.mark.asyncio
async def test_secret_labels(swarm, tmp_secret):
    secret = await swarm.secrets.inspect(secret_id=tmp_secret)
    secret_id1 = secret["ID"]
    secret = await swarm.secrets.inspect(secret_id1)
    version = secret["Version"]["Index"]

    await swarm.secrets.update(
        secret_id=secret_id1, version=version, labels={"label1": "value1"}
    )

    # create a secret with labels
    name = "test_secret2"
    secret = await swarm.secrets.create(
        name=name, data="test secret2", labels={"label2": "value2"}
    )
    secret_id2 = secret["ID"]
    secret = await swarm.secrets.inspect(secret_id2)
    version = secret["Version"]["Index"]
    current_labels = secret["Spec"]["Labels"]
    assert current_labels == {"label2": "value2"}

    # search secret based on labels
    filters = {"label": "label1=value1"}
    filtered_list = await swarm.secrets.list(filters=filters)
    assert len(filtered_list) == 1

    await swarm.secrets.update(
        secret_id=secret_id2, version=version, labels={"label1": "value1"}
    )
    secret = await swarm.secrets.inspect(secret_id2)
    version = secret["Version"]["Index"]
    current_labels = secret["Spec"]["Labels"]
    assert current_labels == {"label1": "value1"}

    filters = {"label": "label1=value1"}
    filtered_list = await swarm.secrets.list(filters=filters)
    assert len(filtered_list) == 2

    await swarm.secrets.delete(secret_id2)


@pytest.mark.asyncio
async def test_secret_update_error(swarm, tmp_secret):
    secret = await swarm.secrets.inspect(secret_id=tmp_secret)
    secret_id = secret["ID"]

    secret = await swarm.secrets.inspect(secret_id)
    version = secret["Version"]["Index"]

    with pytest.raises(DockerError) as error:
        await swarm.secrets.update(
            secret_id=secret_id, version=version, name="new name"
        )
    assert (
        error.value.message == "rpc error: "
        "code = InvalidArgument "
        "desc = only updates to Labels are allowed"
    )


@pytest.mark.asyncio
async def test_secret_create_nodata_error(swarm):
    name = "test_secret-create_nodata_error"
    with pytest.raises(TypeError):
        await swarm.secrets.create(name=name)


@pytest.mark.asyncio
async def test_secret_create_b64_error(swarm):
    name = "test_secret-create_b64_error"
    not_b64 = "I'm not base64 encoded"
    with pytest.raises(DockerError) as error:
        await swarm.secrets.create(name=name, data=not_b64, b64=True)
    assert "illegal base64 data at input byte 1" in error.value.message


@pytest.mark.asyncio
async def test_secret_create_duplicated_error(swarm, tmp_secret):
    secret = await swarm.secrets.inspect(secret_id=tmp_secret)
    name = secret["Spec"]["Name"]
    with pytest.raises(DockerError) as error:
        await swarm.secrets.create(name=name, data="test secret")
    assert (
        error.value.message == "rpc error: "
        "code = AlreadyExists "
        "desc = secret " + name + " already exists"
    )
