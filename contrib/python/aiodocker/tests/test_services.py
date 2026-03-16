import asyncio

import pytest
from async_timeout import timeout


TaskTemplate = {"ContainerSpec": {"Image": "python"}}


async def _wait_service(swarm, service_id):
    for i in range(5):
        tasks = await swarm.tasks.list(filters={"service": service_id})
        if tasks:
            return
        await asyncio.sleep(0.2)
    raise RuntimeError("Waited service %s for too long" % service_id)


@pytest.fixture
async def tmp_service(swarm, random_name):
    service = await swarm.services.create(task_template=TaskTemplate, name=random_name)
    await _wait_service(swarm, service["ID"])
    yield service["ID"]
    await swarm.services.delete(service["ID"])


@pytest.mark.asyncio
async def test_service_list_with_filter(swarm, tmp_service):
    docker_service = await swarm.services.inspect(service_id=tmp_service)
    name = docker_service["Spec"]["Name"]
    filters = {"name": name}
    filtered_list = await swarm.services.list(filters=filters)
    assert len(filtered_list) == 1


@pytest.mark.asyncio
async def test_service_tasks_list(swarm, tmp_service):
    tasks = await swarm.tasks.list()
    assert len(tasks) == 1
    assert tasks[0]["ServiceID"] == tmp_service
    assert await swarm.tasks.inspect(tasks[0]["ID"])


@pytest.mark.asyncio
async def test_service_tasks_list_with_filters(swarm, tmp_service):
    tasks = await swarm.tasks.list(filters={"service": tmp_service})
    assert len(tasks) == 1
    assert tasks[0]["ServiceID"] == tmp_service
    assert await swarm.tasks.inspect(tasks[0]["ID"])


@pytest.mark.asyncio
async def test_logs_services(swarm, image_name):
    TaskTemplate = {
        "ContainerSpec": {
            "Image": image_name,
            "Args": ["python", "-c", "for _ in range(10): print('Hello Python')"],
        },
        "RestartPolicy": {"Condition": "none"},
    }
    service = await swarm.services.create(task_template=TaskTemplate)
    service_id = service["ID"]

    try:
        filters = {"service": service_id}

        # wait till task status is `complete`
        async with timeout(60):
            while True:
                await asyncio.sleep(2)
                task = await swarm.tasks.list(filters=filters)
                if task:
                    status = task[0]["Status"]["State"]
                    if status == "complete":
                        break

        logs = await swarm.services.logs(service_id, stdout=True)

        assert len(logs) == 10
        assert logs[0] == "Hello Python\n"
    finally:
        await swarm.services.delete(service_id)


@pytest.mark.asyncio
async def test_logs_services_stream(swarm, image_name):
    TaskTemplate = {
        "ContainerSpec": {
            "Image": image_name,
            "Args": ["python", "-c", "for _ in range(10): print('Hello Python')"],
        },
        "RestartPolicy": {"Condition": "none"},
    }
    service = await swarm.services.create(task_template=TaskTemplate)
    service_id = service["ID"]

    try:
        filters = {"service": service_id}

        # wait till task status is `complete`
        async with timeout(60):
            while True:
                await asyncio.sleep(2)
                task = await swarm.tasks.list(filters=filters)
                if task:
                    status = task[0]["Status"]["State"]
                    if status == "complete":
                        break

        # the service printed 10 `Hello Python`
        # let's check for them
        count = 0
        try:
            async with timeout(2):
                while True:
                    async for log in swarm.services.logs(
                        service_id, stdout=True, follow=True
                    ):
                        if "Hello Python\n" in log:
                            count += 1
        except asyncio.TimeoutError:
            pass

        assert count == 10
    finally:
        await swarm.services.delete(service_id)


@pytest.mark.asyncio
async def test_service_update(swarm, image_name, image_name_updated):
    name = "service-update"
    initial_image = image_name
    image_after_update = image_name_updated
    TaskTemplate = {"ContainerSpec": {"Image": initial_image}}

    await swarm.services.create(name=name, task_template=TaskTemplate)
    service = await swarm.services.inspect(name)
    current_image = service["Spec"]["TaskTemplate"]["ContainerSpec"]["Image"]
    version = service["Version"]["Index"]
    assert initial_image in current_image

    # update the service
    await swarm.services.update(
        service_id=name, version=version, image=image_after_update
    )
    # wait a few to complete the update of the service
    await asyncio.sleep(1)

    service = await swarm.services.inspect(name)
    current_image = service["Spec"]["TaskTemplate"]["ContainerSpec"]["Image"]
    version = service["Version"]["Index"]
    assert image_after_update in current_image

    # rollback to the previous one
    await swarm.services.update(service_id=name, version=version, rollback=True)
    service = await swarm.services.inspect(name)
    current_image = service["Spec"]["TaskTemplate"]["ContainerSpec"]["Image"]
    assert initial_image in current_image

    await swarm.services.delete(name)


@pytest.mark.asyncio
async def test_service_update_error(swarm):
    name = "service-update"
    TaskTemplate = {"ContainerSpec": {"Image": "python:3.6.1"}}
    await swarm.services.create(name=name, task_template=TaskTemplate)
    await asyncio.sleep(1)
    service = await swarm.services.inspect(name)
    version = service["Version"]["Index"]

    with pytest.raises(ValueError):
        await swarm.services.update(service_id=name, version=version)

    await swarm.services.delete(name)


@pytest.mark.asyncio
async def test_service_create_error(swarm):
    name = "service-test-error"
    TaskTemplate = {"ContainerSpec": {}}
    with pytest.raises(KeyError):
        await swarm.services.create(name=name, task_template=TaskTemplate)


@pytest.mark.asyncio
async def test_service_create_service_with_auth(swarm):
    name = "service-test-with-auth"
    TaskTemplate = {"ContainerSpec": {"Image": "python"}}
    service = await swarm.services.create(
        name=name,
        task_template=TaskTemplate,
        auth={"username": "username", "password": "password"},
        registry="random.registry.com",
    )
    assert service
    try:
        await _wait_service(swarm, service["ID"])
    finally:
        await swarm.services.delete(service["ID"])


@pytest.mark.asyncio
async def test_service_create_error_for_missing_registry(swarm):
    name = "service-test-error-with-auth"
    TaskTemplate = {"ContainerSpec": {"Image": "python"}}
    with pytest.raises(KeyError):
        await swarm.services.create(
            name=name,
            task_template=TaskTemplate,
            auth={"username": "username", "password": "password"},
        )
