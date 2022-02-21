import asyncio
from unittest.mock import MagicMock

from ydb.aio.driver import Driver
import pytest
import ydb


@pytest.mark.asyncio
async def test_async_call(endpoint, database):
    driver_config = ydb.DriverConfig(
        endpoint,
        database,
        credentials=ydb.construct_credentials_from_environ(),
        root_certificates=ydb.load_ydb_root_certificate(),
    )

    driver = Driver(driver_config=driver_config)

    await driver.scheme_client.make_directory("/local/lol")
    await driver.stop()


@pytest.mark.asyncio
async def test_gzip_compression(endpoint, database):
    driver_config = ydb.DriverConfig(
        endpoint,
        database,
        credentials=ydb.construct_credentials_from_environ(),
        root_certificates=ydb.load_ydb_root_certificate(),
        compression=ydb.RPCCompression.Gzip,
    )

    driver = Driver(driver_config=driver_config)

    await driver.scheme_client.make_directory(
        "/local/lol",
        settings=ydb.BaseRequestSettings().with_compression(ydb.RPCCompression.Deflate),
    )
    await driver.stop()


@pytest.mark.asyncio
async def test_other_credentials(endpoint, database):
    driver = Driver(endpoint=endpoint, database=database)

    await driver.scheme_client.make_directory("/local/lol")
    await driver.stop()


@pytest.mark.asyncio
async def test_session(endpoint, database):
    driver_config = ydb.DriverConfig(
        endpoint,
        database,
        credentials=ydb.construct_credentials_from_environ(),
        root_certificates=ydb.load_ydb_root_certificate(),
    )

    driver = Driver(driver_config=driver_config)

    await driver.wait(timeout=10)

    description = (
        ydb.TableDescription()
        .with_primary_keys("key1", "key2")
        .with_columns(
            ydb.Column("key1", ydb.OptionalType(ydb.PrimitiveType.Uint64)),
            ydb.Column("key2", ydb.OptionalType(ydb.PrimitiveType.Uint64)),
            ydb.Column("value", ydb.OptionalType(ydb.PrimitiveType.Utf8)),
        )
        .with_profile(
            ydb.TableProfile().with_partitioning_policy(
                ydb.PartitioningPolicy().with_explicit_partitions(
                    ydb.ExplicitPartitions(
                        (
                            ydb.KeyBound((100,)),
                            ydb.KeyBound((300, 100)),
                            ydb.KeyBound((400,)),
                        )
                    )
                )
            )
        )
    )

    session = await driver.table_client.session().create()
    await session.create_table(database + "/test_session", description)

    response = await session.describe_table(database + "/test_session")
    assert [c.name for c in response.columns] == ["key1", "key2", "value"]
    await driver.stop()


@pytest.mark.asyncio
async def test_raises_when_disconnect(endpoint, database, docker_project):

    driver_config = ydb.DriverConfig(
        endpoint,
        database,
        credentials=ydb.construct_credentials_from_environ(),
        root_certificates=ydb.load_ydb_root_certificate(),
    )

    driver = Driver(driver_config=driver_config)

    await driver.wait(timeout=10)

    async def restart_docker():
        docker_project.stop()

    coros = [driver.scheme_client.describe_path("/local") for i in range(100)]
    coros.append(restart_docker())

    with pytest.raises(ydb.ConnectionLost):
        await asyncio.gather(*coros, return_exceptions=False)

    docker_project.start()
    await driver.stop()


@pytest.mark.asyncio
async def test_disconnect_by_call(endpoint, database, docker_project):
    driver_config = ydb.DriverConfig(
        endpoint,
        database,
        credentials=ydb.construct_credentials_from_environ(),
        root_certificates=ydb.load_ydb_root_certificate(),
    )

    driver = Driver(driver_config=driver_config)

    await driver.wait(timeout=10)

    docker_project.stop()

    try:
        await driver.scheme_client.make_directory("/local/lol")
    except Exception:
        pass

    await asyncio.sleep(5)
    assert len(driver._store.connections) == 0
    docker_project.start()
    await driver.stop()


@pytest.mark.asyncio
async def test_good_discovery_interval(driver):
    await driver.wait(timeout=10)
    mock = MagicMock(return_value=0.1)
    driver._discovery._discovery_interval = mock
    driver._discovery._wake_up_event.set()
    await asyncio.sleep(1)
    assert mock.called
