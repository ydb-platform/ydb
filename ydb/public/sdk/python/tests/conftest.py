import os
import pytest
import ydb
import time


@pytest.fixture(scope="module")
def docker_compose_file(pytestconfig):
    return os.path.join(str(pytestconfig.rootdir), "docker-compose.yml")


def wait_container_ready(driver):
    driver.wait(timeout=10)

    with ydb.SessionPool(driver) as pool:

        started_at = time.time()
        while time.time() - started_at < 30:
            try:
                with pool.checkout() as session:
                    session.execute_scheme(
                        "create table `.sys_health/test_table` (A int32, primary key(A));"
                    )

                return True

            except ydb.Error:
                time.sleep(1)

    raise RuntimeError("Container is not ready after timeout.")


@pytest.fixture(scope="module")
def endpoint(pytestconfig, module_scoped_container_getter):
    with ydb.Driver(endpoint="localhost:2136", database="/local") as driver:
        wait_container_ready(driver)
    yield "localhost:2136"


@pytest.fixture(scope="session")
def secure_endpoint(pytestconfig, session_scoped_container_getter):
    ca_path = os.path.join(str(pytestconfig.rootdir), "ydb_certs/ca.pem")
    iterations = 0
    while not os.path.exists(ca_path) and iterations < 10:
        time.sleep(1)
        iterations += 1

    assert os.path.exists(ca_path)
    os.environ["YDB_SSL_ROOT_CERTIFICATES_FILE"] = ca_path
    with ydb.Driver(
        endpoint="grpcs://localhost:2135",
        database="/local",
        root_certificates=ydb.load_ydb_root_certificate(),
    ) as driver:
        wait_container_ready(driver)
    yield "localhost:2135"


@pytest.fixture(scope="module")
def database():
    return "/local"


@pytest.fixture()
async def aio_connection(endpoint, database):
    """A fixture to wait ydb start"""
    from ydb.aio.connection import Connection
    from ydb.driver import DriverConfig

    config = DriverConfig.default_from_endpoint_and_database(endpoint, database)
    connection = Connection(endpoint, config)
    await connection.connection_ready(ready_timeout=7)
    return connection


@pytest.fixture()
async def driver(endpoint, database, event_loop):
    driver_config = ydb.DriverConfig(
        endpoint,
        database,
        credentials=ydb.construct_credentials_from_environ(),
        root_certificates=ydb.load_ydb_root_certificate(),
    )

    driver = ydb.aio.Driver(driver_config=driver_config)
    await driver.wait(timeout=15)

    yield driver

    await driver.stop(timeout=10)
