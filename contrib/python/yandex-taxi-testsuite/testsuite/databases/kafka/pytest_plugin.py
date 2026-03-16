import contextlib
import typing

import pytest

from . import classes, service


def pytest_addoption(parser):
    group = parser.getgroup('kafka')
    group.addoption('--kafka')
    group.addoption(
        '--no-kafka',
        help='Disable use of Kafka',
        action='store_true',
    )


def pytest_configure(config):
    config.addinivalue_line(
        'markers',
        'kafka: per-test Kafka initialization',
    )


def pytest_service_register(register_service):
    register_service('kafka', service.create_kafka_service)


@pytest.fixture(scope='session')
async def _kafka_global_producer(
    _kafka_service,
    _bootstrap_servers,
) -> typing.AsyncGenerator[classes.KafkaProducer, None]:
    producer = classes.KafkaProducer(
        enabled=_kafka_service,
        bootstrap_servers=_bootstrap_servers,
    )
    await producer.start()

    async with contextlib.aclosing(producer):
        yield producer


@pytest.fixture
async def kafka_producer(
    _kafka_global_producer,
) -> typing.AsyncGenerator[classes.KafkaProducer, None]:
    """
    Per test Kafka producer instance.

    :returns: :py:class:`testsuite.databases.kafka.classes.KafkaProducer`
    """
    yield _kafka_global_producer
    await _kafka_global_producer._flush()


@pytest.fixture(scope='session')
async def _kafka_global_consumer(
    _kafka_service,
    _bootstrap_servers,
) -> typing.AsyncGenerator[classes.KafkaConsumer, None]:
    consumer = classes.KafkaConsumer(
        enabled=_kafka_service,
        bootstrap_servers=_bootstrap_servers,
    )
    await consumer.start()

    async with contextlib.aclosing(consumer):
        yield consumer


@pytest.fixture
async def kafka_consumer(
    _kafka_global_consumer,
) -> typing.AsyncGenerator[classes.KafkaConsumer, None]:
    """
    Per test Kafka consumer instance.

    :returns: :py:class:`testsuite.databases.kafka.classes.KafkaConsumer`
    """
    yield _kafka_global_consumer
    await _kafka_global_consumer._unsubscribe()


@pytest.fixture(scope='session')
def kafka_custom_topics() -> dict[str, int]:
    """
    Redefine this fixture to pass your custom dictionary of topics' settings.
    """

    return service.try_get_custom_topics()


@pytest.fixture(scope='session')
def kafka_local() -> classes.BootstrapServers:
    """
    Override to use custom local cluster bootstrap servers.
    If not empty, no service started.
    """

    return []


@pytest.fixture(scope='session')
def kafka_disabled(pytestconfig) -> bool:
    return pytestconfig.option.no_kafka


@pytest.fixture(scope='session')
def _kafka_service_settings(kafka_custom_topics) -> classes.ServiceSettings:
    return service.get_service_settings(kafka_custom_topics)


@pytest.fixture(scope='session')
def _bootstrap_servers(kafka_local, _kafka_service_settings) -> str:
    if kafka_local:
        return ','.join(kafka_local)

    server_host = _kafka_service_settings.server_host
    server_port = _kafka_service_settings.server_port
    return f'{server_host}:{server_port}'


@pytest.fixture(scope='session')
def _kafka_service(
    ensure_service_started,
    kafka_local,
    kafka_disabled,
    pytestconfig,
    _kafka_service_settings,
) -> bool:
    if kafka_disabled:
        return False
    if not kafka_local and not pytestconfig.option.kafka:
        ensure_service_started('kafka', settings=_kafka_service_settings)
    return True
