import asyncio

import pytest

from . import classes, service


def pytest_addoption(parser):
    group = parser.getgroup('rabbitmq')
    group.addoption('--rabbitmq')
    group.addoption(
        '--no-rabbitmq',
        help='Disable use of RabbitMQ',
        action='store_true',
    )


def pytest_configure(config):
    config.addinivalue_line(
        'markers',
        'rabbitmq: per-test RabbitMQ initialization',
    )


def pytest_service_register(register_service):
    register_service('rabbitmq', service.create_rabbitmq_service)


@pytest.fixture
def rabbitmq(_rabbitmq_connection) -> classes.Control:
    return _rabbitmq_connection


@pytest.fixture(scope='session')
async def _rabbitmq_connection(
    _rabbitmq_service,
    _rabbitmq_service_settings,
) -> classes.Control:
    event_loop = asyncio.get_running_loop()
    control = classes.Control(
        enabled=_rabbitmq_service,
        conn_info=_rabbitmq_service_settings.get_connection_info(),
    )
    yield control
    await control.teardown()


@pytest.fixture(scope='session')
def rabbitmq_disabled(pytestconfig) -> bool:
    return pytestconfig.option.no_rabbitmq


@pytest.fixture(scope='session')
def _rabbitmq_service_settings() -> service.ServiceSettings:
    return service.get_service_settings()


@pytest.fixture(scope='session')
def _rabbitmq_service(
    ensure_service_started,
    rabbitmq_disabled,
    pytestconfig,
    _rabbitmq_service_settings,
):
    if rabbitmq_disabled:
        return False
    if not pytestconfig.option.rabbitmq:
        ensure_service_started('rabbitmq', settings=_rabbitmq_service_settings)
    return True
