import asyncio

import pytest


def pytest_configure(config):
    # Force default asyncio mode
    config.option.asyncio_mode = 'auto'


@pytest.fixture(scope='session')
def event_loop():
    """
    Overrides pytest-asyncio internal `event_loop` fixture. Should not be
    used explicitly.

    Required for compatibility with pytest-asyncio 0.21.x
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()
