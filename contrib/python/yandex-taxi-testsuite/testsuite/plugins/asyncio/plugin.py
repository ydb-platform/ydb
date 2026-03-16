import pytest


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config):
    # Force default asyncio mode
    config.option.asyncio_mode = 'auto'
    # Force fixtures to use session loop
    config._inicache['asyncio_default_fixture_loop_scope'] = 'session'


def pytest_collection_modifyitems(items):
    """Force tests to use session asyncio loop."""
    for item in items:
        mark = item.get_closest_marker('asyncio')
        if mark:
            mark.kwargs.setdefault('loop_scope', 'session')
