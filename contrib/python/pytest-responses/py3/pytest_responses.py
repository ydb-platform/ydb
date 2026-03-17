from packaging.version import Version

import pytest

import responses as responses_


def get_withoutresponses_marker(item):
    if Version(pytest.__version__) >= Version('4.0.0'):
        return item.get_closest_marker('withoutresponses')
    else:
        return item.get_marker('withoutresponses')


# pytest plugin support
def pytest_configure(config):
    config.addinivalue_line(
        'markers',
        'withoutresponses: Tests which need access to external domains.'
    )


def pytest_runtest_setup(item):
    if not get_withoutresponses_marker(item):
        responses_.start()


def pytest_runtest_teardown(item):
    if not get_withoutresponses_marker(item):
        try:
            responses_.stop()
            responses_.reset()
        except (AttributeError, RuntimeError):
            # patcher was already uninstalled (or not installed at all) and
            # responses doesnt let us force maintain it
            pass


@pytest.yield_fixture
def responses():
    with responses_.RequestsMock() as rsps:
        yield rsps
