"""A pytest plugin for using pyfakefs as a fixture

When pyfakefs is installed, the "fs" fixture becomes available.

:Usage:

def my_fakefs_test(fs):
    fs.create_file('/var/data/xx1.txt')
    assert os.path.exists('/var/data/xx1.txt')
"""

import py
import pytest
from _pytest import capture

from pyfakefs.fake_filesystem_unittest import Patcher

try:
    from _pytest import pathlib
except ImportError:
    pathlib = None  # type:ignore[assignment]

Patcher.SKIPMODULES.add(py)
Patcher.SKIPMODULES.add(pytest)
Patcher.SKIPMODULES.add(capture)
if pathlib is not None:
    Patcher.SKIPMODULES.add(pathlib)


@pytest.fixture
def fs(request):
    """Fake filesystem."""
    if hasattr(request, "param"):
        # pass optional parameters via @pytest.mark.parametrize
        patcher = Patcher(*request.param)
    else:
        patcher = Patcher()
    patcher.setUp()
    yield patcher.fs
    patcher.tearDown()


@pytest.fixture(scope="class")
def fs_class(request):
    """Class-scoped fake filesystem fixture."""
    if hasattr(request, "param"):
        patcher = Patcher(*request.param)
    else:
        patcher = Patcher()
    patcher.setUp()
    yield patcher.fs
    patcher.tearDown()


@pytest.fixture(scope="module")
def fs_module(request):
    """Module-scoped fake filesystem fixture."""
    if hasattr(request, "param"):
        patcher = Patcher(*request.param)
    else:
        patcher = Patcher()
    patcher.setUp()
    yield patcher.fs
    patcher.tearDown()


@pytest.fixture(scope="session")
def fs_session(request):
    """Session-scoped fake filesystem fixture."""
    if hasattr(request, "param"):
        patcher = Patcher(*request.param)
    else:
        patcher = Patcher()
    patcher.setUp()
    yield patcher.fs
    patcher.tearDown()


@pytest.hookimpl(tryfirst=True)
def pytest_sessionfinish(session, exitstatus):
    """Make sure that the cache is cleared before the final test shutdown."""
    Patcher.clear_fs_cache()


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_logreport(report):
    """Make sure that patching is not active during reporting."""
    pause = Patcher.PATCHER is not None and report.when == "call"
    if pause:
        Patcher.PATCHER.pause()
    yield


@pytest.hookimpl(hookwrapper=True, trylast=True)
def pytest_runtest_setup(item):
    if Patcher.PATCHER is not None:
        Patcher.PATCHER.resume()
    yield


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_teardown(item, nextitem):
    """Make sure that patching is not active during reporting."""
    if Patcher.PATCHER is not None:
        Patcher.PATCHER.pause()
    yield
