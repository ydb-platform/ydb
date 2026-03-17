from packaging import version

PYTEST_ASYNCIO_VERSION = version.parse('0.22')


def _pytest_asyncio_legacy():
    try:
        import pytest_asyncio
    except ImportError:
        return True
    return version.parse(pytest_asyncio.__version__) < PYTEST_ASYNCIO_VERSION


# type: ignore
if _pytest_asyncio_legacy():
    from .plugin_legacy import *
else:
    from .plugin import *
