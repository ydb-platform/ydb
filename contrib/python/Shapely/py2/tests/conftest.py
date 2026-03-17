import sys

import pytest

from shapely.geos import geos_version


requires_geos_342 = pytest.mark.skipif(geos_version < (3, 4, 2), reason="GEOS > 3.4.2 is required.")


def pytest_addoption(parser):
    parser.addoption("--with-speedups", action="store_true", default=False,
                     help="Run tests with speedups.")
    parser.addoption("--without-speedups", action="store_true", default=False,
                     help="Run tests without speedups.")

def pytest_runtest_setup(item):
    if item.config.getoption("--with-speedups"):
        import shapely.speedups
        if not shapely.speedups.available:
            print("Speedups have been demanded but are unavailable")
            sys.exit(1)
        shapely.speedups.enable()
        assert(shapely.speedups.enabled is True)
        print("Speedups enabled for %s." % item.name)
    elif item.config.getoption("--without-speedups"):
        import shapely.speedups
        shapely.speedups.disable()
        assert(shapely.speedups.enabled is False)
        print("Speedups disabled for %s." % item.name)

def pytest_report_header(config):
    headers = []
    try:
        import numpy
    except ImportError:
        headers.append("numpy: not available")
    else:
        headers.append("numpy: {}".format(numpy.__version__))
    return '\n'.join(headers)
