# Copyright (c) 2010-2019 openpyxl

import pytest
import platform

### Markers ###

def pytest_runtest_setup(item):
    if isinstance(item, pytest.Function):
        try:
            from PIL import Image
        except ImportError:
            Image = False
        if item.get_closest_marker("pil_required") and Image is False:
            pytest.skip("PIL must be installed")
        elif item.get_closest_marker("pil_not_installed") and Image:
            pytest.skip("PIL is installed")
        elif item.get_closest_marker("not_py33"):
            pytest.skip("Ordering is not a given in Python 3")
        elif item.get_closest_marker("defusedxml_required"):
            from openpyxl import DEFUSEDXML
            if not DEFUSEDXML:
                pytest.skip("defusedxml is required to guard against these vulnerabilities")
        elif item.get_closest_marker("lxml_required"):
            from openpyxl import LXML
            if not LXML:
                pytest.skip("LXML is required for some features such as schema validation")
        elif item.get_closest_marker("lxml_buffering"):
            from lxml.etree import LIBXML_VERSION
            if LIBXML_VERSION < (3, 4, 0, 0):
                pytest.skip("LXML >= 3.4 is required")
        elif item.get_closest_marker("no_lxml"):
            from openpyxl import LXML
            if LXML:
                pytest.skip("LXML has a different interface")
        elif item.get_closest_marker("numpy_required"):
            from openpyxl import NUMPY
            if not NUMPY:
                pytest.skip("Numpy must be installed")
        elif item.get_closest_marker("pandas_required"):
            from openpyxl import PANDAS
            if not PANDAS:
                pytest.skip("Pandas must be installed")
        elif item.get_closest_marker("no_pypy"):
            if platform.python_implementation() == "PyPy":
                pytest.skip("Skipping pypy")

