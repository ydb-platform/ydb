import platform
import sys

import fiona
from fiona._env import get_gdal_release_name, get_proj_version_tuple


def show_versions():
    """
    Prints information useful for bug reports
    """

    print("Fiona version:", fiona.__version__)
    print("GDAL version:", get_gdal_release_name())
    print("PROJ version:", ".".join(map(str, get_proj_version_tuple())))
    print()
    print("OS:", platform.system(), platform.release())
    print("Python:", platform.python_version())
    print("Python executable:", sys.executable)
