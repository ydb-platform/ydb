"""
# version.py

Get the current version from package metadata or pyproject.toml
if everything else fails.
"""

import json
import os
from typing import Optional


def _get_version() -> Optional[str]:
    """
    Try all our methods to get the version.
    """

    try:
        # Get the version string using package metadata on Python >= 3.8
        from importlib.metadata import version

        return version("trimesh")
    except BaseException:
        pass

    try:
        # Get the version string using package metadata on Python < 3.8
        from pkg_resources import get_distribution

        return get_distribution("trimesh").version
    except BaseException:
        pass

    try:
        # Get the version string from the pyproject.toml file using
        # relative paths. This will be the only option if the library
        # has not been installed (i.e. in a CI or container environment)
        pyproject = os.path.abspath(
            os.path.join(
                os.path.dirname(os.path.abspath(os.path.expanduser(__file__))),
                "..",
                "pyproject.toml",
            )
        )
        with open(pyproject) as f:
            # json.loads cleans up the string and removes the quotes
            # this logic requires the first use of "version" be the actual version
            return next(json.loads(L.split("=")[1]) for L in f if "version" in L)
    except BaseException:
        pass

    return None


# try all our tricks
__version__ = _get_version()

if __name__ == "__main__":
    # print version if run directly i.e. in a CI script
    print(__version__)
