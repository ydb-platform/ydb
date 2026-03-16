"""
Utility methods to print system info for debugging

adapted from :func:`sklearn.utils._show_versions`
which was adapted from :func:`pandas.show_versions`
"""

import importlib.metadata
import platform
import sys


def _get_sys_info():
    """System information
    Return
    ------
    sys_info : dict
        system and Python version information
    """
    blob = [
        ("python", sys.version.replace("\n", " ")),
        ("executable", sys.executable),
        ("machine", platform.platform()),
    ]

    return dict(blob)


def _get_proj_info():
    """Information on system PROJ

    Returns
    -------
    proj_info: dict
        system PROJ information
    """
    # pylint: disable=import-outside-toplevel
    import pyproj
    from pyproj.database import get_database_metadata
    from pyproj.exceptions import DataDirError

    try:
        data_dir = pyproj.datadir.get_data_dir()
    except DataDirError:
        data_dir = None

    blob = [
        ("pyproj", pyproj.__version__),
        ("PROJ (runtime)", pyproj.__proj_version__),
        ("PROJ (compiled)", pyproj.__proj_compiled_version__),
        ("data dir", data_dir),
        ("user_data_dir", pyproj.datadir.get_user_data_dir()),
        ("PROJ DATA (recommended version)", get_database_metadata("PROJ_DATA.VERSION")),
        (
            "PROJ Database",
            f"{get_database_metadata('DATABASE.LAYOUT.VERSION.MAJOR')}."
            f"{get_database_metadata('DATABASE.LAYOUT.VERSION.MINOR')}",
        ),
        (
            "EPSG Database",
            f"{get_database_metadata('EPSG.VERSION')} "
            f"[{get_database_metadata('EPSG.DATE')}]",
        ),
        (
            "ESRI Database",
            f"{get_database_metadata('ESRI.VERSION')} "
            f"[{get_database_metadata('ESRI.DATE')}]",
        ),
        (
            "IGNF Database",
            f"{get_database_metadata('IGNF.VERSION')} "
            f"[{get_database_metadata('IGNF.DATE')}]",
        ),
    ]

    return dict(blob)


def _get_deps_info():
    """Overview of the installed version of main dependencies
    Returns
    -------
    deps_info: dict
        version information on relevant Python libraries
    """
    deps = ["certifi", "Cython", "setuptools", "pip"]

    def get_version(module):
        try:
            return importlib.metadata.version(module)
        except importlib.metadata.PackageNotFoundError:
            return None

    return {dep: get_version(dep) for dep in deps}


def _print_info_dict(info_dict):
    """Print the information dictionary"""
    for key, stat in info_dict.items():
        print(f"{key:>10}: {stat}")


def show_versions():
    """
    .. versionadded:: 2.2.1

    Print useful debugging information

    Example
    -------
    > python -c "import pyproj; pyproj.show_versions()"

    """
    print("pyproj info:")
    _print_info_dict(_get_proj_info())
    print("\nSystem:")
    _print_info_dict(_get_sys_info())
    print("\nPython deps:")
    _print_info_dict(_get_deps_info())
