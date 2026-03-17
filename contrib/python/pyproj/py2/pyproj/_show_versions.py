"""
Utility methods to print system info for debugging

adapted from :func:`sklearn.utils._show_versions`
which was adapted from :func:`pandas.show_versions`
"""
import importlib
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
    import pyproj
    from pyproj.exceptions import DataDirError

    try:
        data_dir = pyproj.datadir.get_data_dir()
    except DataDirError:
        data_dir = None

    blob = [("PROJ", pyproj.proj_version_str), ("data dir", data_dir)]

    return dict(blob)


def _get_deps_info():
    """Overview of the installed version of main dependencies
    Returns
    -------
    deps_info: dict
        version information on relevant Python libraries
    """
    deps = ["pyproj", "pip", "setuptools", "Cython", "aenum"]

    def get_version(module):
        try:
            return module.__version__
        except AttributeError:
            return module.version

    deps_info = {}

    for modname in deps:
        try:
            if modname in sys.modules:
                mod = sys.modules[modname]
            else:
                mod = importlib.import_module(modname)
            ver = get_version(mod)
            deps_info[modname] = ver
        except ImportError:
            deps_info[modname] = None

    return deps_info


def _print_info_dict(info_dict):
    """Print the information dictionary"""
    for key, stat in info_dict.items():
        print("{key:>10}: {stat}".format(key=key, stat=stat))


def show_versions():
    """Print useful debugging information

    Example
    -------
    > python -c "import pyproj; pyproj.show_versions()"

    """
    print("\nSystem:")
    _print_info_dict(_get_sys_info())
    print("\nPROJ:")
    _print_info_dict(_get_proj_info())
    print("\nPython deps:")
    _print_info_dict(_get_deps_info())
