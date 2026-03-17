"""
Utility methods to print system info for debugging

adapted from :func:`sklearn.utils._show_versions`
which was adapted from :func:`pandas.show_versions`
"""
import importlib
import os
import platform
import sys


def _get_sys_info():
    """System information

    Return
    ------
    dict:
        system and Python version information
    """
    blob = [
        ("python", sys.version.replace("\n", " ")),
        ("executable", sys.executable),
        ("machine", platform.platform()),
    ]

    return dict(blob)


def _get_gdal_info():
    """Information on system GDAL

    Returns
    -------
    dict:
        system GDAL information
    """
    import rasterio

    blob = [
        ("rasterio", rasterio.__version__),
        ("GDAL", rasterio.__gdal_version__),
        ("PROJ", rasterio.__proj_version__),
        ("GEOS", rasterio.__geos_version__),
        ("PROJ DATA", os.pathsep.join(rasterio._env.get_proj_data_search_paths())),
        ("GDAL DATA", rasterio._env.get_gdal_data()),
    ]

    return dict(blob)


def _get_deps_info():
    """Overview of the installed version of main dependencies

    Returns
    -------
    dict:
        version information on relevant Python libraries
    """
    deps = (
        "affine",
        "attrs",
        "certifi",
        "click",
        "cligj",
        "cython",
        "numpy",
        "setuptools",
    )

    deps_info = {}
    for modname in deps:
        try:
            deps_info[modname] = importlib.metadata.version(modname)
        except importlib.metadata.PackageNotFoundError:
            deps_info[modname] = None
    return deps_info


def _print_info_dict(info_dict):
    """Print the information dictionary"""
    for key, stat in info_dict.items():
        print(f"{key:>10}: {stat}")


def show_versions():
    """
    Print useful debugging information

    Example
    -------
    > python -c "import rasterio; rasterio.show_versions()"

    """
    print("rasterio info:")
    _print_info_dict(_get_gdal_info())
    print("\nSystem:")
    _print_info_dict(_get_sys_info())
    print("\nPython deps:")
    _print_info_dict(_get_deps_info())
