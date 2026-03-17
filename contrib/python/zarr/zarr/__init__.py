import functools
import logging
from typing import Literal

from zarr._version import version as __version__
from zarr.api.synchronous import (
    array,
    consolidate_metadata,
    copy,
    copy_all,
    copy_store,
    create,
    create_array,
    create_group,
    create_hierarchy,
    empty,
    empty_like,
    from_array,
    full,
    full_like,
    group,
    load,
    ones,
    ones_like,
    open,
    open_array,
    open_consolidated,
    open_group,
    open_like,
    save,
    save_array,
    save_group,
    tree,
    zeros,
    zeros_like,
)
from zarr.core.array import Array, AsyncArray
from zarr.core.config import config
from zarr.core.group import AsyncGroup, Group

# in case setuptools scm screw up and find version to be 0.0.0
assert not __version__.startswith("0.0.0")

_logger = logging.getLogger(__name__)


def print_debug_info() -> None:
    """
    Print version info for use in bug reports.
    """
    import platform
    from importlib.metadata import version

    def print_packages(packages: list[str]) -> None:
        not_installed = []
        for package in packages:
            try:
                print(f"{package}: {version(package)}")
            except ModuleNotFoundError:
                not_installed.append(package)
        if not_installed:
            print("\n**Not Installed:**")
            for package in not_installed:
                print(package)

    required = [
        "packaging",
        "numpy",
        "numcodecs",
        "typing_extensions",
        "donfig",
    ]
    optional = [
        "botocore",
        "cupy-cuda12x",
        "fsspec",
        "numcodecs",
        "s3fs",
        "gcsfs",
        "universal-pathlib",
        "rich",
        "obstore",
    ]

    print(f"platform: {platform.platform()}")
    print(f"python: {platform.python_version()}")
    print(f"zarr: {__version__}\n")
    print("**Required dependencies:**")
    print_packages(required)
    print("\n**Optional dependencies:**")
    print_packages(optional)


# The decorator ensures this always returns the same handler (and it is only
# attached once).
@functools.cache
def _ensure_handler() -> logging.Handler:
    """
    The first time this function is called, attach a `StreamHandler` using the
    same format as `logging.basicConfig` to the Zarr-Python root logger.

    Return this handler every time this function is called.
    """
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
    _logger.addHandler(handler)
    return handler


def set_log_level(
    level: Literal["NOTSET", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
) -> None:
    """Set the logging level for Zarr-Python.

    Zarr-Python uses the standard library `logging` framework under the root
    logger 'zarr'.  This is a helper function to:

    - set Zarr-Python's root logger level
    - set the root logger handler's level, creating the handler
      if it does not exist yet

    Parameters
    ----------
    level : str
        The logging level to set.
    """
    _logger.setLevel(level)
    _ensure_handler().setLevel(level)


def set_format(log_format: str) -> None:
    """Set the format of logging messages from Zarr-Python.

    Zarr-Python uses the standard library `logging` framework under the root
    logger 'zarr'. This sets the format of log messages from the root logger's StreamHandler.

    Parameters
    ----------
    log_format : str
        A string determining the log format (as defined in the standard library's `logging` module
        for logging.Formatter)
    """
    _ensure_handler().setFormatter(logging.Formatter(fmt=log_format))


__all__ = [
    "Array",
    "AsyncArray",
    "AsyncGroup",
    "Group",
    "__version__",
    "array",
    "config",
    "consolidate_metadata",
    "copy",
    "copy_all",
    "copy_store",
    "create",
    "create_array",
    "create_group",
    "create_hierarchy",
    "empty",
    "empty_like",
    "from_array",
    "full",
    "full_like",
    "group",
    "load",
    "ones",
    "ones_like",
    "open",
    "open_array",
    "open_consolidated",
    "open_group",
    "open_like",
    "print_debug_info",
    "save",
    "save_array",
    "save_group",
    "tree",
    "zeros",
    "zeros_like",
]
