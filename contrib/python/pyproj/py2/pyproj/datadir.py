"""
Set the datadir path to the local data directory
"""
import os
from distutils.spawn import find_executable

from pyproj.exceptions import DataDirError

_USER_PROJ_DATA = None
_VALIDATED_PROJ_DATA = None


def set_data_dir(proj_data_dir):
    """
    Set the data directory for PROJ to use.

    Parameters
    ----------
    proj_data_dir: str
        The path to rhe PROJ data directory.
    """
    global _USER_PROJ_DATA
    global _VALIDATED_PROJ_DATA
    _USER_PROJ_DATA = proj_data_dir
    # set to none to re-validate
    _VALIDATED_PROJ_DATA = None


def append_data_dir(proj_data_dir):
    """
    Add an additional data directory for PROJ to use.

    Parameters
    ----------
    proj_data_dir: str
        The path to rhe PROJ data directory.
    """
    set_data_dir(os.pathsep.join([get_data_dir(), proj_data_dir]))


def get_data_dir():
    """
    The order of preference for the data directory is:

    1. The one set by pyproj.datadir.set_data_dir (if exists & valid)
    2. The internal proj directory (if exists & valid)
    3. The directory in PROJ_LIB (if exists & valid)
    4. The directory on the PATH (if exists & valid)

    Returns
    -------
    str: The valid data directory.

    """
    # to avoid re-validating
    global _VALIDATED_PROJ_DATA
    if _VALIDATED_PROJ_DATA is not None:
        return _VALIDATED_PROJ_DATA

    global _USER_PROJ_DATA
    internal_datadir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "proj_dir", "share", "proj"
    )
    proj_lib_dirs = os.environ.get("PROJ_LIB", "")

    def valid_data_dir(potential_data_dir):
        if potential_data_dir is not None and os.path.exists(
            os.path.join(potential_data_dir, "proj.db")
        ):
            return True
        return False

    def valid_data_dirs(potential_data_dirs):
        if potential_data_dirs is None:
            return False
        for proj_data_dir in potential_data_dirs.split(os.pathsep):
            if valid_data_dir(proj_data_dir):
                return True
                break
        return None

    if valid_data_dirs(_USER_PROJ_DATA):
        _VALIDATED_PROJ_DATA = _USER_PROJ_DATA
    elif valid_data_dir(internal_datadir):
        _VALIDATED_PROJ_DATA = internal_datadir
    elif valid_data_dirs(proj_lib_dirs):
        _VALIDATED_PROJ_DATA = proj_lib_dirs
    else:
        # Use Arcadia resource file
        _VALIDATED_PROJ_DATA = os.sep.join([os.path.dirname(__file__), 'data'])

    if _VALIDATED_PROJ_DATA is None:
        raise DataDirError(
            "Valid PROJ data directory not found. "
            "Either set the path using the environmental variable PROJ_LIB or "
            "with `pyproj.datadir.set_data_dir`."
        )
    return _VALIDATED_PROJ_DATA
