"""Driver policies and utilities

GDAL has many standard and extension format drivers and completeness of
these drivers varies greatly. It's possible to succeed poorly with some
formats and drivers, meaning that easy problems can be solved but that
harder problems are blocked by limitations of the drivers and formats.

NetCDF writing, for example, is presently blacklisted. Rasterio users
should use netcdf4-python instead:
http://unidata.github.io/netcdf4-python/.
"""
import os

from rasterio._base import _raster_driver_extensions
from rasterio.env import ensure_env

# Methods like `rasterio.open()` may use this blacklist to preempt
# combinations of drivers and file modes.
blacklist = {
    # See https://github.com/rasterio/rasterio/issues/638 for discussion
    # about writing NetCDF files.
    'netCDF': ('r+', 'w')}


@ensure_env
def raster_driver_extensions():
    """
    Returns
    -------
    dict:
        Map of extensions to the driver.
    """
    return _raster_driver_extensions()


def driver_from_extension(path):
    """
    Attempt to auto-detect driver based on the extension.

    Parameters
    ----------
    path: str or pathlike object
        The path to the dataset to write with.

    Returns
    -------
    str:
        The name of the driver for the extension.
    """
    try:
        # in case the path is a file handle
        # or a partsed path
        path = path.name
    except AttributeError:
        pass

    driver_extensions = raster_driver_extensions()
    try:
        return driver_extensions[os.path.splitext(path)[-1].lstrip(".").lower()]
    except KeyError:
        raise ValueError("Unable to detect driver. Please specify driver.")


def is_blacklisted(name, mode):
    """Returns True if driver `name` and `mode` are blacklisted."""
    return mode in blacklist.get(name, ())
