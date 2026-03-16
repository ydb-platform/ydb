"""Raster file management."""

include "gdal.pxi"

import logging
import os

from rasterio._io cimport DatasetReaderBase
from rasterio._err cimport exc_wrap_int, exc_wrap_pointer
from rasterio.drivers import driver_from_extension
from rasterio.env import ensure_env_with_credentials
from rasterio._err import CPLE_OpenFailedError
from rasterio.errors import DriverRegistrationError, RasterioIOError
from rasterio._path import _parse_path


log = logging.getLogger(__name__)


@ensure_env_with_credentials
def exists(path):

    """Determine if a dataset exists by attempting to open it.

    Parameters
    ----------
    path : str
        Path to dataset
    """

    cdef GDALDatasetH h_dataset = NULL

    vsipath = _parse_path(path).as_vsi()
    b_path = vsipath.encode('utf-8')
    cdef char* c_path = b_path

    with nogil:
        h_dataset = GDALOpen(c_path, <GDALAccess>0)

    try:
        h_dataset = exc_wrap_pointer(h_dataset)
        return True
    except CPLE_OpenFailedError:
        return False
    finally:
        with nogil:
            if h_dataset != NULL:
                GDALClose(h_dataset)


@ensure_env_with_credentials
def copy(src, dst, driver=None, strict=True, **creation_options):

    """Copy a raster from a path or open dataset handle to a new destination
    with driver specific creation options.

    Parameters
    ----------
    src : str or PathLike or dataset object opened in 'r' mode
        Source dataset
    dst : str or PathLike
        Output dataset path
    driver : str, optional
        Output driver name
    strict : bool, optional.  Default: True
        Indicates if the output must be strictly equivalent or if the
        driver may adapt as necessary
    creation_options : dict, optional
        Creation options for output dataset

    Returns
    -------
    None

    """

    cdef bint c_strictness
    cdef char **options = NULL
    cdef char* c_src_path = NULL
    cdef char* c_dst_path = NULL
    cdef GDALDatasetH src_dataset = NULL
    cdef GDALDatasetH dst_dataset = NULL
    cdef GDALDriverH drv = NULL
    cdef bint close_src = False

    # Creation options
    for key, val in creation_options.items():
        kb, vb = (x.upper().encode('utf-8') for x in (key, str(val)))
        options = CSLSetNameValue(
            options, <const char *>kb, <const char *>vb)
        log.debug("Option %r:%r", kb, vb)

    c_strictness = strict

    # Convert src and dst Paths to strings.
    if isinstance(src, os.PathLike):
        src = os.fspath(src)
    if isinstance(dst, os.PathLike):
        dst = os.fspath(dst)

    if driver is None:
        driver = driver_from_extension(dst)

    driverb = driver.encode('utf-8')
    drv = GDALGetDriverByName(driverb)

    if drv == NULL:
        raise DriverRegistrationError("Unrecognized driver: {}".format(driver))

    # Open a new GDAL dataset if src is a string.
    if isinstance(src, str):

        if _parse_path(src).as_vsi() == _parse_path(dst).as_vsi():
            raise RasterioIOError("{} and {} identify the same dataset.".format(src, dst))

        src = src.encode('utf-8')
        c_src_path = src
        with nogil:
            src_dataset = GDALOpen(c_src_path, <GDALAccess>0)
        src_dataset = exc_wrap_pointer(src_dataset)
        close_src = True

    # Try to use the existing GDAL dataset handle otherwise.
    else:

        if src.name == _parse_path(dst).as_vsi():
            raise RasterioIOError("{} and {} identify the same dataset.".format(src.name, dst))

        src_dataset = (<DatasetReaderBase?>src).handle()
        close_src = False

    dst = dst.encode('utf-8')
    c_dst_path = dst

    try:
        with nogil:
            dst_dataset = GDALCreateCopy(
                drv, c_dst_path, src_dataset, c_strictness, options, NULL, NULL)
        dst_dataset = exc_wrap_pointer(dst_dataset)

    finally:
        CSLDestroy(options)
        with nogil:
            if dst_dataset != NULL:
                GDALClose(dst_dataset)
            if close_src:
                if src_dataset != NULL:
                    GDALClose(src_dataset)


@ensure_env_with_credentials
def copyfiles(src, dst):

    """Copy files associated with a dataset from one location to another.

    Parameters
    ----------
    src : str or PathLike
        Source dataset
    dst : str or PathLike
        Target dataset

    Returns
    -------
    None

    """

    cdef GDALDatasetH h_dataset = NULL
    cdef GDALDriverH h_driver = NULL

    # Convert src and dst Paths to strings.
    if isinstance(src, os.PathLike):
        src = os.fspath(src)
    if isinstance(dst, os.PathLike):
        dst = os.fspath(dst)

    src_path = _parse_path(src)
    dst_path = _parse_path(dst)
    if src_path.as_vsi() == dst_path.as_vsi():
        raise RasterioIOError("{} and {} identify the same dataset.".format(src, dst))

    # VFS paths probably don't work, but its hard to be completely certain
    # so just attempt to use them.
    gdal_src_path = src_path.as_vsi()
    gdal_dst_path = dst_path.as_vsi()
    b_gdal_src_path = gdal_src_path.encode('utf-8')
    b_gdal_dst_path = gdal_dst_path.encode('utf-8')
    cdef char* c_gdal_src_path = b_gdal_src_path
    cdef char* c_gdal_dst_path = b_gdal_dst_path

    with nogil:
            h_dataset = GDALOpen(c_gdal_src_path, <GDALAccess>0)
    try:
        h_dataset = exc_wrap_pointer(h_dataset)
        h_driver = exc_wrap_pointer(GDALGetDatasetDriver(h_dataset))
        with nogil:
            err = GDALCopyDatasetFiles(
                h_driver, c_gdal_dst_path, c_gdal_src_path)
        exc_wrap_int(err)
    except CPLE_OpenFailedError as e:
        raise RasterioIOError(str(e))
    finally:
        with nogil:
            if h_dataset != NULL:
                GDALClose(h_dataset)


@ensure_env_with_credentials
def delete(path, driver=None):

    """Delete a GDAL dataset

    Parameters
    ----------
    path : path
        Path to dataset to delete
    driver : str or None, optional
        Name of driver to use for deleting.  Defaults to whatever GDAL
        determines is the appropriate driver
    """

    cdef GDALDatasetH h_dataset = NULL
    cdef GDALDriverH h_driver = NULL

    gdal_path = _parse_path(path).as_vsi()
    b_path = gdal_path.encode('utf-8')
    cdef char* c_path = b_path

    if driver:
        b_driver = driver.encode('utf-8')
        h_driver = GDALGetDriverByName(b_driver)
        if h_driver == NULL:
            raise DriverRegistrationError(
                "Unrecognized driver: {}".format(driver))

    # Need to determine driver by opening the input dataset
    else:
        with nogil:
            h_dataset = GDALOpen(c_path, <GDALAccess>0)

        try:
            h_dataset = exc_wrap_pointer(h_dataset)
            h_driver = GDALGetDatasetDriver(h_dataset)
            if h_driver == NULL:
                raise DriverRegistrationError(
                    "Could not determine driver for: {}".format(path))
        except CPLE_OpenFailedError:
            raise RasterioIOError(
                "Invalid dataset: {}".format(path))
        finally:
            with nogil:
                if h_dataset != NULL:
                    GDALClose(h_dataset)

    with nogil:
        res = GDALDeleteDataset(h_driver, c_path)
    exc_wrap_int(res)
