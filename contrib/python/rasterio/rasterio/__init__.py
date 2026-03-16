"""Rasterio"""

from collections import namedtuple
from contextlib import ExitStack
import glob
import logging
from logging import NullHandler
import os
import platform

# On Windows we must explicitly register the directories that contain
# the GDAL and supporting DLLs starting with Python 3.8. Presently, we
# support the rasterio-wheels location or directories on the system's
# executable path.
if platform.system() == "Windows":
    _whl_dir = os.path.join(os.path.dirname(__file__), ".libs")
    if os.path.exists(_whl_dir):
        os.add_dll_directory(_whl_dir)
    else:
        if "PATH" in os.environ:
            for p in os.environ["PATH"].split(os.pathsep):
                if p and glob.glob(os.path.join(p, "gdal*.dll")):
                    os.add_dll_directory(os.path.abspath(p))

from rasterio._base import DatasetBase
from rasterio._io import Statistics
from rasterio._vsiopener import _opener_registration
from rasterio._show_versions import show_versions
from rasterio._version import gdal_version, get_geos_version, get_proj_version
from rasterio.crs import CRS
from rasterio.drivers import driver_from_extension, is_blacklisted
from rasterio.dtypes import (
    bool_,
    ubyte,
    sbyte,
    uint8,
    int8,
    uint16,
    int16,
    uint32,
    int32,
    int64,
    uint64,
    float16,
    float32,
    float64,
    complex_,
    check_dtype,
    complex_int16,
)
from rasterio.env import ensure_env_with_credentials, Env
from rasterio.errors import (
    RasterioIOError,
    DriverCapabilityError,
    RasterioDeprecationWarning,
)
from rasterio.io import (
    DatasetReader,
    get_writer_for_path,
    get_writer_for_driver,
    MemoryFile,
)
from rasterio.profiles import default_gtiff_profile
from rasterio.transform import Affine, guard_transform
from rasterio._path import _parse_path, _UnparsedPath

# These modules are imported from the Cython extensions, but are also import
# here to help tools like cx_Freeze find them automatically
import rasterio._err
import rasterio.coords
import rasterio.enums
import rasterio._path

try:
    from rasterio.io import FilePath

    have_vsi_plugin = True
except ImportError:

    class FilePath:
        pass

    have_vsi_plugin = False

__all__ = ['band', 'open', 'pad', 'Band', 'Env', 'CRS']
__version__ = "1.5.0"
__gdal_version__ = gdal_version()
__proj_version__ = ".".join([str(version) for version in get_proj_version()])
__geos_version__ = ".".join([str(version) for version in get_geos_version()])

# Rasterio attaches NullHandler to the 'rasterio' logger and its
# descendents. See
# https://docs.python.org/2/howto/logging.html#configuring-logging-for-a-library
# Applications must attach their own handlers in order to see messages.
# See rasterio/rio/main.py for an example.
log = logging.getLogger(__name__)
log.addHandler(NullHandler())


@ensure_env_with_credentials
def open(
    fp,
    mode="r",
    driver=None,
    width=None,
    height=None,
    count=None,
    crs=None,
    transform=None,
    dtype=None,
    nodata=None,
    sharing=False,
    thread_safe=False,
    opener=None,
    **kwargs
):
    """Open a dataset for reading or writing.

    The dataset may be located in a local file, in a resource located by
    a URL, or contained within a stream of bytes. This function accepts
    different types of fp parameters. However, it is almost always best
    to pass a string that has a dataset name as its value. These are
    passed directly to GDAL protocol and format handlers. A path to
    a zipfile is more efficiently used by GDAL than a Python ZipFile
    object, for example.

    In read ('r') or read/write ('r+') mode, no keyword arguments are
    required: these attributes are supplied by the opened dataset.

    In write ('w' or 'w+') mode, the driver, width, height, count, and
    dtype keywords are strictly required.

    Parameters
    ----------
    fp : str, os.PathLike, file-like, or rasterio.io.MemoryFile
        A filename or URL, a file object opened in binary ('rb') mode,
        a Path object, or one of the rasterio classes that provides the
        dataset-opening interface (has an open method that returns
        a dataset). Use a string when possible: GDAL can more
        efficiently access a dataset if it opens it natively.
    mode : str, optional
        'r' (read, the default), 'r+' (read/write), 'w' (write), or
        'w+' (write/read).
    driver : str, optional
        A short format driver name (e.g. "GTiff" or "JPEG") or a list of
        such names (see GDAL docs at
        https://gdal.org/drivers/raster/index.html). In 'w' or 'w+' modes
        a single name is required. In 'r' or 'r+' modes the driver can
        usually be omitted. Registered drivers will be tried
        sequentially until a match is found. When multiple drivers are
        available for a format such as JPEG2000, one of them can be
        selected by using this keyword argument.
    width : int, optional
        The number of columns of the raster dataset. Required in 'w' or
        'w+' modes, it is ignored in 'r' or 'r+' modes.
    height : int, optional
        The number of rows of the raster dataset. Required in 'w' or
        'w+' modes, it is ignored in 'r' or 'r+' modes.
    count : int, optional
        The count of dataset bands. Required in 'w' or 'w+' modes, it is
        ignored in 'r' or 'r+' modes.
    crs : str, dict, or CRS, optional
        The coordinate reference system. Required in 'w' or 'w+' modes,
        it is ignored in 'r' or 'r+' modes.
    transform : affine.Affine, optional
        Affine transformation mapping the pixel space to geographic
        space. Required in 'w' or 'w+' modes, it is ignored in 'r' or
        'r+' modes.
    dtype : str or numpy.dtype, optional
        The data type for bands. For example: 'uint8' or
        `rasterio.uint16`. Required in 'w' or 'w+' modes, it is
        ignored in 'r' or 'r+' modes.
    nodata : int, float, or nan, optional
        Defines the pixel value to be interpreted as not valid data.
        Required in 'w' or 'w+' modes, it is ignored in 'r' or 'r+'
        modes.
    sharing : bool, optional
        To reduce overhead and prevent programs from running out of file
        descriptors, rasterio maintains a pool of shared low level
        dataset handles. If True this function will use a shared
        handle if one is available. Multithreaded programs must avoid
        sharing and should set *sharing* to False.
    thread_safe: bool, optional
        Open GDAL dataset in `thread safe mode <https://gdal.org/en/stable/user/multithreading.html>`__.
        For multithreaded read-only GDAL dataset operations (e.g. ``GDAL_NUM_THREADS``, `LIBERTIFF driver <https://gdal.org/en/stable/drivers/raster/libertiff.html#open-options>`__).
        Requires rasterio 1.5+ & GDAL 3.10+.
    opener : callable, optional
        A custom dataset opener which can serve GDAL's virtual
        filesystem machinery via Python file-like objects. The
        underlying file-like object is obtained by calling *opener* with
        (*fp*, *mode*) or (*fp*, *mode* + 'b') depending on the format
        driver's native mode. *opener* must return a Python file-like
        object that provides read, seek, tell, and close methods. Note:
        only one opener at a time per fp, mode pair is allowed.
    kwargs : optional
        These are passed to format drivers as directives for creating or
        interpreting datasets. For example: in 'w' or 'w+' modes
        a tiled=True keyword argument will direct the GeoTIFF format
        driver to create a tiled, rather than striped, TIFF.

    Returns
    -------
    :class:`rasterio.io.DatasetReader`
        If mode is 'r'.
    :class:`rasterio.io.DatasetWriter`
        If mode is 'r+', 'w', or 'w+'.

    Raises
    ------
    :class:`TypeError`
        If arguments are of the wrong Python type.
    :class:`rasterio.errors.RasterioIOError`
        If the dataset can not be opened. Such as when there is no
        dataset with the given name.
    :class:`rasterio.errors.DriverCapabilityError`
        If the detected format driver does not support the requested
        opening mode.

    Notes
    -----
    If *fp* is a is a file-like object, its entire contents will be
    read into a MemoryFile instance. It will almost always be better
    to use a path or URL, or the *opener* keyword argument.

    Examples
    --------
    To open a local GeoTIFF dataset for reading using standard driver
    discovery and no directives:

    >>> import rasterio
    >>> with rasterio.open('example.tif') as dataset:
    ...     print(dataset.profile)

    To open a local JPEG2000 dataset using only the JP2OpenJPEG driver:

    >>> with rasterio.open(
    ...         'example.jp2', driver='JP2OpenJPEG') as dataset:
    ...     print(dataset.profile)

    To create a new 8-band, 16-bit unsigned, tiled, and LZW-compressed
    GeoTIFF with a global extent and 0.5 degree resolution:

    >>> from rasterio.transform import from_origin
    >>> with rasterio.open(
    ...         'example.tif', 'w', driver='GTiff', dtype='uint16',
    ...         width=720, height=360, count=8, crs='EPSG:4326',
    ...         transform=from_origin(-180.0, 90.0, 0.5, 0.5),
    ...         nodata=0, tiled=True, compress='lzw') as dataset:
    ...     dataset.write(...)
    """
    if not isinstance(fp, str):
        if not (
            hasattr(fp, "open")
            or hasattr(fp, "read")
            or hasattr(fp, "write")
            or isinstance(fp, (os.PathLike, MemoryFile, FilePath))
        ) or isinstance(fp, DatasetBase):
            raise TypeError(f"invalid path or file: {fp!r}")
    if not isinstance(mode, str):
        raise TypeError(f"invalid mode: {mode!r}")
    elif mode[0] not in ("r", "w"):
        raise ValueError(f"invalid mode: {mode!r}")
    if driver and not isinstance(driver, str):
        raise TypeError(f"invalid driver: {driver!r}")
    if dtype and not check_dtype(dtype):
        raise TypeError(f"invalid dtype: {dtype!r}")
    if nodata is not None:
        nodata = float(nodata)
    if transform:
        transform = guard_transform(transform)

    # Check driver/mode blacklist.
    if driver and is_blacklisted(driver, mode):
        raise RasterioIOError(
            "Blacklisted: file cannot be opened by "
            "driver '{}' in '{}' mode".format(driver, mode)
        )

    # This check should come first, since MemoryFile has read/write methods
    # TODO: test for a shared base class or abstract type.
    elif isinstance(fp, (FilePath, MemoryFile)):
        if mode.startswith("r"):
            dataset = fp.open(driver=driver, sharing=sharing, thread_safe=thread_safe, **kwargs)

        # Note: FilePath does not support writing and an exception will
        # result from this.
        else:
            dataset = fp.open(
                driver=driver,
                width=width,
                height=height,
                count=count,
                crs=crs,
                transform=transform,
                dtype=dtype,
                nodata=nodata,
                sharing=sharing,
                **kwargs
            )

        return dataset

    # If the fp argument is a file-like object we use a MemoryFile to
    # hold fp's contents and store that in an ExitStack attached to the
    # dataset object that we will return. When a dataset's close method
    # is called, this ExitStack will be unwound and the MemoryFile's
    # storage will be cleaned up.
    elif mode == 'r' and hasattr(fp, 'read'):
        memfile = MemoryFile(fp.read())
        fp.seek(0)
        dataset = memfile.open(driver=driver, sharing=sharing, **kwargs)
        dataset._env.enter_context(memfile)
        return dataset

    elif mode in ('w', 'w+') and hasattr(fp, 'write'):
        memfile = MemoryFile()
        dataset = memfile.open(
            driver=driver,
            width=width,
            height=height,
            count=count,
            crs=crs,
            transform=transform,
            dtype=dtype,
            nodata=nodata,
            sharing=sharing,
            **kwargs
        )
        dataset._env.enter_context(memfile)

        # For the writing case we push an extra callback onto the
        # ExitStack. It ensures that the MemoryFile's contents are
        # copied to the open file object.
        def func(*args, **kwds):
            memfile.seek(0)
            fp.write(memfile.read())

        dataset._env.callback(func)
        return dataset

    # At this point, the fp argument is a string or path-like object
    # which can be converted to a string.
    else:
        stack = ExitStack()

        if hasattr(fp, "path") and hasattr(fp, "fs"):
            log.debug("Detected fp is an OpenFile: fp=%r", fp)
            raw_dataset_path = fp.path
            opener = fp.fs.open
        else:
            raw_dataset_path = os.fspath(fp)

        try:
            # when opener is a callable that takes a filename or URL and returns
            # a file-like object with read, seek, tell, and close methods, we
            # can register it with GDAL and use it as the basis for a GDAL
            # virtual filesystem. This is generally better than the FilePath
            # approach introduced in version 1.3.
            if opener:
                vsi_path_ctx = _opener_registration(raw_dataset_path, opener)
                registered_vsi_path = stack.enter_context(vsi_path_ctx)
                path = _UnparsedPath(registered_vsi_path)
            else:
                path = _parse_path(raw_dataset_path)

            if mode == "r":
                dataset = DatasetReader(path, driver=driver, sharing=sharing, thread_safe=thread_safe, **kwargs)
            elif mode == "r+":
                dataset = get_writer_for_path(path, driver=driver)(
                    path, mode, driver=driver, sharing=sharing, **kwargs
                )
            elif mode.startswith("w"):
                if not driver:
                    driver = driver_from_extension(path)
                writer = get_writer_for_driver(driver)
                if writer is not None:
                    dataset = writer(
                        path,
                        mode,
                        driver=driver,
                        width=width,
                        height=height,
                        count=count,
                        crs=crs,
                        transform=transform,
                        dtype=dtype,
                        nodata=nodata,
                        sharing=sharing,
                        **kwargs
                    )
                else:
                    raise DriverCapabilityError(
                        "Writer does not exist for driver: %s" % str(driver)
                    )
            else:
                raise DriverCapabilityError(
                    "mode must be one of 'r', 'r+', or 'w', not %s" % mode)
        except Exception:
            stack.close()
            raise

        dataset._env = stack
        return dataset


Band = namedtuple('Band', ['ds', 'bidx', 'dtype', 'shape'])
Band.__doc__ = """
Band(s) of a Dataset.

Parameters
----------
ds: dataset object
    An opened rasterio dataset object.
bidx: int or sequence of ints
    Band number(s), index starting at 1.
dtype: str
    rasterio data type of the data.
shape: tuple
    Width, height of band.
"""


def band(ds, bidx):
    """A dataset and one or more of its bands

    Parameters
    ----------
    ds: dataset object
        An opened rasterio dataset object.
    bidx: int or sequence of ints
        Band number(s), index starting at 1.

    Returns
    -------
    Band
    """
    return Band(ds, bidx, set(ds.dtypes).pop(), ds.shape)


def pad(array, transform, pad_width, mode=None, **kwargs):
    """pad array and adjust affine transform matrix.

    Parameters
    ----------
    array: numpy.ndarray
        Numpy ndarray, for best results a 2D array
    transform: Affine transform
        transform object mapping pixel space to coordinates
    pad_width: int
        number of pixels to pad array on all four
    mode: str or function
        define the method for determining padded values

    Returns
    -------
    (array, transform): tuple
        Tuple of new array and affine transform

    Notes
    -----
    See :func:`numpy.pad` for details on mode and other kwargs.
    """
    import numpy as np
    transform = guard_transform(transform)
    padded_array = np.pad(array, pad_width, mode, **kwargs)
    padded_trans = list(transform)
    padded_trans[2] -= pad_width * padded_trans[0]
    padded_trans[5] -= pad_width * padded_trans[4]
    return padded_array, Affine(*padded_trans[:6])
