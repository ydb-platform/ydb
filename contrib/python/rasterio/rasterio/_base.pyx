# cython: boundscheck=False, c_string_type=unicode, c_string_encoding=utf8

"""Numpy-free base classes."""

from collections import defaultdict
from contextlib import ExitStack
import logging
import math
import os
import warnings

from libc.string cimport strncmp
from rasterio.crs cimport CRS

from rasterio._err import (
    GDALError, CPLE_BaseError, CPLE_IllegalArgError, CPLE_OpenFailedError,
    CPLE_NotSupportedError)
from rasterio._err cimport exc_wrap_pointer, exc_wrap_int, exc_wrap

from rasterio.control import GroundControlPoint
from rasterio.rpc import RPC
from rasterio import dtypes
from rasterio.coords import BoundingBox
from rasterio.crs import CRS
from rasterio.dtypes import (
    complex64,
    complex128,
    complex_int16,
    float32,
    float64,
    int16,
)

from rasterio.enums import (
    ColorInterp, Compression, Interleaving, MaskFlags, PhotometricInterp)
from rasterio.env import env_ctx_if_needed, _GDAL_AT_LEAST_3_10
from rasterio.errors import (
    BandOverviewError,
    CRSError,
    DatasetAttributeError,
    DriverRegistrationError,
    GDALOptionNotImplementedError,
    NotGeoreferencedWarning,
    RasterBlockError,
    RasterioIOError,
)
from rasterio.profiles import Profile
from rasterio.transform import Affine, guard_transform, tastes_like_gdal
from rasterio._path import _parse_path
from rasterio import windows

cimport cython

include "gdal.pxi"

log = logging.getLogger(__name__)


cdef const char *get_driver_name(GDALDriverH driver):
    """Return Python name of the driver"""
    return GDALGetDriverShortName(driver)


def get_dataset_driver(path):
    """Get the name of the driver that opens a dataset.

    Parameters
    ----------
    path : rasterio.path.Path or str
        A remote or local dataset path.

    Returns
    -------
    str

    """
    cdef GDALDatasetH dataset = NULL
    cdef GDALDriverH driver = NULL

    path = _parse_path(path).as_vsi()
    path = path.encode('utf-8')

    try:
        dataset = exc_wrap_pointer(GDALOpen(<const char *>path, <GDALAccess>0))
        driver = GDALGetDatasetDriver(dataset)
        drivername = get_driver_name(driver)

    except CPLE_OpenFailedError as exc:
        raise TypeError(str(exc))

    finally:
        if dataset != NULL:
            GDALClose(dataset)

    return drivername


def driver_supports_mode(drivername, creation_mode):
    """Return True if the driver supports the mode"""
    cdef GDALDriverH driver = NULL
    cdef char **metadata = NULL

    drivername = drivername.encode('utf-8')
    creation_mode = creation_mode.encode('utf-8')

    driver = GDALGetDriverByName(<const char *>drivername)
    if driver == NULL:
        raise DriverRegistrationError(
            "No such driver registered: %s", drivername)

    metadata = GDALGetMetadata(driver, NULL)
    if metadata == NULL:
        raise ValueError("Driver has no metadata")

    return bool(CSLFetchBoolean(metadata, <const char *>creation_mode, 0))


def driver_can_create(drivername):
    """Return True if the driver has CREATE capability"""
    return driver_supports_mode(drivername, 'DCAP_CREATE')


def driver_can_create_copy(drivername):
    """Return True if the driver has CREATE_COPY capability"""
    return driver_supports_mode(drivername, 'DCAP_CREATECOPY')


def _raster_driver_extensions():
    """
    Logic based on: https://github.com/rasterio/rasterio/issues/265#issuecomment-367044836
    """
    cdef int iii = 0
    cdef int driver_count = GDALGetDriverCount()
    cdef GDALDriverH driver = NULL
    cdef const char* c_extensions = NULL
    cdef const char* c_drivername = NULL
    driver_extensions = {}
    for iii in range(driver_count):
        driver = GDALGetDriver(iii)
        c_drivername = get_driver_name(driver)
        if (
            GDALGetMetadataItem(driver, "DCAP_RASTER", NULL) == NULL
            or (
                GDALGetMetadataItem(driver, "DCAP_CREATE", NULL) == NULL
                and GDALGetMetadataItem(driver, "DCAP_CREATECOPY", NULL) == NULL
            )
        ):
            continue
        c_extensions = GDALGetMetadataItem(driver, "DMD_EXTENSIONS", NULL)
        if c_extensions == NULL or c_drivername == NULL:
            continue

        try:
            extensions = c_extensions
            extensions = extensions.decode("utf-8")
        except AttributeError:
            pass

        try:
            drivername = c_drivername
            drivername = drivername.decode("utf-8")
        except AttributeError:
            pass

        for extension in extensions.split():
            driver_extensions[extension] = drivername

    # ensure default driver for tif to be GTiff instead of COG
    driver_extensions.update(
        {'tif': 'GTiff', 'tiff': 'GTiff'}
    )
    return driver_extensions


cdef _band_dtype(GDALRasterBandH band):
    """Resolve dtype of a given band"""
    cdef const char * ptype
    cdef int gdal_dtype = GDALGetRasterDataType(band)
    return dtypes.dtype_fwd[gdal_dtype]


cdef GDALDatasetH open_dataset(
    object filename,
    int flags,
    object allowed_drivers,
    object open_options,
    bint sharing,
    object siblings,
) except NULL:
    """Open a dataset and return a handle"""

    cdef GDALDatasetH hds = NULL
    cdef const char *fname = NULL
    cdef char **drivers = NULL
    cdef char **options = NULL

    filename = filename.encode('utf-8')
    fname = filename

    # Construct a null terminated C list of driver
    # names for GDALOpenEx.
    if allowed_drivers:
        for name in allowed_drivers:
            name = name.encode('utf-8')
            drivers = CSLAddString(drivers, <const char *>name)

    if open_options:
        for k, v in open_options.items():
            k = k.upper().encode('utf-8')

            # Normalize values consistent with code in _env module.
            if isinstance(v, bool):
                v = ('ON' if v else 'OFF').encode('utf-8')
            else:
                v = str(v).encode('utf-8')

            options = CSLAddNameValue(options, <const char *>k, <const char *>v)

    # Support for sibling files is not yet implemented.
    if siblings:
        raise NotImplementedError(
            "Sibling files are not implemented")

    # Ensure default flags added
    flags = flags | GDAL_OF_RASTER | GDAL_OF_VERBOSE_ERROR
    if sharing:
        flags |= GDAL_OF_SHARED

    with nogil:
        hds = GDALOpenEx(fname, flags, <const char **>drivers, <const char **>options, NULL)
    try:
        return exc_wrap_pointer(hds)
    finally:
        CSLDestroy(drivers)
        CSLDestroy(options)


cdef class DatasetBase:
    """Dataset base class

    Attributes
    ----------
    block_shapes
    bounds
    closed
    colorinterp
    count
    crs
    descriptions
    files
    gcps
    rpcs
    indexes
    mask_flag_enums
    meta
    nodata
    nodatavals
    profile
    res
    subdatasets
    transform
    units
    compression : str
        Compression algorithm's short name
    driver : str
        Format driver used to open the dataset
    interleaving : str
        'pixel' or 'band'
    kwds : dict
        Stored creation option tags
    mode : str
        Access mode
    name : str
        Remote or local dataset name
    options : dict
        Copy of opening options
    photometric : str
        Photometric interpretation's short name
    """
    def __init__(self, path=None, driver=None, sharing=False, thread_safe=False, **kwargs):
        """Construct a new dataset

        Parameters
        ----------
        path : rasterio.path.Path or str
            Path of the local or remote dataset.
        driver : str or list of str
            A single driver name or a list of driver names to consider when
            opening the dataset.
        sharing : bool, optional
            Whether to share underlying GDAL dataset handles (default: False).
        thread_safe: bool, optional
            Open GDAL dataset in `thread safe mode <https://gdal.org/en/stable/user/multithreading.html>`__.
            For multithreaded read-only GDAL dataset operations (e.g. ``GDAL_NUM_THREADS``, `LIBERTIFF driver <https://gdal.org/en/stable/drivers/raster/libertiff.html#open-options>`__).
            Requires rasterio 1.5+ & GDAL 3.10+.
        kwargs : dict
            GDAL dataset opening options.

        Returns
        -------
        dataset
        """
        self._hds = NULL
        cdef flags = GDAL_OF_READONLY
        if thread_safe:
            if not _GDAL_AT_LEAST_3_10:
                raise GDALOptionNotImplementedError("'thread_safe' option requires GDAL 3.10+.")
            flags |= GDAL_OF_THREAD_SAFE

        if path is not None:
            path = _parse_path(path)
            filename = path.as_vsi()

            # driver may be a string or list of strings. If the
            # former, we put it into a list.
            if isinstance(driver, str):
                driver = [driver]

            try:
                self._hds = open_dataset(
                    filename=filename,
                    flags=flags,
                    allowed_drivers=driver,
                    open_options=kwargs,
                    sharing=sharing,
                    siblings=None,
                )
            except CPLE_BaseError as err:
                raise RasterioIOError(str(err))

            self.name = path.name
        else:
            self.name = None

        self.mode = 'r'
        self.options = kwargs.copy()
        self._dtypes = []
        self._block_shapes = None
        self._nodatavals = []
        self._units = ()
        self._descriptions = ()
        self._scales = ()
        self._offsets = ()
        self._gcps = None
        self._rpcs = None
        self._read = False

        self._set_attrs_from_dataset_handle()
        self._env = ExitStack()

    def __repr__(self):
        return "<%s DatasetBase name='%s' mode='%s'>" % (
            self.closed and 'closed' or 'open',
            self.name,
            self.mode)

    def _set_attrs_from_dataset_handle(self):
        cdef GDALDriverH driver = NULL
        driver = GDALGetDatasetDriver(self._hds)
        self.driver = get_driver_name(driver)
        self._count = GDALGetRasterCount(self._hds)
        self.width = GDALGetRasterXSize(self._hds)
        self.height = GDALGetRasterYSize(self._hds)
        self.shape = (self.height, self.width)
        self._transform = self.read_transform()
        self._crs = self.read_crs()

        # touch self.meta, triggering data type evaluation.
        _ = self.meta

        log.debug("Dataset %r is started.", self)

    cdef GDALDatasetH handle(self) except NULL:
        """Return the object's GDAL dataset handle"""
        if self._hds == NULL:
            raise RasterioIOError("Dataset is closed: {}".format(self.name))
        else:
            return self._hds

    cdef GDALRasterBandH band(self, int bidx) except NULL:
        """Return a GDAL raster band handle"""
        cdef GDALRasterBandH band = NULL
        band = GDALGetRasterBand(self.handle(), bidx)
        if band == NULL:
            raise IndexError("No such band index: {!s}".format(bidx))
        return band

    def _has_band(self, bidx):
        cdef GDALRasterBandH band = NULL
        try:
            band = self.band(bidx)
            return True
        except:
            return False

    def _handle_crswkt(self, wkt):
        """Return the GDAL dataset's stored CRS"""
        # No dialect morphing, if the dataset was created using software
        # "speaking" the Esri dialect, we will read Esri WKT.
        if wkt:
            return CRS.from_wkt(wkt)
        else:
            return None

    def _has_gcps_or_rpcs(self):
        """Check if we have gcps or rpcs"""
        cdef int num_gcps

        num_gcps = GDALGetGCPCount(self.handle())
        if num_gcps:
            return True

        rpcs = self.tags(ns="RPC")
        if rpcs:
            return True

        return False

    def read_crs(self):
        """Return the GDAL dataset's stored CRS"""
        cdef const char *wkt = GDALGetProjectionRef(self.handle())
        if wkt == NULL:
            raise ValueError("Unexpected NULL spatial reference")
        return self._handle_crswkt(wkt)

    def read_transform(self):
        """Return the stored GDAL GeoTransform"""
        cdef double gt[6]

        if self._hds == NULL:
            raise ValueError("Null dataset")
        err = GDALGetGeoTransform(self._hds, gt)
        if err == GDALError.failure and not self._has_gcps_or_rpcs():
            warnings.warn(
                ("Dataset has no geotransform, gcps, or rpcs. "
                "The identity matrix will be returned."),
                NotGeoreferencedWarning)

        return [gt[i] for i in range(6)]

    def start(self):
        """Start the dataset's life cycle"""
        pass

    def stop(self):
        """Close the GDAL dataset handle"""
        if self._hds == NULL:
            return
        refcount = GDALDereferenceDataset(self._hds)
        if refcount == 0:
            GDALClose(self._hds)
        self._hds = NULL

    def close(self):
        """Close the dataset and unwind attached exit stack."""
        self.stop()
        if self._env:
            self._env.close()

    def __enter__(self):
        self._env.enter_context(env_ctx_if_needed())
        return self

    def __exit__(self, *exc_details):
        self.close()

    def __dealloc__(self):
        self.stop()

    @property
    def closed(self):
        """Test if the dataset is closed

        Returns
        -------
        bool
        """
        return self._hds == NULL

    @property
    def count(self):
        """The number of raster bands in the dataset

        Returns
        -------
        int
        """
        if not self._count:
            if self._hds == NULL:
                raise ValueError("Can't read closed raster file")
            self._count = GDALGetRasterCount(self._hds)
        return self._count

    @property
    def indexes(self):
        """The 1-based indexes of each band in the dataset

        For a 3-band dataset, this property will be ``[1, 2, 3]``.

        Returns
        -------
        list of int
        """
        return tuple(range(1, self.count+1))

    @property
    def dtypes(self):
        """The data types of each band in index order

        Returns
        -------
        list of str
        """
        cdef GDALRasterBandH band = NULL

        if not self._dtypes:
            for i in range(self._count):
                band = self.band(i + 1)
                self._dtypes.append(_band_dtype(band))

        return tuple(self._dtypes)

    @property
    def block_shapes(self):
        """An ordered list of block shapes for each bands

        Shapes are tuples and have the same ordering as the dataset's
        shape: (count of image rows, count of image columns).

        Returns
        -------
        list
        """
        cdef GDALRasterBandH band = NULL
        cdef int xsize = 0
        cdef int ysize = 0

        if self._block_shapes is None:
            self._block_shapes = []

            for i in range(self._count):
                band = self.band(i + 1)
                GDALGetBlockSize(band, &xsize, &ysize)
                self._block_shapes.append((ysize, xsize))

        return list(self._block_shapes)

    def get_nodatavals(self):
        cdef GDALRasterBandH band = NULL
        cdef double nodataval
        cdef int success = 0

        if not self._nodatavals:

            for i in range(self._count):
                band = self.band(i + 1)
                dtype = _band_dtype(band)

                # To address the issue in #1747.
                if dtype == complex128:
                    dtype = float64
                elif dtype == complex64:
                    dtype = float32
                elif dtype == complex_int16:
                    dtype = int16

                nodataval = GDALGetRasterNoDataValue(band, &success)
                val = nodataval

                # GDALGetRasterNoDataValue() has two ways of telling you that
                # there's no nodata value. The success flag might come back
                # 0 (FALSE). Even if it comes back 1 (TRUE), you still need
                # to check that the return value is within the range of the
                # data type. If so, the band has a nodata value. If not,
                # there's no nodata value.
                if dtype not in dtypes.dtype_ranges:
                    pass
                elif (success == 0 or not dtypes.in_dtype_range(val, dtype)):
                    val = None
                log.debug(
                    "Nodata success: %d, Nodata value: %f", success, nodataval)
                self._nodatavals.append(val)

        return tuple(self._nodatavals)

    @property
    def nodatavals(self):
        """Nodata values for each band

        Notes
        -----
        This may not be set.

        Returns
        -------
        list of float
        """
        return self.get_nodatavals()

    def _set_nodatavals(self, value):
        raise DatasetAttributeError("read-only attribute")

    @property
    def nodata(self):
        """The dataset's single nodata value

        Notes
        -----
        May be set.

        Returns
        -------
        float
        """
        if self.count == 0:
            return None
        return self.nodatavals[0]

    @nodata.setter
    def nodata(self, value):
        self._set_nodatavals([value for old_val in self.nodatavals])

    def _mask_flags(self):
        """Mask flags for each band."""
        cdef GDALRasterBandH band = NULL
        return tuple(GDALGetMaskFlags(self.band(j)) for j in self.indexes)

    @property
    def mask_flag_enums(self):
        """Sets of flags describing the sources of band masks.

        Parameters
        ----------

        all_valid: There are no invalid pixels, all mask values will be
            255. When used this will normally be the only flag set.
        per_dataset: The mask band is shared between all bands on the
            dataset.
        alpha: The mask band is actually an alpha band and may have
            values other than 0 and 255.
        nodata: Indicates the mask is actually being generated from
            nodata values (mutually exclusive of "alpha").

        Returns
        -------
        list [, list*]
            One list of rasterio.enums.MaskFlags members per band.

        Examples
        --------

        For a 3 band dataset that has masks derived from nodata values:

        >>> dataset.mask_flag_enums
        ([<MaskFlags.nodata: 8>], [<MaskFlags.nodata: 8>], [<MaskFlags.nodata: 8>])
        >>> band1_flags = dataset.mask_flag_enums[0]
        >>> rasterio.enums.MaskFlags.nodata in band1_flags
        True
        >>> rasterio.enums.MaskFlags.alpha in band1_flags
        False
        """
        return tuple(
            [flag for flag in MaskFlags if x & flag.value]
            for x in self._mask_flags())

    def _set_crs(self, value):
        raise DatasetAttributeError("read-only attribute")

    @property
    def crs(self):
        """The dataset's coordinate reference system

        In setting this property, the value may be a CRS object or an
        EPSG:nnnn or WKT string.

        Returns
        -------
        CRS
        """
        return self._get_crs()

    @crs.setter
    def crs(self, value):
        self._set_crs(value)

    def _set_all_descriptions(self, value):
        raise DatasetAttributeError("read-only attribute")

    def _set_all_scales(self, value):
        raise DatasetAttributeError("read-only attribute")

    def _set_all_offsets(self, value):
        raise DatasetAttributeError("read-only attribute")

    def _set_all_units(self, value):
        raise DatasetAttributeError("read-only attribute")

    @property
    def descriptions(self):
        """Descriptions for each dataset band

        To set descriptions, one for each band is required.

        Returns
        -------
        tuple[str | None, ...]
        """
        if not self._descriptions:
            descr = [GDALGetDescription(self.band(j)) for j in self.indexes]
            self._descriptions = tuple((d or None) for d in descr)
        return self._descriptions

    @descriptions.setter
    def descriptions(self, value):
        self._set_all_descriptions(value)

    def write_transform(self, value):
        raise DatasetAttributeError("read-only attribute")

    @property
    def transform(self):
        """The dataset's georeferencing transformation matrix

        This transform maps pixel row/column coordinates to coordinates
        in the dataset's coordinate reference system.

        Returns
        -------
        Affine
        """
        return Affine.from_gdal(*self.get_transform())

    @transform.setter
    def transform(self, value):
        self.write_transform(value.to_gdal())

    @property
    def offsets(self):
        """Raster offset for each dataset band

        To set offsets, one for each band is required.

        Returns
        -------
        list of float
        """
        cdef int success = 0
        if not self._offsets:
            offsets = [GDALGetRasterOffset(self.band(j), &success) for j in self.indexes]
            self._offsets = tuple(offsets)
        return self._offsets

    @offsets.setter
    def offsets(self, value):
        self._set_all_offsets(value)

    @property
    def scales(self):
        """Raster scale for each dataset band

        To set scales, one for each band is required.

        Returns
        -------
        list of float
        """
        cdef int success = 0
        if not self._scales:
            scales = [GDALGetRasterScale(self.band(j), &success) for j in self.indexes]
            self._scales = tuple(scales)
        return self._scales

    @scales.setter
    def scales(self, value):
        self._set_all_scales(value)

    @property
    def units(self):
        """A list of str: one units string for each dataset band

        Possible values include 'meters' or 'degC'. See the Pint
        project for a suggested list of units.

        To set units, one for each band is required.

        Returns
        -------
        list of str
        """
        if not self._units:
            units = [GDALGetRasterUnitType(self.band(j)) for j in self.indexes]
            self._units = tuple((u or None) for u in units)
        return self._units

    @units.setter
    def units(self, value):
        self._set_all_units(value)

    def block_window(self, bidx, i, j):
        """Returns the window for a particular block

        Parameters
        ----------
        bidx: int
            Band index, starting with 1.
        i: int
            Row index of the block, starting with 0.
        j: int
            Column index of the block, starting with 0.

        Returns
        -------
        Window
        """
        h, w = self.block_shapes[bidx-1]
        row = i * h
        height = min(h, self.height - row)
        col = j * w
        width = min(w, self.width - col)
        return windows.Window(col, row, width, height)

    def block_size(self, bidx, i, j):
        """Returns the size in bytes of a particular block

        Only useful for TIFF formatted datasets.

        Parameters
        ----------
        bidx: int
            Band index, starting with 1.
        i: int
            Row index of the block, starting with 0.
        j: int
            Column index of the block, starting with 0.

        Returns
        -------
        int
        """
        cdef GDALMajorObjectH obj = NULL
        cdef const char *value = NULL
        cdef const char *key_c = NULL

        obj = self.band(bidx)

        key_b = 'BLOCK_SIZE_{0}_{1}'.format(j, i).encode('utf-8')
        key_c = key_b
        value = GDALGetMetadataItem(obj, key_c, 'TIFF')
        if value == NULL:
            raise RasterBlockError(
                "Block i={0}, j={1} size can't be determined".format(i, j))
        else:
            return int(value)

    def block_windows(self, bidx=0):
        """Iterator over a band's blocks and their windows


        The primary use of this method is to obtain windows to pass to
        `read()` for highly efficient access to raster block data.

        The positional parameter `bidx` takes the index (starting at 1) of the
        desired band.  This iterator yields blocks "left to right" and "top to
        bottom" and is similar to Python's ``enumerate()`` in that the first
        element is the block index and the second is the dataset window.

        Blocks are built-in to a dataset and describe how pixels are grouped
        within each band and provide a mechanism for efficient I/O.  A window
        is a range of pixels within a single band defined by row start, row
        stop, column start, and column stop.  For example, ``((0, 2), (0, 2))``
        defines a ``2 x 2`` window at the upper left corner of a raster band.
        Blocks are referenced by an ``(i, j)`` tuple where ``(0, 0)`` would be
        a band's upper left block.

        Raster I/O is performed at the block level, so accessing a window
        spanning multiple rows in a striped raster requires reading each row.
        Accessing a ``2 x 2`` window at the center of a ``1800 x 3600`` image
        requires reading 2 rows, or 7200 pixels just to get the target 4.  The
        same image with internal ``256 x 256`` blocks would require reading at
        least 1 block (if the window entire window falls within a single block)
        and at most 4 blocks, or at least 512 pixels and at most 2048.

        Given an image that is ``512 x 512`` with blocks that are
        ``256 x 256``, its blocks and windows would look like::

            Blocks:

                    0       256     512
                  0 +--------+--------+
                    |        |        |
                    | (0, 0) | (0, 1) |
                    |        |        |
                256 +--------+--------+
                    |        |        |
                    | (1, 0) | (1, 1) |
                    |        |        |
                512 +--------+--------+


            Windows:

                UL: ((0, 256), (0, 256))
                UR: ((0, 256), (256, 512))
                LL: ((256, 512), (0, 256))
                LR: ((256, 512), (256, 512))


        Parameters
        ----------
        bidx : int, optional
            The band index (using 1-based indexing) from which to extract
            windows. A value less than 1 uses the first band if all bands have
            homogeneous windows and raises an exception otherwise.

        Yields
        ------
        block, window
        """
        cdef int i, j

        block_shapes = self.block_shapes
        if bidx < 1:
            if len(set(block_shapes)) > 1:
                raise ValueError(
                    "A band index must be provided when band block shapes"
                    "are inhomogeneous")
            bidx = 1
        h, w = block_shapes[bidx-1]
        d, m = divmod(self.height, h)
        nrows = d + int(m>0)
        d, m = divmod(self.width, w)
        ncols = d + int(m>0)

        # We could call self.block_window() inside the loops but this
        # is faster and doesn't duplicate much code.
        for j in range(nrows):
            row = j * h
            height = min(h, self.height - row)
            for i in range(ncols):
                col = i * w
                width = min(w, self.width - col)
                yield (j, i), windows.Window(
                    col_off=col, row_off=row, width=width, height=height)

    @property
    def bounds(self):
        """Returns the lower left and upper right bounds of the dataset
        in the units of its coordinate reference system.

        The returned value is a tuple:
        (lower left x, lower left y, upper right x, upper right y)
        """
        a, b, c, d, e, f, _, _, _ = self.transform
        width = self.width
        height = self.height
        if b == d == 0:
            return BoundingBox(c, f + e * height, c + a * width, f)
        else:
            c0x, c0y = c, f
            c1x, c1y = self.transform * (0, height)
            c2x, c2y = self.transform * (width, height)
            c3x, c3y = self.transform * (width, 0)
            xs = (c0x, c1x, c2x, c3x)
            ys = (c0y, c1y, c2y, c3y)
            return BoundingBox(min(xs), min(ys), max(xs), max(ys))

    @property
    def res(self):
        """Returns the (width, height) of pixels in the units of its
        coordinate reference system."""
        a, b, c, d, e, f, _, _, _ = self.transform
        return math.sqrt(a * a+ d * d), math.sqrt(b * b + e * e)

    @property
    def meta(self):
        """The basic metadata of this dataset."""
        if self.count == 0:
            dtype = 'float_'
        else:
            dtype = self.dtypes[0]
        m = {
            'driver': self.driver,
            'dtype': dtype,
            'nodata': self.nodata,
            'width': self.width,
            'height': self.height,
            'count': self.count,
            'crs': self.crs,
            'transform': self.transform,
        }
        self._read = True
        return m

    @property
    def compression(self):
        val = self.tags(ns='IMAGE_STRUCTURE').get('COMPRESSION')
        if val:
            # 'YCbCr JPEG' will be normalized to 'JPEG'
            val = val.split()[-1]
            try:
                return Compression(val)
            except ValueError:
                return val
        else:
            return None

    @property
    def interleaving(self):
        val = self.tags(ns='IMAGE_STRUCTURE').get('INTERLEAVE')
        if val:
            return Interleaving(val)
        else:
            return None

    @property
    def photometric(self):
        val = self.tags(ns='IMAGE_STRUCTURE').get('SOURCE_COLOR_SPACE')
        if val:
            return PhotometricInterp(val)
        else:
            return None

    @property
    def is_tiled(self):
        warnings.warn(
            "is_tiled will be removed in a future version. "
            "Please consider copying the body of this function "
            "into your program or module.",
            PendingDeprecationWarning
        )
        # It's rare but possible that a dataset's bands have different
        # block structure. Therefore we check them all against the
        # width of the dataset.
        return self.block_shapes and all(self.width != w for _, w in self.block_shapes)

    @property
    def profile(self):
        """Basic metadata and creation options of this dataset.

        May be passed as keyword arguments to `rasterio.open()` to
        create a clone of this dataset.
        """
        m = Profile(**self.meta)

        if self.block_shapes:
            m.update(
                blockxsize=self.block_shapes[0][1],
                blockysize=self.block_shapes[0][0],
            )

        # It's rare but possible that a dataset's bands have different
        # block structure. Therefore we check them all against the
        # width of the dataset.
        m.update(tiled=self.block_shapes and all(self.width != w for _, w in self.block_shapes))

        if self.compression:
            m['compress'] = getattr(self.compression, "name", self.compression)
        if self.interleaving:
            m['interleave'] = self.interleaving.name
        if self.photometric:
            m['photometric'] = self.photometric.name
        return m

    def lnglat(self) -> tuple[float, float]:
        """Geographic coordinates of the dataset's center.

        Returns
        -------
        (longitude, latitude) of centroid.

        """
        w, s, e, n = self.bounds
        cx = (w + e) / 2.0
        cy = (s + n) / 2.0
        lng, lat = _transform(self.crs, "EPSG:4326", [cx], [cy], None)
        return lng.pop(), lat.pop()

    def _get_crs(self):
        # _read tells us that the CRS was read before and really is
        # None.
        if not self._read and self._crs is None:
            self._crs = self.read_crs()
        return self._crs

    def get_transform(self):
        """Returns a GDAL geotransform in its native form."""
        if not self._read and self._transform is None:
            self._transform = self.read_transform()
        return self._transform

    @property
    def subdatasets(self):
        """Sequence of subdatasets"""
        tags = self.tags(ns='SUBDATASETS')
        subs = defaultdict(dict)
        for key, val in tags.items():
            _, idx, fld = key.split('_')
            fld = fld.lower()
            if fld == 'desc':
                fld = 'description'
            if fld == 'name':
                val = val.replace('NETCDF', 'netcdf')
            subs[idx][fld] = val.replace('"', '')
        return [subs[idx]['name'] for idx in sorted(subs.keys())]


    def tag_namespaces(self, bidx=0):
        """Get a list of the dataset's metadata domains.

        Returned items may be passed as `ns` to the tags method.

        Parameters
        ----------
        bidx int, optional
            Can be used to select a specific band, otherwise the
            dataset's general metadata domains are returned.

        Returns
        -------
        list of str

        """
        cdef GDALMajorObjectH obj = NULL
        if bidx > 0:
            obj = self.band(bidx)
        else:
            obj = self._hds

        namespaces = GDALGetMetadataDomainList(obj)
        num_items = CSLCount(namespaces)
        try:
            return list([namespaces[i] for i in range(num_items) if str(namespaces[i])])
        finally:
            CSLDestroy(namespaces)


    def tags(self, bidx=0, ns=None):
        """Returns a dict containing copies of the dataset or band's
        tags.

        Tags are pairs of key and value strings. Tags belong to
        namespaces.  The standard namespaces are: default (None) and
        'IMAGE_STRUCTURE'.  Applications can create their own additional
        namespaces.

        The optional bidx argument can be used to select the tags of
        a specific band. The optional ns argument can be used to select
        a namespace other than the default.
        """
        cdef GDALMajorObjectH obj = NULL
        cdef char **metadata = NULL
        cdef char *item = NULL
        cdef const char *domain = NULL
        cdef char *key = NULL
        cdef const char *val = NULL

        if bidx > 0:
            obj = self.band(bidx)
        else:
            obj = self._hds
        if ns:
            ns = ns.encode('utf-8')
            domain = ns

        metadata = GDALGetMetadata(obj, domain)
        num_items = CSLCount(metadata)

        tag_items = []
        if num_items and (domain and domain.startswith("xml")):
            # https://gdal.org/user/raster_data_model.html#xml-domains
            tag_items.append((domain[:], metadata[0][:]))
        else:
            for i in range(num_items):
                item = <char *>metadata[i]
                try:
                    val = CPLParseNameValue(metadata[i], &key)
                    tag_items.append((key[:], val[:]))
                except UnicodeDecodeError:
                    item_bytes = <bytes>item
                    log.warning("Failed to decode metadata item: i=%r, item=%r", i, item_bytes)
                finally:
                    CPLFree(key)

        return dict(tag_items)


    def get_tag_item(self, ns, dm=None, bidx=0, ovr=None):
        """Returns tag item value

        Parameters
        ----------
        ns: str
            The key for the metadata item to fetch.
        dm: str
            The domain to fetch for.
        bidx: int
            Band index, starting with 1.
        ovr: int
            Overview level

        Returns
        -------
        str
        """
        cdef GDALMajorObjectH band = NULL
        cdef GDALMajorObjectH obj = NULL
        cdef const char *value = NULL
        cdef const char *name = NULL
        cdef const char *domain = NULL

        ns = ns.encode('utf-8')
        name = ns

        if dm:
            dm = dm.encode('utf-8')
            domain = dm

        if not bidx > 0 and ovr:
            raise Exception("Band index (bidx) option needed for overview level")

        if bidx > 0:
            band = self.band(bidx)
        else:
            band = self._hds

        if ovr is not None:
            obj = GDALGetOverview(band, ovr)
            if obj == NULL:
              raise BandOverviewError(
                  "Failed to retrieve overview {}".format(ovr))
        else:
            obj = band

        value = GDALGetMetadataItem(obj, name, domain)
        if value == NULL:
            return None
        else:
            return value

    @property
    def colorinterp(self):
        """A sequence of ``ColorInterp.<enum>`` in band order.

        Returns
        -------
        tuple
        """

        cdef GDALRasterBandH band = NULL

        out = []
        for bidx in self.indexes:
            value = exc_wrap_int(
                GDALGetRasterColorInterpretation(self.band(bidx)))
            out.append(ColorInterp(value))
        return tuple(out)

    @colorinterp.setter
    def colorinterp(self, value):

        """Set band color interpretation with a sequence of
        ``ColorInterp.<enum>`` in band order.

        Parameters
        ----------
        value : iter
            A sequence of ``ColorInterp.<enum>``.

        Examples
        --------

        To set color interpretation, provide a sequence of
        ``ColorInterp.<enum>``:

        .. code-block:: python

            import rasterio
            from rasterio.enums import ColorInterp

            with rasterio.open('rgba.tif', 'r+') as src:
                src.colorinterp = (
                    ColorInterp.red,
                    ColorInterp.green,
                    ColorInterp.blue,
                    ColorInterp.alpha)
        """
        if self.mode == 'r':
            raise RasterioIOError(
                "Can only set color interpretation when dataset is "
                "opened in 'r+' or 'w' mode, not '{}'.".format(self.mode))
        if len(value) != len(self.indexes):
            raise ValueError(
                "Must set color interpretation for all bands.  Found "
                "{} bands but attempting to set color interpretation to: "
                "{}".format(len(self.indexes), value))

        for bidx, ci in zip(self.indexes, value):
            exc_wrap_int(
                GDALSetRasterColorInterpretation(self.band(bidx), <GDALColorInterp>ci.value))

    def colormap(self, bidx):
        """Returns a dict containing the colormap for a band.

        Parameters
        ----------
        bidx : int
            Index of the band whose colormap will be returned. Band index
            starts at 1.

        Returns
        -------
        dict
            Mapping of color index value (starting at 0) to RGBA color as a
            4-element tuple.

        Raises
        ------
        ValueError
            If no colormap is found for the specified band (NULL color table).
        IndexError
            If no band exists for the provided index.

        """
        cdef GDALRasterBandH band = NULL
        cdef GDALColorTableH colortable = NULL
        cdef GDALColorEntry *color = NULL
        cdef int i

        band = self.band(bidx)
        colortable = GDALGetRasterColorTable(band)
        if colortable == NULL:
            raise ValueError("NULL color table")
        retval = {}

        for i in range(GDALGetColorEntryCount(colortable)):
            color = <GDALColorEntry*>GDALGetColorEntry(colortable, i)
            if color == NULL:
                log.warn("NULL color at %d, skipping", i)
                continue
            log.debug(
                "Color: (%d, %d, %d, %d)",
                color.c1, color.c2, color.c3, color.c4)
            retval[i] = (color.c1, color.c2, color.c3, color.c4)

        return retval

    def overviews(self, bidx):
        cdef GDALRasterBandH ovrband = NULL
        cdef GDALRasterBandH band = NULL

        band = self.band(bidx)
        num_overviews = GDALGetOverviewCount(band)
        factors = []

        for i in range(num_overviews):
            ovrband = GDALGetOverview(band, i)
            # Compute the overview factor only from the xsize (width).
            xsize = GDALGetRasterBandXSize(ovrband)
            factors.append(int(round(float(self.width)/float(xsize))))

        return factors

    def checksum(self, bidx, window=None):
        """Compute an integer checksum for the stored band

        Parameters
        ----------
        bidx : int
            The band's index (1-indexed).
        window: tuple, optional
            A window of the band. Default is the entire extent of the band.

        Returns
        -------
        An int.
        """
        cdef GDALRasterBandH band = NULL
        cdef int xoff, yoff, width, height

        band = self.band(bidx)
        if not window:
            xoff = yoff = 0
            width, height = self.width, self.height
        else:
            window = windows.evaluate(window, self.height, self.width)
            window = windows.crop(window, self.height, self.width)
            xoff = window.col_off
            width = window.width
            yoff = window.row_off
            height = window.height

        try:
            return exc_wrap(GDALChecksumImage(band, xoff, yoff, width, height))
        except CPLE_BaseError as err:
            raise RasterioIOError(str(err))


    def get_gcps(self):
        """Get GCPs and their associated CRS."""
        cdef const char *wkt_b = GDALGetGCPProjection(self.handle())
        if wkt_b == NULL:
            raise ValueError("Unexpected NULL spatial reference")
        wkt = wkt_b
        crs = self._handle_crswkt(wkt)

        cdef const GDAL_GCP *gcplist = NULL
        gcplist = GDALGetGCPs(self.handle())
        num_gcps = GDALGetGCPCount(self.handle())

        return ([GroundControlPoint(col=gcplist[i].dfGCPPixel,
                                         row=gcplist[i].dfGCPLine,
                                         x=gcplist[i].dfGCPX,
                                         y=gcplist[i].dfGCPY,
                                         z=gcplist[i].dfGCPZ,
                                         id=gcplist[i].pszId,
                                         info=gcplist[i].pszInfo)
                                         for i in range(num_gcps)], crs)

    def _set_gcps(self, values):
        raise DatasetAttributeError("read-only attribute")

    @property
    def gcps(self):
        """ground control points and their coordinate reference system.

        This property is a 2-tuple, or pair: (gcps, crs).

        gcps : list of GroundControlPoint
            Zero or more ground control points.
        crs: CRS
            The coordinate reference system of the ground control points.
        """
        if not self._gcps:
            self._gcps = self.get_gcps()
        return self._gcps

    @gcps.setter
    def gcps(self, value):
        gcps, crs = value
        self._set_gcps(gcps, crs)

    def _get_rpcs(self):
        """Get RPCs if exists"""
        md = self.tags(ns='RPC')
        if md:
            return RPC.from_gdal(md)

    def _set_rpcs(self, values):
        raise DatasetAttributeError("read-only attribute")

    @property
    def rpcs(self):
        """Rational polynomial coefficients mapping between pixel and geodetic coordinates.

        This property is a dict-like object.

        rpcs : RPC instance containing coefficients. Empty if dataset does not have any
        metadata in the "RPC" domain.
        """
        if not self._rpcs:
            self._rpcs = self._get_rpcs()
        return self._rpcs

    @rpcs.setter
    def rpcs(self, value):
        rpcs = value.to_gdal()
        self._set_rpcs(rpcs)

    @property
    def files(self):
        """Returns a sequence of files associated with the dataset.

        Returns
        -------
        tuple
        """
        cdef GDALDatasetH h_dataset = NULL
        h_dataset = self.handle()
        with nogil:
            file_list = GDALGetFileList(h_dataset)
        num_items = CSLCount(file_list)
        try:
            return list([file_list[i] for i in range(num_items)])
        finally:
            CSLDestroy(file_list)


def _transform(src_crs, dst_crs, xs, ys, zs):
    """Transform input arrays from src to dst CRS."""
    cdef double *x = NULL
    cdef double *y = NULL
    cdef double *z = NULL
    cdef OGRCoordinateTransformationH transform = NULL
    cdef int i

    assert len(xs) == len(ys)
    assert zs is None or len(xs) == len(zs)

    cdef CRS src = CRS.from_user_input(src_crs)
    cdef CRS dst = CRS.from_user_input(dst_crs)

    n = len(xs)
    x = <double *>CPLMalloc(n*sizeof(double))
    y = <double *>CPLMalloc(n*sizeof(double))
    for i in range(n):
        x[i] = xs[i]
        y[i] = ys[i]

    if zs is not None:
        z = <double *>CPLMalloc(n*sizeof(double))
        for i in range(n):
            z[i] = zs[i]

    try:
        transform = OCTNewCoordinateTransformation(src._osr, dst._osr)
        transform = exc_wrap_pointer(transform)
        # OCTTransform() returns TRUE/FALSE contrary to most GDAL API functions
        exc_wrap_int(OCTTransform(transform, n, x, y, z) == 0)
    else:
        res_xs = [0]*n
        res_ys = [0]*n
        for i in range(n):
            res_xs[i] = x[i]
            res_ys[i] = y[i]
        if zs is not None:
            res_zs = [0]*n
            for i in range(n):
                res_zs[i] = z[i]
            return (res_xs, res_ys, res_zs)
        else:
            return (res_xs, res_ys)
    finally:
        CPLFree(x)
        CPLFree(y)
        CPLFree(z)
        OCTDestroyCoordinateTransformation(transform)


def _can_create_osr(crs):
    """Evaluate if a valid OGRSpatialReference can be created from crs.

    Specifically, it must not be None or an empty dict or string.

    Parameters
    ----------
    crs: Source coordinate reference system, in rasterio dict format.

    Returns
    -------
    out: bool
        True if source coordinate reference appears valid.
    """

    try:
        wkt = CRS.from_user_input(crs).to_wkt()
        # If input was empty, WKT can be too; otherwise the conversion
        # didn't work properly and indicates an error.
        return bool(wkt)
    except CRSError:
        return False


cdef void osr_set_traditional_axis_mapping_strategy(OGRSpatialReferenceH hSrs):
    OSRSetAxisMappingStrategy(hSrs, OAMS_TRADITIONAL_GIS_ORDER)
