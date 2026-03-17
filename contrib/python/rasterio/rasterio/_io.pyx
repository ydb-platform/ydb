# cython: boundscheck=False

"""Rasterio input/output."""

from enum import Enum, IntEnum
from collections import Counter
from contextlib import contextmanager, ExitStack
import logging
import os
import sys
from uuid import uuid4
import warnings

import attr
import numpy as np

from rasterio._base import tastes_like_gdal
from rasterio._base cimport open_dataset
from rasterio._env import catch_errors
from rasterio._err import (
    GDALError, CPLE_AppDefinedError, CPLE_OpenFailedError, CPLE_IllegalArgError, CPLE_BaseError,
    CPLE_AWSObjectNotFoundError, CPLE_HttpResponseError, stack_errors)
from rasterio.crs import CRS
from rasterio import dtypes
from rasterio.enums import ColorInterp, MaskFlags, Resampling
from rasterio.errors import (
    CRSError, DriverRegistrationError, RasterioIOError,
    NotGeoreferencedWarning, NodataShadowWarning, WindowError,
    UnsupportedOperation, OverviewCreationError, RasterBlockError, InvalidArrayError,
    StatisticsError, RasterioDeprecationWarning
)
from rasterio.dtypes import (
    is_ndarray,
    _is_complex_int,
    _getnpdtype,
    _gdal_typename,
    _get_gdal_dtype,
    int8,
    uint8,
)

from rasterio.sample import sample_gen
from rasterio.transform import Affine
from rasterio._path import _parse_path, _UnparsedPath
from rasterio.vrt import _boundless_vrt_doc
from rasterio.windows import Window, intersection

from libc.stdio cimport FILE

from rasterio.enums import Resampling
from rasterio.env import Env, GDALVersion
from rasterio.errors import ResamplingAlgorithmError, DatasetIOShapeError
from rasterio._base cimport get_driver_name, DatasetBase
from rasterio._err cimport exc_wrap_int, exc_wrap_pointer, exc_wrap_vsilfile, StackChecker

cimport numpy as np

np.import_array()

log = logging.getLogger(__name__)


def validate_resampling(resampling):
    """Validate that the resampling method is compatible of reads/writes."""
    if resampling != Resampling.rms and resampling > 7:
        raise ResamplingAlgorithmError("{!r} can be used for warp operations but not for reads and writes".format(Resampling(resampling)))


cdef int io_band(GDALRasterBandH band, int mode, double x0, double y0,
                 double width, double height, object data, int resampling=0) except -1:
    """Read or write a region of data for the band.

    Implicit are

    1) data type conversion if the dtype of `data` and `band` differ.
    2) decimation if `data` and `band` shapes differ.

    The striding of `data` is passed to GDAL so that it can navigate
    the layout of ndarray views.

    """
    validate_resampling(resampling)

    # GDAL handles all the buffering indexing, so a typed memoryview,
    # as in previous versions, isn't needed.
    cdef void *buf = <void *>np.PyArray_DATA(data)
    cdef int bufxsize = data.shape[1]
    cdef int bufysize = data.shape[0]
    cdef GDALDataType buftype = _get_gdal_dtype(data.dtype.name)
    cdef GSpacing bufpixelspace = data.strides[1]
    cdef GSpacing buflinespace = data.strides[0]

    cdef int xoff = <int>x0
    cdef int yoff = <int>y0
    cdef int xsize = <int>width
    cdef int ysize = <int>height
    cdef int retval = 3

    cdef StackChecker checker
    cdef GDALRasterIOExtraArg extras
    extras.nVersion = 1
    extras.eResampleAlg = <GDALRIOResampleAlg>resampling
    extras.bFloatingPointWindowValidity = 1
    extras.dfXOff = x0
    extras.dfYOff = y0
    extras.dfXSize = width
    extras.dfYSize = height
    extras.pfnProgress = NULL
    extras.pProgressData = NULL

    with stack_errors() as checker:
        with nogil:
            retval = GDALRasterIOEx(
                band, <GDALRWFlag>mode, xoff, yoff, xsize, ysize, buf, bufxsize, bufysize,
                buftype, bufpixelspace, buflinespace, &extras)
        return checker.exc_wrap_int(retval)


cdef int io_multi_band(GDALDatasetH hds, int mode, double x0, double y0,
                       double width, double height, object data,
                       Py_ssize_t[:] indexes, int resampling=0) except -1:
    """Read or write a region of data for multiple bands.

    Implicit are

    1) data type conversion if the dtype of `data` and bands differ.
    2) decimation if `data` and band shapes differ.

    The striding of `data` is passed to GDAL so that it can navigate
    the layout of ndarray views.

    """
    validate_resampling(resampling)

    cdef StackChecker checker
    cdef int i = 0
    cdef int retval = 3
    cdef int *bandmap = NULL
    cdef void *buf = <void *>np.PyArray_DATA(data)
    cdef int bufxsize = data.shape[2]
    cdef int bufysize = data.shape[1]
    cdef GDALDataType buftype = _get_gdal_dtype(data.dtype.name)
    cdef GSpacing bufpixelspace = data.strides[2]
    cdef GSpacing buflinespace = data.strides[1]
    cdef GSpacing bufbandspace = data.strides[0]
    cdef int count = len(indexes)

    cdef int xoff = <int>x0
    cdef int yoff = <int>y0
    cdef int xsize = <int>max(1, width)
    cdef int ysize = <int>max(1, height)

    cdef GDALRasterIOExtraArg extras
    extras.nVersion = 1
    extras.eResampleAlg = <GDALRIOResampleAlg>resampling
    extras.bFloatingPointWindowValidity = 1
    extras.dfXOff = x0
    extras.dfYOff = y0
    extras.dfXSize = width
    extras.dfYSize = height
    extras.pfnProgress = NULL
    extras.pProgressData = NULL

    if len(indexes) != data.shape[0]:
        raise DatasetIOShapeError("Dataset indexes and destination buffer are mismatched")

    bandmap = <int *>CPLMalloc(count*sizeof(int))
    for i in range(count):
        bandmap[i] = <int>indexes[i]

    # Chain errors coming from GDAL.
    try:
        with stack_errors() as checker:
            with nogil:
                retval = GDALDatasetRasterIOEx(
                    hds, <GDALRWFlag>mode, xoff, yoff, xsize, ysize, buf,
                    bufxsize, bufysize, buftype, count, bandmap,
                    bufpixelspace, buflinespace, bufbandspace, &extras)
            return checker.exc_wrap_int(retval)
    finally:
        CPLFree(bandmap)


cdef int io_multi_mask(GDALDatasetH hds, int mode, double x0, double y0,
                       double width, double height, object data,
                       Py_ssize_t[:] indexes, int resampling=0) except -1:
    """Read or write a region of data for multiple band masks.

    Implicit are

    1) data type conversion if the dtype of `data` and bands differ.
    2) decimation if `data` and band shapes differ.

    The striding of `data` is passed to GDAL so that it can navigate
    the layout of ndarray views.

    """
    validate_resampling(resampling)

    cdef int i = 0
    cdef int j = 0
    cdef int retval = 3
    cdef GDALRasterBandH band = NULL
    cdef GDALRasterBandH hmask = NULL
    cdef void *buf = NULL
    cdef int bufxsize = data.shape[2]
    cdef int bufysize = data.shape[1]
    cdef GDALDataType buftype = _get_gdal_dtype(data.dtype.name)
    cdef GSpacing bufpixelspace = data.strides[2]
    cdef GSpacing buflinespace = data.strides[1]
    cdef int count = len(indexes)

    cdef int xoff = <int>x0
    cdef int yoff = <int>y0
    cdef int xsize = <int>max(1, width)
    cdef int ysize = <int>max(1, height)

    cdef StackChecker checker
    cdef GDALRasterIOExtraArg extras
    extras.nVersion = 1
    extras.eResampleAlg = <GDALRIOResampleAlg>resampling
    extras.bFloatingPointWindowValidity = 1
    extras.dfXOff = x0
    extras.dfYOff = y0
    extras.dfXSize = width
    extras.dfYSize = height
    extras.pfnProgress = NULL
    extras.pProgressData = NULL

    if len(indexes) != data.shape[0]:
        raise DatasetIOShapeError("Dataset indexes and destination buffer are mismatched")

    for i in range(count):
        j = <int>indexes[i]
        band = GDALGetRasterBand(hds, j)
        if band == NULL:
            raise ValueError("Null band")
        hmask = GDALGetMaskBand(band)
        if hmask == NULL:
            raise ValueError("Null mask band")
        buf = <void *>np.PyArray_DATA(data[i])
        if buf == NULL:
            raise ValueError("NULL data")

        with stack_errors() as checker:
            with nogil:
                retval = GDALRasterIOEx(
                    hmask, <GDALRWFlag>mode, xoff, yoff, xsize, ysize, buf, bufxsize,
                    bufysize, <GDALDataType>1, bufpixelspace, buflinespace, &extras)
            retval = checker.exc_wrap_int(retval)

    return retval


cdef bint in_dtype_range(value, dtype):
    """Returns True if value is in the range of dtype, else False."""
    infos = {
        'c': np.finfo,
        'f': np.finfo,
        'i': np.iinfo,
        'u': np.iinfo,
        # Cython 0.22 returns dtype.kind as an int and will not cast to a char
        99: np.finfo,
        102: np.finfo,
        105: np.iinfo,
        117: np.iinfo}

    key = _getnpdtype(dtype).kind
    if np.isnan(value):
        return key in ('c', 'f', 99, 102)
    if key == "f" and np.isinf(value):
        return True

    rng = infos[key](dtype)
    return rng.min <= value <= rng.max


cdef int io_auto(data, GDALRasterBandH band, bint write, int resampling=0) except -1:
    """Convenience function to handle IO with a GDAL band.

    :param data: a numpy.ndarray
    :param band: an instance of GDALGetRasterBand
    :param write: 1 (True) uses write mode (writes data into band),
                  0 (False) uses read mode (reads band into data)
    :return: the return value from the data-type specific IO function
    """
    cdef int ndims = len(data.shape)
    cdef float height = data.shape[-2]
    cdef float width = data.shape[-1]

    try:

        if ndims == 2:
            return io_band(band, write, 0.0, 0.0, width, height, data, resampling=resampling)

        elif ndims == 3:
            indexes = np.arange(1, data.shape[0] + 1, dtype=np.intp)
            return io_multi_band(band, write, 0.0, 0.0, width, height, data, indexes, resampling=resampling)

        else:
            raise ValueError("Specified data must have 2 or 3 dimensions")

    # TODO: don't handle, it's inconsistent with the other io_* functions.
    except CPLE_BaseError as cplerr:
        raise RasterioIOError(str(cplerr))


cdef char **convert_options(kwargs):
    cdef char **options = NULL

    tiled = kwargs.get("tiled", False) or kwargs.get("TILED", False)
    if isinstance(tiled, str):
        tiled = (tiled.lower() in ("true", "yes"))

    for k, v in kwargs.items():
        if k.lower() in ['affine']:
            continue
        elif k in ['BLOCKXSIZE'] and not tiled:
            continue

        # Special cases for enums and tuples.
        elif isinstance(v, (IntEnum, Enum)):
            v = v.name.upper()
        elif isinstance(v, tuple):
            v = ",".join([str(it) for it in v])

        k, v = k.upper(), str(v)
        key_b = k.encode('utf-8')
        val_b = v.encode('utf-8')
        key_c = key_b
        val_c = val_b
        options = CSLSetNameValue(options, key_c, val_c)

    return options


@attr.s(slots=True, frozen=True)
class Statistics:
    """Raster band statistics.

    Attributes
    ----------
    min, max, mean, std : float
        Basic stats of a raster band.

    """
    min = attr.ib()
    max = attr.ib()
    mean = attr.ib()
    std = attr.ib()


cdef class DatasetReaderBase(DatasetBase):
    """Provides data and metadata reading methods."""

    def read(self, indexes=None, out=None, window=None, masked=False,
            out_shape=None, boundless=False, resampling=Resampling.nearest,
            fill_value=None, out_dtype=None):
        """Read band data and, optionally, mask as an array.

        A smaller (or larger) region of the dataset may be specified and
        it may be resampled and/or converted to a different data type.

        Parameters
        ----------
        indexes : int or list, optional
            If `indexes` is a list, the result is a 3D array, but is
            a 2D array if it is a band index number.
        out : numpy.ndarray, optional
            As with Numpy ufuncs, this is an optional reference to an
            output array into which data will be placed. If the height
            and width of `out` differ from that of the specified
            window (see below), the raster image will be decimated or
            replicated using the specified resampling method (also see
            below). This parameter cannot be combined with `out_shape`.

            *Note*: the method's return value may be a view on this
            array. In other words, `out` is likely to be an
            incomplete representation of the method's results.
        out_dtype : str or numpy.dtype
            The desired output data type. For example: 'uint8' or
            rasterio.uint16.
        out_shape : tuple, optional
            A tuple describing the shape of a new output array. See
            `out` (above) for notes on image decimation and replication.
            This parameter cannot be combined with `out`.
        window : Window, optional
            The region (slice) of the dataset from which data will be
            read. The default is the entire dataset.
        masked : bool, optional
            If `masked` is `True` the return value will be a masked
            array. Otherwise (the default) the return value will be a
            regular array. Masks will be exactly the inverse of the
            GDAL RFC 15 conforming arrays returned by read_masks().
        boundless : bool, optional (default `False`)
            If `True`, windows that extend beyond the dataset's extent
            are permitted and partially or completely filled arrays will
            be returned as appropriate.
        resampling : Resampling
            By default, pixel values are read raw or interpolated using
            a nearest neighbor algorithm from the band cache. Other
            resampling algorithms may be specified. Resampled pixels
            are not cached.
        fill_value : scalar
            Fill value applied in the `boundless=True` case only. Like
            the fill_value of :class:`numpy.ma.MaskedArray`, should be value
            valid for the dataset's data type.

        Returns
        -------
        Numpy ndarray or a view on a Numpy ndarray

        Raises
        ------
        RasterioIOError
            If the write fails.

        Notes
        -----
        This data is read from the dataset's band cache, which means
        that repeated reads of the same windows may avoid I/O.

        As with Numpy ufuncs, an object is returned even if you use the
        optional `out` argument and the return value shall be
        preferentially used by callers.

        """
        cdef GDALRasterBandH band = NULL

        if self.mode == "w":
            raise UnsupportedOperation("not readable")

        return2d = False
        if indexes is None:
            indexes = self.indexes
        elif isinstance(indexes, int):
            indexes = [indexes]
            return2d = True

            if out is not None and out.ndim == 2:
                out.shape = (1,) + out.shape

        if not indexes:
            raise ValueError("No indexes to read")

        check_dtypes = set()
        nodatavals = []

        # Check each index before processing 3D array
        for bidx in indexes:

            if bidx not in self.indexes:
                raise IndexError("band index {} out of range (not in {})".format(bidx, self.indexes))

            idx = self.indexes.index(bidx)
            dtype = self.dtypes[idx]
            check_dtypes.add(dtype)

            ndv = self._nodatavals[idx]

            log.debug("Output nodata value read from file: %r", ndv)

            if ndv is not None and not _is_complex_int(dtype):
                kind = _getnpdtype(dtype).kind

                if kind in "iu":
                    info = np.iinfo(dtype)
                    dt_min, dt_max = info.min, info.max
                elif kind in "cf":
                    info = np.finfo(dtype)
                    dt_min, dt_max = info.min, info.max
                else:
                    dt_min, dt_max = False, True

                if ndv < dt_min:
                    ndv = dt_min
                elif ndv > dt_max:
                    ndv = dt_max

            nodatavals.append(ndv)

        log.debug("Output nodata values: %r", nodatavals)

        # Mixed dtype reads are not supported at this time.
        if len(check_dtypes) > 1:
            raise ValueError("more than one 'dtype' found")
        elif len(check_dtypes) == 0:
            dtype = self.dtypes[0]
        else:
            dtype = check_dtypes.pop()

        if out_dtype is not None:
            dtype = out_dtype

        # Ensure we have a numpy dtype.
        dtype = _getnpdtype(dtype)

        # Get the natural shape of the read window, boundless or not.
        # The window can have float values. In this case, we round up
        # when computing the shape.

        # Stub the win_shape.
        win_shape = (len(indexes),)

        if window:

            if isinstance(window, tuple):
                window = Window.from_slices(
                    *window, height=self.height, width=self.width,
                    boundless=boundless)

            if not boundless:
                window = window.crop(self.height, self.width)

            int_window = window.round_lengths()
            win_shape += (int(int_window.height), int(int_window.width))

        else:
            win_shape += self.shape

        if out is not None and out_shape is not None:
            raise ValueError("out and out_shape are exclusive")

        # `out` takes precedence over `out_shape` and `out_dtype`.
        elif out is not None:

            if out.dtype != dtype:
                dtype == out.dtype

            if out.shape[0] != win_shape[0]:
                raise ValueError("'out' shape {} does not match window shape {}".format(out.shape, win_shape))

        else:
            if out_shape is not None:
                if len(out_shape) == 2:
                    out_shape = (len(indexes),) + out_shape
            else:
                out_shape = win_shape

            # We're filling in both the bounded and boundless cases.
            # TODO: profile and see if we should avoid this in the
            # bounded case.

            if boundless:
                out = np.zeros(out_shape, dtype=dtype)
            else:
                out = np.empty(out_shape, dtype=dtype)

        # Masking
        # -------
        #
        # If masked is True, we check the GDAL mask flags using
        # GDALGetMaskFlags. If GMF_ALL_VALID for all bands, we do not
        # call read_masks(), but pass `mask=False` to the masked array
        # constructor. Else, we read the GDAL mask bands using
        # read_masks(), invert them and use them in constructing masked
        # arrays.

        enums = self.mask_flag_enums
        all_valid = all([MaskFlags.all_valid in flags for flags in enums])
        log.debug("all_valid: %s", all_valid)
        log.debug("mask_flags: %r", enums)

        # We can jump straight to _read() in some cases. We can ignore
        # the boundless flag if there's no given window.
        if not boundless or not window:

            log.debug("Jump straight to _read()")
            log.debug("Window: %r", window)

            out = self._read(indexes, out, window, dtype, resampling=resampling)

            if masked or fill_value is not None:
                if all_valid:
                    mask = np.ma.nomask
                else:
                    mask = np.zeros(out.shape, dtype=np.uint8)
                    mask = ~self._read(
                        indexes, mask, window, dtype=uint8, masks=True,
                        resampling=resampling).astype(bool)

                kwds = {'mask': mask}
                # Set a fill value only if the read bands share a
                # single nodata value.
                if fill_value is not None:
                    kwds['fill_value'] = fill_value
                elif len(set(nodatavals)) == 1:
                    if nodatavals[0] is not None:
                        kwds['fill_value'] = nodatavals[0]

                out = np.ma.array(out, **kwds)

                if not masked:
                    out = out.filled(fill_value)

        # If this is a boundless read we will create an in-memory VRT
        # in order to use GDAL's windowing and compositing logic.
        else:

            if fill_value is not None:
                nodataval = fill_value
            else:
                nodataval = ndv

            vrt_doc = _boundless_vrt_doc(
                self, nodata=nodataval, background=nodataval,
                width=max(self.width, window.width) + 1,
                height=max(self.height, window.height) + 1,
                transform=self.window_transform(window),
                resampling=resampling
            )
            vrt_kwds = {'driver': 'VRT'}

            with DatasetReaderBase(_UnparsedPath(vrt_doc), **vrt_kwds) as vrt:
                out = vrt._read(
                    indexes, out, Window(0, 0, window.width, window.height), None)

                if masked:
                    # Below we use another VRT to compute the valid data mask
                    # in this special case where all source pixels are valid.
                    if all_valid:
                        log.debug("Boundless read: self.transform=%r, self.window_transform(window)=%r", self.transform, self.window_transform(window))

                        mask_vrt_doc = _boundless_vrt_doc(
                            self, nodata=nodataval,
                            width=max(self.width, window.width) + 1,
                            height=max(self.height, window.height) + 1,
                            transform=self.window_transform(window),
                            masked=True,
                            resampling=resampling
                        )

                        with DatasetReaderBase(_UnparsedPath(mask_vrt_doc), **vrt_kwds) as mask_vrt:
                            mask = np.zeros(out.shape, dtype=np.uint8)
                            mask = ~mask_vrt._read(
                                indexes, mask, Window(0, 0, window.width, window.height), None).astype(bool)

                    else:
                        mask = np.zeros(out.shape, dtype=np.uint8)
                        window = Window(0, 0, window.width, window.height)
                        log.debug("Boundless read: window=%r", window)
                        mask = ~vrt._read(
                            indexes, mask, window, None, masks=True).astype(bool)

                    kwds = {'mask': mask}

                    # Set a fill value only if the read bands share a
                    # single nodata value.
                    if fill_value is not None:
                        kwds['fill_value'] = fill_value

                    elif len(set(nodatavals)) == 1:
                        if nodatavals[0] is not None:
                            kwds['fill_value'] = nodatavals[0]

                    log.debug("Boundless read: out=%r, mask=%r", out, mask)
                    out = np.ma.array(out, **kwds)

        if return2d:
            out.shape = out.shape[1:]

        return out


    def read_masks(self, indexes=None, out=None, out_shape=None, window=None,
                   boundless=False, resampling=Resampling.nearest):
        """Read band masks as an array.

        A smaller (or larger) region of the dataset may be specified and
        it may be resampled and/or converted to a different data type.

        Parameters
        ----------
        indexes : int or list, optional
            If `indexes` is a list, the result is a 3D array, but is
            a 2D array if it is a band index number.
        out : numpy.ndarray, optional
            As with Numpy ufuncs, this is an optional reference to an
            output array into which data will be placed. If the height
            and width of `out` differ from that of the specified
            window (see below), the raster image will be decimated or
            replicated using the specified resampling method (also see
            below). This parameter cannot be combined with `out_shape`.

            *Note*: the method's return value may be a view on this
            array. In other words, `out` is likely to be an
            incomplete representation of the method's results.
        out_shape : tuple, optional
            A tuple describing the shape of a new output array. See
            `out` (above) for notes on image decimation and replication.
            This parameter cannot be combined with `out`.
        window : Window, optional
            The region (slice) of the dataset from which data will be
            read. The default is the entire dataset.
        boundless : bool, optional (default `False`)
            If `True`, windows that extend beyond the dataset's extent
            are permitted and partially or completely filled arrays will
            be returned as appropriate.
        resampling : Resampling
            By default, pixel values are read raw or interpolated using
            a nearest neighbor algorithm from the band cache. Other
            resampling algorithms may be specified. Resampled pixels
            are not cached.

        Returns
        -------
        Numpy ndarray or a view on a Numpy ndarray

        Raises
        ------
        RasterioIOError
            If the write fails.

        Notes
        -----
        This data is read from the dataset's band cache, which means
        that repeated reads of the same windows may avoid I/O.

        As with Numpy ufuncs, an object is returned even if you use the
        optional `out` argument and the return value shall be
        preferentially used by callers.

        """
        if self.mode == "w":
            raise UnsupportedOperation("not readable")

        return2d = False
        if indexes is None:
            indexes = self.indexes
        elif isinstance(indexes, int):
            indexes = [indexes]
            return2d = True
            if out is not None and out.ndim == 2:
                out.shape = (1,) + out.shape
        if not indexes:
            raise ValueError("No indexes to read")

        # Get the natural shape of the read window, boundless or not.

        # Stub the win_shape.
        win_shape = (len(indexes),)

        if window:

            if isinstance(window, tuple):
                window = Window.from_slices(
                    *window, height=self.height, width=self.width,
                    boundless=boundless)

            if not boundless:
                window = window.crop(self.height, self.width)

            int_window = window.round_lengths()
            win_shape += (int(int_window.height), int(int_window.width))

        else:
            win_shape += self.shape

        dtype = np.uint8

        if out is not None and out_shape is not None:
            raise ValueError("out and out_shape are exclusive")
        elif out_shape is not None:
            if len(out_shape) == 2:
                out_shape = (len(indexes),) + out_shape
            out = np.zeros(out_shape, dtype=np.uint8)

        if out is not None:
            if out.dtype != _getnpdtype(dtype):
                raise ValueError(
                    "the out array's dtype '%s' does not match '%s'"
                    % (out.dtype, dtype))
            if out.shape[0] != win_shape[0]:
                raise ValueError(
                    "'out' shape %s does not match window shape %s" %
                    (out.shape, win_shape))
        else:
            out = np.zeros(win_shape, dtype=np.uint8)

        # We can jump straight to _read() in some cases. We can ignore
        # the boundless flag if there's no given window.
        if not boundless or not window:
            out = self._read(indexes, out, window, dtype, masks=True,
                             resampling=resampling)

        # If this is a boundless read we will create an in-memory VRT
        # in order to use GDAL's windowing and compositing logic.
        else:

            enums = self.mask_flag_enums
            all_valid = all([MaskFlags.all_valid in flags for flags in enums])
            vrt_kwds = {'driver': 'VRT'}

            if all_valid:
                blank_path = _UnparsedPath('/vsimem/blank-{}.tif'.format(uuid4()))
                transform = Affine.translation(self.transform.xoff, self.transform.yoff) * (Affine.scale(self.width / 3, self.height / 3) * (Affine.translation(-self.transform.xoff, -self.transform.yoff) * self.transform))
                with DatasetWriterBase(
                        blank_path, 'w',
                        driver='GTiff', count=self.count, height=3, width=3,
                        dtype=uint8, crs=self.crs, transform=transform) as blank_dataset:
                    blank_dataset.write(
                        np.full((self.count, 3, 3), 255, dtype=np.uint8))

                with DatasetReaderBase(blank_path) as blank_dataset:
                    mask_vrt_doc = _boundless_vrt_doc(
                        blank_dataset, nodata=0,
                        width=max(self.width, window.width) + 1,
                        height=max(self.height, window.height) + 1,
                        transform=self.window_transform(window),
                        resampling=resampling
                    )

                    with DatasetReaderBase(_UnparsedPath(mask_vrt_doc), **vrt_kwds) as mask_vrt:
                        out = np.zeros(out.shape, dtype=np.uint8)
                        out = mask_vrt._read(
                            indexes, out, Window(0, 0, window.width, window.height), None).astype(bool)

            else:
                vrt_doc = _boundless_vrt_doc(
                    self, width=max(self.width, window.width) + 1,
                    height=max(self.height, window.height) + 1,
                    transform=self.window_transform(window),
                    resampling=resampling
                )

                with DatasetReaderBase(_UnparsedPath(vrt_doc), **vrt_kwds) as vrt:
                    out = vrt._read(
                        indexes, out, Window(0, 0, window.width, window.height), None, masks=True)

        if return2d:
            out.shape = out.shape[1:]

        return out

    def _read(self, indexes, out, window, dtype, masks=False, resampling=Resampling.nearest):
        """Read raster bands as a multidimensional array

        If `indexes` is a list, the result is a 3D array, but
        is a 2D array if it is a band index number.

        Optional `out` argument is a reference to an output array with the
        same dimensions and shape.

        See `read_band` for usage of the optional `window` argument.

        The return type will be either a regular NumPy array, or a masked
        NumPy array depending on the `masked` argument. The return type is
        forced if either `True` or `False`, but will be chosen if `None`.
        For `masked=None` (default), the array will be the same type as
        `out` (if used), or will be masked if any of the nodatavals are
        not `None`.
        """
        cdef int aix, bidx, indexes_count
        cdef double height, width, xoff, yoff
        cdef int retval = 0
        cdef GDALDatasetH dataset = NULL

        if out is None:
            raise ValueError("An output array is required.")

        dataset = self.handle()

        if window:
            if not isinstance(window, Window):
                raise WindowError("window must be an instance of Window")

            yoff = window.row_off
            xoff = window.col_off
            width = window.width
            height = window.height
        else:
            xoff = yoff = <int>0
            width = <int>self.width
            height = <int>self.height

        log.debug(
            "IO window xoff=%s yoff=%s width=%s height=%s",
            xoff, yoff, width, height)

        # Call io_multi* functions with C type args so that they
        # can release the GIL.
        indexes_arr = np.array(indexes, dtype=np.intp)
        indexes_count = <int>indexes_arr.shape[0]

        try:
            if masks:
                # Warn if nodata attribute is shadowing an alpha band.
                if self.count == 4 and self.colorinterp[3] == ColorInterp.alpha:
                    for flags in self.mask_flag_enums:
                        if MaskFlags.nodata in flags:
                            warnings.warn(NodataShadowWarning())

                io_multi_mask(self._hds, 0, xoff, yoff, width, height, out, indexes_arr, resampling=resampling.value)

            else:
                io_multi_band(self._hds, 0, xoff, yoff, width, height, out, indexes_arr, resampling=resampling.value)

        except CPLE_BaseError as cplerr:
            raise RasterioIOError("Read failed. See previous exception for details.") from cplerr

        return out

    def dataset_mask(self, out=None, out_shape=None, window=None,
                     boundless=False, resampling=Resampling.nearest):
        """Get the dataset's 2D valid data mask.

        Parameters
        ----------
        out : numpy.ndarray, optional
            As with Numpy ufuncs, this is an optional reference to an
            output array with the same dimensions and shape into which
            data will be placed.

            *Note*: the method's return value may be a view on this
            array. In other words, `out` is likely to be an
            incomplete representation of the method's results.

            Cannot be combined with `out_shape`.

        out_shape : tuple, optional
            A tuple describing the output array's shape.  Allows for decimated
            reads without constructing an output Numpy array.

            Cannot be combined with `out`.

        window : a pair (tuple) of pairs of ints or Window, optional
            The optional `window` argument is a 2 item tuple. The first
            item is a tuple containing the indexes of the rows at which
            the window starts and stops and the second is a tuple
            containing the indexes of the columns at which the window
            starts and stops. For example, ((0, 2), (0, 2)) defines
            a 2x2 window at the upper left of the raster dataset.

        boundless : bool, optional (default `False`)
            If `True`, windows that extend beyond the dataset's extent
            are permitted and partially or completely filled arrays will
            be returned as appropriate.

        resampling : Resampling
            By default, pixel values are read raw or interpolated using
            a nearest neighbor algorithm from the band cache. Other
            resampling algorithms may be specified. Resampled pixels
            are not cached.

        Returns
        -------
        Numpy ndarray or a view on a Numpy ndarray
            The dtype of this array is uint8. 0 = nodata, 255 = valid
            data.

        Notes
        -----
        Note: as with Numpy ufuncs, an object is returned even if you
        use the optional `out` argument and the return value shall be
        preferentially used by callers.

        The dataset mask is calculated based on the individual band
        masks according to the following logic, in order of precedence:

        1. If a .msk file, dataset-wide alpha, or internal mask exists
           it will be used for the dataset mask.
        2. Else if the dataset is a 4-band  with a shadow nodata value, band 4 will be
           used as the dataset mask.
        3. If a nodata value exists, use the binary OR (|) of the band
           masks 4. If no nodata value exists, return a mask filled with
           255.

        Note that this differs from read_masks and GDAL RFC15 in that it
        applies per-dataset, not per-band (see
        https://trac.osgeo.org/gdal/wiki/rfc15_nodatabitmask)

        """
        kwargs = {
            'out': out,
            'out_shape': out_shape,
            'window': window,
            'boundless': boundless,
            'resampling': resampling}

        if MaskFlags.per_dataset in self.mask_flag_enums[0]:
            return self.read_masks(1, **kwargs)

        elif self.count == 4 and self.colorinterp[0] == ColorInterp.red:
            return self.read_masks(4, **kwargs)

        elif out is not None:
            kwargs.pop("out", None)
            kwargs["out_shape"] = (self.count, out.shape[-2], out.shape[-1])
            out = np.logical_or.reduce(self.read_masks(**kwargs)) * np.uint8(255)
            return out

        elif out_shape is not None:
            kwargs["out_shape"] = (self.count, out_shape[-2], out_shape[-1])

        return np.logical_or.reduce(self.read_masks(**kwargs)) * np.uint8(255)

    def sample(self, xy, indexes=None, masked=False):
        """Get the values of a dataset at certain positions

        Values are from the nearest pixel. They are not interpolated.

        Parameters
        ----------
        xy : iterable
            Pairs of x, y coordinates (floats) in the dataset's
            reference system.
        indexes : int or list of int
            Indexes of dataset bands to sample.
        masked : bool, default: False
            Whether to mask samples that fall outside the extent of the
            dataset.

        Returns
        ------
        iterable
            Arrays of length equal to the number of specified indexes
            containing the dataset values for the bands corresponding to
            those indexes.

        """
        # In https://github.com/rasterio/rasterio/issues/378 a user has
        # found what looks to be a Cython generator bug. Until that can
        # be confirmed and fixed, the workaround is a pure Python
        # generator implemented in sample.py.
        return sample_gen(self, xy, indexes=indexes, masked=masked)

    def stats(self, *, indexes=None, approx=False):
        """Update stored statistics for all dataset bands.

        If no stats are provided, statistics will be computed for all
        bands of the dataset.

        Parameters
        ----------
        indexes : sequence of ints, optional
            A sequence of band indexes, 1-indexed. If not provided,
            defaults to the indexes of all dataset bands.
        approx : bool, optional. Default: False
            Set to True if approximate, faster computation of statistics
            based on overviews or a subset of tiles is acceptable.

        Returns
        -------
        List of Statistics objects.
        """
        cdef double min, max, mean, std
        cdef GDALRasterBandH band = NULL

        if isinstance(indexes, int):
            indexes = [indexes]

        band_indexes = indexes or self.indexes
        results = [None for i in band_indexes]

        for i, bidx in enumerate(band_indexes):
            band = self.band(bidx)
            GDALGetRasterStatistics(band, int(approx), 1, &min, &max, &mean, &std)
            results[i] = Statistics(min, max, mean, std)

        return results

    def statistics(self, bidx, approx=False, clear_cache=False):
        """Get min, max, mean, and standard deviation of a raster band.

        Parameters
        ----------
        bidx : int
            The band's index (1-indexed).
        approx : bool, optional
            If True, statistics will be calculated from reduced
            resolution data.
        clear_cache : bool, optional
            If True, saved stats will be deleted and statistics will be
            recomputed. Requires GDAL version >= 3.2.

        Returns
        -------
        Statistics

        Notes
        -----
        GDAL will preferentially use statistics kept in raster metadata
        like images tags or an XML sidecar. If that metadata is out of
        date, the statistics may not correspond to the actual data.

        Additionally, GDAL will save statistics to file metadata as a
        side effect if that metadata does not already exist.

        """
        cdef double min, max, mean, std
        cdef GDALRasterBandH band = NULL

        warnings.warn("statistics() will be removed in 2.0.0. Please switch to stats().", RasterioDeprecationWarning)

        band = self.band(bidx)

        if clear_cache:
            GDALDatasetClearStatistics(self._hds)

        try:
            exc_wrap_int(
                GDALGetRasterStatistics(band, int(approx), 1, &min, &max, &mean, &std)
            )
        except CPLE_AppDefinedError as exc:
            raise StatisticsError("No valid pixels found in sampling.") from exc
        else:
            return Statistics(min, max, mean, std)


@contextmanager
def silence_errors():
    """Intercept GDAL errors"""
    try:
        CPLPushErrorHandler(CPLQuietErrorHandler)
        yield None
    finally:
        CPLPopErrorHandler()


cdef class MemoryFileBase:
    """Base for a BytesIO-like class backed by an in-memory file."""

    def __init__(self, file_or_bytes=None, dirname=None, filename=None, ext='.tif'):
        """A file in an in-memory filesystem.

        Parameters
        ----------
        file_or_bytes : file or bytes
            A file opened in binary mode or bytes
        filename : str
            A filename for the in-memory file under /vsimem
        ext : str
            A file extension for the in-memory file under /vsimem. Ignored if
            filename was provided.

        """
        cdef VSILFILE *fp = NULL

        if file_or_bytes:
            if hasattr(file_or_bytes, "read"):
                initial_bytes = file_or_bytes.read()
            elif isinstance(file_or_bytes, bytes):
                initial_bytes = file_or_bytes
            elif hasattr(file_or_bytes, "itemsize"):
                initial_bytes = bytes(file_or_bytes)
            else:
                raise TypeError(
                    "Constructor argument must be a file opened in binary "
                    "mode or bytes.")
        else:
            initial_bytes = b''

        # Make an in-memory directory specific to this dataset to help organize
        # auxiliary files.
        self._dirname = dirname or str(uuid4())
        self._filename = filename or f"{self._dirname}.{ext.lstrip('.')}"

        VSIMkdir(f"/vsimem/{self._dirname}".encode('utf-8'), 0666)

        self.name = f"/vsimem/{self._dirname}/{self._filename}"
        self._path = self.name.encode('utf-8')

        self._initial_bytes = initial_bytes
        cdef unsigned char *buffer = self._initial_bytes

        if self._initial_bytes:
            self._vsif = VSIFileFromMemBuffer(
               self._path, buffer, len(self._initial_bytes), 0)
            self.mode = "r"

        else:
            self._vsif = VSIFOpenL(self._path, "w+")
            self.mode = "w+"

        if self._vsif == NULL:
            raise OSError("Failed to open in-memory file.")

        self._env = ExitStack()

    @property
    def closed(self):
        """Test if the dataset is closed

        Returns
        -------
        bool
        """
        return self._vsif == NULL

    def exists(self):
        """Test if the in-memory file exists.

        Returns
        -------
        bool
            True if the in-memory file exists.

        """
        cdef VSIStatBufL st_buf
        return VSIStatL(self._path, &st_buf) == 0

    def __len__(self):
        """Length of the file's buffer in number of bytes.

        Returns
        -------
        int
        """
        return self.getbuffer().size

    def getbuffer(self):
        """Return a view on bytes of the file."""
        cdef unsigned char *buffer = NULL
        cdef vsi_l_offset buffer_len = 0
        cdef np.uint8_t [:] buff_view

        buffer = VSIGetMemFileBuffer(self._path, &buffer_len, 0)

        if buffer == NULL or buffer_len == 0:
            buff_view = np.array([], dtype=np.uint8)
        else:
            buff_view = <np.uint8_t[:buffer_len]>buffer
        return buff_view

    def close(self):
        if self._vsif == NULL:
            return

        VSIFCloseL(self._vsif)
        VSIRmdirRecursive("/vsimem/{}".format(self._dirname).encode("utf-8"))
        self._vsif = NULL

    def seek(self, offset, whence=0):
        if self.closed:
            raise ValueError("I/O operation on closed MemoryFile")
        return VSIFSeekL(self._vsif, offset, whence)

    def tell(self):
        if self.closed:
            raise ValueError("I/O operation on closed MemoryFile")
        return VSIFTellL(self._vsif)

    def read(self, size=-1):
        """Read bytes from MemoryFile.

        Parameters
        ----------
        size : int
            Number of bytes to read. Default is -1 (all bytes).

        Returns
        -------
        bytes
            String of bytes read.

        """
        cdef bytes result
        cdef unsigned char *buffer = NULL
        cdef vsi_l_offset buffer_len = 0

        if self.closed:
            raise ValueError("I/O operation on closed MemoryFile")

        if size < 0:
            buffer = VSIGetMemFileBuffer(self._path, &buffer_len, 0)
            size = buffer_len

        buffer = <unsigned char *>CPLMalloc(size)

        try:
            objects_read = VSIFReadL(buffer, 1, size, self._vsif)
            result = <bytes>buffer[:objects_read]

        finally:
            CPLFree(buffer)

        return result

    def write(self, data):
        """Write data bytes to MemoryFile.

        Parameters
        ----------
        data : bytes

        Returns
        -------
        int
            Number of bytes written.

        """
        if self.closed:
            raise ValueError("I/O operation on closed MemoryFile")
        cdef const unsigned char *view = <bytes>data
        n = len(data)
        result = VSIFWriteL(view, 1, n, self._vsif)
        VSIFFlushL(self._vsif)
        return result


cdef class DatasetWriterBase(DatasetReaderBase):
    """Read-write access to raster data and metadata
    """

    def __init__(self, path, mode, driver=None, width=None, height=None,
                 count=None, crs=None, transform=None, dtype=None, nodata=None,
                 gcps=None, rpcs=None, sharing=False, **kwargs):
        """Create a new dataset writer or updater

        Parameters
        ----------
        path : rasterio.path.Path
            A remote or local dataset path.
        mode : str
            'r+' (read/write), 'w' (write), or 'w+' (write/read).
        driver : str, optional
            A short format driver name (e.g. "GTiff" or "JPEG") or a list of
            such names (see GDAL docs at
            http://www.gdal.org/formats_list.html). In 'w' or 'w+' modes
            a single name is required. In 'r' or 'r+' modes the driver can
            usually be omitted. Registered drivers will be tried
            sequentially until a match is found. When multiple drivers are
            available for a format such as JPEG2000, one of them can be
            selected by using this keyword argument.
        width, height : int, optional
            The numbers of rows and columns of the raster dataset. Required
            in 'w' or 'w+' modes, they are ignored in 'r' or 'r+' modes.
        count : int, optional
            The count of dataset bands. Required in 'w' or 'w+' modes, it is
            ignored in 'r' or 'r+' modes.
        crs : str, dict, or CRS; optional
            The coordinate reference system. Required in 'w' or 'w+' modes,
            it is ignored in 'r' or 'r+' modes.
        transform : Affine instance, optional
            Affine transformation mapping the pixel space to geographic
            space. Required in 'w' or 'w+' modes, it is ignored in 'r' or
            'r+' modes.
        dtype : str or numpy.dtype
            The data type for bands. For example: 'uint8' or
            ``rasterio.uint16``. Required in 'w' or 'w+' modes, it is
            ignored in 'r' or 'r+' modes.
        nodata : int, float, or nan; optional
            Defines the pixel value to be interpreted as not valid data.
            Required in 'w' or 'w+' modes, it is ignored in 'r' or 'r+'
            modes.
        gcps : Sequence of GroundControlPoint, optional
            Zero or more ground control points mapping pixel space to
            geographic space locations. Ignored in 'r' or 'r+' modes.
        rpcs : RPC or dict, optional
            Rational polynomial coefficients mapping geographic space (x, y, z)
            coordinates to pixel space coordinates (row, column). If passing a dict,
            should be in a form suitable as input to `RPC.from_gdal` method.
            Ignored in 'r' or 'r+' modes.
        sharing : bool
            A flag that allows sharing of dataset handles. Default is
            `False`. Should be set to `False` in a multithreaded:w program.
        kwargs : optional
            These are passed to format drivers as directives for creating or
            interpreting datasets. For example: in 'w' or 'w+' modes
            a `tiled=True` keyword argument will direct the GeoTIFF format
            driver to create a tiled, rather than striped, TIFF.

        Returns
        -------
        dataset

        """
        cdef char **options = NULL
        cdef char *key_c = NULL
        cdef char *val_c = NULL
        cdef const char *drv_name = NULL
        cdef GDALDriverH drv = NULL
        cdef GDALRasterBandH band = NULL
        cdef const char *fname = NULL

        # Validate write mode arguments.
        if mode in ('w', 'w+'):
            if not isinstance(driver, str):
                raise TypeError("A driver name string is required.")
            try:
                width = int(width)
                height = int(height)
            except TypeError:
                raise TypeError("Integer width and height are required.")
            try:
                count = int(count)
            except TypeError:
                raise TypeError("Integer band count is required.")

            if _is_complex_int(dtype):
                self._init_dtype = dtype
            else:
                try:
                    assert dtype is not None
                    self._init_dtype = _getnpdtype(dtype).name
                except Exception:
                    raise TypeError("A valid dtype is required.")

        internal_path = _parse_path(path)

        # Make and store a GDAL dataset handle.
        filename = internal_path.name
        vsi_path = internal_path.as_vsi()
        name_b = vsi_path.encode('utf-8')
        fname = name_b

        # Process dataset opening options.
        # "tiled" affects the meaning of blocksize, so we need it
        # before iterating.
        tiled = kwargs.pop("tiled", False) or kwargs.pop("TILED", False)
        if isinstance(tiled, str):
            tiled = (tiled.lower() in ("true", "yes"))

        if tiled:
            blockxsize = kwargs.get("blockxsize", None)
            blockysize = kwargs.get("blockysize", None)
            if blockxsize is None or blockysize is None:
                # ignore if only one provided
                kwargs.pop("blockxsize", None)
                kwargs.pop("blockysize", None)
            kwargs["tiled"] = "TRUE"

        if mode in ('w', 'w+'):
            options = convert_options(kwargs)
            driver_b = driver.encode('utf-8')
            drv_name = driver_b

            try:
                drv = exc_wrap_pointer(GDALGetDriverByName(drv_name))
            except Exception as err:
                raise DriverRegistrationError(str(err))

            # Find the equivalent GDAL data type or raise an exception
            # We've mapped numpy scalar types to GDAL types so see
            # if we can crosswalk those.
            gdal_dtype = _get_gdal_dtype(self._init_dtype)

            # Create a GDAL dataset handle.
            try:
                self._hds = exc_wrap_pointer(
                    GDALCreate(drv, fname, width, height, count, gdal_dtype, options)
                )

            except CPLE_IllegalArgError as exc:
                if "must be a multiple of 16" in str(exc):  # For GDAL 3.12+.
                    raise RasterBlockError(
                        "The height and width of TIFF dataset blocks must be multiples of 16"
                    )
                else:
                    raise RasterioIOError(str(exc))
            except CPLE_AppDefinedError as exc:
                if "Bad value" in str(exc):  # For GDAL < 3.12.
                    raise RasterBlockError(
                        "The height and width of TIFF dataset blocks must be multiples of 16"
                    )
                else:
                    raise RasterioIOError(str(exc))
            except CPLE_BaseError as exc:
                raise RasterioIOError(str(exc))

            finally:
                if options != NULL:
                    CSLDestroy(options)

            if nodata is not None:

                if _is_complex_int(dtype):
                    pass
                elif not in_dtype_range(nodata, dtype):
                    raise ValueError(
                        "Given nodata value, %s, is beyond the valid "
                        "range of its data type, %s." % (
                            nodata, dtype))

                # Broadcast the nodata value to all bands.
                for i in range(count):
                    band = self.band(i + 1)
                    try:
                        exc_wrap_int(
                            GDALSetRasterNoDataValue(band, nodata))
                    except Exception as err:
                        raise RasterioIOError(str(err))

        elif mode == 'r+':

            # driver may be a string or list of strings. If the
            # former, put it into a list.
            if isinstance(driver, str):
                driver = [driver]

            try:
                self._hds = open_dataset(
                    filename=vsi_path,
                    flags=GDAL_OF_UPDATE,
                    allowed_drivers=driver,
                    open_options=kwargs,
                    sharing=sharing,
                    siblings=None,
                )
            except CPLE_OpenFailedError as err:
                raise RasterioIOError(str(err))

        else:
            # Raise an exception if we have any other mode.
            raise ValueError("Invalid mode: '%s'", mode)

        self.name = filename
        self.mode = mode
        self.driver = driver
        self.width = width
        self.height = height
        self._count = count
        self._init_nodata = nodata
        self._count = count
        self._crs = crs
        if transform is not None:
            self._transform = transform.to_gdal()
        self._gcps = None
        self._rpcs = None
        self._init_gcps = gcps
        self._init_rpcs = rpcs
        self._dtypes = []
        self._nodatavals = []
        self._units = ()
        self._descriptions = ()
        self._options = kwargs.copy()

        if self.mode in ('w', 'w+'):
            if self._transform:
                self.write_transform(self._transform)
            if self._crs:
                self._set_crs(self._crs)
            if self._init_gcps:
                self._set_gcps(self._init_gcps, self.crs)
            if self._init_rpcs:
                self._set_rpcs(self._init_rpcs)

        drv = GDALGetDatasetDriver(self._hds)
        drv_name = GDALGetDriverShortName(drv)
        self.driver = drv_name.decode('utf-8')

        self._count = GDALGetRasterCount(self._hds)
        self.width = GDALGetRasterXSize(self._hds)
        self.height = GDALGetRasterYSize(self._hds)
        self.shape = (self.height, self.width)

        self._transform = self.read_transform()
        self._crs = self.read_crs()
        _ = self.meta
        self._env = ExitStack()

    def __repr__(self):
        return "<%s RasterUpdater name='%s' mode='%s'>" % (
            self.closed and 'closed' or 'open',
            self.name,
            self.mode)

    def _set_crs(self, crs):
        """Writes a coordinate reference system to the dataset."""
        crs = CRS.from_user_input(crs)
        wkt_b = crs.to_wkt().encode('utf-8')
        cdef const char *wkt_c = wkt_b
        exc_wrap_int(GDALSetProjection(self.handle(), wkt_c))
        self._crs = crs

    def _set_all_descriptions(self, value):
        """Supports the descriptions property setter"""
        # require that we have a description for every band.
        if len(value) == self.count:
            for i, val in zip(self.indexes, value):
                self.set_band_description(i, val)
        else:
            raise ValueError("One description for each band is required")

    def _set_all_scales(self, value):
        """Supports the scales property setter"""
        cdef GDALRasterBandH hband = NULL
        if len(value) == self.count:
            for (bidx, val) in zip(self.indexes, value):
                hband = self.band(bidx)
                GDALSetRasterScale(hband, val)
            # Invalidate cached scales.
            self._scales = ()
        else:
            raise ValueError("One scale for each band is required")

    def _set_all_offsets(self, value):
        """Supports the offsets property setter"""
        cdef GDALRasterBandH hband = NULL
        if len(value) == self.count:
            for (bidx, val) in zip(self.indexes, value):
                hband = self.band(bidx)
                GDALSetRasterOffset(hband, val)
            # Invalidate cached offsets.
            self._offsets = ()
        else:
            raise ValueError("One offset for each band is required")

    def _set_all_units(self, value):
        """Supports the units property setter"""
        # require that we have a unit for every band.
        if len(value) == self.count:
            for i, val in zip(self.indexes, value):
                self.set_band_unit(i, val)
        else:
            raise ValueError("One unit for each band is required")

    def write_transform(self, transform):
        if self._hds == NULL:
            raise ValueError("Can't read closed raster file")

        if [abs(v) for v in transform] == [0, 1, 0, 0, 0, 1]:
            warnings.warn(
                "The given matrix is equal to Affine.identity or its flipped counterpart. "
                "GDAL may ignore this matrix and save no geotransform without raising an error. "
                "This behavior is somewhat driver-specific.",
                NotGeoreferencedWarning
            )

        cdef double gt[6]
        for i in range(6):
            gt[i] = transform[i]
        err = GDALSetGeoTransform(self._hds, gt)
        if err:
            raise ValueError("transform not set: %s" % transform)
        self._transform = transform

    def _set_nodatavals(self, vals):
        cdef GDALRasterBandH band = NULL
        cdef double nodataval
        cdef int success = -1

        for i, val in zip(self.indexes, vals):
            band = self.band(i)
            if val is None:
                success = GDALDeleteRasterNoDataValue(band)
            else:
                nodataval = val
                success = GDALSetRasterNoDataValue(band, nodataval)
            if success:
                raise ValueError("Invalid nodata value: %r", val)
        self._nodatavals = vals

    def write(self, arr, indexes=None, window=None, masked=False):
        """Write the arr array into indexed bands of the dataset.

        If given a Numpy MaskedArray and masked is True, the input's
        data and mask will be written to the dataset's bands and band
        mask. If masked is False, no band mask is written. Instead, the
        input array's masked values are filled with the dataset's nodata
        value (if defined) or the input's own fill value.

        Parameters
        ----------
        arr : array-like
            This may be a :class:`numpy.ma.MaskedArray`.
        indexes : int or list, optional
            Which bands of the dataset to write to. The default is all.
        window : Window, optional
            The region (slice) of the dataset to which arr will be
            written. The default is the entire dataset.
        masked : bool, optional
            Whether or not to write to the dataset's band mask.

        Returns
        -------
        None

        Raises
        ------
        RasterioIOError
            If the write fails.

        """
        cdef int height, width, xoff, yoff, indexes_count
        cdef int retval = 0

        if self._hds == NULL:
            raise ValueError("can't write to closed raster file")

        if not is_ndarray(arr):
            raise InvalidArrayError("Positional argument arr must be an array-like object")

        if isinstance(arr, np.ma.MaskedArray):
            if masked:
                self.write_mask(~arr.mask, window=window)
            else:
                if len(set(self.nodatavals)) == 1 and self.nodatavals[0] is not None:
                    fill_value = self.nodatavals[0]
                else:
                    fill_value = None

                arr = arr.filled(fill_value)

        else:
            arr = np.asanyarray(arr)

        if indexes is None:
            indexes = self.indexes

        elif isinstance(indexes, int):
            indexes = [indexes]
            arr = np.stack((arr,))

        if len(arr.shape) != 3 or arr.shape[0] != len(indexes):
            raise ValueError(
                "Source shape {} is inconsistent with given indexes {}"
                .format(arr.shape, len(indexes)))

        check_dtypes = set()

        # Check each index before processing 3D array
        for bidx in indexes:
            if bidx not in self.indexes:
                raise IndexError("band index {} out of range (not in {})".format(bidx, self.indexes))
            idx = self.indexes.index(bidx)
            check_dtypes.add(self.dtypes[idx])

        if len(check_dtypes) > 1:
            raise ValueError("more than one 'dtype' found")
        elif len(check_dtypes) == 0:
            dtype = self.dtypes[0]
        else:  # unique dtype; normal case
            dtype = check_dtypes.pop()

        dtype = _getnpdtype(dtype)

        if dtype.name == int8:
            arr = arr.astype(np.uint8)

        # Require C-continguous arrays (see #108).
        arr = np.require(arr, dtype=dtype, requirements='C')

        # Prepare the IO window.
        if window:
            if isinstance(window, tuple):
                window = Window.from_slices(*window, self.height, self.width)

            yoff = window.row_off
            xoff = window.col_off
            height = window.height
            width = window.width
        else:
            xoff = yoff = <int>0
            width = <int>self.width
            height = <int>self.height

        indexes_arr = np.array(indexes, dtype=np.intp)
        indexes_count = <int>indexes_arr.shape[0]

        try:
            io_multi_band(self._hds, 1, xoff, yoff, width, height, arr, indexes_arr)
        except CPLE_BaseError as cplerr:
            raise RasterioIOError("Write failed. See previous exception for details.") from cplerr

    def write_band(self, bidx, src, window=None):
        """Write the src array into the `bidx` band.

        Band indexes begin with 1: read_band(1) returns the first band.

        The optional `window` argument takes a tuple like:

            ((row_start, row_stop), (col_start, col_stop))

        specifying a raster subset to write into.
        """
        self.write(src, bidx, window=window)

    def clear_stats(self):
        """Clear stored statistics for all dataset bands.

        Returns
        -------
        None
        """
        GDALDatasetClearStatistics(self._hds)

    def update_stats(self, *, stats=None, indexes=None, approx=False):
        """Update stored statistics for all dataset bands.

        If no stats are provided, statistics will be computed for all
        bands of the dataset.

        Parameters
        ----------
        stats : sequence of Statistics, optional
            A sequence of band statistics objects. Will be zipped
            together with the indexes sequence.
        indexes : sequence of ints, optional
            A sequence of band indexes, 1-indexed. If not provided,
            defaults to src.indexes.
        approx : bool, optional. Default: False
            Set to True if approximate, faster computation of statistics
            based on overviews or a subset of tiles is acceptable.

        Returns
        -------
        None
        """
        cdef double min, max, mean, std
        cdef GDALRasterBandH band = NULL

        if isinstance(indexes, int):
            indexes = [indexes]

        if isinstance(stats, Statistics):
            stats = [stats]

        if not stats and not indexes:
            for bidx in self.indexes:
                band = self.band(bidx)
                # Following call sets statistics as a side-effect.
                GDALComputeRasterStatistics(band, int(approx), NULL, NULL, NULL, NULL, NULL, NULL)
        else:
            for stat, bidx in zip(stats, indexes):
                band = self.band(bidx)
                GDALSetRasterStatistics(band, stat.min, stat.max, stat.mean, stat.std)

    def update_tags(self, bidx=0, ns=None, **kwargs):
        """Updates the tags of a dataset or one of its bands.

        Tags are pairs of key and value strings. Tags belong to
        namespaces.  The standard namespaces are: default (None) and
        'IMAGE_STRUCTURE'.  Applications can create their own additional
        namespaces.

        The optional bidx argument can be used to select the dataset
        band. The optional ns argument can be used to select a namespace
        other than the default.
        """
        cdef char *key_c = NULL
        cdef char *value_c = NULL
        cdef GDALMajorObjectH hobj = NULL
        cdef const char *domain_c = NULL
        cdef char **papszStrList = NULL
        if bidx > 0:
            hobj = self.band(bidx)
        else:
            hobj = self._hds
        if ns:
            domain_b = ns.encode('utf-8')
            domain_c = domain_b
        else:
            domain_c = NULL

        papszStrList = CSLDuplicate(
            GDALGetMetadata(hobj, domain_c))

        for key, value in kwargs.items():
            key_b = str(key).encode('utf-8')
            value_b = str(value).encode('utf-8')
            key_c = key_b
            value_c = value_b
            papszStrList = CSLSetNameValue(
                    papszStrList, key_c, value_c)

        retval = GDALSetMetadata(hobj, papszStrList, domain_c)
        if papszStrList != NULL:
            CSLDestroy(papszStrList)

        if retval == 2:
            log.warning("Tags accepted but may not be persisted.")
        elif retval == 3:
            raise RuntimeError("Tag update failed.")

    def set_band_description(self, bidx, value):
        """Sets the description of a dataset band.

        Parameters
        ----------
        bidx : int
            Index of the band (starting with 1).

        value: string
            A description of the band.

        Returns
        -------
        None
        """
        cdef GDALRasterBandH hband = NULL

        hband = self.band(bidx)
        GDALSetDescription(hband, (value or '').encode('utf-8'))
        # Invalidate cached descriptions.
        self._descriptions = ()

    def set_band_unit(self, bidx, value):
        """Sets the unit of measure of a dataset band.

        Parameters
        ----------
        bidx : int
            Index of the band (starting with 1).
        value : str
            A label for the band's unit of measure such as 'meters' or
            'degC'.  See the Pint project for a suggested list of units.

        Returns
        -------
        None

        """
        cdef GDALRasterBandH hband = NULL

        hband = self.band(bidx)
        GDALSetRasterUnitType(hband, (value or '').encode('utf-8'))
        # Invalidate cached units.
        self._units = ()

    def write_colormap(self, bidx, colormap):
        """Write a colormap for a band to the dataset.

        A colormap maps pixel values of a single-band dataset to RGB or
        RGBA colors.

        Parameters
        ----------
        bidx : int
            Index of the band (starting with 1).
        colormap : Mapping
            Keys are integers and values are 3 or 4-tuples of ints.

        Returns
        -------
        None

        """
        cdef GDALRasterBandH hBand = NULL
        cdef GDALColorTableH hTable = NULL
        cdef GDALColorEntry color

        hBand = self.band(bidx)

        # RGB only for now. TODO: the other types.
        # GPI_Gray=0,  GPI_RGB=1, GPI_CMYK=2, GPI_HLS=3
        hTable = GDALCreateColorTable(1)

        try:
            for i, rgba in colormap.items():
                if len(rgba) == 3:
                    rgba = tuple(rgba) + (255,)
                color.c1, color.c2, color.c3, color.c4 = rgba
                GDALSetColorEntry(hTable, i, &color)

            # TODO: other color interpretations?
            GDALSetRasterColorInterpretation(hBand, GCI_PaletteIndex)
            GDALSetRasterColorTable(hBand, hTable)

        finally:
            GDALDestroyColorTable(hTable)

    def write_mask(self, mask_array, window=None):
        """Write to the dataset's band mask.

        Values > 0 represent valid data.

        Parameters
        ----------
        mask_array : ndarray
            Values of 0 represent invalid or missing data. Values > 0
            represent valid data.
        window : Window, optional
            A subset of the dataset's band mask.

        Returns
        -------
        None

        Raises
        ------
        RasterioIOError
            When no mask is written.

        """
        cdef GDALRasterBandH band = NULL
        cdef GDALRasterBandH mask = NULL

        band = self.band(1)

        if not all(MaskFlags.per_dataset in flags for flags in self.mask_flag_enums):
            try:
                exc_wrap_int(GDALCreateMaskBand(band, MaskFlags.per_dataset))
                log.debug("Created mask band")
            except CPLE_BaseError:
                raise RasterioIOError("Failed to create mask.")

        try:
            mask = exc_wrap_pointer(GDALGetMaskBand(band))
        except CPLE_BaseError:
            raise RasterioIOError("Failed to get mask.")

        if window:
            if isinstance(window, tuple):
                window = Window.from_slices(*window, self.height, self.width)
            yoff = window.row_off
            xoff = window.col_off
            height = window.height
            width = window.width
        else:
            xoff = yoff = 0
            width = self.width
            height = self.height

        try:
            if mask_array is True or mask_array is np.True_:
                GDALFillRaster(mask, 255, 0)
            elif mask_array is False or mask_array is np.False_:
                GDALFillRaster(mask, 0, 0)
            elif mask_array.dtype == bool:
                array = 255 * mask_array.astype(np.uint8)
                io_band(mask, 1, xoff, yoff, width, height, array)
            else:
                io_band(mask, 1, xoff, yoff, width, height, mask_array)

        except CPLE_BaseError as cplerr:
            raise RasterioIOError("Write failed. See previous exception for details.") from cplerr

    def build_overviews(self, factors, resampling=Resampling.nearest):
        """Build overviews at one or more decimation factors for all
        bands of the dataset."""
        cdef int *factors_c = NULL
        cdef const char *resampling_c = NULL

        try:
            # GDALBuildOverviews() takes a string algo name, not a
            # Resampling enum member (like warping) and accepts only
            # a subset of the warp algorithms. 'NONE' is omitted below
            # (what does that even mean?) and so is 'AVERAGE_MAGPHASE'
            # (no corresponding member in the warp enum).
            resampling_map = {
                0: 'NEAREST',
                1: 'BILINEAR',
                2: 'CUBIC',
                3: 'CUBICSPLINE',
                4: 'LANCZOS',
                5: 'AVERAGE',
                6: 'MODE',
                7: 'GAUSS',
                14: 'RMS',
            }
            resampling_alg = resampling_map[Resampling(resampling.value)]

        except (KeyError, ValueError):
            raise ValueError(
                "resampling must be one of: {0}".format(", ".join(
                    ['Resampling.{0}'.format(Resampling(k).name) for k in
                     resampling_map.keys()])))

        # Check factors
        ovr_shapes = Counter([(int((self.height + f - 1) / f), int((self.width + f - 1) / f)) for f in factors])
        if ovr_shapes[(1, 1)] > 1:
            raise OverviewCreationError("Too many overviews levels of 1x1 dimension were requested")

        # Allocate arrays.
        if factors:
            factors_c = <int *>CPLMalloc(len(factors)*sizeof(int))

            for i, factor in enumerate(factors):
                factors_c[i] = factor

            try:
                resampling_b = resampling_alg.encode('utf-8')
                resampling_c = resampling_b
                GDALFlushCache(self._hds)
                exc_wrap_int(
                    GDALBuildOverviews(self._hds, resampling_c,
                                       len(factors), factors_c, 0, NULL, NULL,
                                       NULL))
            finally:
                if factors_c != NULL:
                    CPLFree(factors_c)

    def _set_gcps(self, gcps, crs=None):
        cdef char *srcwkt = NULL
        cdef GDAL_GCP *gcplist = <GDAL_GCP *>CPLMalloc(len(gcps) * sizeof(GDAL_GCP))

        try:
            for i, obj in enumerate(gcps):
                ident = str(i).encode('utf-8')
                info = "".encode('utf-8')
                gcplist[i].pszId = ident
                gcplist[i].pszInfo = info
                gcplist[i].dfGCPPixel = obj.col
                gcplist[i].dfGCPLine = obj.row
                gcplist[i].dfGCPX = obj.x
                gcplist[i].dfGCPY = obj.y
                gcplist[i].dfGCPZ = obj.z or 0.0

            # Try to use the primary crs if possible.
            if crs is None:
                crs = self.crs
            else:
                crs = CRS.from_user_input(crs)
            srcwkt_b = crs.to_wkt().encode('utf-8')
            srcwkt = srcwkt_b
            GDALSetGCPs(self.handle(), len(gcps), gcplist, srcwkt)
        finally:
            CPLFree(gcplist)

        # Invalidate cached value.
        self._gcps = None

    def _set_rpcs(self, rpcs):
        if hasattr(rpcs, 'to_gdal'):
            rpcs = rpcs.to_gdal()
        self.update_tags(ns='RPC', **rpcs)
        self._rpcs = None


cdef class MemoryDataset(DatasetWriterBase):
    def __init__(self, arr, transform=None, gcps=None, rpcs=None, crs=None, copy=False):
        """Dataset wrapped around in-memory array.

        This class is intended for internal use only within rasterio to
        support IO with GDAL, where a Dataset object is needed.

        MemoryDataset supports the NumPy array interface.

        Parameters
        ----------
        arr : ndarray
            Array to use for dataset
        transform : Transform
            Dataset transform
        gcps : list
            List of GroundControlPoints, a CRS
        rpcs : list
            Dataset rational polynomial coefficients
        crs : CRS
            Dataset coordinate reference system
        copy : bool, optional
            Create an internal copy of the array. If set to False,
            caller must make sure that arr is valid while this object
            lives. Default is to copy only as needed.

        """
        self._array = np.asanyarray(arr)
        if copy and self._array is arr:
            self._array = self._array.copy()

        dtype = self._array.dtype

        if self._array.ndim == 2:
            count = 1
            height, width = arr.shape
        elif self._array.ndim == 3:
            count, height, width = arr.shape
        else:
            raise ValueError("arr must be 2D or 3D array")

        arr_info = self._array.__array_interface__
        info = {
            "DATAPOINTER": arr_info["data"][0],
            "PIXELS": width,
            "LINES": height,
            "BANDS": count,
            "DATATYPE": _gdal_typename(arr.dtype.name)
        }
        strides = arr_info.get("strides", None)

        if strides is not None:
            if len(strides) == 2:
                lineoffset, pixeloffset = strides
                info.update(LINEOFFSET=lineoffset, PIXELOFFSET=pixeloffset)
            else:
                bandoffset, lineoffset, pixeloffset = strides
                info.update(BANDOFFSET=bandoffset, LINEOFFSET=lineoffset, PIXELOFFSET=pixeloffset)

        dataset_options = ",".join(f"{name}={val}" for name, val in info.items())
        datasetname = f"MEM:::{dataset_options}"

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with Env(GDAL_MEM_ENABLE_OPEN=True):
                super().__init__(_parse_path(datasetname), "r+")
            if crs is not None:
                self.crs = crs
            if transform is not None:
                self.transform = transform
            if gcps is not None and crs is not None:
                self.gcps = (gcps, crs)
            if rpcs is not None:
                self.rpcs = rpcs

    def __array__(self):
        return self._array


cdef class BufferedDatasetWriterBase(DatasetWriterBase):

    def __repr__(self):
        return "<%s IndirectRasterUpdater name='%s' mode='%s'>" % (
            self.closed and 'closed' or 'open',
            self.name,
            self.mode)

    def __init__(self, path, mode='r', driver=None, width=None, height=None,
                 count=None, crs=None, transform=None, dtype=None, nodata=None,
                 gcps=None, rpcs=None, sharing=False, **kwargs):
        """Construct a new dataset

        Parameters
        ----------
        path : rasterio.path.Path
            A remote or local dataset path.
        mode : str, optional
            'r' (read, the default), 'r+' (read/write), 'w' (write), or
            'w+' (write/read).
        driver : str, optional
            A short format driver name (e.g. "GTiff" or "JPEG") or a list of
            such names (see GDAL docs at
            http://www.gdal.org/formats_list.html). In 'w' or 'w+' modes
            a single name is required. In 'r' or 'r+' modes the driver can
            usually be omitted. Registered drivers will be tried
            sequentially until a match is found. When multiple drivers are
            available for a format such as JPEG2000, one of them can be
            selected by using this keyword argument.
        width, height : int, optional
            The numbers of rows and columns of the raster dataset. Required
            in 'w' or 'w+' modes, they are ignored in 'r' or 'r+' modes.
        count : int, optional
            The count of dataset bands. Required in 'w' or 'w+' modes, it is
            ignored in 'r' or 'r+' modes.
        crs : str, dict, or CRS; optional
            The coordinate reference system. Required in 'w' or 'w+' modes,
            it is ignored in 'r' or 'r+' modes.
        transform : Affine instance, optional
            Affine transformation mapping the pixel space to geographic
            space. Required in 'w' or 'w+' modes, it is ignored in 'r' or
            'r+' modes.
        dtype : str or numpy.dtype
            The data type for bands. For example: 'uint8' or
            ``rasterio.uint16``. Required in 'w' or 'w+' modes, it is
            ignored in 'r' or 'r+' modes.
        nodata : int, float, or nan; optional
            Defines the pixel value to be interpreted as not valid data.
            Required in 'w' or 'w+' modes, it is ignored in 'r' or 'r+'
            modes.
        sharing : bool
            A flag that allows sharing of dataset handles. Default is
            `False`. Should be set to `False` in a multithreaded program.
        kwargs : optional
            These are passed to format drivers as directives for creating or
            interpreting datasets. For example: in 'w' or 'w+' modes
            a `tiled=True` keyword argument will direct the GeoTIFF format
            driver to create a tiled, rather than striped, TIFF.
            driver : str or list of str, optional
            A single driver name or a list of names to be considered when
            opening the dataset. Required to create a new dataset.

        Returns
        -------
        dataset
        """
        cdef char **options = NULL
        cdef char *key_c = NULL
        cdef char *val_c = NULL
        cdef GDALDriverH drv = NULL
        cdef GDALRasterBandH band = NULL
        cdef const char *drv_name = NULL
        cdef GDALDriverH memdrv = NULL
        cdef GDALDatasetH temp = NULL

        # Validate write mode arguments.

        log.debug("Path: %s, mode: %s, driver: %s", path, mode, driver)
        if mode in ('w', 'w+'):
            if not isinstance(driver, str):
                raise TypeError("A driver name string is required.")
            try:
                width = int(width)
                height = int(height)
            except:
                raise TypeError("Integer width and height are required.")
            try:
                count = int(count)
            except:
                raise TypeError("Integer band count is required.")

            try:
                assert dtype is not None
                _ = _getnpdtype(dtype)
            except Exception:
                raise TypeError("A valid dtype is required.")

        self._init_dtype = _getnpdtype(dtype).name

        self.name = path.name
        self.mode = mode
        self.driver = driver
        self.width = width
        self.height = height
        self._count = count
        self._init_nodata = nodata
        self._count = count
        self._crs = crs
        if transform is not None:
            self._transform = transform.to_gdal()
        self._gcps = None
        self._init_gcps = gcps
        self._rpcs = None
        self._init_rpcs = rpcs
        self._dtypes = []
        self._nodatavals = []
        self._units = ()
        self._descriptions = ()
        self._options = kwargs.copy()

        # Make and store a GDAL dataset handle.

        # Parse the path to determine if there is scheme-specific
        # configuration to be done.
        vsi_filename = path.as_vsi()
        name_b = vsi_filename.encode('utf-8')

        memdrv = GDALGetDriverByName("MEM")

        if self.mode in ('w', 'w+'):
            # Find the equivalent GDAL data type or raise an exception
            # We've mapped numpy scalar types to GDAL types so see
            # if we can crosswalk those.
            if hasattr(self._init_dtype, 'type'):
                tp = self._init_dtype.type
                gdal_dtype = _get_gdal_dtype(tp)
            else:
                gdal_dtype = _get_gdal_dtype(self._init_dtype)

            self._hds = exc_wrap_pointer(
                GDALCreate(memdrv, "temp", self.width, self.height,
                           self._count, gdal_dtype, options))

            if self._init_nodata is not None:
                for i in range(self._count):
                    band = self.band(i+1)
                    GDALSetRasterNoDataValue(
                        band, self._init_nodata)
            if self._transform:
                self.write_transform(self._transform)
            if self._crs:
                self._set_crs(self._crs)
            if self._init_gcps:
                self._set_gcps(self._init_gcps, self._crs)
            if self._init_rpcs:
                self._set_rpcs(self._init_rpcs)

        elif self.mode == 'r+':
            try:
                temp = exc_wrap_pointer(GDALOpen(<const char *>name_b, <GDALAccess>0))
            except Exception as exc:
                raise RasterioIOError(str(exc))

            self._hds = exc_wrap_pointer(
                GDALCreateCopy(memdrv, "temp", temp, 1, NULL, NULL, NULL))

            drv = GDALGetDatasetDriver(temp)
            self.driver = get_driver_name(drv).decode('utf-8')
            GDALClose(temp)

        # Instead of calling _begin() we do the following.

        self._count = GDALGetRasterCount(self._hds)
        self.width = GDALGetRasterXSize(self._hds)
        self.height = GDALGetRasterYSize(self._hds)
        self.shape = (self.height, self.width)

        self._transform = self.read_transform()
        self._crs = self.read_crs()

        if options != NULL:
            CSLDestroy(options)

        _ = self.meta
        self._env = ExitStack()

    def stop(self):
        if self._hds == NULL:
            return

        cdef const char *drv_name = NULL
        cdef char **options = NULL
        cdef char *key_c = NULL
        cdef char *val_c = NULL
        cdef GDALDriverH drv = NULL
        cdef GDALDatasetH temp = NULL
        cdef const char *fname = NULL

        name_b = self.name.encode('utf-8')
        fname = name_b
        driver_b = self.driver.encode('utf-8')
        drv_name = driver_b
        drv = GDALGetDriverByName(drv_name)
        if drv == NULL:
            raise ValueError("NULL driver for %s", self.driver)

        options = convert_options(self._options)

        try:
            temp = exc_wrap_pointer(
                GDALCreateCopy(drv, fname, self._hds, 1, options, NULL, NULL))
            log.debug("Created copy from MEM file: %s", self.name)
        finally:
            if options != NULL:
                CSLDestroy(options)
            if temp != NULL:
                GDALClose(temp)
            if self._hds != NULL:
                refcount = GDALDereferenceDataset(self._hds)
                if refcount == 0:
                    GDALClose(self._hds)
            self._hds = NULL


def virtual_file_to_buffer(filename):
    """Read content of a virtual file into a Python bytes buffer."""
    cdef unsigned char *buff = NULL
    cdef const char *cfilename = NULL
    cdef vsi_l_offset buff_len = 0
    filename_b = filename if not isinstance(filename, str) else filename.encode('utf-8')
    cfilename = filename_b
    buff = VSIGetMemFileBuffer(cfilename, &buff_len, 0)
    n = buff_len
    log.debug("Buffer length: %d bytes", n)
    cdef np.uint8_t[:] buff_view = <np.uint8_t[:n]>buff
    return buff_view
