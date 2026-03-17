# cython: boundscheck=False, c_string_type=unicode, c_string_encoding=utf8

"""Transforms."""

include "gdal.pxi"

from contextlib import ExitStack
import logging
import warnings

import numpy as np

from rasterio._err import GDALError
from rasterio._err cimport exc_wrap_pointer
from rasterio.errors import NotGeoreferencedWarning, TransformWarning

log = logging.getLogger(__name__)


def _transform_from_gcps(gcps):
    cdef double gt[6]

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

        err = GDALGCPsToGeoTransform(len(gcps), gcplist, gt, 0)
        if err == GDALError.failure:
                warnings.warn(
                "Could not get geotransform set from gcps. The identity matrix may be returned.",
                NotGeoreferencedWarning)

    finally:
            CPLFree(gcplist)

    return [gt[i] for i in range(6)]


cdef class RPCTransformerBase:
    """
    Rational Polynomial Coefficients (RPC) transformer base class
    """
    cdef void *_transformer

    def __cinit__(self):
        self._transformer = NULL

    def __dealloc__(self):
        self.close()

    def __init__(self, rpcs, **kwargs):
        """
        Construct a new RPC transformer

        The RPCs map geographic coordinates referenced against the WGS84 ellipsoid (longitude, latitude, height)
        to image pixel/line coordinates. The reverse is done through an iterative solver implemented
        in GDAL.

        Parameters
        ----------
        rpcs : rasterio.rpc.RPC or dict
            RPCs for a dataset. If passing a dict, should be in the form expected
            by rasterio.rpc.RPC.from_gdal.
        kwargs : dict
            GDALCreateRPCTransformer options. See
            https://gdal.org/api/gdal_alg.html#_CPPv426GDALCreateRPCTransformerV2PK13GDALRPCInfoV2idPPc.

        Notes
        -----
        Explicit control of the transformer (and open datasets if RPC_DEM
        is specified) can be achieved by use within a context manager or 
        by calling `close()` method e.g.

        >>> with rasterio.transform.RPCTransformer(rpcs) as transform:
        ...    transform.xy(0, 0)
        >>> transform.xy(0, 0)
        ValueError: Unexpected NULL transformer

        Coordinate transformations using RPCs are typically:
            1. Only well-defined over the extent of the dataset the RPCs were generated.
            2. Require accurate height values in order to provide accurate results.
               Consider using RPC_DEM to supply a DEM to sample accurate height measurements
               from.
        """
        super().__init__()
        cdef char **papszMD = NULL
        cdef char **options = NULL
        cdef int bReversed = 1
        cdef double dfPixErrThreshold = 0.1  # GDAL default
        cdef GDALRPCInfo rpcinfo

        if hasattr(rpcs, 'to_gdal'):
            rpcs = rpcs.to_gdal()
        for key, val in rpcs.items():
            key = key.upper().encode('utf-8')
            val = str(val).encode('utf-8')
            papszMD = CSLSetNameValue(
                papszMD, <const char *>key, <const char *>val)

        for key, val in kwargs.items():
            key = key.upper().encode('utf-8')
            if key == b"RPC_DEM":
                # don't .upper() since might be a path
                val = str(val).encode('utf-8')
            else:
                val = str(val).upper().encode('utf-8')
            options = CSLSetNameValue(
                options, <const char *>key, <const char *>val)
            log.debug("Set RPCTransformer option {0!r}={1!r}".format(key, val))

        try:
            GDALExtractRPCInfo(papszMD, &rpcinfo)
            self._transformer = exc_wrap_pointer(GDALCreateRPCTransformer(&rpcinfo, bReversed, dfPixErrThreshold, options))
        finally:
            CSLDestroy(options)
            CSLDestroy(papszMD)

    def _transform(self, xs, ys, zs, transform_direction):
        """
        General computation of dataset pixel/line <-> lon/lat/height coordinates using RPCs

        Parameters
        ----------
        xs, ys, zs : ndarray
            List of coordinates to be transformed. May be either pixel/line/height or
            lon/lat/height)
        transform_direction : TransformDirection
            The transform direction i.e. forward implies pixel/line -> lon/lat/height
            while reverse implies lon/lat/height -> pixel/line. 

        Raises
        ------
        ValueError
            If transformer is NULL

        Warns
        -----
        rasterio.errors.TransformWarning
            If one or more coordinates failed to transform

        Returns
        -------
        tuple of ndarray
    
        Notes
        -----
        When RPC_DEM option is used, height (zs) values in _transform are ignored by GDAL and instead sampled from a DEM

        """
        if self._transformer == NULL:
            raise ValueError("Unexpected NULL transformer")

        cdef Py_ssize_t i
        cdef int n
        cdef double *x = NULL
        cdef double *y = NULL
        cdef double *z = NULL
        cdef int bDstToSrc = transform_direction
        cdef int src_count
        cdef int *panSuccess = NULL

        cdef double[:] rxview, ryview, rzview
        cdef tuple xyz

        bi = np.broadcast(xs, ys, zs)
        n = bi.size
        x = <double *>CPLMalloc(n * sizeof(double))
        y = <double *>CPLMalloc(n * sizeof(double))
        z = <double *>CPLMalloc(n * sizeof(double))
        panSuccess = <int *>CPLMalloc(n * sizeof(int))

        for i, xyz in enumerate(bi):
            x[i] = <double>xyz[0]
            y[i] = <double>xyz[1]
            z[i] = <double>xyz[2]

        try:
            err = GDALRPCTransform(self._transformer, bDstToSrc, n, x, y, z, panSuccess)
            if err == GDALError.failure:
                warnings.warn(
                "Could not transform points using RPCs.",
                TransformWarning)
            res_xs = np.zeros(n)
            res_ys = np.zeros(n)
            #res_zs = np.zeros(n)
            rxview = res_xs
            ryview = res_ys
            checked = False
            for i in range(n):
                # GDALRPCTransformer may return a success overall despite individual points failing. Warn once.
                if not checked and not panSuccess[i]:
                    warnings.warn(
                    "One or more points could not be transformed using RPCs.",
                    TransformWarning)
                    checked = True
                rxview[i] = x[i]
                ryview[i] = y[i]
                #res_zs[i] = z[i]
        finally:
            CPLFree(x)
            CPLFree(y)
            CPLFree(z)
            CPLFree(panSuccess)

        return (res_xs, res_ys)

    def close(self):
        """
        Destroy transformer
        """
        if self._transformer == NULL:
            return

        GDALDestroyRPCTransformer(self._transformer)
        self._transformer = NULL

    @property
    def closed(self):
        """
        Returns if transformer is NULL
        """
        return self._transformer == NULL


cdef class GCPTransformerBase:
    cdef void *_transformer
    cdef bint _tps

    def __cinit__(self):
        self._transformer = NULL

    def __dealloc__(self):
        self.close()

    def __init__(self, gcps, tps=False):
        """
        Construct a new GCP transformer

        Ground Control Points (GCPs) can be used to map specific world coordinates to pixel coordinates
        within an image. GDAL can use GCP interpolation in order to compute image pixel or geographic/
        projected coordinates as needed. Rasterio allows GDAL to determine the appropriate kind of 
        interpolation (up to cubic) depending on the number of GCPs available.

        Parameters
        ----------
        gcps : a sequence of GroundControlPoint
            Ground Control Points for a dataset.
        tps : bool
            If True, use GDALs thin plate spline transformer instead of polynomials.
        """
        super().__init__()
        cdef int bReversed = 1
        cdef int nReqOrder = 0  # let GDAL determine polynomial order
        cdef GDAL_GCP *gcplist = <GDAL_GCP *>CPLMalloc(len(gcps) * sizeof(GDAL_GCP))
        cdef int nGCPCount = len(gcps)
        self._tps = tps

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
            if self._tps:
                self._transformer = exc_wrap_pointer(GDALCreateTPSTransformer(nGCPCount, gcplist, bReversed))
            else:
                self._transformer = exc_wrap_pointer(GDALCreateGCPTransformer(nGCPCount, gcplist, nReqOrder, bReversed))
        finally:
            CPLFree(gcplist)

    def _transform(self, xs, ys, zs, transform_direction):
        """
        General computation of dataset pixel/line <-> lon/lat/height coordinates using GCPs

        Parameters
        ----------
        xs, ys, zs : ndarray
            List of coordinates to be transformed. May be either pixel/line/height or
            lon/lat/height)
        transform_direction : TransformDirection
            The transform direction i.e. forward implies pixel/line -> lon/lat/height
            while reverse implies lon/lat/height -> pixel/line. 

        Raises
        ------
        ValueError
            If transformer is NULL

        Warns
        -----
        rasterio.errors.TransformWarning
            If one or more coordinates failed to transform

        Returns
        -------
        tuple of ndarray
        """
        if self._transformer == NULL:
            raise ValueError("Unexpected NULL transformer")

        cdef Py_ssize_t i
        cdef int n
        cdef double *x = NULL
        cdef double *y = NULL
        cdef double *z = NULL
        cdef int bDstToSrc = transform_direction
        cdef int src_count
        cdef int *panSuccess = NULL

        cdef double[:] rxview, ryview, rzview
        cdef tuple xyz

        bi = np.broadcast(xs, ys, zs)
        n = bi.size
        x = <double *>CPLMalloc(n * sizeof(double))
        y = <double *>CPLMalloc(n * sizeof(double))
        z = <double *>CPLMalloc(n * sizeof(double))
        panSuccess = <int *>CPLMalloc(n * sizeof(int))

        for i, xyz in enumerate(bi):
            x[i] = <double>xyz[0]
            y[i] = <double>xyz[1]
            z[i] = <double>xyz[2]

        try:
            if self._tps:
                err = GDALTPSTransform(self._transformer, bDstToSrc, n, x, y, z, panSuccess)
            else:
                err = GDALGCPTransform(self._transformer, bDstToSrc, n, x, y, z, panSuccess)
            if err == GDALError.failure:
                warnings.warn(
                "Could not transform points using GCPs.",
                TransformWarning)
            res_xs = np.zeros(n)
            res_ys = np.zeros(n)
            #res_zs = np.zeros(n)
            rxview = res_xs
            ryview = res_ys
            checked = False
            for i in range(n):
                # GDALGCPTransformer or GDALTPSTransformer may return a success overall despite individual points failing. Warn once.
                if not checked and not panSuccess[i]:
                    warnings.warn(
                    "One or more points could not be transformed using GCPs.",
                    TransformWarning)
                    checked = True
                rxview[i] = x[i]
                ryview[i] = y[i]
                #res_zs[i] = z[i]
        finally:
            CPLFree(x)
            CPLFree(y)
            CPLFree(z)
            CPLFree(panSuccess)

        return (res_xs, res_ys)

    def close(self):
        """
        Destroy transformer
        """
        if self._transformer == NULL:
            return
        if self._tps:
            GDALDestroyTPSTransformer(self._transformer)
        else:
            GDALDestroyGCPTransformer(self._transformer)
        self._transformer = NULL

    @property
    def closed(self):
        """
        Returns if transformer is NULL
        """
        return self._transformer == NULL
