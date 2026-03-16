# distutils: language=c++

"""Raster and vector warping and reprojection."""

from collections import UserDict
from collections.abc import Mapping
from contextlib import ExitStack
import logging
import uuid
import warnings
import xml.etree.ElementTree as ET

from affine import Affine, identity
import numpy as np

import rasterio
from rasterio._base import _transform
from rasterio._base cimport open_dataset
from rasterio._err import (
    CPLE_BaseError, CPLE_IllegalArgError, CPLE_NotSupportedError,
    CPLE_AppDefinedError, CPLE_OpenFailedError, stack_errors)
from rasterio import dtypes
from rasterio.control import GroundControlPoint
from rasterio.enums import Resampling, MaskFlags, ColorInterp
from rasterio.env import Env, GDALVersion
from rasterio.crs import CRS
from rasterio.errors import (
    GDALOptionNotImplementedError,
    DriverRegistrationError, CRSError, RasterioIOError,
    RasterioDeprecationWarning, WarpOptionsError, WarpedVRTError,
    WarpOperationError)
from rasterio.transform import Affine, from_bounds, guard_transform, tastes_like_gdal

cimport cython
cimport numpy as np
from libc.math cimport HUGE_VAL

from rasterio._base cimport get_driver_name
from rasterio._err cimport exc_wrap, exc_wrap_pointer, exc_wrap_int, StackChecker
from rasterio._io cimport (
    DatasetReaderBase, MemoryDataset, in_dtype_range, io_auto, io_band, io_multi_band)
from rasterio._features cimport GeomBuilder, OGRGeomBuilder
from rasterio.crs cimport CRS

np.import_array()
log = logging.getLogger(__name__)

# Gauss (7) is not supported for warp
SUPPORTED_RESAMPLING = [r for r in Resampling if r.value != 7]


def recursive_round(val, precision):
    """Recursively round coordinates."""
    if isinstance(val, (int, float)):
        return round(val, precision)
    else:
        return [recursive_round(part, precision) for part in val]


cdef dict TRANSFORMER_OPTIONS = None
IF (CTE_GDAL_MAJOR_VERSION, CTE_GDAL_MINOR_VERSION) >= (3, 11):
    TRANSFORMER_OPTIONS = {
        option.get("name"): option.get("description") for option
        in ET.fromstring(
            GDALGetGenImgProjTranformerOptionList().decode("utf-8")
        ).findall("Option")
    }


cdef bint is_transformer_option(bytes key):
    if TRANSFORMER_OPTIONS is None:
        return True
    return key.decode("utf-8") in TRANSFORMER_OPTIONS


cdef object _transform_single_geom(
    object single_geom,
    OGRGeometryFactory *factory,
    void *transform,
    char **options,
    int precision
):
    cdef OGRGeometryH src_geom = NULL
    cdef OGRGeometryH dst_geom = NULL

    try:
        src_geom = exc_wrap_pointer(OGRGeomBuilder().build(single_geom))
        dst_geom = exc_wrap_pointer(
            factory.transformWithOptions(
                <const OGRGeometry *>src_geom,
                <OGRCoordinateTransformation *>transform,
                options))
    else:
        result = GeomBuilder().build(dst_geom)
    finally:
        OGR_G_DestroyGeometry(dst_geom)
        OGR_G_DestroyGeometry(src_geom)

    if precision >= 0:
        # TODO: Geometry collections.
        result['coordinates'] = recursive_round(result['coordinates'], precision)

    return result


def _transform_geom(
    src_crs,
    dst_crs,
    geom,
    int precision,
):
    """Return a transformed geometry."""
    cdef char **options = NULL
    cdef OGRCoordinateTransformationH transform = NULL
    cdef OGRGeometryFactory *factory = NULL

    cdef CRS src = CRS.from_user_input(src_crs)
    cdef CRS dst = CRS.from_user_input(dst_crs)

    transform = exc_wrap_pointer(OCTNewCoordinateTransformation(src._osr, dst._osr))

    factory = new OGRGeometryFactory()

    try:
        if isinstance(geom, (dict, Mapping, UserDict)) or hasattr(geom, "__geo_interface__"):
            out_geom = _transform_single_geom(geom, factory, transform, options, precision)
        else:
            out_geom = [
                _transform_single_geom(single_geom, factory, transform, options, precision)
                for single_geom in geom
            ]
    finally:
        del factory
        OCTDestroyCoordinateTransformation(transform)
        if options != NULL:
            CSLDestroy(options)

    return out_geom


cdef GDALWarpOptions * create_warp_options(
        GDALResampleAlg resampling, object src_nodata, object dst_nodata, int src_count,
        object dst_alpha, object src_alpha, int warp_mem_limit, GDALDataType working_data_type, const char **options) except NULL:
    """Return a pointer to a GDALWarpOptions composed from input params

    This is used in _reproject() and the WarpedVRT constructor. It sets
    up warp options in almost exactly the same way as gdawarp.

    Parameters
    ----------
    dst_alpha : int
        This parameter specifies a destination alpha band for the
        warper.
    src_alpha : int
        This parameter specifies the source alpha band for the warper.

    """

    cdef GDALWarpOptions *psWOptions = GDALCreateWarpOptions()

    # Note: warp_extras is pointed to different memory locations on every
    # call to CSLSetNameValue call below, but needs to be set here to
    # get the defaults.
    cdef char **warp_extras = psWOptions.papszWarpOptions

    # See https://gdal.org/doxygen/structGDALWarpOptions.html#a0ed77f9917bb96c7a9aabd73d4d06e08
    # for a list of supported options. Copying unsupported options
    # is fine.

    # Use the same default nodata logic as gdalwarp.
    warp_extras = CSLSetNameValue(
        warp_extras, "UNIFIED_SRC_NODATA", "YES")

    warp_extras = CSLMerge(warp_extras, <char **>options)

    psWOptions.eWorkingDataType = <GDALDataType>working_data_type
    psWOptions.eResampleAlg = <GDALResampleAlg>resampling

    if warp_mem_limit > 0:
        psWOptions.dfWarpMemoryLimit = <double>warp_mem_limit * 1024 * 1024
        log.debug("Warp Memory Limit set: {!r}".format(warp_mem_limit))

    band_count = src_count

    if src_alpha:
        psWOptions.nSrcAlphaBand = src_alpha

    if dst_alpha:
        psWOptions.nDstAlphaBand = dst_alpha

    # Assign nodata values.
    # We don't currently support an imaginary component.

    if src_nodata is not None:
        psWOptions.padfSrcNoDataReal = <double*>CPLMalloc(band_count * sizeof(double))
        psWOptions.padfSrcNoDataImag = <double*>CPLMalloc(band_count * sizeof(double))

        for i in range(band_count):
            psWOptions.padfSrcNoDataReal[i] = float(src_nodata)
            psWOptions.padfSrcNoDataImag[i] = 0.0

    if dst_nodata is not None:
        psWOptions.padfDstNoDataReal = <double*>CPLMalloc(band_count * sizeof(double))
        psWOptions.padfDstNoDataImag = <double*>CPLMalloc(band_count * sizeof(double))

        for i in range(band_count):
            psWOptions.padfDstNoDataReal[i] = float(dst_nodata)
            psWOptions.padfDstNoDataImag[i] = 0.0


    # Important: set back into struct or values set above are lost
    # This is because CSLSetNameValue returns a new list each time
    psWOptions.papszWarpOptions = warp_extras

    # Set up band info
    if psWOptions.nBandCount == 0:
        psWOptions.nBandCount = band_count

        psWOptions.panSrcBands = <int*>CPLMalloc(band_count * sizeof(int))
        psWOptions.panDstBands = <int*>CPLMalloc(band_count * sizeof(int))

        for i in range(band_count):
            psWOptions.panSrcBands[i] = i + 1
            psWOptions.panDstBands[i] = i + 1

    return psWOptions


def _reproject(
        source, destination,
        src_transform=None,
        gcps=None,
        rpcs=None,
        src_crs=None,
        src_nodata=None,
        dst_transform=None,
        dst_crs=None,
        dst_nodata=None,
        dst_alpha=0,
        src_alpha=0,
        resampling=Resampling.nearest,
        init_dest_nodata=True,
        tolerance=0.125,
        num_threads=1,
        warp_mem_limit=0,
        working_data_type=0,
        src_geoloc_array=None,
        **kwargs):
    """
    Reproject a source raster to a destination raster.

    If the source and destination are ndarrays, coordinate reference
    system definitions and geolocation parameters are required for
    reprojection. Only one of src_transform, gcps, rpcs, or
    src_geoloc_array can be used.

    If the source and destination are rasterio Bands, shorthand for
    bands of datasets on disk, the coordinate reference systems and
    transforms will be read from the appropriate datasets.

    Parameters
    ------------
    source : ndarray or rasterio Band
        Source raster.
    destination : ndarray or rasterio Band
        Target raster.
    src_transform : affine.Affine(), optional
        Source affine transformation.  Required if source and destination
        are ndarrays.  Will be derived from source if it is a rasterio Band.
    gcps : sequence of `GroundControlPoint` instances, optional
        Ground control points for the source. May be used in place of
        src_transform.
    rpcs : RPC or dict, optional
        Rational polynomial coefficients for the source. May be used
        in place of src_transform.
    src_geoloc_array : array-like, optional
        A pair of 2D arrays holding x and y coordinates, like a like
        a dense array of ground control points that may be used in place
        of src_transform.
    src_crs : dict, optional
        Source coordinate reference system, in rasterio dict format.
        Required if source and destination are ndarrays.  Will be
        derived from source if it is a rasterio Band.  Example:
        'EPSG:4326'.
    src_nodata : int or float, optional
        The source nodata value.  Pixels with this value will not be used
        for interpolation.  If not set, it will be default to the
        nodata value of the source image if a masked ndarray or rasterio band,
        if available.
    dst_transform : affine.Affine(), optional
        Target affine transformation.  Required if source and destination
        are ndarrays.  Will be derived from target if it is a rasterio Band.
    dst_crs : dict, optional
        Target coordinate reference system.  Required if source and destination
        are ndarrays.  Will be derived from target if it is a rasterio Band.
    dst_nodata : int or float, optional
        The nodata value used to initialize the destination; it will remain
        in all areas not covered by the reprojected source.  Defaults to the
        nodata value of the destination image (if set), the value of
        src_nodata, or 0 (gdal default).
    src_alpha : int, optional
        Index of a band to use as the source alpha band when warping.
    dst_alpha : int, optional
        Index of a band to use as the destination alpha band when warping.
    resampling : int
        Resampling method to use.  One of the following:
            Resampling.nearest,
            Resampling.bilinear,
            Resampling.cubic,
            Resampling.cubic_spline,
            Resampling.lanczos,
            Resampling.average,
            Resampling.mode
    init_dest_nodata : bool
        Flag to specify initialization of nodata in destination;
        prevents overwrite of previous warps. Defaults to True.
    tolerance : float, optional
        The maximum error tolerance in input pixels when
        approximating the warp transformation. Default: 0.125,
        or one-eigth of a pixel.
    num_threads : int
        Number of worker threads.
    warp_mem_limit : int, optional
        The warp operation memory limit in MB. Larger values allow the
        warp operation to be carried out in fewer chunks. The amount of
        memory required to warp a 3-band uint8 2000 row x 2000 col
        raster to a destination of the same size is approximately
        56 MB. The default (0) means 64 MB with GDAL 2.2.
        The warp operation's memory limit in MB. The default (0)
        means 64 MB with GDAL 2.2.
    kwargs :  dict, optional
        Additional arguments passed to both the image to image
        transformer GDALCreateGenImgProjTransformer2() (for example,
        MAX_GCP_ORDER=2) and to the Warper (for example,
        INIT_DEST=NO_DATA).

    Returns
    ---------
    out : None
        Output is written to destination.
    """
    cdef int src_count
    cdef GDALDatasetH src_dataset = NULL
    cdef GDALDatasetH dst_dataset = NULL
    cdef char **warp_extras = NULL
    cdef const char* pszWarpThread = NULL
    cdef int i
    cdef void *hTransformArg = NULL
    cdef GDALTransformerFunc pfnTransformer = NULL
    cdef GDALWarpOptions *psWOptions = NULL
    cdef bint bUseApproxTransformer = True

    # Source geolocation parameters are mutually exclusive.
    if sum(param is not None for param in (src_transform, gcps, rpcs, src_geoloc_array)) > 1:
        raise ValueError("Source geolocation parameters are mutually exclusive.")

    # Validate nodata values immediately.
    if src_nodata is not None:
        if not in_dtype_range(src_nodata, source.dtype):
            raise ValueError("src_nodata must be in valid range for "
                             "source dtype")

    if dst_nodata is not None:
        if not in_dtype_range(dst_nodata, destination.dtype):
            raise ValueError("dst_nodata must be in valid range for "
                             "destination dtype")

    def format_transform(in_transform):
        if not in_transform:
            return in_transform
        in_transform = guard_transform(in_transform)
        # If working with identity transform, assume it is crs-less data
        # and that translating the matrix very slightly will avoid #674 and #1272
        eps = 1e-100
        if in_transform.almost_equals(identity) or in_transform.almost_equals(Affine(1, 0, 0, 0, -1, 0)):
            in_transform = in_transform.translation(eps, eps)
        return in_transform

    cdef:
        MemoryDataset mem_raster = None
        MemoryDataset src_mem = None
        char **imgProjOptions = NULL
        GDALRasterBandH hBand = NULL
        GDALWarpOperation oWarper
        int rows
        int cols
        StackChecker checker

    with ExitStack() as exit_stack:
        # If the source is an ndarray, we copy to a MEM dataset.
        # We need a src_transform and src_dst in this case. These will
        # be copied to the MEM dataset.
        if dtypes.is_ndarray(source):
            if not src_crs:
                raise CRSError("Missing src_crs.")

            # ensure data converted to numpy array
            if hasattr(source, "mask"):
                source = np.ma.asanyarray(source)
            else:
                source = np.asanyarray(source)

            # Convert 2D single-band arrays to 3D multi-band.
            if len(source.shape) == 2:
                source = source.reshape(1, *source.shape)

            src_count = source.shape[0]
            src_bidx = range(1, src_count + 1)

            if hasattr(source, "mask"):
                if source.mask is np.ma.nomask:
                    if source.ndim == 2:
                        mask_shape = source.shape
                    else:
                        mask_shape = source.shape[1:]
                    mask = np.full(mask_shape, np.uint8(255))
                else:
                    mask = ~np.logical_or.reduce(source.mask) * np.uint8(255)
                source_arr = np.concatenate((source.data, [mask]))
                src_alpha = src_alpha or source_arr.shape[0]
            else:
                source_arr = source

            src_mem = exit_stack.enter_context(
                MemoryDataset(
                    source_arr,
                    transform=format_transform(src_transform),
                    gcps=gcps,
                    rpcs=rpcs,
                    crs=src_crs,
                    copy=True,
                )
            )
            src_dataset = src_mem.handle()

            if src_alpha:
                for i in range(source_arr.shape[0]):
                    GDALDeleteRasterNoDataValue(GDALGetRasterBand(src_dataset, i+1))
                GDALSetRasterColorInterpretation(
                    GDALGetRasterBand(src_dataset, src_alpha),
                    <GDALColorInterp>6,
                )

        # If the source is a rasterio MultiBand, no copy necessary.
        # A MultiBand is a tuple: (dataset, bidx, dtype, shape(2d))
        elif isinstance(source, tuple):
            rdr, src_bidx, dtype, shape = source
            if isinstance(src_bidx, int):
                src_bidx = [src_bidx]
            src_count = len(src_bidx)
            src_dataset = (<DatasetReaderBase?>rdr).handle()
            if src_nodata is None:
                src_nodata = rdr.nodata

        else:
            raise ValueError("Invalid source")

        # Next, do the same for the destination raster.
        dest_2d = False

        if dtypes.is_ndarray(destination):
            if not dst_crs:
                raise CRSError("Missing dst_crs.")

            dst_nodata = dst_nodata or src_nodata

            if hasattr(destination, "mask"):
                destination = np.ma.asanyarray(destination)
            else:
                destination = np.asanyarray(destination)

            if len(destination.shape) == 2:
                dest_2d = True
                destination = destination.reshape(1, *destination.shape)

            if destination.shape[0] == src_count:
                # Output shape matches number of bands being extracted
                dst_bidx = [i + 1 for i in range(src_count)]
            else:
                # Assume src and dst are the same shape
                if max(src_bidx) > destination.shape[0]:
                    raise ValueError("Invalid destination shape")
                dst_bidx = src_bidx

            if hasattr(destination, "mask"):
                count, height, width = destination.shape
                msk = np.logical_or.reduce(destination.mask)
                if msk == np.ma.nomask:
                    msk = np.zeros((height, width), dtype=bool)
                msk = ~msk * np.uint8(255)
                dest_arr = np.concatenate((destination.data, [msk]))
                dst_alpha = dst_alpha or dest_arr.shape[0]
            else:
                dest_arr = destination

            mem_raster = exit_stack.enter_context(
                MemoryDataset(dest_arr, transform=format_transform(dst_transform), crs=dst_crs)
            )
            dst_dataset = mem_raster.handle()

            if dst_alpha:
                for i in range(dest_arr.shape[0]):
                    GDALDeleteRasterNoDataValue(GDALGetRasterBand(dst_dataset, i+1))
                GDALSetRasterColorInterpretation(
                    GDALGetRasterBand(dst_dataset, dst_alpha),
                    <GDALColorInterp>6,
                )

            GDALSetDescription(
                dst_dataset, "Temporary destination dataset for _reproject()")

            log.debug("Created temp destination dataset.")

        elif isinstance(destination, tuple):
            udr, dst_bidx, _, _ = destination
            if isinstance(dst_bidx, int):
                dst_bidx = [dst_bidx]
            dst_dataset = (<DatasetReaderBase?>udr).handle()
            if dst_nodata is None:
                dst_nodata = udr.nodata
        else:
            raise ValueError("Invalid destination")


        # Set up GDALCreateGenImgProjTransformer2 keyword arguments.
        imgProjOptions = CSLSetNameValue(imgProjOptions, "GCPS_OK", "TRUE")

        if rpcs:
            imgProjOptions = CSLSetNameValue(imgProjOptions, "SRC_METHOD", "RPC")
        elif src_geoloc_array is not None:
            arr = np.stack((src_geoloc_array[0], src_geoloc_array[1]))
            geoloc_dataset = exit_stack.enter_context(MemoryDataset(arr, crs=src_crs))
            log.debug("Geoloc dataset created: geoloc_dataset=%r", geoloc_dataset)
            imgProjOptions = CSLSetNameValue(
                imgProjOptions, "SRC_GEOLOC_ARRAY", geoloc_dataset.name.encode("utf-8")
            )
            imgProjOptions = CSLSetNameValue(
                imgProjOptions, "SRC_SRS", src_crs.to_string().encode("utf-8")
            )
            bUseApproxTransformer = False

        # See https://gdal.org/doxygen/gdal__alg_8h.html#a94cd172f78dbc41d6f407d662914f2e3
        # for a list of supported options. I (Sean) don't see harm in
        # copying all the function's keyword arguments to the image to
        # image transformer options mapping; unsupported options should be
        # okay.
        for key, val in kwargs.items():
            key = key.upper().encode('utf-8')

            if not is_transformer_option(key):
                continue

            if key in {b"RPC_DEM", b"COORDINATE_OPERATION"}:
                # don't .upper() since might be a path
                val = str(val).encode('utf-8')

                if rpcs:
                    bUseApproxTransformer = False
            else:
                val = str(val).upper().encode('utf-8')
            imgProjOptions = CSLSetNameValue(
                imgProjOptions, <const char *>key, <const char *>val)
            log.debug("Set _reproject Transformer option {0!r}={1!r}".format(key, val))

        try:
            with Env(GDAL_MEM_ENABLE_OPEN=True):
                hTransformArg = exc_wrap_pointer(
                    GDALCreateGenImgProjTransformer2(
                        src_dataset, dst_dataset, imgProjOptions))
            if bUseApproxTransformer:
                hTransformArg = exc_wrap_pointer(
                    GDALCreateApproxTransformer(
                        GDALGenImgProjTransform, hTransformArg, tolerance))
                pfnTransformer = GDALApproxTransform
                GDALApproxTransformerOwnsSubtransformer(hTransformArg, 1)
                log.debug("Created approximate transformer")
            else:
                pfnTransformer = GDALGenImgProjTransform
                log.debug("Created exact transformer")

            log.debug("Created transformer and options.")
        except:
            if bUseApproxTransformer:
                GDALDestroyApproxTransformer(hTransformArg)
            else:
                GDALDestroyGenImgProjTransformer(hTransformArg)
            CPLFree(imgProjOptions)
            raise

        valb = str(num_threads).encode('utf-8')
        warp_extras = CSLSetNameValue(warp_extras, "NUM_THREADS", <const char *>valb)

        log.debug("Setting NUM_THREADS option: %d", num_threads)

        if init_dest_nodata:
            warp_extras = CSLSetNameValue(warp_extras, "INIT_DEST",
                                        "NO_DATA" if dst_nodata is not None else "0")

        # See https://gdal.org/doxygen/structGDALWarpOptions.html#a0ed77f9917bb96c7a9aabd73d4d06e08
        # for a list of supported options. Copying unsupported options
        # is fine.
        for key, val in kwargs.items():
            key = key.upper().encode('utf-8')
            val = str(val).upper().encode('utf-8')
            warp_extras = CSLSetNameValue(
                warp_extras, <const char *>key, <const char *>val)

        psWOptions = create_warp_options(
            <GDALResampleAlg>resampling,
            src_nodata,
            dst_nodata,
            src_count,
            dst_alpha,
            src_alpha,
            warp_mem_limit,
            <GDALDataType>working_data_type,
            <const char **>warp_extras
        )

        psWOptions.pfnTransformer = pfnTransformer
        psWOptions.pTransformerArg = hTransformArg
        psWOptions.hSrcDS = src_dataset
        psWOptions.hDstDS = dst_dataset

        for idx, (s, d) in enumerate(zip(src_bidx, dst_bidx)):
            psWOptions.panSrcBands[idx] = s
            psWOptions.panDstBands[idx] = d
            log.debug('Configured to warp src band %d to destination band %d' % (s, d))

        log.debug("Set transformer options")

        # Now that the transformer and warp options are set up, we init
        # and run the warper.
        try:
            exc_wrap_int(oWarper.Initialize(psWOptions))
            if isinstance(destination, tuple):
                rows, cols = destination[3]
            else:
                rows, cols = destination.shape[-2:]

            log.debug(
                "Chunk and warp window: %d, %d, %d, %d.",
                0, 0, cols, rows)

            try:
                with stack_errors() as checker, Env(GDAL_MEM_ENABLE_OPEN=True):
                    if num_threads > 1:
                        with nogil:
                            err = oWarper.ChunkAndWarpMulti(0, 0, cols, rows)
                    else:
                        with nogil:
                            err = oWarper.ChunkAndWarpImage(0, 0, cols, rows)

                    err = checker.exc_wrap_int(err)

            except CPLE_BaseError as base:
                raise WarpOperationError("Chunk and warp failed") from base

            if mem_raster is not None:
                count, height, width = dest_arr.shape
                if hasattr(destination, "mask"):
                    # Pick off the alpha band and make a mask of it.
                    # TODO: do this efficiently, not copying unless necessary.
                    indexes = np.arange(1, count, dtype='intp')
                    io_multi_band(dst_dataset, 0, 0.0, 0.0, width, height, destination, indexes)
                    alpha_arr = np.empty((height, width), dtype=dest_arr.dtype)
                    io_band(mem_raster.band(count), 0, 0.0, 0.0, width, height, alpha_arr)
                    destination = np.ma.masked_array(
                        destination.data,
                        mask=np.repeat(
                            ~(alpha_arr.astype("bool"))[np.newaxis, :, :],
                            count - 1,
                            axis=0,
                        )
                    )
                else:
                    exc_wrap_int(io_auto(destination, dst_dataset, 0))

                if dest_2d:
                    destination = destination[0]

                return destination

        # Clean up transformer, warp options, and dataset handles.
        finally:
            if bUseApproxTransformer:
                GDALDestroyApproxTransformer(hTransformArg)
            else:
                GDALDestroyGenImgProjTransformer(hTransformArg)

            GDALDestroyWarpOptions(psWOptions)
            CPLFree(imgProjOptions)
            CSLDestroy(warp_extras)


def _calculate_default_transform(
    src_crs,
    dst_crs,
    width,
    height,
    left=None,
    bottom=None,
    right=None,
    top=None,
    gcps=None,
    rpcs=None,
    src_geoloc_array=None,
    **kwargs
):
    """Wraps GDAL's algorithm."""
    cdef void *hTransformArg = NULL
    cdef int npixels = 0
    cdef int nlines = 0
    cdef double extent[4]
    cdef double geotransform[6]
    cdef char *wkt = NULL
    cdef GDALDatasetH hds = NULL
    cdef char **imgProjOptions = NULL
    cdef char **papszMD = NULL
    cdef bint bUseApproxTransformer = True

    # Source geolocation parameters are mutually exclusive.
    # if sum(param is not None for param in (left, gcps, rpcs, src_geoloc_array)) > 1:
    #     raise ValueError("Source geolocation parameters are mutually exclusive.")

    extent[:] = [0.0, 0.0, 0.0, 0.0]
    geotransform[:] = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0]

    if all(x is not None for x in (left, bottom, right, top)):
        transform = from_bounds(left, bottom, right, top, width, height)
    elif any(x is not None for x in (left, bottom, right, top)):
        raise ValueError(
            "Some, but not all, bounding box parameters were provided.")
    else:
        transform = None

    dst_crs = CRS.from_user_input(dst_crs)
    wkt_b = dst_crs.to_wkt().encode('utf-8')
    wkt = wkt_b

    if src_crs is not None:
        src_crs = CRS.from_user_input(src_crs)

    # The transformer at the heart of this function requires a dataset.
    # We use an in-memory VRT dataset.
    vrt_doc = _suggested_proxy_vrt_doc(
        width,
        height,
        transform=transform,
        crs=src_crs,
        gcps=gcps,
        rpcs=rpcs,
    ).decode('utf-8')

    hds = open_dataset(
        filename=vrt_doc,
        flags=GDAL_OF_READONLY,
        allowed_drivers=['VRT'],
        open_options={},
        sharing=False,
        siblings=None,
    )

    with ExitStack() as exit_stack:
        try:
            imgProjOptions = CSLSetNameValue(imgProjOptions, "GCPS_OK", "TRUE")
            imgProjOptions = CSLSetNameValue(imgProjOptions, "MAX_GCP_ORDER", "0")
            imgProjOptions = CSLSetNameValue(imgProjOptions, "DST_SRS", wkt)

            for key, val in kwargs.items():
                key = key.upper().encode('utf-8')

                if not is_transformer_option(key):
                    continue

                if key in {b"RPC_DEM", b"COORDINATE_OPERATION"}:
                    # don't .upper() since might be a path.
                    val = str(val).encode('utf-8')
                else:
                    val = str(val).upper().encode('utf-8')

                imgProjOptions = CSLSetNameValue(imgProjOptions, <const char *>key, <const char *>val)
                log.debug("Set image projection option {0!r}={1!r}".format(key, val))

            if rpcs:
                if hasattr(rpcs, 'to_gdal'):
                    rpcs = rpcs.to_gdal()

                for key, val in rpcs.items():
                    key = key.upper().encode('utf-8')
                    val = str(val).encode('utf-8')
                    papszMD = CSLSetNameValue(papszMD, <const char *>key, <const char *>val)

                exc_wrap_int(GDALSetMetadata(hds, papszMD, "RPC"))
                imgProjOptions = CSLSetNameValue(imgProjOptions, "SRC_METHOD", "RPC")
                bUseApproxTransformer = False

            elif src_geoloc_array is not None:
                arr = np.stack((src_geoloc_array[0], src_geoloc_array[1]))
                geoloc_dataset = exit_stack.enter_context(MemoryDataset(arr, crs=src_crs))
                log.debug("Geoloc dataset created: geoloc_dataset=%r", geoloc_dataset)
                imgProjOptions = CSLSetNameValue(
                    imgProjOptions, "SRC_GEOLOC_ARRAY", geoloc_dataset.name.encode("utf-8")
                )
                imgProjOptions = CSLSetNameValue(
                    imgProjOptions, "SRC_SRS", src_crs.to_string().encode("utf-8")
                )
                bUseApproxTransformer = False

            with Env(GDAL_MEM_ENABLE_OPEN=True):
                hTransformArg = exc_wrap_pointer(
                    GDALCreateGenImgProjTransformer2(hds, NULL, imgProjOptions)
                )

            pfnTransformer = GDALGenImgProjTransform
            log.debug("Created exact transformer")

            exc_wrap_int(
                GDALSuggestedWarpOutput2(
                    hds,
                    pfnTransformer,
                    hTransformArg,
                    geotransform,
                    &npixels,
                    &nlines,
                    extent,
                    0
                )
            )

        except CPLE_NotSupportedError as err:
            raise CRSError(err.errmsg)

        finally:
            if hTransformArg != NULL:
                GDALDestroyGenImgProjTransformer(hTransformArg)
            if hds != NULL:
                GDALClose(hds)
            if imgProjOptions != NULL:
                CPLFree(imgProjOptions)
            if papszMD != NULL:
                CSLDestroy(papszMD)

    dst_affine = Affine.from_gdal(*[geotransform[i] for i in range(6)])
    dst_width = npixels
    dst_height = nlines

    return (dst_affine, dst_width, dst_height)


DEFAULT_NODATA_FLAG = object()

cdef GDALDatasetH auto_create_warped_vrt(
    GDALDatasetH hSrcDS,
    const char *pszSrcWKT,
    const char *pszDstWKT,
    GDALResampleAlg eResampleAlg,
    double dfMaxError,
    const GDALWarpOptions *psOptions,
    const char **transformer_options,
) except NULL:
    """Makes a best-fit WarpedVRT around a dataset.

    Returns
    -------
    GDALDatasetH
        Owned by the caller.

    """
    cdef GDALDatasetH hds_warped = NULL

    try:
        # This function may put errors on GDAL's error stack while
        # still returning 0 (no error). Thus we always check and
        # clear the stack and ignore the error if the function
        # succeeds.
        with nogil:
            hds_warped = GDALAutoCreateWarpedVRTEx(
                hSrcDS,
                pszSrcWKT,
                pszDstWKT,
                eResampleAlg,
                dfMaxError,
                psOptions,
                transformer_options,
            )
        _ = exc_wrap(1)

    except CPLE_AppDefinedError as err:
        if hds_warped != NULL:
            log.info("Ignoring error: err=%r", err)
        else:
            raise err

    return hds_warped


cdef class WarpedVRTReaderBase(DatasetReaderBase):

    def __init__(self, src_dataset, src_crs=None, crs=None,
                 resampling=Resampling.nearest, tolerance=0.125,
                 src_nodata=DEFAULT_NODATA_FLAG, nodata=DEFAULT_NODATA_FLAG,
                 width=None, height=None,
                 src_transform=None, transform=None,
                 init_dest_nodata=True, src_alpha=0, dst_alpha=0, add_alpha=False,
                 warp_mem_limit=0, dtype=None, **warp_extras):
        """Make a virtual warped dataset

        Parameters
        ----------
        src_dataset : dataset object
            The warp source dataset. Must be opened in "r" mode.
        src_crs : CRS or str, optional
            Overrides the coordinate reference system of `src_dataset`.
        src_transfrom : Affine, optional
            Overrides the transform of `src_dataset`.
        src_nodata : float, optional
            Overrides the nodata value of `src_dataset`, which is the
            default.
        crs : CRS or str, optional
            The coordinate reference system at the end of the warp
            operation.  Default: the crs of `src_dataset`. dst_crs was
            a deprecated alias for this parameter.
        transform : Affine, optional
            The transform for the virtual dataset. Default: will be
            computed from the attributes of `src_dataset`. dst_transform
            was a deprecated alias for this parameter.
        height, width: int, optional
            The dimensions of the virtual dataset. Defaults: will be
            computed from the attributes of `src_dataset`. dst_height
            and dst_width were deprecated alias for these parameters.
        nodata : float, optional
            Nodata value for the virtual dataset. Default: the nodata
            value of `src_dataset` or 0.0. dst_nodata was a deprecated
            alias for this parameter.
        resampling : Resampling, optional
            Warp resampling algorithm. Default: `Resampling.nearest`.
        tolerance : float, optional
            The maximum error tolerance in input pixels when
            approximating the warp transformation. Default: 0.125,
            or one-eigth of a pixel.
        src_alpha : int, optional
            Index of a source band to use as an alpha band for warping.
        dst_alpha : int, optional
            Index of a destination band to use as an alpha band for warping.
        add_alpha : bool, optional
            Whether to add an alpha masking band to the virtual dataset.
            Default: False. This option will cause deletion of the VRT
            nodata value.
        init_dest_nodata : bool, optional
            Whether or not to initialize output to `nodata`. Default:
            True.
        warp_mem_limit : int, optional
            The warp operation's memory limit in MB. The default (0)
            means 64 MB with GDAL 2.2.
        dtype : str, optional
            The working data type for warp operation and output.
        warp_extras : dict, optional
            GDAL extra warp options. See:
            https://gdal.org/doxygen/structGDALWarpOptions.html.
            Also, GDALCreateGenImgProjTransformer2() options.
            Requires rasterio 1.3+, GDAL 3.2+. See:
            https://gdal.org/doxygen/gdal__alg_8h.html#a94cd172f78dbc41d6f407d662914f2e3

        Returns
        -------
        WarpedVRT

        """
        if src_dataset.mode != "r":
            warnings.warn("Source dataset should be opened in read-only mode. Use of datasets opened in modes other than 'r' will be disallowed in a future version.", RasterioDeprecationWarning, stacklevel=2)

        # Guard against invalid or unsupported resampling algorithms.
        try:
            if resampling == 7:
                raise ValueError("Gauss resampling is not supported")
            Resampling(resampling)

        except ValueError:
            raise ValueError(
                "resampling must be one of: {0}".format(", ".join(['Resampling.{0}'.format(r.name) for r in SUPPORTED_RESAMPLING])))

        self.mode = 'r'
        self.options = {}
        self._count = 0
        self._dtypes = []
        self._block_shapes = None
        self._nodatavals = []
        self._units = ()
        self._descriptions = ()
        self._crs = None
        self._gcps = None
        self._read = False

        # kwargs become warp options.
        self.src_dataset = src_dataset
        self.src_crs = src_dataset.crs if src_crs is None else CRS.from_user_input(src_crs)
        self.src_transform = src_transform
        self.src_gcps = None
        self.name = "WarpedVRT({})".format(src_dataset.name)
        self.resampling = resampling
        self.tolerance = tolerance
        self.working_dtype = dtype

        self.src_nodata = self.src_dataset.nodata if src_nodata is DEFAULT_NODATA_FLAG else src_nodata
        self.dst_nodata = self.src_nodata if nodata is DEFAULT_NODATA_FLAG else nodata
        self.dst_width = width
        self.dst_height = height
        self.dst_transform = transform
        self.warp_extras = warp_extras.copy()
        if init_dest_nodata is True and 'init_dest' not in warp_extras:
            self.warp_extras['init_dest'] = 'NO_DATA' if self.dst_nodata is not None else '0'

        cdef GDALDriverH driver = NULL
        cdef GDALDatasetH hds = NULL
        cdef GDALDatasetH hds_warped = NULL
        cdef const char *cypath = NULL
        cdef char *src_crs_wkt = NULL
        cdef char *dst_crs_wkt = NULL
        cdef OGRSpatialReferenceH osr = NULL
        cdef char **c_warp_extras = NULL
        cdef char **c_transformer_options = NULL
        cdef GDALWarpOptions *psWOptions = NULL
        cdef float c_tolerance = tolerance
        cdef GDALResampleAlg c_resampling = resampling
        cdef int c_width = self.dst_width or 0
        cdef int c_height = self.dst_height or 0
        cdef double src_gt[6]
        cdef double dst_gt[6]
        cdef void *hTransformArg = NULL
        cdef GDALRasterBandH hband = NULL
        cdef GDALRasterBandH hmask = NULL
        cdef int mask_block_xsize = 0
        cdef int mask_block_ysize = 0

        hds = exc_wrap_pointer((<DatasetReaderBase?>self.src_dataset).handle())

        if not self.src_transform:
            self.src_transform = self.src_dataset.transform

        if not self.src_crs and self.src_transform.almost_equals(identity):
            try:
                self.src_gcps, self.src_crs = self.src_dataset.get_gcps()
            except ValueError:
                pass

        self.dst_crs = CRS.from_user_input(crs) if crs is not None else self.src_crs

        # Convert CRSes to C WKT strings.
        if self.src_crs is not None:
            src_crs_wkt_b = self.src_crs.to_wkt().encode("utf-8")
            src_crs_wkt = src_crs_wkt_b

        if self.dst_crs is not None:
            dst_crs_wkt_b = self.dst_crs.to_wkt().encode("utf-8")
            dst_crs_wkt = dst_crs_wkt_b

        log.debug("Exported CRS to WKT.")

        log.debug("Warp_extras: %r", self.warp_extras)

        for key, val in self.warp_extras.items():
            key = key.upper().encode('utf-8')
            val = str(val).upper().encode('utf-8')
            c_warp_extras = CSLSetNameValue(
                c_warp_extras, <const char *>key, <const char *>val)
            if is_transformer_option(key):
                c_transformer_options = CSLSetNameValue(
                    c_transformer_options, <const char *>key, <const char *>val)

        cdef GDALRasterBandH hBand = NULL
        src_alpha_band = 0
        dst_alpha_band = 0
        for bidx in src_dataset.indexes:
            hBand = GDALGetRasterBand(hds, bidx)
            if GDALGetRasterColorInterpretation(hBand) == GCI_AlphaBand:
                src_alpha_band = bidx
                dst_alpha_band = bidx

        # Adding an alpha band when the source has one is trouble.
        # It will result in surprisingly unmasked data. We will
        # raise an exception instead.

        if add_alpha:
            if src_alpha_band:
                raise WarpOptionsError(
                    "The VRT already has an alpha band, adding a new one is not supported")
            else:
                dst_alpha_band = src_dataset.count + 1
                self.dst_nodata = None

        if dst_alpha:
            dst_alpha_band = dst_alpha

        if src_alpha:
            src_alpha_band = src_alpha

        gdal_dtype = dtypes._get_gdal_dtype(self.working_dtype)
        psWOptions = create_warp_options(
            <GDALResampleAlg>c_resampling, self.src_nodata,
            self.dst_nodata, src_dataset.count, dst_alpha_band,
            src_alpha_band, warp_mem_limit, <GDALDataType>gdal_dtype,
            <const char **>c_warp_extras)

        if psWOptions == NULL:
            raise RuntimeError("Warp options are NULL")

        psWOptions.hSrcDS = hds

        # We handle four different use cases.
        #
        # 1. Destination transform, height, and width are provided by
        #    the caller.
        # 2. Pure scaling: source and destination CRS are the same,
        #    destination height and width are provided by the caller,
        #    destination transform is not provided; it is computed by
        #    scaling the source transform.
        # 3. Warp with scaling: CRS are different, destination height
        #    and width are provided by the caller, destination transform
        #    is computed and scaled to preserve given dimensions.
        # 4. Warp with destination height, width, and transform
        #    auto-generated by GDAL.

        # Case 4
        if not self.dst_transform and (not self.dst_width or not self.dst_height):
            try:
                hds_warped = auto_create_warped_vrt(
                    hds,
                    src_crs_wkt,
                    dst_crs_wkt,
                    c_resampling,
                    c_tolerance,
                    psWOptions,
                    <const char **>c_transformer_options,
                )
            finally:
                CSLDestroy(c_warp_extras)
                CSLDestroy(c_transformer_options)
                if psWOptions != NULL:
                    GDALDestroyWarpOptions(psWOptions)

        else:
            # Case 1
            if self.dst_transform and self.dst_width and self.dst_height:
                pass

            # Case 2
            elif (
                self.src_crs == self.dst_crs
                and self.dst_width
                and self.dst_height
                and not self.dst_transform
                and not self.src_gcps
                and not self.src_dataset.rpcs
            ):
                # Note: scaling on the right hand side of multiplication
                # preserves the origin of the geotransform matrix.
                self.dst_transform = self.src_transform * Affine.scale(
                    self.src_dataset.width / self.dst_width,
                    self.src_dataset.height / self.dst_height
                )

            # Case 3
            elif (
                (self.src_crs != self.dst_crs or self.src_gcps or self.src_dataset.rpcs)
                and self.dst_width
                and self.dst_height
                and not self.dst_transform
            ):
                left, bottom, right, top = self.src_dataset.bounds
                self.dst_transform, width, height = _calculate_default_transform(
                    self.src_crs,
                    self.dst_crs,
                    self.src_dataset.width,
                    self.src_dataset.height,
                    left=left,
                    bottom=bottom,
                    right=right,
                    top=top,
                    gcps=self.src_gcps,
                    rpcs=self.src_dataset.rpcs,
                    **self.warp_extras,
                )
                self.dst_transform = self.dst_transform * Affine.scale(
                    width / self.dst_width, height / self.dst_height
                )

            # If we get here it's because the tests above are buggy.
            # We raise a Python exception to indicate that.
            else:
                CSLDestroy(c_warp_extras)
                CSLDestroy(c_transformer_options)
                if psWOptions != NULL:
                    GDALDestroyWarpOptions(psWOptions)
                raise RuntimeError("Parameterization error")

            # Continue with finishing cases 1-3, which have been
            # generalized.
            t = self.src_transform.to_gdal()
            for i in range(6):
                src_gt[i] = t[i]

            t = self.dst_transform.to_gdal()
            for i in range(6):
                dst_gt[i] = t[i]

            try:
                hTransformArg = exc_wrap_pointer(
                    GDALCreateGenImgProjTransformer3(
                        src_crs_wkt, src_gt, dst_crs_wkt, dst_gt))

                if c_tolerance > 0.0:

                    hTransformArg = exc_wrap_pointer(
                        GDALCreateApproxTransformer(
                            GDALGenImgProjTransform,
                            hTransformArg,
                            c_tolerance))

                    psWOptions.pfnTransformer = GDALApproxTransform
                    GDALApproxTransformerOwnsSubtransformer(hTransformArg, 1)

                psWOptions.pTransformerArg = hTransformArg

                with nogil:
                    hds_warped = GDALCreateWarpedVRT(hds, c_width, c_height, dst_gt, psWOptions)
                    GDALSetProjection(hds_warped, dst_crs_wkt)

            finally:
                CSLDestroy(c_warp_extras)
                CSLDestroy(c_transformer_options)
                if psWOptions != NULL:
                    GDALDestroyWarpOptions(psWOptions)

        # End of the 4 cases.

        try:
            self._hds = exc_wrap_pointer(hds_warped)

        except CPLE_OpenFailedError as err:
            raise RasterioIOError(err.errmsg)

        if self.dst_nodata is None:
            for i in self.indexes:
                GDALDeleteRasterNoDataValue(self.band(i))

        else:
            for i in self.indexes:
                GDALSetRasterNoDataValue(self.band(i), self.dst_nodata)

        if dst_alpha:
            GDALSetRasterColorInterpretation(self.band(dst_alpha), <GDALColorInterp>6)

        self._set_attrs_from_dataset_handle()
        self._nodatavals = [self.dst_nodata for i in self.indexes]

        if dst_alpha and len(self._nodatavals) == 3:
            self._nodatavals[dst_alpha - 1] = None

        self._env = ExitStack()

    @property
    def crs(self):
        """The dataset's coordinate reference system"""
        return self.dst_crs

    def read(self, indexes=None, out=None, window=None, masked=False, out_shape=None, resampling=Resampling.nearest, fill_value=None, out_dtype=None, **kwargs):
        """Read a dataset's raw pixels as an N-d array

        This data is read from the dataset's band cache, which means
        that repeated reads of the same windows may avoid I/O.

        Parameters
        ----------
        indexes : list of ints or a single int, optional
            If `indexes` is a list, the result is a 3D array, but is
            a 2D array if it is a band index number.

        out : numpy.ndarray, optional
            As with Numpy ufuncs, this is an optional reference to an
            output array into which data will be placed. If the height
            and width of `out` differ from that of the specified
            window (see below), the raster image will be decimated or
            replicated using the specified resampling method (also see
            below).

            *Note*: the method's return value may be a view on this
            array. In other words, `out` is likely to be an
            incomplete representation of the method's results.

            This parameter cannot be combined with `out_shape`.

        out_dtype : str or numpy.dtype
            The desired output data type. For example: 'uint8' or
            rasterio.uint16.

        out_shape : tuple, optional
            A tuple describing the shape of a new output array. See
            `out` (above) for notes on image decimation and
            replication.

            Cannot combined with `out`.

        window : a pair (tuple) of pairs of ints or Window, optional
            The optional `window` argument is a 2 item tuple. The first
            item is a tuple containing the indexes of the rows at which
            the window starts and stops and the second is a tuple
            containing the indexes of the columns at which the window
            starts and stops. For example, ((0, 2), (0, 2)) defines
            a 2x2 window at the upper left of the raster dataset.

        masked : bool, optional
            If `masked` is `True` the return value will be a masked
            array. Otherwise (the default) the return value will be a
            regular array. Masks will be exactly the inverse of the
            GDAL RFC 15 conforming arrays returned by read_masks().

        resampling : Resampling
            By default, pixel values are read raw or interpolated using
            a nearest neighbor algorithm from the band cache. Other
            resampling algorithms may be specified. Resampled pixels
            are not cached.

        fill_value : scalar
            Fill value applied in the `boundless=True` case only.

        kwargs : dict
            This is only for backwards compatibility. No keyword arguments
            are supported other than the ones named above.

        Returns
        -------
        numpy.ndarray or a view on a numpy.ndarray

        Note: as with Numpy ufuncs, an object is returned even if you
        use the optional `out` argument and the return value shall be
        preferentially used by callers.

        """
        if kwargs.get("boundless", False):
            raise ValueError("WarpedVRT does not permit boundless reads")
        else:
            return super().read(indexes=indexes, out=out, window=window, masked=masked, out_shape=out_shape, resampling=resampling, fill_value=fill_value, out_dtype=out_dtype)

    def read_masks(self, indexes=None, out=None, out_shape=None, window=None, resampling=Resampling.nearest, **kwargs):
        """Read raster band masks as a multidimensional array"""
        if kwargs.get("boundless", False):
            raise ValueError("WarpedVRT does not permit boundless reads")
        else:
            return super().read_masks(indexes=indexes, out=out, window=window, out_shape=out_shape, resampling=resampling)


def _suggested_proxy_vrt_doc(width, height, transform=None, crs=None, gcps=None, rpcs=None):
    """Make a VRT XML document to serve _calculate_default_transform."""
    vrtdataset = ET.Element('VRTDataset')
    vrtdataset.attrib['rasterYSize'] = str(height)
    vrtdataset.attrib['rasterXSize'] = str(width)
    vrtrasterband = ET.SubElement(vrtdataset, 'VRTRasterBand')

    srs = ET.SubElement(vrtdataset, 'SRS')
    srs.text = crs.wkt if crs else ""

    if gcps:
        gcplist = ET.SubElement(vrtdataset, 'GCPList')
        gcplist.attrib['Projection'] = crs.wkt if crs else ""
        for point in gcps:
            gcp = ET.SubElement(gcplist, 'GCP')
            gcp.attrib['Id'] = str(point.id)
            gcp.attrib['Info'] = str(point.info)
            gcp.attrib['Pixel'] = str(point.col)
            gcp.attrib['Line'] = str(point.row)
            gcp.attrib['X'] = str(point.x)
            gcp.attrib['Y'] = str(point.y)
            gcp.attrib['Z'] = str(point.z)
    elif transform:
        geotransform = ET.SubElement(vrtdataset, 'GeoTransform')
        geotransform.text = ','.join([str(v) for v in transform.to_gdal()])
    elif rpcs:
        metadata = ET.SubElement(vrtdataset, "Metadata")
        metadata.attrib["domain"] = "RPC"
        for key in (
            "line_off", "samp_off", "height_off", "lat_off", "long_off",
            "line_scale", "samp_scale", "height_scale", "lat_scale", "long_scale",
            "err_bias", "err_rand",
        ):
            item = ET.SubElement(metadata, "MDI")
            item.attrib["key"] = key.upper()
            item.text = str(getattr(rpcs, key))
        for key in ("line_num_coeff", "line_den_coeff", "samp_num_coeff", "samp_den_coeff"):
            item = ET.SubElement(metadata, "MDI")
            item.attrib["key"] = key.upper()
            item.text = " ".join(["{:+e}".format(val) for val in getattr(rpcs, key)])

    return ET.tostring(vrtdataset)


@cython.boundscheck(False)
@cython.wraparound(False)
cdef double antimeridian_min(data):
    """
    Handles the case when longitude values cross the antimeridian
    when calculating the minimum.

    Note: The data array must be in a linear ring.

    Note: This requires a densified ring with at least 2 additional
          points per edge to correctly handle global extents.

    If only 1 additional point:

      |        |
      |RL--x0--|RL--
      |        |
    -180    180|-180

    If they are evenly spaced and it crosses the antimeridian:
    x0 - L = 180
    R - x0 = -180

    For example:
    Let R = -179.9, x0 = 0.1, L = -179.89
    x0 - L = 0.1 - -179.9 = 180
    R - x0 = -179.89 - 0.1 ~= -180
    This is the same in the case when it didn't cross the antimeridian.

    If you have 2 additional points:
      |            |
      |RL--x0--x1--|RL--
      |            |
    -180        180|-180

    If they are evenly spaced and it crosses the antimeridian:
    x0 - L = 120
    x1 - x0 = 120
    R - x1 = -240

    For example:
    Let R = -179.9, x0 = -59.9, x1 = 60.1 L = -179.89
    x0 - L = 59.9 - -179.9 = 120
    x1 - x0 = 60.1 - 59.9 = 120
    R - x1 = -179.89 - 60.1 ~= -240

    However, if they are evenly spaced and it didn't cross the antimeridian:
    x0 - L = 120
    x1 - x0 = 120
    R - x1 = 120

    From this, we have a delta that is guaranteed to be significantly
    large enough to tell the difference reguarless of the direction
    the antimeridian was crossed.

    However, even though the spacing was even in the source projection, it isn't
    guaranteed in the target geographic projection. So, instead of 240, 200 is used
    as it significantly larger than 120 to be sure that the antimeridian was crossed
    but smalller than 240 to account for possible irregularities in distances
    when re-projecting. Also, 200 ensures latitudes are ignored for axis order handling.
    """
    cdef Py_ssize_t arr_len = len(data)
    cdef int iii = 0
    cdef int prev_iii = arr_len - 1
    cdef double positive_min = HUGE_VAL
    cdef double min_value = HUGE_VAL
    cdef double delta = 0
    cdef int crossed_meridian_count = 0
    cdef bint positive_meridian = False

    for iii in range(0, arr_len):
        prev_iii = iii - 1
        if prev_iii == -1:
            prev_iii = arr_len - 1
        # check if crossed meridian
        delta = data[prev_iii] - data[iii]
        # 180 -> -180
        if delta >= 200:
            if crossed_meridian_count == 0:
                positive_min = min_value
            crossed_meridian_count += 1
            positive_meridian = False
        # -180 -> 180
        elif delta <= -200:
            if crossed_meridian_count == 0:
                positive_min = data[iii]
            crossed_meridian_count += 1
            positive_meridian = True
        # positive meridian side min
        if positive_meridian and data[iii] < positive_min:
            positive_min = data[iii]
        # track general min value
        if data[iii] < min_value:
            min_value = data[iii]
    if crossed_meridian_count == 2:
        return positive_min
    elif crossed_meridian_count == 4:
        # bounds extends beyond -180/180
        return -180
    return min_value


@cython.boundscheck(False)
@cython.wraparound(False)
cdef double antimeridian_max(data):
    """
    Handles the case when longitude values cross the antimeridian
    when calculating the minimum.

    Note: The data array must be in a linear ring.

    Note: This requires a densified ring with at least 2 additional
          points per edge to correctly handle global extents.

    See antimeridian_min docstring for reasoning.
    """
    cdef Py_ssize_t arr_len = len(data)
    cdef int iii = 0
    cdef int prev_iii = arr_len - 1
    cdef double negative_max = -HUGE_VAL
    cdef double max_value = -HUGE_VAL
    cdef double delta = 0
    cdef bint negative_meridian = False
    cdef int crossed_meridian_count = 0
    for iii in range(0, arr_len):
        prev_iii = iii - 1
        if prev_iii == -1:
            prev_iii = arr_len - 1
        # check if crossed meridian
        delta = data[prev_iii] - data[iii]
        # 180 -> -180
        if delta >= 200:
            if crossed_meridian_count == 0:
                negative_max = data[iii]
            crossed_meridian_count += 1
            negative_meridian = True
        # -180 -> 180
        elif delta <= -200:
            if crossed_meridian_count == 0:
                negative_max = max_value
            negative_meridian = False
            crossed_meridian_count += 1
        # negative meridian side max
        if (negative_meridian
            and (data[iii] > negative_max or negative_max == HUGE_VAL)
            and data[iii] != HUGE_VAL
        ):
            negative_max = data[iii]
        # track general max value
        if (data[iii] > max_value or max_value == HUGE_VAL) and data[iii] != HUGE_VAL:
            max_value = data[iii]
    if crossed_meridian_count == 2:
        return negative_max
    elif crossed_meridian_count == 4:
        # bounds extends beyond -180/180
        return 180
    return max_value



@cython.boundscheck(False)
@cython.wraparound(False)
def _transform_bounds(
    CRS src_crs,
    CRS dst_crs,
    double left,
    double bottom,
    double right,
    double top,
    int densify_pts,
):
    cdef double out_left = np.inf
    cdef double out_bottom = np.inf
    cdef double out_right = np.inf
    cdef double out_top = np.inf
    cdef OGRCoordinateTransformationH transform = NULL
    transform = OCTNewCoordinateTransformation(src_crs._osr, dst_crs._osr)
    transform = exc_wrap_pointer(transform)

    # OCTTransformBounds() returns TRUE/FALSE contrary to most GDAL API functions
    cdef int status = 0

    try:
        status = OCTTransformBounds(
            transform,
            left, bottom, right, top,
            &out_left, &out_bottom, &out_right, &out_top,
            densify_pts
        )
        exc_wrap_int(status == 0)
    finally:
        OCTDestroyCoordinateTransformation(transform)
    return out_left, out_bottom, out_right, out_top
