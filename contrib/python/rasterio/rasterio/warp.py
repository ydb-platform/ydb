"""Raster warping and reprojection."""

from math import ceil, floor
import warnings

from affine import Affine
import numpy as np

import rasterio

from rasterio._base import _transform
from rasterio.crs import CRS
from rasterio.enums import Resampling
from rasterio.env import ensure_env
from rasterio.errors import TransformError, RPCError, RasterioDeprecationWarning
from rasterio.transform import array_bounds
from rasterio._warp import (
    _calculate_default_transform,
    _reproject,
    _transform_bounds,
    _transform_geom,
    SUPPORTED_RESAMPLING,
)

@ensure_env
def transform(src_crs, dst_crs, xs, ys, zs=None):

    """Transform vectors from source to target coordinate reference system.

    Transform vectors of x, y and optionally z from source
    coordinate reference system into target.

    Parameters
    ------------
    src_crs: CRS or dict
        Source coordinate reference system, as a rasterio CRS object.
        Example: CRS({'init': 'EPSG:4326'})
    dst_crs: CRS or dict
        Target coordinate reference system.
    xs: array_like
        Contains x values.  Will be cast to double floating point values.
    ys:  array_like
        Contains y values.
    zs: array_like, optional
        Contains z values.  Assumed to be all 0 if absent.

    Returns
    ---------
    out: tuple of array_like, (xs, ys, [zs])
        Tuple of x, y, and optionally z vectors, transformed into the target
        coordinate reference system.

    """
    if len(xs) != len(ys):
        raise TransformError("xs and ys arrays must be the same length")
    elif zs is not None and len(xs) != len(zs):
        raise TransformError("zs, xs, and ys arrays must be the same length")
    if len(xs) == 0:
        return ([], [], []) if zs is not None else ([], [])
    else:
        return _transform(src_crs, dst_crs, xs, ys, zs)


@ensure_env
def transform_geom(
    src_crs,
    dst_crs,
    geom,
    antimeridian_cutting=None,
    antimeridian_offset=None,
    precision=-1,
):
    """Transform geometry from source coordinate reference system into target.

    Parameters
    ------------
    src_crs: CRS or dict
        Source coordinate reference system, in rasterio dict format.
        Example: CRS({'init': 'EPSG:4326'})
    dst_crs: CRS or dict
        Target coordinate reference system.
    geom: GeoJSON like dict object or iterable of GeoJSON like objects.
    antimeridian_cutting: bool
        DEPRECATED: Always enabled since GDAL 2.2.
    antimeridian_offset: float
        DEPRECATED: No longer has any effect since GDAL 2.2.
    precision: float
        If >= 0, geometry coordinates will be rounded to this number of decimal
        places after the transform operation, otherwise original coordinate
        values will be preserved (default).

    Returns
    ---------
    out: GeoJSON like dict object or list of GeoJSON like objects.
        Transformed geometry(s) in GeoJSON dict format
    """
    if antimeridian_cutting is not None or antimeridian_offset is not None:
        warnings.warn(
            "The antimeridian_cutting and antimeridian_offset parameters are "
            "deprecated and no longer have any effect.",
            RasterioDeprecationWarning,
        )
    return _transform_geom(
        src_crs,
        dst_crs,
        geom,
        precision,
    )


def transform_bounds(
        src_crs,
        dst_crs,
        left,
        bottom,
        right,
        top,
        densify_pts=21):
    """Transform bounds from src_crs to dst_crs.

    Optionally densifying the edges (to account for nonlinear transformations
    along these edges) and extracting the outermost bounds.

    Note: antimeridian support added in version 1.3.0

    Parameters
    ----------
    src_crs: CRS or dict
        Source coordinate reference system, in rasterio dict format.
        Example: CRS({'init': 'EPSG:4326'})
    dst_crs: CRS or dict
        Target coordinate reference system.
    left, bottom, right, top: float
        Bounding coordinates in src_crs, from the bounds property of a raster.
    densify_pts: uint, optional
        Number of points to add to each edge to account for nonlinear
        edges produced by the transform process.  Large numbers will produce
        worse performance.  Default: 21 (gdal default).

    Returns
    -------
    left, bottom, right, top: float
        Outermost coordinates in target coordinate reference system.
    """
    src_crs = CRS.from_user_input(src_crs)
    dst_crs = CRS.from_user_input(dst_crs)
    return _transform_bounds(
        src_crs,
        dst_crs,
        left,
        bottom,
        right,
        top,
        densify_pts,
    )


@ensure_env
def reproject(
    source,
    destination=None,
    src_transform=None,
    gcps=None,
    rpcs=None,
    src_crs=None,
    src_nodata=None,
    dst_transform=None,
    dst_crs=None,
    dst_nodata=None,
    dst_resolution=None,
    src_alpha=0,
    dst_alpha=0,
    masked=False,
    resampling=Resampling.nearest,
    num_threads=1,
    init_dest_nodata=True,
    warp_mem_limit=0,
    src_geoloc_array=None,
    **kwargs
):
    """Reproject a source raster to a destination raster.

    If the source and destination are ndarrays, coordinate reference
    system definitions and geolocation parameters are required for
    reprojection. Only one of src_transform, gcps, rpcs, or
    src_geoloc_array can be used.

    If the source and destination are rasterio Bands, shorthand for
    bands of datasets on disk, the coordinate reference systems and
    transforms will be read from the appropriate datasets.

    Parameters
    ------------
    source: ndarray or Band
        The source is a 2 or 3-D ndarray, or a single or a multiple
        Rasterio Band object. The dimensionality of source
        and destination must match, i.e., for multiband reprojection
        the lengths of the first axes of the source and destination
        must be the same.
    destination: ndarray or Band, optional
        The destination is a 2 or 3-D ndarray, or a single or a multiple
        Rasterio Band object. The dimensionality of source
        and destination must match, i.e., for multiband reprojection
        the lengths of the first axes of the source and destination
        must be the same.
    src_transform: affine.Affine(), optional
        Source affine transformation. Required if source and
        destination are ndarrays. Will be derived from source if it is
        a rasterio Band. An error will be raised if this parameter is
        defined together with gcps.
    gcps: sequence of GroundControlPoint, optional
        Ground control points for the source. An error will be raised
        if this parameter is defined together with src_transform or rpcs.
    rpcs: RPC or dict, optional
        Rational polynomial coefficients for the source. An error will
        be raised if this parameter is defined together with src_transform
        or gcps.
    src_geoloc_array : array-like, optional
        A pair of 2D arrays holding x and y coordinates, like
        a dense array of ground control points that may be used in place
        of src_transform.
    src_crs: CRS or dict, optional
        Source coordinate reference system, in rasterio dict format.
        Required if source and destination are ndarrays.
        Will be derived from source if it is a rasterio Band.
        Example: CRS({'init': 'EPSG:4326'})
    src_nodata: int or float, optional
        The source nodata value. Pixels with this value will not be
        used for interpolation. If not set, it will default to the
        nodata value of the source image if a masked ndarray or
        rasterio band, if available.
    dst_transform: affine.Affine(), optional
        Target affine transformation. Required if source and
        destination are ndarrays. Will be derived from target if it is
        a rasterio Band.
    dst_crs: CRS or dict, optional
        Target coordinate reference system. Required if source and
        destination are ndarrays. Will be derived from target if it
        is a rasterio Band.
    dst_nodata: int or float, optional
        The nodata value used to initialize the destination; it will
        remain in all areas not covered by the reprojected source.
        Defaults to the nodata value of the destination image (if set),
        the value of src_nodata, or 0 (GDAL default).
    dst_resolution: tuple (x resolution, y resolution) or float, optional
        Target resolution, in units of target coordinate reference
        system.
    src_alpha : int, optional
        Index of a band to use as the alpha band when warping.
    dst_alpha : int, optional
        Index of a band to use as the alpha band when warping.
    masked: bool, optional
        If True and destination is None, return a masked array.
    resampling: int, rasterio.enums.Resampling
        Resampling method to use.
        Default is :attr:`rasterio.enums.Resampling.nearest`.
        An exception will be raised for a method not supported by the running
        version of GDAL.
    num_threads : int, optional
        The number of warp worker threads. Default: 1.
    init_dest_nodata: bool
        Flag to specify initialization of nodata in destination;
        prevents overwrite of previous warps. Defaults to True.
    warp_mem_limit : int, optional
        The warp operation memory limit in MB. Larger values allow the
        warp operation to be carried out in fewer chunks. The amount of
        memory required to warp a 3-band uint8 2000 row x 2000 col
        raster to a destination of the same size is approximately
        56 MB. The default (0) means 64 MB with GDAL 2.2.
    kwargs:  dict, optional
        Additional arguments passed to both the image to image
        transformer :cpp:func:`GDALCreateGenImgProjTransformer2` (for example,
        MAX_GCP_ORDER=2) and the :cpp:struct:`GDALWarpOptions` (for example,
        INIT_DEST=NO_DATA).

    Returns
    ---------
    destination: ndarray or Band
        The transformed ndarray or Band.
    dst_transform: Affine
        The affine transformation matrix of the destination.
    """
    # Source geolocation parameters are mutually exclusive.
    if (
        sum(
            param is not None for param in (src_transform, gcps, rpcs, src_geoloc_array)
        )
        > 1
    ):
        raise ValueError(
            "src_transform, gcps, rpcs, and src_geoloc_array are mutually "
            "exclusive parameters and may not be used together."
        )

    # Guard against invalid or unsupported resampling algorithms.
    try:
        if resampling == 7:
            raise ValueError("Gauss resampling is not supported")
        Resampling(resampling)
    except ValueError:
        raise ValueError(
            "resampling must be one of: {}".format(
                ", ".join([f"Resampling.{r.name}" for r in SUPPORTED_RESAMPLING])
            )
        )

    if destination is None and dst_transform is not None:
        raise ValueError("Must provide destination if dst_transform is provided.")

    # calculate the destination transform if not provided
    if dst_transform is None and (destination is None or isinstance(destination, np.ndarray)):
        src_bounds = tuple([None] * 4)
        if isinstance(source, np.ndarray):
            if source.ndim == 3:
                src_count, src_height, src_width = source.shape
            else:
                src_count = 1
                src_height, src_width = source.shape

            # try to compute src_bounds if we don't have gcps
            if not (gcps or rpcs):
                src_bounds = array_bounds(src_height, src_width, src_transform)
        else:
            src_rdr, src_bidx, _, src_shape = source

            # dataset.bounds will return raster extents in pixel coordinates
            # if dataset does not have geotransform, which is not useful here.
            if not (src_rdr.transform.is_identity and src_rdr.crs is None):
                src_bounds = src_rdr.bounds

            src_crs = src_crs or src_rdr.crs

            # raise exception when reprojecting with rpcs using a CRS that is not EPSG:4326
            if rpcs:
                if isinstance(src_crs, str):
                    src_crs_obj = rasterio.crs.CRS.from_string(src_crs)
                else:
                    src_crs_obj = src_crs
                if src_crs is not None and src_crs_obj.to_epsg() != 4326:
                    raise RPCError("Reprojecting with rational polynomial coefficients using source CRS other than EPSG:4326")

            if isinstance(src_bidx, int):
                src_bidx = [src_bidx]

            src_count = len(src_bidx)
            src_height, src_width = src_shape
            gcps = src_rdr.gcps[0] if src_rdr.gcps[0] and not rpcs else None

        dst_height = None
        dst_width = None
        dst_count = src_count

        if isinstance(destination, np.ndarray):
            if destination.ndim == 3:
                dst_count, dst_height, dst_width = destination.shape
            else:
                dst_count = 1
                dst_height, dst_width = destination.shape

        left, bottom, right, top = src_bounds
        dst_transform, dst_width, dst_height = calculate_default_transform(
            src_crs=src_crs,
            dst_crs=dst_crs,
            width=src_width,
            height=src_height,
            left=left,
            bottom=bottom,
            right=right,
            top=top,
            gcps=gcps,
            rpcs=rpcs,
            src_geoloc_array=src_geoloc_array,
            dst_width=dst_width,
            dst_height=dst_height,
            resolution=dst_resolution,
        )

        if destination is None:
            destination = np.empty(
                (int(dst_count), int(dst_height), int(dst_width)), dtype=source.dtype
            )
            if masked:
                destination = np.ma.masked_array(destination)

    dest = _reproject(
        source,
        destination,
        src_transform=src_transform,
        gcps=gcps,
        rpcs=rpcs,
        src_crs=src_crs,
        src_nodata=src_nodata,
        dst_transform=dst_transform,
        dst_crs=dst_crs,
        dst_nodata=dst_nodata,
        dst_alpha=dst_alpha,
        src_alpha=src_alpha,
        resampling=resampling,
        init_dest_nodata=init_dest_nodata,
        num_threads=num_threads,
        warp_mem_limit=warp_mem_limit,
        src_geoloc_array=src_geoloc_array,
        **kwargs
    )

    return dest, dst_transform


def aligned_target(transform, width, height, resolution):
    """Aligns target to specified resolution

    Parameters
    ----------
    transform : Affine
        Input affine transformation matrix
    width, height: int
        Input dimensions
    resolution: tuple (x resolution, y resolution) or float
        Target resolution, in units of target coordinate reference
        system.

    Returns
    -------
    transform: Affine
        Output affine transformation matrix
    width, height: int
        Output dimensions

    """
    if isinstance(resolution, (float, int)):
        res = (float(resolution), float(resolution))
    else:
        res = resolution

    xmin = transform.xoff
    ymin = transform.yoff + height * transform.e
    xmax = transform.xoff + width * transform.a
    ymax = transform.yoff

    xmin = floor(xmin / res[0]) * res[0]
    xmax = ceil(xmax / res[0]) * res[0]
    ymin = floor(ymin / res[1]) * res[1]
    ymax = ceil(ymax / res[1]) * res[1]
    dst_transform = Affine(res[0], 0, xmin, 0, -res[1], ymax)
    dst_width = max(int(ceil((xmax - xmin) / res[0])), 1)
    dst_height = max(int(ceil((ymax - ymin) / res[1])), 1)

    return dst_transform, dst_width, dst_height


@ensure_env
def calculate_default_transform(
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
    resolution=None,
    dst_width=None,
    dst_height=None,
    src_geoloc_array=None,
    **kwargs
):
    """Computes the default dimensions and transform for a reprojection.

    Destination width and height (and resolution if not provided), are
    calculated using GDAL's method for suggest warp output.  The
    destination transform is anchored at the left, top coordinate.

    Source georeferencing can be specified using either ground control
    points (GCPs), rational polynomial coefficients (RPCs), geolocation
    arrays, or spatial bounds (left, bottom, right, top). These forms of
    georeferencing are mutually exclusive.

    Source and destination coordinate reference systems and source
    width and height are the first four, required, parameters.

    Parameters
    ----------
    src_crs : CRS or dict
        Source coordinate reference system, in rasterio dict format.
        Example: CRS({'init': 'EPSG:4326'})
    dst_crs : CRS or dict
        Target coordinate reference system.
    width, height : int
        Source raster width and height.
    left, bottom, right, top : float, optional
        Bounding coordinates in src_crs, from the bounds property of a
        raster. Required unless using gcps.
    gcps : sequence of GroundControlPoint, optional
        Instead of a bounding box for the source, a sequence of ground
        control points may be provided.
    rpcs : RPC or dict, optional
        Instead of a bounding box for the source, rational polynomial
        coefficients may be provided.
    src_geoloc_array : array-like, optional
        A pair of 2D arrays holding x and y coordinates, like a like
        a dense array of ground control points that may be used in place
        of src_transform.
    resolution : tuple (x resolution, y resolution) or float, optional
        Target resolution, in units of target coordinate reference
        system.
    dst_width, dst_height : int, optional
        Output file size in pixels and lines. Cannot be used together
        with resolution.
    kwargs :  dict, optional
        Additional arguments passed to transformation function.

    Returns
    -------
    transform: Affine
        Output affine transformation matrix
    width, height: int
        Output dimensions

    Notes
    -----
    Some behavior of this function is determined by the
    CHECK_WITH_INVERT_PROJ environment variable:

        YES
            constrain output raster to extents that can be inverted
            avoids visual artifacts and coordinate discontinuties.
        NO
            reproject coordinates beyond valid bound limits
    """
    if sum(param is not None for param in (left, gcps, rpcs, src_geoloc_array)) < 1:
        raise ValueError("Bounds, gcps, rpcs, or src_geoloc_array are required.")

    # Source geolocation parameters are mutually exclusive.
    elif sum(param is not None for param in (left, gcps, rpcs, src_geoloc_array)) > 1:
        raise ValueError(
            "Bounds, gcps, rpcs, and src_geoloc_array are mutually "
            "exclusive parameters and may not be used together."
        )

    if (dst_width is None) != (dst_height is None):
        raise ValueError("Either dst_width and dst_height must be specified "
                         "or none of them.")

    if all(x is not None for x in (dst_width, dst_height)):
        dimensions = (dst_width, dst_height)
    else:
        dimensions = None

    if resolution and dimensions:
        raise ValueError("Resolution cannot be used with dst_width and dst_height.")

    dst_affine, dst_width, dst_height = _calculate_default_transform(
        src_crs,
        dst_crs,
        width,
        height,
        left=left,
        bottom=bottom,
        right=right,
        top=top,
        gcps=gcps,
        rpcs=rpcs,
        src_geoloc_array=src_geoloc_array,
        **kwargs
    )

    # If resolution is specified, Keep upper-left anchored
    # adjust the transform resolutions
    # adjust the width/height by the ratio of estimated:specified res (ceil'd)
    if resolution:
        # resolutions argument into tuple
        try:
            res = (float(resolution), float(resolution))
        except TypeError:
            res = (
                (resolution[0], resolution[0])
                if len(resolution) == 1
                else resolution[0:2]
            )

        # Assume yres is provided as positive,
        # needs to be negative for north-up affine
        xres = res[0]
        yres = -res[1]

        xratio = dst_affine.a / xres
        yratio = dst_affine.e / yres

        dst_affine = Affine(xres, dst_affine.b, dst_affine.c,
                            dst_affine.d, yres, dst_affine.f)

        dst_width = ceil(dst_width * xratio)
        dst_height = ceil(dst_height * yratio)

    if dimensions:
        xratio = dst_width / dimensions[0]
        yratio = dst_height / dimensions[1]

        dst_width = dimensions[0]
        dst_height = dimensions[1]

        dst_affine = Affine(dst_affine.a * xratio, dst_affine.b, dst_affine.c,
                            dst_affine.d, dst_affine.e * yratio, dst_affine.f)

    return dst_affine, dst_width, dst_height
