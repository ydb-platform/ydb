"""Copy valid pixels from input files to an output file."""

import cmath
import logging
import math
import numbers
import os
import warnings
from contextlib import ExitStack, contextmanager

import numpy as np

import rasterio
from rasterio import windows
from rasterio.enums import Resampling
from rasterio.errors import (
    MergeError,
    RasterioDeprecationWarning,
    RasterioError,
    WindowError,
)
from rasterio.io import DatasetWriter
from rasterio.transform import Affine
from rasterio.windows import subdivide

logger = logging.getLogger(__name__)


def copy_first(merged_data, new_data, merged_mask, new_mask, **kwargs):
    """Returns the first available pixel."""
    mask = np.empty_like(merged_mask, dtype=bool)
    np.logical_not(new_mask, out=mask)
    np.logical_and(merged_mask, mask, out=mask)
    np.copyto(merged_data, new_data, where=mask, casting="unsafe")


def copy_last(merged_data, new_data, merged_mask, new_mask, **kwargs):
    """Returns the last available pixel."""
    mask = np.empty_like(merged_mask, dtype=bool)
    np.logical_not(new_mask, out=mask)
    np.copyto(merged_data, new_data, where=mask, casting="unsafe")


def copy_min(merged_data, new_data, merged_mask, new_mask, **kwargs):
    """Returns the minimum value pixel."""
    mask = np.empty_like(merged_mask, dtype=bool)
    np.logical_or(merged_mask, new_mask, out=mask)
    np.logical_not(mask, out=mask)
    np.minimum(merged_data, new_data, out=merged_data, where=mask, casting="unsafe")
    np.logical_not(new_mask, out=mask)
    np.logical_and(merged_mask, mask, out=mask)
    np.copyto(merged_data, new_data, where=mask, casting="unsafe")


def copy_max(merged_data, new_data, merged_mask, new_mask, **kwargs):
    """Returns the maximum value pixel."""
    mask = np.empty_like(merged_mask, dtype=bool)
    np.logical_or(merged_mask, new_mask, out=mask)
    np.logical_not(mask, out=mask)
    np.maximum(merged_data, new_data, out=merged_data, where=mask, casting="unsafe")
    np.logical_not(new_mask, out=mask)
    np.logical_and(merged_mask, mask, out=mask)
    np.copyto(merged_data, new_data, where=mask, casting="unsafe")


def copy_sum(merged_data, new_data, merged_mask, new_mask, **kwargs):
    """Returns the sum of all pixel values."""
    mask = np.empty_like(merged_mask, dtype=bool)
    np.logical_or(merged_mask, new_mask, out=mask)
    np.logical_not(mask, out=mask)
    np.add(merged_data, new_data, out=merged_data, where=mask, casting="unsafe")
    np.logical_not(new_mask, out=mask)
    np.logical_and(merged_mask, mask, out=mask)
    np.copyto(merged_data, new_data, where=mask, casting="unsafe")


def copy_count(merged_data, new_data, merged_mask, new_mask, **kwargs):
    """Returns the count of valid pixels."""
    mask = np.empty_like(merged_mask, dtype=bool)
    np.logical_or(merged_mask, new_mask, out=mask)
    np.logical_not(mask, out=mask)
    np.add(merged_data, mask, out=merged_data, where=mask, casting="unsafe")
    np.logical_not(new_mask, out=mask)
    np.logical_and(merged_mask, mask, out=mask)
    np.copyto(merged_data, mask, where=mask, casting="unsafe")


MERGE_METHODS = {
    "first": copy_first,
    "last": copy_last,
    "min": copy_min,
    "max": copy_max,
    "sum": copy_sum,
    "count": copy_count,
}


def merge(
    sources,
    bounds=None,
    res=None,
    nodata=None,
    dtype=None,
    precision=None,
    indexes=None,
    output_count=None,
    resampling=Resampling.nearest,
    method="first",
    target_aligned_pixels=False,
    mem_limit=64,
    use_highest_res=False,
    masked=False,
    dst_path=None,
    dst_kwds=None,
):
    """Copy valid pixels from input files to an output file.

    All files must have the same number of bands, data type, and
    coordinate reference system. Rotated, flipped, or upside-down
    rasters cannot be merged.

    Input files are merged in their listed order using the reverse
    painter's algorithm (default) or another method. If the output file
    exists, its values will be overwritten by input values.

    Geospatial bounds and resolution of a new output file in the units
    of the input file coordinate reference system may be provided and
    are otherwise taken from the first input file.

    Parameters
    ----------
    sources : list
        A sequence of dataset objects opened in 'r' mode or Path-like
        objects.
    bounds: tuple, optional
        Bounds of the output image (left, bottom, right, top).
        If not set, bounds are determined from bounds of input rasters.
    res: tuple, optional
        Output resolution in units of coordinate reference system. If
        not set, a source resolution will be used. If a single value is
        passed, output pixels will be square.
    use_highest_res: bool, optional. Default: False.
        If True, the highest resolution of all sources will be used. If
        False, the first source's resolution will be used.
    nodata: float, optional
        nodata value to use in output file. If not set, uses the nodata
        value in the first input raster.
    masked: bool, optional. Default: False.
        If True, return a masked array. Note: nodata is always set in
        the case of file output.
    dtype: numpy.dtype or string
        dtype to use in outputfile. If not set, uses the dtype value in
        the first input raster.
    precision: int, optional
        This parameters is unused, deprecated in rasterio 1.3.0, and
        will be removed in version 2.0.0.
    indexes : list of ints or a single int, optional
        bands to read and merge
    output_count: int, optional
        If using callable it may be useful to have additional bands in
        the output in addition to the indexes specified for read
    resampling : Resampling, optional
        Resampling algorithm used when reading input files.
        Default: `Resampling.nearest`.
    method : str or callable
        pre-defined method:

            * first: reverse painting
            * last: paint valid new on top of existing
            * min: pixel-wise min of existing and new
            * max: pixel-wise max of existing and new
            * sum: pixel-wise sum of existing and new
            * count: pixel-wise count of valid pixels

        or custom callable with signature:
            merged_data : array_like
                array to update with new_data
            new_data : array_like
                data to merge
                same shape as merged_data
            merged_mask, new_mask : array_like
                boolean masks where merged/new data pixels are invalid
                same shape as merged_data
            index: int
                index of the current dataset within the merged dataset
                collection
            roff: int
                row offset in base array
            coff: int
                column offset in base array

    target_aligned_pixels : bool, optional
        Whether to adjust output image bounds so that pixel coordinates
        are integer multiples of pixel size, matching the ``-tap``
        options of GDAL utilities.  Default: False.
    mem_limit : int, optional
        Process merge output in chunks of mem_limit MB in size.
    dst_path : str or PathLike, optional
        Path of output dataset
    dst_kwds : dict, optional
        Dictionary of creation options and other parameters that will be
        overlaid on the profile of the output dataset.

    Returns
    -------
    tuple
        Two elements:
            dest: numpy.ndarray
                Contents of all input rasters in single array
            out_transform: affine.Affine()
                Information for mapping pixel coordinates in `dest` to
                another coordinate system

    Raises
    ------
    MergeError
        When sources cannot be merged due to incompatibility between
        them or limitations of the tool.
    """
    if precision is not None:
        warnings.warn(
            "The precision parameter is unused, deprecated, and will be removed in 2.0.0.",
            RasterioDeprecationWarning,
        )

    if method in MERGE_METHODS:
        copyto = MERGE_METHODS[method]
    elif callable(method):
        copyto = method
    else:
        raise ValueError(
            "Unknown method {}, must be one of {} or callable".format(
                method, list(MERGE_METHODS.keys())
            )
        )

    # Create a dataset_opener object to use in several places in this function.
    if isinstance(sources[0], (str, os.PathLike)):
        dataset_opener = rasterio.open
    else:

        @contextmanager
        def nullcontext(obj):
            try:
                yield obj
            finally:
                pass

        dataset_opener = nullcontext

    dst = None

    with ExitStack() as exit_stack:
        with dataset_opener(sources[0]) as first:
            first_profile = first.profile
            first_crs = first.crs
            best_res = first.res
            first_nodataval = first.nodatavals[0]
            nodataval = first_nodataval
            dt = first.dtypes[0]

            if indexes is None:
                src_count = first.count
            elif isinstance(indexes, int):
                src_count = indexes
            else:
                src_count = len(indexes)

            try:
                first_colormap = first.colormap(1)
            except ValueError:
                first_colormap = None

        if not output_count:
            output_count = src_count

        # Extent from option or extent of all inputs
        if bounds:
            dst_w, dst_s, dst_e, dst_n = bounds
        else:
            # scan input files
            xs = []
            ys = []

            for i, dataset in enumerate(sources):
                with dataset_opener(dataset) as src:
                    src_transform = src.transform

                    if use_highest_res:
                        best_res = min(
                            best_res,
                            src.res,
                            key=lambda x: (
                                x
                                if isinstance(x, numbers.Number)
                                else math.sqrt(x[0] ** 2 + x[1] ** 2)
                            ),
                        )

                    # The merge tool requires non-rotated rasters with origins at their
                    # upper left corner. This limitation may be lifted in the future.
                    if not src_transform.is_rectilinear:
                        raise MergeError(
                            "Rotated, non-rectilinear rasters cannot be merged."
                        )
                    if src_transform.a < 0:
                        raise MergeError(
                            'Rasters with negative pixel width ("flipped" rasters) cannot be merged.'
                        )
                    if src_transform.e > 0:
                        raise MergeError(
                            'Rasters with negative pixel height ("upside down" rasters) cannot be merged.'
                        )

                    left, bottom, right, top = src.bounds

                xs.extend([left, right])
                ys.extend([bottom, top])

            dst_w, dst_s, dst_e, dst_n = min(xs), min(ys), max(xs), max(ys)

        # Resolution/pixel size
        if not res:
            res = best_res
        elif isinstance(res, numbers.Number):
            res = (res, res)
        elif len(res) == 1:
            res = (res[0], res[0])

        if target_aligned_pixels:
            dst_w = math.floor(dst_w / res[0]) * res[0]
            dst_e = math.ceil(dst_e / res[0]) * res[0]
            dst_s = math.floor(dst_s / res[1]) * res[1]
            dst_n = math.ceil(dst_n / res[1]) * res[1]

        # Compute output array shape. We guarantee it will cover the output
        # bounds completely
        output_width = int(round((dst_e - dst_w) / res[0]))
        output_height = int(round((dst_n - dst_s) / res[1]))

        output_transform = Affine.translation(dst_w, dst_n) * Affine.scale(
            res[0], -res[1]
        )

        if dtype is not None:
            dt = dtype
            logger.debug("Set dtype: %s", dt)

        if nodata is not None:
            nodataval = nodata
            logger.debug("Set nodataval: %r", nodataval)

        inrange = False
        if nodataval is not None:
            # Only fill if the nodataval is within dtype's range
            if np.issubdtype(dt, np.integer):
                info = np.iinfo(dt)
                inrange = info.min <= nodataval <= info.max
            else:
                if cmath.isfinite(nodataval):
                    info = np.finfo(dt)
                    inrange = info.min <= nodataval <= info.max
                    nodata_dt = np.min_scalar_type(nodataval)
                    inrange = inrange & np.can_cast(nodata_dt, dt)
                else:
                    inrange = True

            if not inrange:
                warnings.warn(
                    f"Ignoring nodata value. The nodata value, {nodataval}, cannot safely be represented "
                    f"in the chosen data type, {dt}. Consider overriding it "
                    "using the --nodata option for better results. "
                    "Falling back to first source's nodata value."
                )
                nodataval = first_nodataval
        else:
            logger.debug("Set nodataval to 0")
            nodataval = 0

        # When dataset output is selected, we might need to create one
        # and will also provide the option of merging by chunks.
        dout_window = windows.Window(0, 0, output_width, output_height)
        if dst_path is not None:
            if isinstance(dst_path, DatasetWriter):
                dst = dst_path
            else:
                out_profile = first_profile
                out_profile.update(**(dst_kwds or {}))
                out_profile["transform"] = output_transform
                out_profile["height"] = output_height
                out_profile["width"] = output_width
                out_profile["count"] = output_count
                out_profile["dtype"] = dt
                if nodata is not None:
                    out_profile["nodata"] = nodata
                dst = rasterio.open(dst_path, "w", **out_profile)
                exit_stack.enter_context(dst)

            max_pixels = mem_limit * 1.0e6 / (np.dtype(dt).itemsize * output_count)

            if output_width * output_height < max_pixels:
                chunks = [dout_window]
            else:
                n = math.floor(math.sqrt(max_pixels))
                chunks = subdivide(dout_window, n, n)
        else:
            chunks = [dout_window]

        def _intersect_bounds(bounds1, bounds2, transform):
            """Based on gdal_merge.py."""
            int_w = max(bounds1[0], bounds2[0])
            int_e = min(bounds1[2], bounds2[2])

            if int_w >= int_e:
                raise ValueError

            if transform.e < 0:
                # north up
                int_s = max(bounds1[1], bounds2[1])
                int_n = min(bounds1[3], bounds2[3])
                if int_s >= int_n:
                    raise ValueError
            else:
                int_s = min(bounds1[1], bounds2[1])
                int_n = max(bounds1[3], bounds2[3])
                if int_n >= int_s:
                    raise ValueError

            return int_w, int_s, int_e, int_n

        for chunk in chunks:
            dst_w, dst_s, dst_e, dst_n = windows.bounds(chunk, output_transform)
            dest = np.zeros((output_count, chunk.height, chunk.width), dtype=dt)
            if inrange:
                dest.fill(nodataval)

            # From gh-2221
            chunk_bounds = windows.bounds(chunk, output_transform)
            chunk_transform = windows.transform(chunk, output_transform)

            def win_align(window):
                """Equivalent to rounding both offsets and lengths.

                This method computes offsets, width, and height that are
                useful for compositing arrays into larger arrays and
                datasets without seams. It is used by Rasterio's merge
                tool and is based on the logic in gdal_merge.py.

                Returns
                -------
                Window
                """
                row_off = math.floor(window.row_off + 0.1)
                col_off = math.floor(window.col_off + 0.1)
                height = math.floor(window.height + 0.5)
                width = math.floor(window.width + 0.5)
                return windows.Window(col_off, row_off, width, height)

            for idx, dataset in enumerate(sources):
                with dataset_opener(dataset) as src:

                    # Intersect source bounds and tile bounds
                    if first_crs != src.crs:
                        raise RasterioError(f"CRS mismatch with source: {dataset}")

                    try:
                        ibounds = _intersect_bounds(
                            src.bounds, chunk_bounds, chunk_transform
                        )
                        sw = windows.from_bounds(*ibounds, src.transform)
                        cw = windows.from_bounds(*ibounds, chunk_transform)
                    except (ValueError, WindowError):
                        logger.info(
                            "Skipping source: src=%r, bounds=%r", src, src.bounds
                        )
                        continue

                    cw = win_align(cw)
                    rows, cols = cw.toslices()
                    region = dest[:, rows, cols]

                    if cmath.isnan(nodataval):
                        region_mask = np.isnan(region)
                    elif not np.issubdtype(region.dtype, np.integer):
                        region_mask = np.isclose(region, nodataval)
                    else:
                        region_mask = region == nodataval

                    data = src.read(
                        out_shape=(src_count, cw.height, cw.width),
                        indexes=indexes,
                        masked=True,
                        window=sw,
                        resampling=resampling,
                    )

                    copyto(
                        region,
                        data,
                        region_mask,
                        data.mask,
                        index=idx,
                        roff=cw.row_off,
                        coff=cw.col_off,
                    )

            if dst:
                dw = windows.from_bounds(*chunk_bounds, output_transform)
                dw = win_align(dw)
                dst.write(dest, window=dw)

        if dst is None:
            if masked:
                dest = np.ma.masked_equal(dest, nodataval, copy=False)
            return dest, output_transform
        else:
            if first_colormap:
                dst.write_colormap(1, first_colormap)
