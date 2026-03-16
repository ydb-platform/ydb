"""Raster stacking tool."""

from collections.abc import Iterable
from contextlib import ExitStack, contextmanager
import logging
import os
import math
import cmath
import warnings
import numbers

import numpy as np

import rasterio
from rasterio.coords import disjoint_bounds
from rasterio.enums import Resampling
from rasterio.errors import RasterioError, StackError
from rasterio.io import DatasetWriter
from rasterio import windows
from rasterio.transform import Affine
from rasterio.windows import subdivide

logger = logging.getLogger(__name__)


def stack(
    sources,
    bounds=None,
    res=None,
    nodata=None,
    dtype=None,
    indexes=None,
    output_count=None,
    resampling=Resampling.nearest,
    target_aligned_pixels=False,
    mem_limit=64,
    use_highest_res=False,
    masked=False,
    dst_path=None,
    dst_kwds=None,
):
    """Copy valid pixels from input files to an output file.

    All files must have the same data type, and
    coordinate reference system. Rotated, flipped, or upside-down
    rasters cannot be stacked.

    Geospatial bounds and resolution of a new output file in the units
    of the input file coordinate reference system may be provided and
    are otherwise taken from the source datasets.

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
    masked: bool, optional. Default: False.
        If True, return a masked array. Note: nodata is always set in
        the case of file output.
    nodata: float, optional
        nodata value to use in output file. If not set, uses the nodata
        value in the first input raster.
    dtype: numpy.dtype or string
        dtype to use in outputfile. If not set, uses the dtype value in
        the first input raster.
    indexes : list of ints or a single int, optional
        bands to read and stack.
    output_count: int, optional
        If using callable it may be useful to have additional bands in
        the output in addition to the indexes specified for read.
    resampling : Resampling, optional
        Resampling algorithm used when reading input files.
        Default: `Resampling.nearest`.
    target_aligned_pixels : bool, optional
        Whether to adjust output image bounds so that pixel coordinates
        are integer multiples of pixel size, matching the ``-tap``
        options of GDAL utilities.  Default: False.
    mem_limit : int, optional
        Process stack output in chunks of mem_limit MB in size.
    dst_path : str or PathLike, optional
        Path of output dataset.
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
    StackError
        When sources cannot be stacked due to incompatibility between
        them or limitations of the tool.
    """
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
                indexes = [None for s in sources]

            try:
                first_colormap = first.colormap(1)
            except ValueError:
                first_colormap = None

        # scan input files
        xs = []
        ys = []
        output_count = 0

        for i, (dataset, src_indexes) in enumerate(zip(sources, indexes)):
            with dataset_opener(dataset) as src:
                src_transform = src.transform

                if src_indexes is None:
                    output_count += src.count
                elif isinstance(src_indexes, int):
                    output_count += 1
                else:
                    output_count += len(src_indexes)

                if use_highest_res:
                    best_res = min(
                        best_res,
                        src.res,
                        key=lambda x: x
                        if isinstance(x, numbers.Number)
                        else math.sqrt(x[0] ** 2 + x[1] ** 2),
                    )

                # The stack tool requires non-rotated rasters with origins at their
                # upper left corner. This limitation may be lifted in the future.
                if not src_transform.is_rectilinear:
                    raise StackError(
                        "Rotated, non-rectilinear rasters cannot be stacked."
                    )
                if src_transform.a < 0:
                    raise StackError(
                        'Rasters with negative pixel width ("flipped" rasters) cannot be stacked.'
                    )
                if src_transform.e > 0:
                    raise StackError(
                        'Rasters with negative pixel height ("upside down" rasters) cannot be stacked.'
                    )

                left, bottom, right, top = src.bounds

            xs.extend([left, right])
            ys.extend([bottom, top])

        # Extent from option or extent of all inputs
        if bounds:
            dst_w, dst_s, dst_e, dst_n = bounds
        else:
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

        logger.debug("Chunks=%r", chunks)

        for chunk in chunks:
            dst_w, dst_s, dst_e, dst_n = windows.bounds(chunk, output_transform)
            dest = np.zeros((output_count, chunk.height, chunk.width), dtype=dt)
            if inrange:
                dest.fill(nodataval)

            dst_idx = 0
            for idx, (dataset, src_indexes) in enumerate(zip(sources, indexes)):
                with dataset_opener(dataset) as src:
                    if disjoint_bounds((dst_w, dst_s, dst_e, dst_n), src.bounds):
                        logger.debug(
                            "Skipping source: src=%r, bounds=%r",
                            src,
                            (dst_w, dst_s, dst_e, dst_n),
                        )
                        continue

                    if first_crs != src.crs:
                        raise RasterioError(f"CRS mismatch with source: {dataset}")

                    src_window = windows.from_bounds(
                        dst_w, dst_s, dst_e, dst_n, src.transform
                    ).round(3)

                    if src_indexes is None:
                        src_indexes = src.indexes
                    elif isinstance(src_indexes, int):
                        src_indexes = [src_indexes]

                    temp_shape = (len(src_indexes), chunk.height, chunk.width)

                    temp_src = src.read(
                        out_shape=temp_shape,
                        window=src_window,
                        boundless=True,
                        masked=True,
                        indexes=src_indexes,
                        resampling=resampling,
                    )

                if isinstance(src_indexes, int):
                    region = dest[dst_idx, :, :]
                    dst_idx += 1
                elif isinstance(src_indexes, Iterable):
                    region = dest[dst_idx : dst_idx + len(src_indexes), :, :]
                    dst_idx += len(src_indexes)

                if cmath.isnan(nodataval):
                    region_mask = np.isnan(region)
                elif not np.issubdtype(region.dtype, np.integer):
                    region_mask = np.isclose(region, nodataval)
                else:
                    region_mask = region == nodataval

                # Ensure common shape, resolving issue #2202.
                temp = temp_src[:, : region.shape[1], : region.shape[2]]
                temp_mask = np.ma.getmask(temp)

                np.copyto(
                    region,
                    temp,
                    casting="unsafe",
                )

            if dst:
                dst_window = windows.from_bounds(
                    dst_w, dst_s, dst_e, dst_n, output_transform
                ).round(3)
                dst.write(dest, window=dst_window)

        if dst is None:
            if masked:
                dest = np.ma.masked_equal(dest, nodataval, copy=False)
            return dest, output_transform
        else:
            if first_colormap:
                dst.write_colormap(1, first_colormap)
            dst.close()
