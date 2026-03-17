"""Raster stack CLI command."""

from itertools import zip_longest
import logging

import click

import rasterio
from rasterio.enums import Resampling
from rasterio.rio import options
from rasterio.rio.helpers import resolve_inout
from rasterio.stack import stack as stack_tool

logger = logging.getLogger(__name__)


@click.command(short_help="Stack a number of bands into a multiband dataset.")
@options.files_inout_arg
@options.output_opt
@options.format_opt
@options.rgb_opt
@options.bounds_opt
@options.resolution_opt
@click.option(
    "--resampling",
    type=click.Choice([r.name for r in Resampling if r.value <= 7]),
    default="nearest",
    help="Resampling method.",
    show_default=True,
)
@options.nodata_opt
@options.dtype_opt
@options.bidx_magic_opt
@options.overwrite_opt
@click.option(
    "--target-aligned-pixels/--no-target-aligned-pixels",
    default=False,
    help="Align the output bounds based on the resolution.",
)
@click.option(
    "--mem-limit",
    type=int,
    default=64,
    help="Limit on memory used to perform calculations, in MB.",
)
@click.option(
    "--use-highest-res/--use-first-res",
    default=False,
    help="Use the highest resolution of sources or the resolution of the first source argument.",
)
@options.creation_options
@click.pass_context
def stack(
    ctx,
    files,
    output,
    driver,
    photometric,
    bounds,
    res,
    resampling,
    nodata,
    dtype,
    bidx,
    overwrite,
    target_aligned_pixels,
    mem_limit,
    use_highest_res,
    creation_options,
):
    """Stack a number of bands from one or more input files into a
    multiband dataset.

    Input datasets must be of a kind: same data type, dimensions, etc. The
    output is cloned from the first input.

    By default, rio-stack will take all bands from each input and write them
    in same order to the output. Optionally, bands for each input may be
    specified using a simple syntax:

      --bidx N takes the Nth band from the input (first band is 1).

      --bidx M,N,0 takes bands M, N, and O.

      --bidx M..O takes bands M-O, inclusive.

      --bidx ..N takes all bands up to and including N.

      --bidx N.. takes all bands from N to the end.

    Examples, using the Rasterio testing dataset, which produce a copy.

      rio stack RGB.byte.tif -o stacked.tif

      rio stack RGB.byte.tif --bidx 1,2,3 -o stacked.tif

      rio stack RGB.byte.tif --bidx 1..3 -o stacked.tif

      rio stack RGB.byte.tif --bidx ..2 RGB.byte.tif --bidx 3.. -o stacked.tif

    """
    output, files = resolve_inout(files=files, output=output, overwrite=overwrite)
    resampling = Resampling[resampling]

    output_count = 0
    indexes = []

    for path, item in zip_longest(files, bidx, fillvalue=None):
        with rasterio.open(path) as src:
            src_indexes = src.indexes
        if item is None:
            indexes.append(src_indexes)
            output_count += len(src_indexes)
        elif ".." in item:
            start, stop = map(lambda x: int(x) if x else None, item.split(".."))
            if start is None:
                start = 1
            indexes.append(src_indexes[slice(start - 1, stop)])
            output_count += len(src_indexes[slice(start - 1, stop)])
        else:
            parts = list(map(int, item.split(",")))
            if len(parts) == 1:
                indexes.append(parts[0])
                output_count += 1
            else:
                parts = list(parts)
                indexes.append(parts)
                output_count += len(parts)

    dst_kwds = {}

    if driver:
        dst_kwds["driver"] = driver
    if photometric:
        dst_kwds["photometric"] = photometric

    with ctx.obj["env"]:
        stack_tool(
            files,
            bounds=bounds,
            res=res,
            nodata=nodata,
            dtype=dtype,
            indexes=indexes,
            output_count=output_count,
            resampling=resampling,
            target_aligned_pixels=target_aligned_pixels,
            mem_limit=mem_limit,
            use_highest_res=use_highest_res,
            dst_path=output,
            dst_kwds=dst_kwds,
        )
