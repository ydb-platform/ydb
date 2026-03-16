"""The rio merge CLI command."""

import warnings

import click

from rasterio.enums import Resampling
from rasterio.errors import RasterioDeprecationWarning
from rasterio.rio import options
from rasterio.rio.helpers import resolve_inout
from rasterio.merge import MERGE_METHODS


def deprecated_precision(ctx, param, value):
    if value is not None:
        warnings.warn(
            "The --precision option is unused, deprecated, and will be removed in 2.0.0.",
            RasterioDeprecationWarning,
        )
    return None


@click.command(short_help="Merge a stack of raster datasets.")
@options.files_inout_arg
@options.output_opt
@options.format_opt
@options.bounds_opt
@options.resolution_opt
@click.option('--resampling',
              type=click.Choice([r.name for r in Resampling if r.value <= 7]),
              default='nearest', help="Resampling method.",
              show_default=True)
@click.option('--method',
              type=click.Choice(list(MERGE_METHODS)),
              default='first', help="Merging strategy.",
              show_default=True)
@options.nodata_opt
@options.dtype_opt
@options.bidx_mult_opt
@options.overwrite_opt
@click.option(
    "--precision",
    type=int,
    default=None,
    callback=deprecated_precision,
    help="Unused, deprecated, and will be removed in 2.0.0.",
)
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
def merge(
    ctx,
    files,
    output,
    driver,
    bounds,
    res,
    resampling,
    method,
    nodata,
    dtype,
    bidx,
    overwrite,
    precision,
    target_aligned_pixels,
    mem_limit,
    use_highest_res,
    creation_options,
):
    """Copy valid pixels from input files to an output file.

    All files must have the same number of bands, data type, and
    coordinate reference system. Rotated rasters cannot be merged.

    Input files are merged in their listed order using the reverse
    painter's algorithm. If the output file exists, its values will be
    overwritten by input values.

    Geospatial bounds and resolution of a new output file in the
    units of the input file coordinate reference system may be provided
    and are otherwise taken from the first input file.

    Note: --res changed from 2 parameters in 0.25.

    \b
      --res 0.1 0.1  => --res 0.1 (square)
      --res 0.1 0.2  => --res 0.1 --res 0.2  (rectangular)

    """
    from rasterio.merge import merge as merge_tool

    output, files = resolve_inout(
        files=files, output=output, overwrite=overwrite)

    resampling = Resampling[resampling]
    if driver:
        creation_options.update(driver=driver)

    with ctx.obj["env"]:
        merge_tool(
            files,
            bounds=bounds,
            res=res,
            nodata=nodata,
            dtype=dtype,
            indexes=(bidx or None),
            resampling=resampling,
            method=method,
            target_aligned_pixels=target_aligned_pixels,
            mem_limit=mem_limit,
            use_highest_res=use_highest_res,
            dst_path=output,
            dst_kwds=creation_options,
        )
