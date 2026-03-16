"""The rio create command."""

import click
import json
import os

import rasterio
from rasterio.crs import CRS
from rasterio.errors import (
    CRSError,
    FileOverwriteError,
    RasterioIOError,
)
from rasterio.rio import options
from rasterio.transform import Affine, guard_transform


def crs_handler(ctx, param, value):
    """Get crs value from the command line."""
    retval = None
    if value is not None:
        try:
            retval = CRS.from_string(value)
        except CRSError:
            raise click.BadParameter(
                f"{value} is not a recognized CRS.", param=param, param_hint="crs"
            )
    return retval


def transform_handler(ctx, param, value):
    """Get transform value from the command line."""
    retval = None
    if value is not None:
        try:
            value = json.loads(value)
            retval = guard_transform(value)
        except Exception:
            raise click.BadParameter(
                f"{value} is not recognized as a transformarray.",
                param=param,
                param_hint="transform",
            )
    return retval


@click.command(short_help="Create an empty or filled dataset.")
@options.file_out_arg
@options.format_opt
@options.dtype_opt
@click.option("--count", "-n", type=int, help="Number of raster bands.")
@click.option("--height", "-h", type=int, help="Raster height, or number of rows.")
@click.option("--width", "-w", type=int, help="Raster width, or number of columns.")
@options.nodata_opt
@click.option(
    "--crs", callback=crs_handler, default=None, help="Coordinate reference system."
)
@click.option(
    "--transform",
    callback=transform_handler,
    help="Affine transform matrix. Overrides any given bounds option.",
)
@options.bounds_opt
@options.overwrite_opt
@options.creation_options
@click.pass_context
def create(
    ctx,
    output,
    driver,
    dtype,
    count,
    height,
    width,
    nodata,
    crs,
    transform,
    bounds,
    overwrite,
    creation_options,
):
    """Create an empty dataset.

    The fundamental, required parameters are: format driver name, data
    type, count of bands, height and width in pixels. Long and short
    options are provided for each of these. Coordinate reference system
    and affine transformation matrix are not strictly required and have
    long options only. All other format specific creation outputs must
    be specified using the --co option.

    Simple north-up, non-rotated georeferencing can be set by using the
    --bounds option. The --transform option will assign an arbitrarily
    rotated affine transformation matrix to the dataset. Ground control
    points, rational polynomial coefficients, and geolocation matrices
    are not supported.

    The pixel values of an empty dataset are format specific. "Smart"
    formats like GTiff use 0 or the nodata value if provided.

    Example:

    \b
         $ rio create new.tif -f GTiff -t uint8 -n 3 -h 512 -w 512 \\
         > --co tiled=true --co blockxsize=256 --co blockysize=256

    The command above produces a 3-band GeoTIFF with 256 x 256 internal
    tiling.
    """
    # Preventing rio create from overwriting local and remote files,
    # objects, and datasets is complicated.
    if os.path.exists(output):
        if not overwrite:
            raise FileOverwriteError(
                "File exists and won't be overwritten without use of the '--overwrite' option."
            )
    else:  # Check remote or other non-file output.
        try:
            with rasterio.open(output) as dataset:
                # Dataset exists. May or may not be overwritten.
                if not overwrite:
                    raise FileOverwriteError(
                        "Dataset exists and won't be overwritten without use of the '--overwrite' option."
                    )
        except RasterioIOError as exc:
            # TODO: raise a different exception from rasterio.open() in
            # this case?
            if "No such file or directory" in str(exc):
                pass  # Good, output does not exist. Continue with no error.
            else:
                # Remote output exists, but is not a rasterio dataset.
                if not overwrite:
                    raise FileOverwriteError(
                        "Object exists and won't be overwritten without use of the '--overwrite' option."
                    )

    # Prepare the dataset's georeferencing.
    geo_transform = None

    if bounds:
        left, bottom, right, top = bounds
        sx = (right - left) / width
        sy = (bottom - top) / height
        geo_transform = Affine.translation(left, top) * Affine.scale(sx, sy)
    if transform:
        if geo_transform is not None:
            click.echo(
                "--transform value is overriding --bounds value. "
                "Use only one of these options to avoid this warning.",
                err=True,
            )
        geo_transform = transform

    profile = dict(
        driver=driver,
        dtype=dtype,
        count=count,
        height=height,
        width=width,
        nodata=nodata,
        crs=crs,
        transform=geo_transform,
        **creation_options,
    )

    with ctx.obj["env"], rasterio.open(output, "w", **profile) as dataset:
        pass
