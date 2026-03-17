"""File translation command"""

import logging

import click

from .helpers import resolve_inout
from . import options
import rasterio
from rasterio.coords import disjoint_bounds
from rasterio.crs import CRS
from rasterio.enums import MaskFlags
from rasterio import windows

logger = logging.getLogger(__name__)

# Geographic (default), projected, or Mercator switch.
projection_geographic_opt = click.option(
    '--geographic',
    'projection',
    flag_value='geographic',
    help="Bounds in geographic coordinates.")

projection_projected_opt = click.option(
    '--projected',
    'projection',
    flag_value='projected',
    default=True,
    help="Bounds in input's own projected coordinates (the default).")

data_window_options = click.option(
    "--to-data-window",
    flag_value="datawindow",
    help="Clip the raster to the region of valid data by removing areas of surrounding NoData values.",
)

# Clip command
@click.command(short_help='Clip a raster to given bounds.')
@click.argument(
    'files',
    nargs=-1,
    type=click.Path(),
    required=True,
    metavar="INPUT OUTPUT")
@options.output_opt
@options.bounds_opt
@data_window_options
@click.option(
    '--like',
    type=click.Path(exists=True),
    help='Raster dataset to use as a template for bounds'
    )
@options.format_opt
@options.nodata_opt
@projection_geographic_opt
@projection_projected_opt
@options.overwrite_opt
@options.creation_options
@click.option(
    "--with-complement/--without-complement",
    default=False,
    help="Include the relative complement of the raster in the given bounds (giving a larger result), else return results only from the intersection of the raster and the bounds (the default).",
)
@click.pass_context
def clip(
    ctx,
    files,
    output,
    bounds,
    like,
    driver,
    nodata,
    projection,
    overwrite,
    creation_options,
    with_complement,
    to_data_window,
):
    """Clips a raster using projected or geographic bounds.

    The values of --bounds are presumed to be from the coordinate
    reference system of the input dataset unless the --geographic option
    is used, in which case the values may be longitude and latitude
    bounds. Either JSON, for example "[west, south, east, north]", or
    plain text "west south east north" representations of a bounding box
    are acceptable.

    If using --like, bounds will automatically be transformed to match
    the coordinate reference system of the input.

    Datasets with non-rectilinear geo transforms (i.e. with rotation
    and/or shear) may not be cropped using this command. They must be
    processed with rio-warp.

    Examples
    --------
    $ rio clip input.tif output.tif --bounds xmin ymin xmax ymax

    $ rio clip input.tif output.tif --like template.tif

    """
    from rasterio.warp import transform_bounds

    with ctx.obj['env']:

        output, files = resolve_inout(files=files, output=output, overwrite=overwrite)
        input = files[0]

        with rasterio.open(input) as src:
            if not src.transform.is_rectilinear:
                raise click.BadParameter(
                    "Non-rectilinear rasters (i.e. with rotation or shear) cannot be clipped"
                )

            if bounds:
                if projection == 'geographic':
                    bounds = transform_bounds(CRS.from_epsg(4326), src.crs, *bounds)
                if disjoint_bounds(bounds, src.bounds):
                    raise click.BadParameter(
                        "must overlap the extent of " "the input raster",
                        param="--bounds",
                        param_hint="--bounds",
                    )
                bounds_window = src.window(*bounds)
            elif like:
                with rasterio.open(like) as template_ds:
                    bounds = template_ds.bounds
                    if template_ds.crs != src.crs:
                        bounds = transform_bounds(template_ds.crs, src.crs,
                                                  *bounds)

                    if disjoint_bounds(bounds, src.bounds):
                        raise click.BadParameter(
                            "must overlap the extent of " "the input raster",
                            param="--like",
                            param_hint="--like",
                        )
                bounds_window = src.window(*bounds)

            elif to_data_window:
                src_win = windows.Window(0, 0, width=src.width, height=src.height)
                data_wins = []
                for sw in windows.subdivide(src_win, 512, 512):
                    dw = windows.get_data_window(src.read(1, masked=True, window=sw))
                    if dw.width * dw.height > 0:
                        # Adjust offsets
                        dw = windows.Window(col_off=dw.col_off + sw.col_off,
                                            row_off=dw.row_off + sw.row_off,
                                            width=dw.width, height=dw.height)
                        data_wins.append(dw)
                bounds_window = windows.union(data_wins)

            else:
                raise click.UsageError("--bounds, --like, or --to-data-window required")

            if not with_complement:
                bounds_window = bounds_window.intersection(
                    windows.Window(0, 0, src.width, src.height)
                )

            # Align window, as in gdal_translate.
            out_window = bounds_window.round_lengths()
            out_window = out_window.round_offsets()

            height = int(out_window.height)
            width = int(out_window.width)

            out_kwargs = src.profile

            if driver:
                out_kwargs["driver"] = driver

            if nodata is not None:
                out_kwargs["nodata"] = nodata

            out_kwargs.update({
                'height': height,
                'width': width,
                'transform': src.window_transform(out_window)})

            out_kwargs.update(**creation_options)

            if "blockxsize" in out_kwargs and int(out_kwargs["blockxsize"]) > width:
                del out_kwargs["blockxsize"]
                logger.warning(
                    "Blockxsize removed from creation options to accommodate small output width"
                )
            if "blockysize" in out_kwargs and int(out_kwargs["blockysize"]) > height:
                del out_kwargs["blockysize"]
                logger.warning(
                    "Blockysize removed from creation options to accommodate small output height"
                )

            with rasterio.open(output, "w", **out_kwargs) as out:
                dest_win = windows.Window(0, 0, width, height)
                for dw, sw in zip(windows.subdivide(dest_win, 512, 512), windows.subdivide(out_window, 512, 512)):
                    out_shape = (src.count, sw.height, sw.width)
                    out.write(
                        src.read(
                            window=sw,
                            out_shape=out_shape,
                            boundless=True,
                            masked=True,
                        ),
                        window=dw
                    )

                    if MaskFlags.per_dataset in src.mask_flag_enums[0]:
                        out.write_mask(
                            src.read_masks(
                                window=sw,
                                out_shape=out_shape,
                                boundless=True,
                            )[0],
                            window=dw
                        )

                    # TODO: copy other properties (GCPs etc). Several other
                    # programs need the same utility.
