import json
import logging

import click

from .helpers import resolve_inout
from . import options
import rasterio
import rasterio.shutil
from rasterio.mask import mask as mask_tool

logger = logging.getLogger(__name__)


# Mask command
@click.command(short_help='Mask in raster using features.')
@options.files_inout_arg
@options.output_opt
@click.option('-j', '--geojson-mask', 'geojson_mask',
              type=click.Path(), default=None,
              help='GeoJSON file to use for masking raster.  Use "-" to read '
                   'from stdin.  If not provided, original raster will be '
                   'returned')
@options.format_opt
@options.all_touched_opt
@click.option('--crop', is_flag=True, default=False,
              help='Crop output raster to the extent of the geometries. '
                   'GeoJSON must overlap input raster to use --crop')
@click.option('-i', '--invert', is_flag=True, default=False,
              help='Inverts the mask, so that areas covered by features are'
                   'masked out and areas not covered are retained.  Ignored '
                   'if using --crop')
@options.overwrite_opt
@options.creation_options
@click.pass_context
def mask(
        ctx,
        files,
        output,
        geojson_mask,
        driver,
        all_touched,
        crop,
        invert,
        overwrite,
        creation_options):
    """Masks in raster using GeoJSON features (masks out all areas not covered
    by features), and optionally crops the output raster to the extent of the
    features.  Features are assumed to be in the same coordinate reference
    system as the input raster.

    GeoJSON must be the first input file or provided from stdin:

    > rio mask input.tif output.tif --geojson-mask features.json

    > rio mask input.tif output.tif --geojson-mask - < features.json

    If the output raster exists, it will be completely overwritten with the
    results of this operation.

    The result is always equal to or within the bounds of the input raster.

    --crop and --invert options are mutually exclusive.

    --crop option is not valid if features are completely outside extent of
    input raster.
    """

    output, files = resolve_inout(
        files=files, output=output, overwrite=overwrite)
    input = files[0]

    if geojson_mask is None:
        click.echo('No GeoJSON provided, INPUT will be copied to OUTPUT',
                   err=True)
        rasterio.shutil.copyfiles(input, output)
        return

    if crop and invert:
        click.echo('Invert option ignored when using --crop', err=True)
        invert = False

    with ctx.obj['env']:
        try:
            with click.open_file(geojson_mask) as fh:
                geojson = json.loads(fh.read())
        except ValueError:
            raise click.BadParameter('GeoJSON could not be read from '
                                     '--geojson-mask or stdin',
                                     param_hint='--geojson-mask')

        if 'features' in geojson:
            geometries = [f['geometry'] for f in geojson['features']]
        elif 'geometry' in geojson:
            geometries = (geojson['geometry'], )
        elif 'coordinates' in geojson:
            geometries = (geojson, )
        else:
            raise click.BadParameter('Invalid GeoJSON', param=input,
                                     param_hint='input')

        with rasterio.open(input) as src:
            try:
                out_image, out_transform = mask_tool(src, geometries,
                                                     crop=crop, invert=invert,
                                                     all_touched=all_touched)
            except ValueError as e:
                if e.args[0] == 'Input shapes do not overlap raster.':
                    if crop:
                        raise click.BadParameter('not allowed for GeoJSON '
                                                 'outside the extent of the '
                                                 'input raster',
                                                 param=crop,
                                                 param_hint='--crop')
                else:  # pragma: no cover
                    raise e

            profile = src.profile
            profile.update(**creation_options)
            profile.pop("driver", None)
            if driver:
                profile["driver"] = driver
            profile.update({
                'driver': driver,
                'height': out_image.shape[1],
                'width': out_image.shape[2],
                'transform': out_transform
            })

            with rasterio.open(output, 'w', **profile) as out:
                out.write(out_image)
