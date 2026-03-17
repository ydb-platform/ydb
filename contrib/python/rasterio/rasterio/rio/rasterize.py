"""$ rio rasterize"""


import json
import logging
from math import ceil

from affine import Affine
import click

import rasterio
from rasterio.errors import CRSError
from rasterio.coords import disjoint_bounds
from rasterio.rio import options
from rasterio.rio.helpers import resolve_inout
import rasterio.shutil


logger = logging.getLogger(__name__)


def files_handler(ctx, param, value):
    """Process and validate input file names"""
    return value


# Unlike the version in cligj, this one doesn't require values.
files_inout_arg = click.argument(
    'files',
    nargs=-1,
    type=click.Path(),
    metavar="INPUTS... OUTPUT",
    callback=files_handler)


@click.command(short_help='Rasterize features.')
@files_inout_arg
@options.output_opt
@options.format_opt
@options.like_file_opt
@options.bounds_opt
@options.dimensions_opt
@options.resolution_opt
@click.option('--src-crs', '--src_crs', 'src_crs', default=None,
              help='Source coordinate reference system.  Limited to EPSG '
              'codes for now.  Used as output coordinate system if output '
              'does not exist or --like option is not used. '
              'Default: EPSG:4326')
@options.all_touched_opt
@click.option('--default-value', '--default_value', 'default_value',
              type=float, default=1, help='Default value for rasterized pixels')
@click.option('--fill', type=float, default=0,
              help='Fill value for all pixels not overlapping features.  Will '
              'be evaluated as NoData pixels for output.  Default: 0')
@click.option('--property', 'prop', type=str, default=None, help='Property in '
              'GeoJSON features to use for rasterized values.  Any features '
              'that lack this property will be given --default_value instead.')
@options.overwrite_opt
@options.nodata_opt
@options.creation_options
@click.pass_context
def rasterize(
        ctx,
        files,
        output,
        driver,
        like,
        bounds,
        dimensions,
        res,
        src_crs,
        all_touched,
        default_value,
        fill,
        prop,
        overwrite,
        nodata,
        creation_options):
    """Rasterize GeoJSON into a new or existing raster.

    If the output raster exists, rio-rasterize will rasterize feature
    values into all bands of that raster.  The GeoJSON is assumed to be
    in the same coordinate reference system as the output unless
    --src-crs is provided.

    --default_value or property values when using --property must be
    using a data type valid for the data type of that raster.

    If a template raster is provided using the --like option, the affine
    transform and data type from that raster will be used to create the
    output.  Only a single band will be output.

    The GeoJSON is assumed to be in the same coordinate reference system
    unless --src-crs is provided.

    --default_value or property values when using --property must be
    using a data type valid for the data type of that raster.

    --driver, --bounds, --dimensions, --res, --nodata are ignored when
    output exists or --like raster is provided

    If the output does not exist and --like raster is not provided, the
    input GeoJSON will be used to determine the bounds of the output
    unless provided using --bounds.

    --dimensions or --res are required in this case.

    If --res is provided, the bottom and right coordinates of bounds are
    ignored.

    Note
    ----

    The GeoJSON is not projected to match the coordinate reference
    system of the output or --like rasters at this time.  This
    functionality may be added in the future.

    """

    from rasterio.crs import CRS
    from rasterio.features import rasterize
    from rasterio.features import bounds as calculate_bounds

    output, files = resolve_inout(
        files=files, output=output, overwrite=overwrite)

    bad_param = click.BadParameter('invalid CRS.  Must be an EPSG code.',
                                   ctx, param=src_crs, param_hint='--src_crs')
    has_src_crs = src_crs is not None
    try:
        src_crs = CRS.from_string(src_crs) if has_src_crs else CRS.from_string('EPSG:4326')
    except CRSError:
        raise bad_param

    # If values are actually meant to be integers, we need to cast them
    # as such or rasterize creates floating point outputs
    if default_value == int(default_value):
        default_value = int(default_value)
    if fill == int(fill):
        fill = int(fill)

    with ctx.obj['env']:

        def feature_value(feature):
            if prop and 'properties' in feature:
                return feature['properties'].get(prop, default_value)
            return default_value

        with click.open_file(files.pop(0) if files else '-') as gj_f:
            geojson = json.loads(gj_f.read())
        if 'features' in geojson:
            geometries = []
            for f in geojson['features']:
                geometries.append((f['geometry'], feature_value(f)))
        elif 'geometry' in geojson:
            geometries = ((geojson['geometry'], feature_value(geojson)), )
        else:
            raise click.BadParameter('Invalid GeoJSON', param=input,
                                     param_hint='input')

        geojson_bounds = geojson.get('bbox', calculate_bounds(geojson))

        if rasterio.shutil.exists(output):
            with rasterio.open(output, 'r+') as out:
                if has_src_crs and src_crs != out.crs:
                    raise click.BadParameter('GeoJSON does not match crs of '
                                             'existing output raster',
                                             param='input', param_hint='input')

                if disjoint_bounds(geojson_bounds, out.bounds):
                    click.echo("GeoJSON outside bounds of existing output "
                               "raster. Are they in different coordinate "
                               "reference systems?",
                               err=True)

                meta = out.meta

                result = rasterize(
                    geometries,
                    out_shape=(meta['height'], meta['width']),
                    transform=meta.get('affine', meta['transform']),
                    all_touched=all_touched,
                    dtype=meta.get('dtype', None),
                    default_value=default_value,
                    fill=fill)

                for bidx in range(1, meta['count'] + 1):
                    data = out.read(bidx, masked=True)
                    # Burn in any non-fill pixels, and update mask accordingly
                    ne = result != fill
                    data[ne] = result[ne]
                    if data.mask.any():
                        data.mask[ne] = False
                    out.write(data, indexes=bidx)

        else:
            if like is not None:
                with rasterio.open(like) as template_ds:

                  if has_src_crs and src_crs != template_ds.crs:
                      raise click.BadParameter('GeoJSON does not match crs of '
                                             '--like raster',
                                             param='input', param_hint='input')

                  if disjoint_bounds(geojson_bounds, template_ds.bounds):
                      click.echo("GeoJSON outside bounds of --like raster. "
                               "Are they in different coordinate reference "
                               "systems?",
                               err=True)

                  kwargs = template_ds.profile
                  kwargs['count'] = 1
                  kwargs['transform'] = template_ds.transform

            else:
                bounds = bounds or geojson_bounds

                if src_crs.is_geographic:
                    if (bounds[0] < -180 or bounds[2] > 180 or
                            bounds[1] < -80 or bounds[3] > 80):
                        raise click.BadParameter(
                            "Bounds are beyond the valid extent for "
                            "EPSG:4326.",
                            ctx, param=bounds, param_hint='--bounds')

                if dimensions:
                    width, height = dimensions
                    res = (
                        (bounds[2] - bounds[0]) / float(width),
                        (bounds[3] - bounds[1]) / float(height)
                    )

                else:
                    if not res:
                        raise click.BadParameter(
                            'pixel dimensions are required',
                            ctx, param=res, param_hint='--res')

                    elif len(res) == 1:
                        res = (res[0], res[0])

                    width = max(int(ceil((bounds[2] - bounds[0]) /
                                float(res[0]))), 1)
                    height = max(int(ceil((bounds[3] - bounds[1]) /
                                 float(res[1]))), 1)

                kwargs = {
                    'count': 1,
                    'crs': src_crs,
                    'width': width,
                    'height': height,
                    'transform': Affine(res[0], 0, bounds[0], 0, -res[1],
                                        bounds[3]),
                    'driver': driver
                }
                if driver:
                    kwargs["driver"] = driver

            kwargs.update(**creation_options)

            if nodata is not None:
                kwargs['nodata'] = nodata

            result = rasterize(
                geometries,
                out_shape=(kwargs['height'], kwargs['width']),
                transform=kwargs['transform'],
                all_touched=all_touched,
                dtype=kwargs.get('dtype', None),
                default_value=default_value,
                fill=fill)

            if 'dtype' not in kwargs:
                kwargs['dtype'] = result.dtype

            with rasterio.open(output, 'w', **kwargs) as out:
                out.write(result, indexes=1)
