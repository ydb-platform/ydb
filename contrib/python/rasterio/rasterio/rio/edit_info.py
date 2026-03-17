"""Fetch and edit raster dataset metadata from the command line."""


import json
import warnings

import click

import rasterio
import rasterio.crs
from rasterio.crs import CRS
from rasterio.dtypes import in_dtype_range
from rasterio.enums import ColorInterp
from rasterio.errors import CRSError
from rasterio.rio import options
from rasterio.transform import guard_transform


# Handlers for info module options.


def all_handler(ctx, param, value):
    """Get tags from a template file or command line."""
    if ctx.obj and ctx.obj.get('like') and value is not None:
        ctx.obj['all_like'] = value
        value = ctx.obj.get('like')
    return value


def crs_handler(ctx, param, value):
    """Get crs value from a template file or command line."""
    retval = options.from_like_context(ctx, param, value)
    if retval is None and value:
        try:
            retval = json.loads(value)
        except ValueError:
            retval = value
        try:
            if isinstance(retval, dict):
                retval = CRS(retval)
            else:
                retval = CRS.from_string(retval)
        except CRSError:
            raise click.BadParameter(
                "'%s' is not a recognized CRS." % retval,
                param=param, param_hint='crs')
    return retval


def tags_handler(ctx, param, value):
    """Get tags from a template file or command line."""
    retval = options.from_like_context(ctx, param, value)
    if retval is None and value:
        try:
            retval = dict(p.split('=') for p in value)
        except Exception:
            raise click.BadParameter(
                "'%s' contains a malformed tag." % value,
                param=param, param_hint='transform')
    return retval


def transform_handler(ctx, param, value):
    """Get transform value from a template file or command line."""
    retval = options.from_like_context(ctx, param, value)
    if retval is None and value:
        try:
            value = json.loads(value)
        except ValueError:
            pass
        try:
            retval = guard_transform(value)
        except Exception:
            raise click.BadParameter(
                "'%s' is not recognized as an Affine array." % value,
                param=param, param_hint='transform')
    return retval


def colorinterp_handler(ctx, param, value):

    """Validate a string like ``red,green,blue,alpha`` and convert to
    a tuple.  Also handle ``RGB`` and ``RGBA``.
    """

    if value is None:
        return value
    # Using '--like'
    elif value.lower() == 'like':
        return options.from_like_context(ctx, param, value)
    elif value.lower() == 'rgb':
        return ColorInterp.red, ColorInterp.green, ColorInterp.blue
    elif value.lower() == 'rgba':
        return ColorInterp.red, ColorInterp.green, ColorInterp.blue, ColorInterp.alpha
    else:
        colorinterp = tuple(value.split(','))
        for ci in colorinterp:
            if ci not in ColorInterp.__members__:
                raise click.BadParameter(
                    "color interpretation '{ci}' is invalid.  Must be one of: "
                    "{valid}".format(
                        ci=ci, valid=', '.join(ColorInterp.__members__)))
        return tuple(ColorInterp[ci] for ci in colorinterp)


@click.command('edit-info', short_help="Edit dataset metadata.")
@options.file_in_arg
@options.bidx_opt
@options.edit_nodata_opt
@click.option('--unset-nodata', default=False, is_flag=True,
              help="Unset the dataset's nodata value.")
@click.option('--crs', callback=crs_handler, default=None,
              help="New coordinate reference system")
@click.option('--unset-crs', default=False, is_flag=True,
              help="Unset the dataset's CRS value.")
@click.option('--transform', callback=transform_handler,
              help="New affine transform matrix")
@click.option('--units', help="Edit units of a band (requires --bidx)")
@click.option('--description',
              help="Edit description of a band (requires --bidx)")
@click.option('--tag', 'tags', callback=tags_handler, multiple=True,
              metavar='KEY=VAL', help="New tag.")
@click.option('--all', 'allmd', callback=all_handler, flag_value='like',
              is_eager=True,
              help="Copy all metadata items from the template file.")
@click.option(
    '--colorinterp', callback=colorinterp_handler,
    metavar="name[,name,...]|RGB|RGBA|like",
    help="Set color interpretation for all bands like 'red,green,blue,alpha'. "
         "Can also use 'RGBA' as shorthand for 'red,green,blue,alpha' and "
         "'RGB' for the same sans alpha band.  Use 'like' to inherit color "
         "interpretation from '--like'.")
@options.like_opt
@click.pass_context
def edit(ctx, input, bidx, nodata, unset_nodata, crs, unset_crs, transform,
         units, description, tags, allmd, like, colorinterp):
    """Edit a dataset's metadata: coordinate reference system, affine
    transformation matrix, nodata value, and tags.

    The coordinate reference system may be either a PROJ.4 or EPSG:nnnn
    string,

      --crs 'EPSG:4326'

    or a JSON text-encoded PROJ.4 object.

      --crs '{"proj": "utm", "zone": 18, ...}'

    Transforms are JSON-encoded Affine objects like:

      --transform '[300.038, 0.0, 101985.0, 0.0, -300.042, 2826915.0]'

    Prior to Rasterio 1.0 GDAL geotransforms were supported for --transform,
    but are no longer supported.

    Metadata items may also be read from an existing dataset using a
    combination of the --like option with at least one of --all,
    `--crs like`, `--nodata like`, and `--transform like`.

      rio edit-info example.tif --like template.tif --all

    To get just the transform from the template:

      rio edit-info example.tif --like template.tif --transform like

    """
    # If '--all' is given before '--like' on the commandline then 'allmd'
    # is the string 'like'.  This is caused by '--like' not having an
    # opportunity to populate metadata before '--all' is evaluated.
    if allmd == 'like':
        allmd = ctx.obj['like']

    with ctx.obj['env'], rasterio.open(input, 'r+') as dst:

        if allmd:
            nodata = allmd['nodata']
            crs = allmd['crs']
            transform = allmd['transform']
            tags = allmd['tags']
            colorinterp = allmd['colorinterp']

        if unset_nodata and nodata is not None:
            raise click.BadParameter(
                "--unset-nodata and --nodata cannot be used together."
            )

        if unset_crs and crs:
            raise click.BadParameter("--unset-crs and --crs cannot be used together.")

        if unset_nodata:
            # Setting nodata to None will raise NotImplementedError
            # if GDALDeleteRasterNoDataValue() isn't present in the
            # GDAL library.
            try:
                dst.nodata = None
            except NotImplementedError as exc:  # pragma: no cover
                raise click.ClickException(str(exc))

        elif nodata is not None:
            dtype = dst.dtypes[0]
            if nodata is not None and not in_dtype_range(nodata, dtype):
                raise click.BadParameter(
                    "outside the range of the file's data type (%s)." % dtype,
                    param=nodata,
                    param_hint="nodata",
                )
            dst.nodata = nodata

        if unset_crs:
            dst.crs = None
        elif crs:
            dst.crs = crs

        if transform:
            dst.transform = transform

        if tags:
            dst.update_tags(**tags)

        if units:
            dst.set_band_unit(bidx, units)

        if description:
            dst.set_band_description(bidx, description)

        if colorinterp:
            if like and len(colorinterp) != dst.count:
                raise click.ClickException(
                    "When using '--like' for color interpretation the "
                    "template and target images must have the same number "
                    "of bands.  Found {template} color interpretations for "
                    "template image and {target} bands in target "
                    "image.".format(
                        template=len(colorinterp),
                        target=dst.count))
            try:
                dst.colorinterp = colorinterp
            except ValueError as e:
                raise click.ClickException(str(e))

    # Post check - ensure that crs was unset properly
    if unset_crs:
        with ctx.obj['env'], rasterio.open(input, 'r') as src:
            if src.crs:
                warnings.warn(
                    'CRS was not unset. Availability of his functionality '
                    'differs depending on GDAL version and driver')
