"""
Registry of common rio CLI options.  See cligj for more options.

.. code-block:: none

    -a, --all: Use all pixels touched by features.  In rio-mask, rio-rasterize
    --as-mask/--not-as-mask: interpret band as mask or not.  In rio-shapes
    --band/--mask: use band or mask.  In rio-shapes
    --bbox:
    -b, --bidx: band index(es) (singular or multiple value versions).
        In rio-info, rio-sample, rio-shapes, rio-stack (different usages)
    --bounds: bounds in world coordinates.
        In rio-info, rio-rasterize (different usages)
    --count: count of bands.  In rio-info
    --crop: Crop raster to extent of features.  In rio-mask
    --crs: CRS of input raster.  In rio-info
    --default-value: default for rasterized pixels.  In rio-rasterize
    --dimensions: Output width, height.  In rio-rasterize
    --dst-crs: destination CRS.  In rio-transform
    --fill: fill value for pixels not covered by features.  In rio-rasterize
    --formats: list available formats.  In rio-info
    --height: height of raster.  In rio-info
    -i, --invert: Invert mask created from features: In rio-mask
    -j, --geojson-mask: GeoJSON for masking raster.  In rio-mask
    --lnglat: geographic coordinates of center of raster.  In rio-info
    --masked/--not-masked: read masked data from source file.
        In rio-calc, rio-info
    -m, --mode: output file mode (r, r+).  In rio-insp
    --name: input file name alias.  In rio-calc
    --nodata: nodata value.  In rio-info, rio-merge (different usages)
    --photometric: photometric interpretation.  In rio-stack
    --property: GeoJSON property to use as values for rasterize.  In rio-rasterize
    -r, --res: output resolution.
        In rio-info, rio-rasterize (different usages.  TODO: try to combine
        usages, prefer rio-rasterize version)
    --sampling: Inverse of sampling fraction.  In rio-shapes
    --shape: shape (width, height) of band.  In rio-info
    --src-crs: source CRS.
        In rio-insp, rio-rasterize (different usages.  TODO: consolidate usages)
    --stats: print raster stats.  In rio-inf
    -t, --dtype: data type.  In rio-calc, rio-info (different usages)
    --width: width of raster.  In rio-info
    --with-nodata/--without-nodata: include nodata regions or not.  In rio-shapes.
    -v, --tell-me-more, --verbose
    --vfs: virtual file system.
"""

import logging
import os
import re

import click

import rasterio
import rasterio.shutil
from rasterio.dtypes import (
    ubyte,
    uint8,
    uint16,
    uint32,
    uint64,
    int8,
    int16,
    int32,
    int64,
    float16,
    float32,
    float64
)
from rasterio._path import _parse_path, _UnparsedPath


logger = logging.getLogger(__name__)


def _cb_key_val(ctx, param, value):

    """
    click callback to validate `--opt KEY1=VAL1 --opt KEY2=VAL2` and collect
    in a dictionary like the one below, which is what the CLI function receives.
    If no value or `None` is received then an empty dictionary is returned.

        {
            'KEY1': 'VAL1',
            'KEY2': 'VAL2'
        }

    Note: `==VAL` breaks this as `str.split('=', 1)` is used.
    """

    if not value:
        return {}
    else:
        out = {}
        for pair in value:
            if "=" not in pair:
                raise click.BadParameter(f"Invalid syntax for KEY=VAL arg: {pair}")
            else:
                k, v = pair.split('=', 1)
                k = k.lower()
                v = v.lower()
                out[k] = None if v.lower() in ['none', 'null', 'nil', 'nada'] else v
        return out


def abspath_forward_slashes(path):
    """Return forward-slashed version of os.path.abspath"""
    return '/'.join(os.path.abspath(path).split(os.path.sep))


def file_in_handler(ctx, param, value):
    """Normalize ordinary filesystem and VFS paths"""
    try:
        path = _parse_path(value)

        if isinstance(path, _UnparsedPath):

            if os.path.exists(path.path) and rasterio.shutil.exists(value):
                return abspath_forward_slashes(path.path)
            else:
                return path.name

        elif path.scheme and path.is_remote:
            return path.name

        elif path.archive:
            if os.path.exists(path.archive) and rasterio.shutil.exists(value):
                archive = abspath_forward_slashes(path.archive)
                return f"{path.scheme}://{archive}!{path.path}"
            else:
                raise OSError(f"Input archive {path.archive} does not exist")

        else:
            if os.path.exists(path.path) and rasterio.shutil.exists(value):
                return abspath_forward_slashes(path.path)
            else:
                raise OSError(f"Input file {path.path} does not exist")

    except Exception:
        raise click.BadParameter(f"{value} is not a valid input file")


def files_in_handler(ctx, param, value):
    """Process and validate input file names"""
    return tuple(file_in_handler(ctx, param, item) for item in value)


def files_inout_handler(ctx, param, value):
    """Process and validate input file names"""
    return tuple(file_in_handler(ctx, param, item) for item in value[:-1]) + tuple(value[-1:])


def from_like_context(ctx, param, value):
    """Return the value for an option from the context if the option
    or `--all` is given, else return None."""
    if ctx.obj and ctx.obj.get('like') and (
            value == 'like' or ctx.obj.get('all_like')):
        return ctx.obj['like'][param.name]
    else:
        return None


def like_handler(ctx, param, value):
    """Copy a dataset's meta property to the command context for access
    from other callbacks."""
    if ctx.obj is None:
        ctx.obj = {}
    if value:
        with rasterio.open(value) as src:
            metadata = src.meta
            ctx.obj['like'] = metadata
            ctx.obj['like']['transform'] = metadata['transform']
            ctx.obj['like']['tags'] = src.tags()
            ctx.obj['like']['colorinterp'] = src.colorinterp
        return True
    else:  # pragma: no cover
        return False


def nodata_handler(ctx, param, value):
    """Return a float or None"""
    if value is None or value.lower() in ["null", "nil", "none", "nada"]:
        return None
    else:
        try:
            return float(value)
        except (TypeError, ValueError):
            raise click.BadParameter(
                f"{value!r} is not a number", param=param, param_hint="nodata"
            )


def edit_nodata_handler(ctx, param, value):
    """Get nodata value from a template file or command line.

    Expected values are 'like', 'null', a numeric value, 'nan', or None.

    Returns
    -------
    float or None

    Raises
    ------
    click.BadParameter

    """
    if value == "like" or value is None:
        retval = from_like_context(ctx, param, value)
        if retval is not None:
            return retval

    return nodata_handler(ctx, param, value)


def bounds_handler(ctx, param, value):
    """Handle different forms of bounds."""
    retval = from_like_context(ctx, param, value)
    if retval is None and value is not None:
        try:
            value = value.strip(', []')
            retval = tuple(float(x) for x in re.split(r'[,\s]+', value))
            assert len(retval) == 4
            return retval
        except Exception:
            raise click.BadParameter(
                "{!r} is not a valid bounding box representation".format(value)
            )
    else:  # pragma: no cover
        return retval


# Singular input file
file_in_arg = click.argument('INPUT', callback=file_in_handler)

# Singular output file
file_out_arg = click.argument("OUTPUT", type=click.Path())

# Multiple input files.
files_in_arg = click.argument(
    'files',
    nargs=-1,
    type=click.Path(),
    required=True,
    metavar="INPUTS...",
    callback=files_in_handler)

# Multiple files, last of which is an output file.
files_inout_arg = click.argument(
    'files',
    nargs=-1,
    type=click.Path(),
    required=True,
    metavar="INPUTS... OUTPUT",
    callback=files_inout_handler)

bidx_opt = click.option(
    '-b', '--bidx',
    type=int,
    default=1,
    help="Input file band index (default: 1).")

bidx_mult_opt = click.option(
    '-b', '--bidx',
    type=int,
    multiple=True,
    help="Indexes of input file bands.")

# The 'magic' --bidx option is especially for rio-stack, which supports
# selection of multiple bands from multiple files. It has 'str' type
# instead of 'int' to support ranges in a syntax like `--bidx 1..3`.
bidx_magic_opt = click.option(
    '-b', '--bidx',
    multiple=True,
    help="Indexes of input file bands.")

# TODO: may be better suited to cligj?
bounds_opt = click.option(
    "--bounds",
    default=None,
    callback=bounds_handler,
    help="Bounds: 'left bottom right top' or '[left, bottom, right, top]'.",
)

dimensions_opt = click.option(
    '--dimensions',
    nargs=2, type=int, default=None,
    help='Output dataset width, height in number of pixels.')

dtype_opt = click.option(
    '-t', '--dtype',
    type=click.Choice([
        ubyte,
        uint8,
        uint16,
        uint32,
        uint64,
        int8,
        int16,
        uint32,
        int32,
        int64,
        float16,
        float32,
        float64
    ]),
    default=None,
    help="Output data type.")

like_file_opt = click.option(
    '--like',
    type=click.Path(exists=True),
    help='Raster dataset to use as a template for obtaining affine '
         'transform (bounds and resolution), crs, data type, and driver '
         'used to create the output.')

masked_opt = click.option(
    '--masked/--not-masked',
    default=True,
    help="Evaluate expressions using masked arrays (the default) or ordinary "
         "numpy arrays.")

output_opt = click.option(
    '-o', '--output',
    default=None,
    type=click.Path(),
    help="Path to output file (optional alternative to a positional arg).")

resolution_opt = click.option(
    '-r', '--res',
    multiple=True, type=float, default=None,
    help='Output dataset resolution in units of coordinate '
         'reference system. Pixels assumed to be square if this option '
         'is used once, otherwise use: '
         '--res pixel_width --res pixel_height.')

creation_options = click.option(
    '--co', '--profile', 'creation_options',
    metavar='NAME=VALUE',
    multiple=True,
    callback=_cb_key_val,
    help="Driver specific creation options. "
         "See the documentation for the selected output driver for "
         "more information.")

rgb_opt = click.option(
    '--rgb', 'photometric',
    flag_value='rgb',
    help="Set RGB photometric interpretation.")

overwrite_opt = click.option(
    '--overwrite', 'overwrite',
    is_flag=True, type=bool, default=False,
    help="Always overwrite an existing output file.")

nodata_opt = click.option(
    "--nodata",
    callback=nodata_handler,
    default=None,
    metavar="NUMBER|nan",
    help="Set a Nodata value.",
)

edit_nodata_opt = click.option(
    "--nodata",
    callback=edit_nodata_handler,
    default=None,
    metavar="NUMBER|nan|null",
    help="Modify the Nodata value.",
)

like_opt = click.option(
    '--like',
    type=click.Path(exists=True),
    callback=like_handler,
    is_eager=True,
    help="Raster dataset to use as a template for obtaining affine "
         "transform (bounds and resolution), crs, and nodata values.")

all_touched_opt = click.option(
    '-a', '--all', '--all_touched', 'all_touched',
    is_flag=True,
    default=False,
    help='Use all pixels touched by features, otherwise (default) use only '
         'pixels whose center is within the polygon or that are selected by '
         'Bresenhams line algorithm')

# Feature collection or feature sequence switch.
sequence_opt = click.option(
    '--sequence/--collection',
    default=True,
    help="Write a LF-delimited sequence of texts containing individual "
         "objects (the default) or write a single JSON text containing a "
         "feature collection object.")

format_opt = click.option(
    "-f", "--format", "--driver", "driver", help="Output format driver."
)


def geojson_type_opt(*, allowed, default):
    """GeoJSON output mode"""
    def verify_geojson_type(ctx, param, value):
        geojson_type = ctx.params.setdefault("geojson_type", {})
        geojson_type[param.name] = value
        if len(geojson_type) == len(allowed):
            # We've now seen all options for geojson_type, so verify their contents.
            specified = [
                name.removeprefix("geojson_type_")
                for name, value in geojson_type.items()
                if value
            ]
            if len(specified) > 1:
                names = ",".join(f"--{name}" for name in specified)
                raise click.BadParameter(
                    f"too many GeoJSON output formats: {names} may not be specified together"
                )
            ctx.params["geojson_type"] = specified[0] if specified else default

    def wrapper(func):
        options = {
            "collection": "feature collection(s)",
            "feature": "feature(s)",
            "bbox": "bounding box array(s)",
        }

        for name in allowed:
            if name not in options:
                raise click.UsageError(f"{name} is not a valid GeoJSON output mode")
            help_suffix = " (default)" if name == default else ""
            opt = click.option(
                f"--{name}",
                f"geojson_type_{name}",
                is_flag=True,
                expose_value=False,
                callback=verify_geojson_type,
                help=f"Output as GeoJSON {options[name]}{help_suffix}",
            )
            func = opt(func)
        return func

    return wrapper
