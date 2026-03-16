"""$ rio shapes"""



import logging

import click
import cligj
import rasterio

from rasterio.rio import options
from rasterio.features import dataset_features
from rasterio.rio.helpers import write_features

logger = logging.getLogger(__name__)


@click.command(short_help="Write shapes extracted from bands or masks.")
@options.file_in_arg
@options.output_opt
@cligj.precision_opt
@cligj.indent_opt
@cligj.compact_opt
@cligj.projection_geographic_opt
@cligj.projection_projected_opt
@options.sequence_opt
@cligj.use_rs_opt
@options.geojson_type_opt(allowed=('feature', 'bbox'), default='feature')
@click.option('--band/--mask', default=True,
              help="Choose to extract from a band (the default) or a mask.")
@click.option('--bidx', 'bandidx', type=int, default=None,
              help="Index of the band or mask that is the source of shapes.")
@click.option('--sampling', type=int, default=1,
              help="Inverse of the sampling fraction; "
                   "a value of 10 decimates.")
@click.option('--with-nodata/--without-nodata', default=False,
              help="Include or do not include (the default) nodata regions.")
@click.option('--as-mask/--not-as-mask', default=False,
              help="Interpret a band as a mask and output only one class of "
                   "valid data shapes.")
@click.pass_context
def shapes(
        ctx, input, output, precision, indent, compact, projection, sequence,
        use_rs, geojson_type, band, bandidx, sampling, with_nodata, as_mask):
    """Extracts shapes from one band or mask of a dataset and writes
    them out as GeoJSON. Unless otherwise specified, the shapes will be
    transformed to WGS 84 coordinates.

    The default action of this command is to extract shapes from the
    first band of the input dataset. The shapes are polygons bounding
    contiguous regions (or features) of the same raster value. This
    command performs poorly for int16 or float type datasets.

    Bands other than the first can be specified using the `--bidx`
    option:

      $ rio shapes --bidx 3 tests/data/RGB.byte.tif

    The valid data footprint of a dataset's i-th band can be extracted
    by using the `--mask` and `--bidx` options:

      $ rio shapes --mask --bidx 1 tests/data/RGB.byte.tif

    Omitting the `--bidx` option results in a footprint extracted from
    the conjunction of all band masks. This is generally smaller than
    any individual band's footprint.

    A dataset band may be analyzed as though it were a binary mask with
    the `--as-mask` option:

      $ rio shapes --as-mask --bidx 1 tests/data/RGB.byte.tif
    """
    # These import numpy, which we don't want to do unless it's needed.
    dump_kwds = {'sort_keys': True}
    if indent:
        dump_kwds['indent'] = indent
    if compact:
        dump_kwds['separators'] = (',', ':')

    stdout = click.open_file(
        output, 'w') if output else click.get_text_stream('stdout')

    bidx = 1 if bandidx is None and band else bandidx

    if not sequence:
        geojson_type = 'collection'

    geographic = True if projection == 'geographic' else False

    with ctx.obj["env"] as env:
        with rasterio.open(input) as src:
            write_features(
                stdout,
                feature_gen(
                    src,
                    env,
                    bidx,
                    sampling=sampling,
                    band=band,
                    as_mask=as_mask,
                    with_nodata=with_nodata,
                    geographic=geographic,
                    precision=precision,
                ),
                sequence=sequence,
                geojson_type=geojson_type,
                use_rs=use_rs,
                **dump_kwds
            )


def feature_gen(src, env, *args, **kwargs):
    class Collection:

        def __init__(self, env):
            self.bboxes = []
            self.env = env

        @property
        def bbox(self):
            minxs, minys, maxxs, maxys = zip(*self.bboxes)
            return min(minxs), min(minys), max(maxxs), max(maxys)

        def __call__(self):
            for f in dataset_features(src, *args, **kwargs):
                self.bboxes.append(f['bbox'])
                yield f

    return Collection(env)
