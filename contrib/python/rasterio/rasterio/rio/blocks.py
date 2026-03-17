"""rio blocks prints a dataset's blocks as GeoJSON features."""

import json
import logging
import os.path

import click
import cligj

import rasterio
from rasterio.rio import options
from rasterio.rio.helpers import write_features
from rasterio.warp import transform_bounds


logger = logging.getLogger(__name__)


class _Collection:

    """For use with `rasterio.rio.helpers.write_features()`."""

    def __init__(self, dataset, bidx, precision=6, geographic=True):

        """Export raster dataset windows to GeoJSON polygon features.

        Parameters
        ----------
        dataset : a dataset object opened in 'r' mode
            Source dataset
        bidx : int
            Extract windows from this band
        precision : int, optional
            Coordinate precision
        geographic : bool, optional
            Reproject geometries to ``EPSG:4326`` if ``True``

        Yields
        ------
        dict
            GeoJSON polygon feature
        """

        self._src = dataset
        self._bidx = bidx
        self._precision = precision
        self._geographic = geographic

    def _normalize_bounds(self, bounds):
        if self._geographic:
            bounds = transform_bounds(self._src.crs, 'EPSG:4326', *bounds)
        if self._precision >= 0:
            bounds = (round(v, self._precision) for v in bounds)
        return bounds

    @property
    def bbox(self):
        return tuple(self._normalize_bounds(self._src.bounds))

    def __call__(self):
        gen = self._src.block_windows(bidx=self._bidx)
        for idx, (block, window) in enumerate(gen):
            bounds = self._normalize_bounds(self._src.window_bounds(window))
            xmin, ymin, xmax, ymax = bounds
            yield {
                "type": "Feature",
                "id": f"{os.path.basename(self._src.name)}:{idx}",
                "properties": {
                    "block": json.dumps(block),
                    "window": window.todict(),
                },
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [[
                        (xmin, ymin),
                        (xmin, ymax),
                        (xmax, ymax),
                        (xmax, ymin)
                    ]]
                }
            }


@click.command()
@options.file_in_arg
@options.output_opt
@cligj.precision_opt
@cligj.indent_opt
@cligj.compact_opt
@cligj.projection_projected_opt
@cligj.sequence_opt
@cligj.use_rs_opt
@click.option(
    '--bidx', type=click.INT, default=0,
    help="Index of the band that is the source of shapes.")
@click.pass_context
def blocks(
    ctx, input, output, precision, indent, compact, projection, sequence, use_rs, bidx
):
    """Write dataset blocks as GeoJSON features.

    This command prints features describing a raster's internal blocks,
    which are used directly for raster I/O.  These features can be used
    to visualize how a windowed operation would operate using those
    blocks.

    Output features have two JSON encoded properties: block and window.
    Block is a two element array like [0, 0] describing the window's
    position in the input band's window layout. Window is a JSON
    serialization of rasterio's Window class like {"col_off": 0,
    "height": 3, "row_off": 705, "width": 791}.

    Block windows are extracted from the dataset (all bands must have
    matching block windows) by default, or from the band specified using
    the --bidx option:
    \b

        rio blocks --bidx 3 tests/data/RGB.byte.tif

    By default a GeoJSON FeatureCollection is written, but the
    --sequence option produces a GeoJSON feature stream instead.
    \b

        rio blocks tests/data/RGB.byte.tif --sequence

    Output features are reprojected to OGC:CRS84 (WGS 84) unless the
    --projected flag is provided, which causes the output to be kept in
    the input datasource's coordinate reference system.

    For more information on exactly what blocks and windows represent,
    see block_windows().

    """
    dump_kwds = {'sort_keys': True}

    if indent:
        dump_kwds['indent'] = indent
    if compact:
        dump_kwds['separators'] = (',', ':')

    stdout = click.open_file(
        output, 'w') if output else click.get_text_stream('stdout')

    with ctx.obj['env'], rasterio.open(input) as src:

        if bidx and bidx not in src.indexes:
            raise click.BadParameter("Not a valid band index")

        collection = _Collection(
            dataset=src,
            bidx=bidx,
            precision=precision,
            geographic=projection != "projected",
        )
        write_features(
            stdout,
            collection,
            sequence=sequence,
            geojson_type="feature" if sequence else "collection",
            use_rs=use_rs,
            **dump_kwds
        )
