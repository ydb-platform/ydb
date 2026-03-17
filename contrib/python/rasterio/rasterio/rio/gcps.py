"""Command access to dataset metadata, stats, and more."""


import json

import click
from cligj import (
    compact_opt, use_rs_opt, projection_geographic_opt,
    projection_projected_opt, precision_opt, indent_opt)

import rasterio
import rasterio.crs
from rasterio.rio import options
from rasterio.warp import transform_geom


# Feature collection or feature sequence switch, defaulting to the
# latter, the opposite of cligj's default.
sequence_opt = click.option(
    '--sequence/--no-sequence',
    default=True,
    help="Write a LF-delimited sequence of texts containing individual "
         "objects or write a single JSON text containing a feature "
         "collection object (the default).")


@click.command(short_help="Print ground control points as GeoJSON.")
@options.file_in_arg
@options.geojson_type_opt(allowed=('collection', 'feature'), default='feature')
@projection_geographic_opt
@projection_projected_opt
@precision_opt
@use_rs_opt
@indent_opt
@compact_opt
@click.pass_context
def gcps(ctx, input, geojson_type, projection, precision, use_rs, indent, compact):
    """Print GeoJSON representations of a dataset's control points.

    Each ground control point is represented as a GeoJSON feature. The
    'properties' member of each feature contains a JSON representation
    of the control point with the following items:

    \b
        row, col:
            row (or line) and col (or pixel) coordinates.
        x, y, z:
            x, y, and z spatial coordinates.
        crs:
            The coordinate reference system for x, y, and z.
        id:
            A unique (within the dataset) identifier for the control
            point.
        info:
            A brief description of the control point.
    """

    # Handle the invalid combinations of args.
    if geojson_type == 'feature' and indent and not use_rs:
        raise click.BadParameter(
            "Pretty-printing a sequence of Features requires the --rs option")

    with ctx.obj['env'], rasterio.open(input) as src:
        gcps, crs = src.gcps
        proj = crs.to_string()
        proj = proj.split('=')[1].upper() if proj.startswith('+init=epsg') else proj

        def update_props(data, **kwds):
            data['properties'].update(**kwds)
            return data

        def transform(feat):
            dst_crs = 'epsg:4326' if projection == 'geographic' else crs
            geom = transform_geom(crs, dst_crs, feat['geometry'],
                                  precision=precision)
            feat['geometry'] = geom
            return feat

        # Specifying --collection overrides --sequence.
        if geojson_type == 'collection':

            if projection == 'geographic' or precision >= 0:
                features = [transform(update_props(p.__geo_interface__, crs=proj)) for p in gcps]
            else:
                features = [update_props(p.__geo_interface__, crs=proj) for p in gcps]

            click.echo(
                json.dumps(
                    {"type": "FeatureCollection", "features": features},
                    separators=(",", ":") if compact else None,
                    indent=indent,
                )
            )

        else:
            for p in gcps:
                if use_rs:
                    click.echo("\x1e", nl=False)

                if projection == "geographic" or precision >= 0:
                    feat = transform(update_props(p.__geo_interface__, crs=proj))
                else:
                    feat = update_props(p.__geo_interface__, crs=proj)

                click.echo(
                    json.dumps(
                        feat, separators=(",", ":") if compact else None, indent=indent
                    )
                )
