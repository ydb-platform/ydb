"""fio-cat"""

import json
import warnings

import click
import cligj

import fiona
from fiona.transform import transform_geom
from fiona.model import Feature, ObjectEncoder
from fiona.fio import options, with_context_env
from fiona.fio.helpers import recursive_round
from fiona.errors import AttributeFilterError

warnings.simplefilter("default")


# Cat command
@click.command(short_help="Concatenate and print the features of datasets")
@click.argument("files", nargs=-1, required=True, metavar="INPUTS...")
@click.option(
    "--layer",
    default=None,
    multiple=True,
    callback=options.cb_multilayer,
    help="Input layer(s), specified as 'fileindex:layer` "
    "For example, '1:foo,2:bar' will concatenate layer foo "
    "from file 1 and layer bar from file 2",
)
@cligj.precision_opt
@cligj.indent_opt
@cligj.compact_opt
@click.option(
    "--ignore-errors/--no-ignore-errors",
    default=False,
    help="log errors but do not stop serialization.",
)
@options.dst_crs_opt
@cligj.use_rs_opt
@click.option(
    "--bbox",
    default=None,
    metavar="w,s,e,n",
    help="filter for features intersecting a bounding box",
)
@click.option(
    "--where",
    default=None,
    help="attribute filter using SQL where clause",
)
@click.option(
    "--cut-at-antimeridian",
    is_flag=True,
    default=False,
    help="Optionally cut geometries at the anti-meridian. To be used only for a geographic destination CRS.",
)
@click.option('--where', default=None,
              help="attribute filter using SQL where clause")
@options.open_opt
@click.pass_context
@with_context_env
def cat(
    ctx,
    files,
    precision,
    indent,
    compact,
    ignore_errors,
    dst_crs,
    use_rs,
    bbox,
    where,
    cut_at_antimeridian,
    layer,
    open_options,
):
    """
    Concatenate and print the features of input datasets as a sequence of
    GeoJSON features.

    When working with a multi-layer dataset the first layer is used by default.
    Use the '--layer' option to select a different layer.

    """
    dump_kwds = {"sort_keys": True}
    if indent:
        dump_kwds["indent"] = indent
    if compact:
        dump_kwds["separators"] = (",", ":")

    # Validate file idexes provided in --layer option
    # (can't pass the files to option callback)
    if layer:
        options.validate_multilayer_file_index(files, layer)

    # first layer is the default
    for i in range(1, len(files) + 1):
        if str(i) not in layer.keys():
            layer[str(i)] = [0]

    try:
        if bbox:
            try:
                bbox = tuple(map(float, bbox.split(",")))
            except ValueError:
                bbox = json.loads(bbox)

        for i, path in enumerate(files, 1):
            for lyr in layer[str(i)]:
                with fiona.open(path, layer=lyr, **open_options) as src:
                    for i, feat in src.items(bbox=bbox, where=where):
                        geom = feat.geometry

                        if dst_crs:
                            geom = transform_geom(
                                src.crs,
                                dst_crs,
                                geom,
                                antimeridian_cutting=cut_at_antimeridian,
                            )

                        if precision >= 0:
                            geom = recursive_round(geom, precision)

                        feat = Feature(
                            id=feat.id,
                            properties=feat.properties,
                            geometry=geom,
                            bbox=fiona.bounds(geom),
                        )

                        if use_rs:
                            click.echo("\x1e", nl=False)

                        click.echo(json.dumps(feat, cls=ObjectEncoder, **dump_kwds))

    except AttributeFilterError as e:
        raise click.BadParameter("'where' clause is invalid: " + str(e))
