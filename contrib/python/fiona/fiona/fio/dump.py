"""fio-dump"""

from functools import partial
import json
import logging

import click
import cligj

import fiona
from fiona.fio import helpers, options, with_context_env
from fiona.model import Feature, ObjectEncoder
from fiona.transform import transform_geom


@click.command(short_help="Dump a dataset to GeoJSON.")
@click.argument('input', required=True)
@click.option('--layer', metavar="INDEX|NAME", callback=options.cb_layer,
              help="Print information about a specific layer.  The first "
                   "layer is used by default.  Layers use zero-based "
                   "numbering when accessed by index.")
@click.option('--encoding', help="Specify encoding of the input file.")
@cligj.precision_opt
@cligj.indent_opt
@cligj.compact_opt
@click.option('--record-buffered/--no-record-buffered', default=False,
              help="Economical buffering of writes at record, not collection "
                   "(default), level.")
@click.option('--ignore-errors/--no-ignore-errors', default=False,
              help="log errors but do not stop serialization.")
@click.option('--with-ld-context/--without-ld-context', default=False,
              help="add a JSON-LD context to JSON output.")
@click.option('--add-ld-context-item', multiple=True,
              help="map a term to a URI and add it to the output's JSON LD "
                   "context.")
@options.open_opt
@click.pass_context
@with_context_env
def dump(
    ctx,
    input,
    encoding,
    precision,
    indent,
    compact,
    record_buffered,
    ignore_errors,
    with_ld_context,
    add_ld_context_item,
    layer,
    open_options,
):

    """Dump a dataset either as a GeoJSON feature collection (the default)
    or a sequence of GeoJSON features."""

    logger = logging.getLogger(__name__)
    sink = click.get_text_stream('stdout')

    dump_kwds = {'sort_keys': True}
    if indent:
        dump_kwds['indent'] = indent
    if compact:
        dump_kwds['separators'] = (',', ':')
    item_sep = compact and ',' or ', '

    if encoding:
        open_options["encoding"] = encoding
    if layer:
        open_options["layer"] = layer

    def transformer(crs, feat):
        tg = partial(
            transform_geom,
            crs,
            "EPSG:4326",
            antimeridian_cutting=True,
            precision=precision,
        )
        return Feature(
            id=feat.id, properties=feat.properties, geometry=tg(feat.geometry)
        )

    with fiona.open(input, **open_options) as source:
        meta = source.meta
        meta["fields"] = dict(source.schema["properties"].items())

        if record_buffered:
            # Buffer GeoJSON data at the feature level for smaller
            # memory footprint.
            indented = bool(indent)
            rec_indent = "\n" + " " * (2 * (indent or 0))

            collection = {
                "type": "FeatureCollection",
                "fiona:schema": meta["schema"],
                "fiona:crs": meta["crs"],
                "features": [],
            }
            if with_ld_context:
                collection["@context"] = helpers.make_ld_context(add_ld_context_item)

            head, tail = json.dumps(collection, **dump_kwds).split("[]")

            sink.write(head)
            sink.write("[")

            itr = iter(source)

            # Try the first record.
            try:
                i, first = 0, next(itr)
                first = transformer(first)
                if with_ld_context:
                    first = helpers.id_record(first)
                if indented:
                    sink.write(rec_indent)
                sink.write(
                    json.dumps(first, cls=ObjectEncoder, **dump_kwds).replace(
                        "\n", rec_indent
                    )
                )
            except StopIteration:
                pass
            except Exception as exc:
                # Ignoring errors is *not* the default.
                if ignore_errors:
                    logger.error(
                        "failed to serialize file record %d (%s), " "continuing", i, exc
                    )
                else:
                    # Log error and close up the GeoJSON, leaving it
                    # more or less valid no matter what happens above.
                    logger.critical(
                        "failed to serialize file record %d (%s), " "quitting", i, exc
                    )
                    sink.write("]")
                    sink.write(tail)
                    if indented:
                        sink.write("\n")
                    raise

            # Because trailing commas aren't valid in JSON arrays
            # we'll write the item separator before each of the
            # remaining features.
            for i, rec in enumerate(itr, 1):
                rec = transformer(rec)
                try:
                    if with_ld_context:
                        rec = helpers.id_record(rec)
                    if indented:
                        sink.write(rec_indent)
                    sink.write(item_sep)
                    sink.write(
                        json.dumps(rec, cls=ObjectEncoder, **dump_kwds).replace(
                            "\n", rec_indent
                        )
                    )
                except Exception as exc:
                    if ignore_errors:
                        logger.error(
                            "failed to serialize file record %d (%s), "
                            "continuing",
                            i, exc)
                    else:
                        logger.critical(
                            "failed to serialize file record %d (%s), "
                            "quitting",
                            i, exc)
                        sink.write("]")
                        sink.write(tail)
                        if indented:
                            sink.write("\n")
                        raise

            # Close up the GeoJSON after writing all features.
            sink.write("]")
            sink.write(tail)
            if indented:
                sink.write("\n")

        else:
            # Buffer GeoJSON data at the collection level. The default.
            collection = {
                "type": "FeatureCollection",
                "fiona:schema": meta["schema"],
                "fiona:crs": meta["crs"].to_string(),
            }
            if with_ld_context:
                collection["@context"] = helpers.make_ld_context(add_ld_context_item)
                collection["features"] = [
                    helpers.id_record(transformer(rec)) for rec in source
                ]
            else:
                collection["features"] = [
                    transformer(source.crs, rec) for rec in source
                ]
            json.dump(collection, sink, cls=ObjectEncoder, **dump_kwds)
