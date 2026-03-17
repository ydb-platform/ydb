"""fio-collect"""

from functools import partial
import json
import logging

import click
import cligj

from fiona.fio import helpers, options, with_context_env
from fiona.model import Geometry, ObjectEncoder
from fiona.transform import transform_geom


@click.command(short_help="Collect a sequence of features.")
@cligj.precision_opt
@cligj.indent_opt
@cligj.compact_opt
@click.option(
    "--record-buffered/--no-record-buffered",
    default=False,
    help="Economical buffering of writes at record, not collection "
    "(default), level.",
)
@click.option(
    "--ignore-errors/--no-ignore-errors",
    default=False,
    help="log errors but do not stop serialization.",
)
@options.src_crs_opt
@click.option(
    "--with-ld-context/--without-ld-context",
    default=False,
    help="add a JSON-LD context to JSON output.",
)
@click.option(
    "--add-ld-context-item",
    multiple=True,
    help="map a term to a URI and add it to the output's JSON LD " "context.",
)
@click.option(
    "--parse/--no-parse",
    default=True,
    help="load and dump the geojson feature (default is True)",
)
@click.pass_context
@with_context_env
def collect(
    ctx,
    precision,
    indent,
    compact,
    record_buffered,
    ignore_errors,
    src_crs,
    with_ld_context,
    add_ld_context_item,
    parse,
):
    """Make a GeoJSON feature collection from a sequence of GeoJSON
    features and print it."""
    logger = logging.getLogger(__name__)
    stdin = click.get_text_stream("stdin")
    sink = click.get_text_stream("stdout")

    dump_kwds = {"sort_keys": True}
    if indent:
        dump_kwds["indent"] = indent
    if compact:
        dump_kwds["separators"] = (",", ":")
    item_sep = compact and "," or ", "

    if src_crs:
        if not parse:
            raise click.UsageError("Can't specify --src-crs with --no-parse")
        transformer = partial(
            transform_geom,
            src_crs,
            "EPSG:4326",
            antimeridian_cutting=True,
            precision=precision,
        )
    else:

        def transformer(x):
            return x

    first_line = next(stdin)

    # If parsing geojson
    if parse:
        # If input is RS-delimited JSON sequence.
        if first_line.startswith("\x1e"):

            def feature_text_gen():
                buffer = first_line.strip("\x1e")
                for line in stdin:
                    if line.startswith("\x1e"):
                        if buffer:
                            feat = json.loads(buffer)
                            feat["geometry"] = transformer(
                                Geometry.from_dict(**feat["geometry"])
                            )
                            yield json.dumps(feat, cls=ObjectEncoder, **dump_kwds)
                        buffer = line.strip("\x1e")
                    else:
                        buffer += line
                else:
                    feat = json.loads(buffer)
                    feat["geometry"] = transformer(
                        Geometry.from_dict(**feat["geometry"])
                    )
                    yield json.dumps(feat, cls=ObjectEncoder, **dump_kwds)

        else:

            def feature_text_gen():
                feat = json.loads(first_line)
                feat["geometry"] = transformer(Geometry.from_dict(**feat["geometry"]))
                yield json.dumps(feat, cls=ObjectEncoder, **dump_kwds)

                for line in stdin:
                    feat = json.loads(line)
                    feat["geometry"] = transformer(
                        Geometry.from_dict(**feat["geometry"])
                    )
                    yield json.dumps(feat, cls=ObjectEncoder, **dump_kwds)

    # If *not* parsing geojson
    else:
        # If input is RS-delimited JSON sequence.
        if first_line.startswith("\x1e"):

            def feature_text_gen():
                buffer = first_line.strip("\x1e")
                for line in stdin:
                    if line.startswith("\x1e"):
                        if buffer:
                            yield buffer
                        buffer = line.strip("\x1e")
                    else:
                        buffer += line
                else:
                    yield buffer

        else:

            def feature_text_gen():
                yield first_line
                yield from stdin

    source = feature_text_gen()

    if record_buffered:
        # Buffer GeoJSON data at the feature level for smaller
        # memory footprint.
        indented = bool(indent)
        rec_indent = "\n" + " " * (2 * (indent or 0))

        collection = {"type": "FeatureCollection", "features": []}
        if with_ld_context:
            collection["@context"] = helpers.make_ld_context(add_ld_context_item)

        head, tail = json.dumps(collection, cls=ObjectEncoder, **dump_kwds).split("[]")

        sink.write(head)
        sink.write("[")

        # Try the first record.
        try:
            i, first = 0, next(source)
            if with_ld_context:
                first = helpers.id_record(first)
            if indented:
                sink.write(rec_indent)
            sink.write(first.replace("\n", rec_indent))
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
        for i, rec in enumerate(source, 1):
            try:
                if with_ld_context:
                    rec = helpers.id_record(rec)
                if indented:
                    sink.write(rec_indent)
                sink.write(item_sep)
                sink.write(rec.replace("\n", rec_indent))
            except Exception as exc:
                if ignore_errors:
                    logger.error(
                        "failed to serialize file record %d (%s), " "continuing",
                        i,
                        exc,
                    )
                else:
                    logger.critical(
                        "failed to serialize file record %d (%s), " "quitting",
                        i,
                        exc,
                    )
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
        collection = {"type": "FeatureCollection", "features": []}
        if with_ld_context:
            collection["@context"] = helpers.make_ld_context(add_ld_context_item)

        head, tail = json.dumps(collection, cls=ObjectEncoder, **dump_kwds).split("[]")
        sink.write(head)
        sink.write("[")
        sink.write(",".join(source))
        sink.write("]")
        sink.write(tail)
        sink.write("\n")
