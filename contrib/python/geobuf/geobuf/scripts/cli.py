"""
Pygeobuf's command line interface.

`geobuf encode` and `geobuf decode` read and write to the standard IO
streams.

  $ geobuf encode < foo.json > foo.pbf
  $ geobuf decode < foo.pbf > bar.json

"""


import json
import logging
import sys

import click

import geobuf


logging.basicConfig(stream=sys.stderr, level=logging.INFO)


def print_version(ctx, param, value):
    """Print module version and exit."""
    if not value or ctx.resilient_parsing:
        return
    click.echo(geobuf.__version__)
    ctx.exit()


@click.group(help="Pygeobuf command line interface.")
@click.option('--version', is_flag=True, callback=print_version,
              expose_value=False, is_eager=True,
              help="Print Pygeobuf version.")
def cli():
    """Create a context for this command group."""


@cli.command(short_help="Encode a GeoJSON feature collection.")
@click.option('--precision', type=int, default=6,
              help="Decimal encoding precision of coordinates.")
@click.option('--with-z/--without-z', default=False,
              help="Encode Z coordinate values as well as X, Y.")
def encode(precision, with_z):
    """Given GeoJSON on stdin, writes a geobuf file to stdout."""
    logger = logging.getLogger('geobuf')
    stdin = click.get_text_stream('stdin')
    sink = click.get_binary_stream('stdout')
    try:
        data = json.load(stdin)
        pbf = geobuf.encode(
            data,
            precision if precision >= 0 else 6,
            3 if with_z else 2)
        sink.write(pbf)
        sys.exit(0)
    except Exception:
        logger.exception("Failed. Exception caught")
        sys.exit(1)


@cli.command(short_help="Decode a Geobuf byte string.")
def decode():
    """Given a Geobuf byte string on stdin, write a GeoJSON feature
    collection to stdout."""
    logger = logging.getLogger('geobuf')
    stdin = click.get_binary_stream('stdin')
    sink = click.get_text_stream('stdout')
    try:
        pbf = stdin.read()
        data = geobuf.decode(pbf)
        json.dump(data, sink)
        sys.exit(0)
    except Exception:
        logger.exception("Failed. Exception caught")
        sys.exit(1)
