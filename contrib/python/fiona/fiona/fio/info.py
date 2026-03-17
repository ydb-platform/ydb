"""$ fio info"""


import logging
import json

import click
from cligj import indent_opt

import fiona
import fiona.crs
from fiona.errors import DriverError
from fiona.fio import options, with_context_env

logger = logging.getLogger(__name__)


@click.command()
# One or more files.
@click.argument('input', required=True)
@click.option('--layer', metavar="INDEX|NAME", callback=options.cb_layer,
              help="Print information about a specific layer.  The first "
                   "layer is used by default.  Layers use zero-based "
                   "numbering when accessed by index.")
@indent_opt
# Options to pick out a single metadata item and print it as
# a string.
@click.option('--count', 'meta_member', flag_value='count',
              help="Print the count of features.")
@click.option('-f', '--format', '--driver', 'meta_member', flag_value='driver',
              help="Print the format driver.")
@click.option('--crs', 'meta_member', flag_value='crs',
              help="Print the CRS as a PROJ.4 string.")
@click.option('--bounds', 'meta_member', flag_value='bounds',
              help="Print the boundary coordinates "
                   "(left, bottom, right, top).")
@click.option('--name', 'meta_member', flag_value='name',
              help="Print the datasource's name.")
@options.open_opt
@click.pass_context
@with_context_env
def info(ctx, input, indent, meta_member, layer, open_options):
    """
    Print information about a dataset.

    When working with a multi-layer dataset the first layer is used by default.
    Use the '--layer' option to select a different layer.

    """
    with fiona.open(input, layer=layer, **open_options) as src:
        info = src.meta
        info.update(name=src.name)

        try:
            info.update(bounds=src.bounds)
        except DriverError:
            info.update(bounds=None)
            logger.debug(
                "Setting 'bounds' to None - driver was not able to calculate bounds"
            )

        try:
            info.update(count=len(src))
        except TypeError:
            info.update(count=None)
            logger.debug(
                "Setting 'count' to None/null - layer does not support counting"
            )

        info["crs"] = src.crs.to_string()

        if meta_member:
            if isinstance(info[meta_member], (list, tuple)):
                click.echo(" ".join(map(str, info[meta_member])))
            else:
                click.echo(info[meta_member])
        else:
            click.echo(json.dumps(info, indent=indent))
