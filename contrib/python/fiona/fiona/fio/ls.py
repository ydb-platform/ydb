"""$ fiona ls"""


import json

import click
from cligj import indent_opt

import fiona
from fiona.fio import options, with_context_env


@click.command()
@click.argument('input', required=True)
@indent_opt
@options.open_opt
@click.pass_context
@with_context_env
def ls(ctx, input, indent, open_options):
    """
    List layers in a datasource.
    """
    result = fiona.listlayers(input, **open_options)
    click.echo(json.dumps(result, indent=indent))
