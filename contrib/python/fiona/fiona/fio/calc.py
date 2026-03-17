import json

import click
from cligj import use_rs_opt

from .helpers import obj_gen, eval_feature_expression
from fiona.fio import with_context_env
from fiona.model import ObjectEncoder


@click.command(short_help="Calculate GeoJSON property by Python expression")
@click.argument('property_name')
@click.argument('expression')
@click.option('--overwrite', is_flag=True, default=False,
              help="Overwrite properties, default: False")
@use_rs_opt
@click.pass_context
@with_context_env
def calc(ctx, property_name, expression, overwrite, use_rs):
    """
    Create a new property on GeoJSON features using the specified expression.

    \b
    The expression is evaluated in a restricted namespace containing:
        - sum, pow, min, max and the imported math module
        - shape (optional, imported from shapely.geometry if available)
        - bool, int, str, len, float type conversions
        - f (the feature to be evaluated,
             allows item access via javascript-style dot notation using munch)

    The expression will be evaluated for each feature and its
    return value will be added to the properties
    as the specified property_name. Existing properties will not
    be overwritten by default (an Exception is raised).

    Example

    \b
    $ fio cat data.shp | fio calc sumAB  "f.properties.A + f.properties.B"

    """
    stdin = click.get_text_stream('stdin')
    source = obj_gen(stdin)

    for i, obj in enumerate(source):
        features = obj.get("features") or [obj]

        for j, feat in enumerate(features):

            if not overwrite and property_name in feat["properties"]:
                raise click.UsageError(
                    f"{property_name} already exists in properties; "
                    "rename or use --overwrite"
                )

            feat["properties"][property_name] = eval_feature_expression(
                feat, expression
            )

            if use_rs:
                click.echo("\x1e", nl=False)

            click.echo(json.dumps(feat, cls=ObjectEncoder))
