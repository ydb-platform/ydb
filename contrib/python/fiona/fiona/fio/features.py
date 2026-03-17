"""Fiona CLI commands."""

from collections import defaultdict
from copy import copy
import itertools
import json
import logging
import warnings

import click
from cligj import use_rs_opt  # type: ignore

from fiona.features import map_feature, reduce_features
from fiona.fio import with_context_env
from fiona.fio.helpers import obj_gen, eval_feature_expression  # type: ignore

log = logging.getLogger(__name__)


@click.command(
    "map",
    short_help="Map a pipeline expression over GeoJSON features.",
)
@click.argument("pipeline")
@click.option(
    "--raw",
    "-r",
    is_flag=True,
    default=False,
    help="Print raw result, do not wrap in a GeoJSON Feature.",
)
@click.option(
    "--no-input",
    "-n",
    is_flag=True,
    default=False,
    help="Do not read input from stream.",
)
@click.option(
    "--dump-parts",
    is_flag=True,
    default=False,
    help="Dump parts of geometries to create new inputs before evaluating pipeline.",
)
@use_rs_opt
def map_cmd(pipeline, raw, no_input, dump_parts, use_rs):
    """Map a pipeline expression over GeoJSON features.

    Given a sequence of GeoJSON features (RS-delimited or not) on stdin
    this prints copies with geometries that are transformed using a
    provided transformation pipeline. In "raw" output mode, this
    command prints pipeline results without wrapping them in a feature
    object.

    The pipeline is a string that, when evaluated by fio-map, produces
    a new geometry object. The pipeline consists of expressions in the
    form of parenthesized lists that may contain other expressions.
    The first item in a list is the name of a function or method, or an
    expression that evaluates to a function. The second item is the
    function's first argument or the object to which the method is
    bound. The remaining list items are the positional and keyword
    arguments for the named function or method. The names of the input
    feature and its geometry in the context of these expressions are
    "f" and "g".

    For example, this pipeline expression

        '(simplify (buffer g 100.0) 5.0)'

    buffers input geometries and then simplifies them so that no
    vertices are closer than 5 units. Keyword arguments for the shapely
    methods are supported. A keyword argument is preceded by ':' and
    followed immediately by its value. For example:

        '(simplify g 5.0 :preserve_topology true)'

    and

        '(buffer g 100.0 :resolution 8 :join_style 1)'

    Numerical and string arguments may be replaced by expressions. The
    buffer distance could be a function of a geometry's area.

        '(buffer g (/ (area g) 100.0))'

    """
    if no_input:
        features = [None]
    else:
        stdin = click.get_text_stream("stdin")
        features = obj_gen(stdin)

    for feat in features:
        for i, value in enumerate(map_feature(pipeline, feat, dump_parts=dump_parts)):
            if use_rs:
                click.echo("\x1e", nl=False)
            if raw:
                click.echo(json.dumps(value))
            else:
                new_feat = copy(feat)
                new_feat["id"] = f"{feat.get('id', '0')}:{i}"
                new_feat["geometry"] = value
                click.echo(json.dumps(new_feat))


@click.command(
    "filter",
    short_help="Evaluate pipeline expressions to filter GeoJSON features.",
)
@click.argument("pipeline")
@use_rs_opt
@click.option(
    "--snuggs-only",
    "-s",
    is_flag=True,
    default=False,
    help="Strictly require snuggs style expressions and skip check for type of expression.",
)
@click.pass_context
@with_context_env
def filter_cmd(ctx, pipeline, use_rs, snuggs_only):
    """Evaluate pipeline expressions to filter GeoJSON features.

    The pipeline is a string that, when evaluated, gives a new value for
    each input feature. If the value evaluates to True, the feature
    passes through the filter. Otherwise the feature does not pass.

    The pipeline consists of expressions in the form of parenthesized
    lists that may contain other expressions. The first item in a list
    is the name of a function or method, or an expression that evaluates
    to a function. The second item is the function's first argument or
    the object to which the method is bound. The remaining list items
    are the positional and keyword arguments for the named function or
    method. The names of the input feature and its geometry in the
    context of these expressions are "f" and "g".

    For example, this pipeline expression

        '(< (distance g (Point 4 43)) 1)'

    lets through all features that are less than one unit from the given
    point and filters out all other features.

    *New in version 1.10*: these parenthesized list expressions.

    The older style Python expressions like

        'f.properties.area > 1000.0'

    are deprecated and will not be supported in version 2.0.

    """
    stdin = click.get_text_stream("stdin")
    features = obj_gen(stdin)

    if not snuggs_only:
        try:
            from pyparsing.exceptions import ParseException
            from fiona._vendor.snuggs import ExpressionError, expr

            if not pipeline.startswith("("):
                test_string = f"({pipeline})"
            expr.parseString(test_string)
        except ExpressionError:
            # It's a snuggs expression.
            log.info("Detected a snuggs expression.")
            pass
        except ParseException:
            # It's likely an old-style Python expression.
            log.info("Detected a legacy Python expression.")
            warnings.warn(
                "This style of filter expression is deprecated. "
                "Version 2.0 will only support the new parenthesized list expressions.",
                FutureWarning,
            )
            for i, obj in enumerate(features):
                feats = obj.get("features") or [obj]
                for j, feat in enumerate(feats):
                    if not eval_feature_expression(feat, pipeline):
                        continue
                    if use_rs:
                        click.echo("\x1e", nl=False)
                    click.echo(json.dumps(feat))
            return

    for feat in features:
        for value in map_feature(pipeline, feat):
            if value:
                if use_rs:
                    click.echo("\x1e", nl=False)
                click.echo(json.dumps(feat))


@click.command("reduce", short_help="Reduce a stream of GeoJSON features to one value.")
@click.argument("pipeline")
@click.option(
    "--raw",
    "-r",
    is_flag=True,
    default=False,
    help="Print raw result, do not wrap in a GeoJSON Feature.",
)
@use_rs_opt
@click.option(
    "--zip-properties",
    is_flag=True,
    default=False,
    help="Zip the items of input feature properties together for output.",
)
def reduce_cmd(pipeline, raw, use_rs, zip_properties):
    """Reduce a stream of GeoJSON features to one value.

    Given a sequence of GeoJSON features (RS-delimited or not) on stdin
    this prints a single value using a provided transformation pipeline.

    The pipeline is a string that, when evaluated, produces
    a new geometry object. The pipeline consists of expressions in the
    form of parenthesized lists that may contain other expressions.
    The first item in a list is the name of a function or method, or an
    expression that evaluates to a function. The second item is the
    function's first argument or the object to which the method is
    bound. The remaining list items are the positional and keyword
    arguments for the named function or method. The set of geometries
    of the input features in the context of these expressions is named
    "c".

    For example, the pipeline expression

        '(unary_union c)'

    dissolves the geometries of input features.

    To keep the properties of input features while reducing them to a
    single feature, use the --zip-properties flag. The properties of the
    input features will surface in the output feature as lists
    containing the input values.

    """
    stdin = click.get_text_stream("stdin")
    features = (feat for feat in obj_gen(stdin))

    if zip_properties:
        prop_features, geom_features = itertools.tee(features)
        properties = defaultdict(list)
        for feat in prop_features:
            for key, val in feat["properties"].items():
                properties[key].append(val)
    else:
        geom_features = features
        properties = {}

    for result in reduce_features(pipeline, geom_features):
        if use_rs:
            click.echo("\x1e", nl=False)
        if raw:
            click.echo(json.dumps(result))
        else:
            click.echo(
                json.dumps(
                    {
                        "type": "Feature",
                        "properties": properties,
                        "geometry": result,
                        "id": "0",
                    }
                )
            )
