"""$ fio bounds"""

import json

import click
from cligj import precision_opt, use_rs_opt

import fiona
from fiona.fio.helpers import obj_gen
from fiona.fio import with_context_env
from fiona.model import ObjectEncoder


@click.command(short_help="Print the extent of GeoJSON objects")
@precision_opt
@click.option('--explode/--no-explode', default=False,
              help="Explode collections into features (default: no).")
@click.option('--with-id/--without-id', default=False,
              help="Print GeoJSON ids and bounding boxes together "
                   "(default: without).")
@click.option('--with-obj/--without-obj', default=False,
              help="Print GeoJSON objects and bounding boxes together "
                   "(default: without).")
@use_rs_opt
@click.pass_context
@with_context_env
def bounds(ctx, precision, explode, with_id, with_obj, use_rs):
    """Print the bounding boxes of GeoJSON objects read from stdin.

    Optionally explode collections and print the bounds of their
    features.

    To print identifiers for input objects along with their bounds
    as a {id: identifier, bbox: bounds} JSON object, use --with-id.

    To print the input objects themselves along with their bounds
    as GeoJSON object, use --with-obj. This has the effect of updating
    input objects with {id: identifier, bbox: bounds}.

    """
    stdin = click.get_text_stream('stdin')
    source = obj_gen(stdin)

    for i, obj in enumerate(source):
        obj_id = obj.get("id", "collection:" + str(i))
        xs = []
        ys = []
        features = obj.get("features") or [obj]

        for j, feat in enumerate(features):
            feat_id = feat.get("id", "feature:" + str(i))
            w, s, e, n = fiona.bounds(feat)

            if precision > 0:
                w, s, e, n = (round(v, precision) for v in (w, s, e, n))
            if explode:

                if with_id:
                    rec = {"parent": obj_id, "id": feat_id, "bbox": (w, s, e, n)}
                elif with_obj:
                    feat.update(parent=obj_id, bbox=(w, s, e, n))
                    rec = feat
                else:
                    rec = (w, s, e, n)

                if use_rs:
                    click.echo('\x1e', nl=False)

                click.echo(json.dumps(rec, cls=ObjectEncoder))

            else:
                xs.extend([w, e])
                ys.extend([s, n])

        if not explode:
            w, s, e, n = (min(xs), min(ys), max(xs), max(ys))

            if with_id:
                rec = {"id": obj_id, "bbox": (w, s, e, n)}
            elif with_obj:
                obj.update(id=obj_id, bbox=(w, s, e, n))
                rec = obj
            else:
                rec = (w, s, e, n)

            if use_rs:
                click.echo("\x1e", nl=False)

            click.echo(json.dumps(rec, cls=ObjectEncoder))
