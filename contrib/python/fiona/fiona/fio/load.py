"""$ fio load"""

from functools import partial

import click
import cligj

import fiona
from fiona.fio import options, with_context_env
from fiona.model import Feature, Geometry
from fiona.transform import transform_geom


@click.command(short_help="Load GeoJSON to a dataset in another format.")
@click.argument("output", required=True)
@click.option("-f", "--format", "--driver", "driver", help="Output format driver name.")
@options.src_crs_opt
@click.option(
    "--dst-crs",
    "--dst_crs",
    help="Destination CRS.  Defaults to --src-crs when not given.",
)
@cligj.features_in_arg
@click.option(
    "--layer",
    metavar="INDEX|NAME",
    callback=options.cb_layer,
    help="Load features into specified layer.  Layers use "
    "zero-based numbering when accessed by index.",
)
@options.creation_opt
@options.open_opt
@click.option("--append", is_flag=True, help="Open destination layer in append mode.")
@click.pass_context
@with_context_env
def load(
    ctx,
    output,
    driver,
    src_crs,
    dst_crs,
    features,
    layer,
    creation_options,
    open_options,
    append,
):
    """Load features from JSON to a file in another format.

    The input is a GeoJSON feature collection or optionally a sequence of
    GeoJSON feature objects.

    """
    dst_crs = dst_crs or src_crs

    if src_crs and dst_crs and src_crs != dst_crs:
        transformer = partial(
            transform_geom, src_crs, dst_crs, antimeridian_cutting=True
        )
    else:

        def transformer(x):
            return Geometry.from_dict(**x)

    def feature_gen():
        """Convert stream of JSON to features.

        Yields
        ------
        Feature

        """
        try:
            for feat in features:
                feat["geometry"] = transformer(Geometry.from_dict(**feat["geometry"]))
                yield Feature.from_dict(**feat)
        except TypeError:
            raise click.ClickException("Invalid input.")

    source = feature_gen()

    # Use schema of first feature as a template.
    # TODO: schema specified on command line?
    try:
        first = next(source)
    except TypeError:
        raise click.ClickException("Invalid input.")

    # TODO: this inference of a property's type from its value needs some
    # work. It works reliably only for the basic JSON serializable types.
    # The fio-load command does require JSON input but that may change
    # someday.
    schema = {"geometry": first.geometry.type}
    schema["properties"] = {
        k: type(v if v is not None else "").__name__
        for k, v in first.properties.items()
    }

    if append:
        opener = fiona.open(output, "a", layer=layer, **open_options)
    else:
        opener = fiona.open(
            output,
            "w",
            driver=driver,
            crs=dst_crs,
            schema=schema,
            layer=layer,
            **creation_options
        )

    with opener as dst:
        dst.write(first)
        dst.writerecords(source)
