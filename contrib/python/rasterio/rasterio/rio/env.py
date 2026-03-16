"""Fetch and edit raster dataset metadata from the command line."""

import json
import os

import click

from rasterio._env import GDALDataFinder, PROJDataFinder


@click.command(short_help="Print information about the Rasterio environment.")
@click.option(
    "--formats",
    "key",
    flag_value="formats",
    default=True,
    help="Enumerate the available formats.",
)
@click.option(
    "--credentials",
    "key",
    flag_value="credentials",
    default=False,
    help="Print credentials.",
)
@click.option(
    "--gdal-data",
    "key",
    flag_value="gdal_data",
    default=False,
    help="Print GDAL data path.",
)
@click.option(
    "--proj-data",
    "key",
    flag_value="proj_data",
    default=False,
    help="Print PROJ data path.",
)
@click.pass_context
def env(ctx, key):
    """Print information about the Rasterio environment."""
    with ctx.obj["env"] as env:
        if key == "credentials":
            click.echo(json.dumps(env.session.credentials))
        elif key == "gdal_data":
            click.echo(os.environ.get("GDAL_DATA") or GDALDataFinder().search())
        elif key == "proj_data":
            click.echo(
                os.environ.get("PROJ_DATA", os.environ.get("PROJ_LIB"))
                or PROJDataFinder().search()
            )
        else:
            for k, v in sorted(env.drivers().items()):
                click.echo(f"{k}: {v}")
