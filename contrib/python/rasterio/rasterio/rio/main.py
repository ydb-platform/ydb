"""
Main command group for Rasterio's CLI.

Subcommands developed as a part of the Rasterio package have their own
modules under ``rasterio.rio`` (like ``rasterio/rio/info.py``) and are
registered in the 'rasterio.rio_commands' entry point group in
Rasterio's ``setup.py``:

    entry_points='''
        [console_scripts]
        rio=rasterio.rio.main:main_group

        [rasterio.rio_commands]
        bounds=rasterio.rio.bounds:bounds
        calc=rasterio.rio.calc:calc
        ...

Users may create their own ``rio`` subcommands by writing modules that
register entry points in Rasterio's 'rasterio.rio_plugins' group. See
for example https://github.com/sgillies/rio-plugin-example, which has
been published to PyPI as ``rio-metasay``.

There's no advantage to making a ``rio`` subcommand which doesn't
import rasterio. But if you are using rasterio, you may profit from
Rasterio's CLI infrastructure and the network of existing commands.
Please add yours to the registry

  https://github.com/rasterio/rasterio/wiki/Rio-plugin-registry

so that other ``rio`` users may find it.
"""

import itertools
import logging
import sys
from importlib.metadata import entry_points

import click
import cligj

import rasterio
from rasterio.session import AWSSession
from rasterio._vendor.click_plugins import with_plugins


def configure_logging(verbosity):
    log_level = max(10, 30 - 10 * verbosity)
    logging.basicConfig(stream=sys.stderr, level=log_level)


def gdal_version_cb(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return

    click.echo(f"{rasterio.__gdal_version__}", color=ctx.color)
    ctx.exit()

def show_versions_cb(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return

    rasterio.show_versions()
    ctx.exit()


@with_plugins(
    itertools.chain(
        entry_points(group="rasterio.rio_commands"),
        entry_points(group="rasterio.rio_plugins"),
    )
)
@click.group()
@cligj.verbose_opt
@cligj.quiet_opt
@click.option(
    "--aws-profile", help="Select a profile from the AWS credentials file"
)
@click.option("--aws-no-sign-requests", is_flag=True, help="Make requests anonymously")
@click.option(
    "--aws-requester-pays", is_flag=True, help="Requester pays data transfer costs"
)
@click.version_option(version=rasterio.__version__, message="%(version)s")
@click.option("--gdal-version", is_eager=True, is_flag=True, callback=gdal_version_cb)
@click.option("--show-versions", help="Show dependency versions", is_eager=True, is_flag=True, callback=show_versions_cb)
@click.pass_context
def main_group(
    ctx,
    verbose,
    quiet,
    aws_profile,
    aws_no_sign_requests,
    aws_requester_pays,
    gdal_version,
    show_versions,
):
    """Rasterio command line interface.
    """
    verbosity = verbose - quiet
    configure_logging(verbosity)
    ctx.obj = {}
    ctx.obj["verbosity"] = verbosity
    ctx.obj["aws_profile"] = aws_profile
    envopts = {"CPL_DEBUG": (verbosity > 2)}
    if aws_profile or aws_no_sign_requests or aws_requester_pays:
        ctx.obj["env"] = rasterio.Env(
            session=AWSSession(
                profile_name=aws_profile,
                aws_unsigned=aws_no_sign_requests,
                requester_pays=aws_requester_pays,
            ), **envopts)
    else:
        ctx.obj["env"] = rasterio.Env(**envopts)
