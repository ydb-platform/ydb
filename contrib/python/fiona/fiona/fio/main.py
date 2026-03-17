"""
Main click group for the CLI.  Needs to be isolated for entry-point loading.
"""


import itertools
import logging
import sys

import click
from click_plugins import with_plugins
from cligj import verbose_opt, quiet_opt

if sys.version_info < (3, 10):
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points

import fiona
from fiona import __version__ as fio_version
from fiona.session import AWSSession, DummySession
from fiona.fio.bounds import bounds
from fiona.fio.calc import calc
from fiona.fio.cat import cat
from fiona.fio.collect import collect
from fiona.fio.distrib import distrib
from fiona.fio.dump import dump
from fiona.fio.env import env
from fiona.fio.info import info
from fiona.fio.insp import insp
from fiona.fio.load import load
from fiona.fio.ls import ls
from fiona.fio.rm import rm

# The "calc" extras require pyparsing and shapely.
try:
    import pyparsing
    import shapely
    from fiona.fio.features import filter_cmd, map_cmd, reduce_cmd

    supports_calc = True
except ImportError:
    supports_calc = False


def configure_logging(verbosity):
    log_level = max(10, 30 - 10 * verbosity)
    logging.basicConfig(stream=sys.stderr, level=log_level)


@with_plugins(
    itertools.chain(
        entry_points(group="fiona.fio_plugins"),
    )
)
@click.group()
@verbose_opt
@quiet_opt
@click.option(
    "--aws-profile",
    help="Select a profile from the AWS credentials file")
@click.option(
    "--aws-no-sign-requests",
    is_flag=True,
    help="Make requests anonymously")
@click.option(
    "--aws-requester-pays",
    is_flag=True,
    help="Requester pays data transfer costs")
@click.version_option(fio_version)
@click.version_option(fiona.__gdal_version__, '--gdal-version',
                      prog_name='GDAL')
@click.version_option(sys.version, '--python-version', prog_name='Python')
@click.pass_context
def main_group(
        ctx, verbose, quiet, aws_profile, aws_no_sign_requests,
        aws_requester_pays):
    """Fiona command line interface.
    """
    verbosity = verbose - quiet
    configure_logging(verbosity)
    ctx.obj = {}
    ctx.obj["verbosity"] = verbosity
    ctx.obj["aws_profile"] = aws_profile
    envopts = {"CPL_DEBUG": (verbosity > 2)}
    if aws_profile or aws_no_sign_requests:
        session = AWSSession(
            profile_name=aws_profile,
            aws_unsigned=aws_no_sign_requests,
            requester_pays=aws_requester_pays,
        )
    else:
        session = DummySession()
    ctx.obj["env"] = fiona.Env(session=session, **envopts)


main_group.add_command(bounds)
main_group.add_command(calc)
main_group.add_command(cat)
main_group.add_command(collect)
main_group.add_command(distrib)
main_group.add_command(dump)
main_group.add_command(env)
main_group.add_command(info)
main_group.add_command(insp)
main_group.add_command(load)
main_group.add_command(ls)
main_group.add_command(rm)

if supports_calc:
    main_group.add_command(map_cmd)
    main_group.add_command(filter_cmd)
    main_group.add_command(reduce_cmd)
