"""Manage overviews of a dataset."""

from functools import reduce
import operator

import click

from . import options
import rasterio
from rasterio.enums import OverviewResampling


def build_handler(ctx, param, value):
    if value:
        try:
            if '^' in value:
                base, exp_range = value.split('^')
                exp_min, exp_max = (int(v) for v in exp_range.split('..'))
                value = [pow(int(base), k) for k in range(exp_min, exp_max + 1)]
            elif ',' in value:
                value = [int(v) for v in value.split(',')]
            elif value == "auto":
                pass
            else:
                raise Exception
        except Exception:
            raise click.BadParameter("must match 'n,n,n,…', 'n^n..n', or 'auto'.")
    return value


def get_maximum_overview_level(width, height, minsize=256):
    """
    Calculate the maximum overview level of a dataset at which
    the smallest overview is smaller than `minsize`.

    Attributes
    ----------
    width : int
        Width of the dataset.
    height : int
        Height of the dataset.
    minsize : int (default: 256)
        Minimum overview size.

    Returns
    -------
    overview_level: int
        overview level.

    """
    overview_level = 0
    overview_factor = 1
    while min(width // overview_factor, height // overview_factor) > minsize:
        overview_factor *= 2
        overview_level += 1

    return overview_level


@click.command('overview', short_help="Construct overviews in an existing dataset.")
@options.file_in_arg
@click.option(
    "--build",
    callback=build_handler,
    metavar="f1,f2,…|b^min..max|auto",
    help="A sequence of decimation factors specified as "
    "comma-separated list of numbers or a base and range of "
    "exponents, or 'auto' to automatically determine the maximum factor.",
)
@click.option(
    "--ls", help="Print the overviews for each band.", is_flag=True, default=False
)
@click.option(
    "--rebuild", help="Reconstruct existing overviews.", is_flag=True, default=False
)
@click.option(
    "--resampling",
    help="Resampling algorithm.",
    type=click.Choice([it.name for it in OverviewResampling]),
    default="nearest",
    show_default=True,
)
@click.pass_context
def overview(ctx, input, build, ls, rebuild, resampling):
    """Construct overviews in an existing dataset.

    A pyramid of overviews computed once and stored in the dataset can
    improve performance in some applications.

    The decimation levels at which to build overviews can be specified as
    a comma separated list

      rio overview --build 2,4,8,16

    or a base and range of exponents

      rio overview --build 2^1..4

    or 'auto' to automatically determine the maximum decimation level at
    which the smallest overview is smaller than 256 pixels in size.

      rio overview --build auto

    Note that overviews can not currently be removed and are not
    automatically updated when the dataset's primary bands are
    modified.

    Information about existing overviews can be printed using the --ls
    option.

      rio overview --ls

    """
    with ctx.obj['env']:
        if ls:
            with rasterio.open(input, 'r') as dst:
                resampling_method = dst.tags(
                    ns='rio_overview').get('resampling') or 'unknown'

                click.echo("Overview factors:")
                for idx in dst.indexes:
                    click.echo("  Band %d: %s (method: '%s')" % (
                        idx, dst.overviews(idx) or 'None', resampling_method))
        elif rebuild:
            with rasterio.open(input, 'r+') as dst:
                # Build the same overviews for all bands.
                factors = reduce(
                    operator.or_,
                    [set(dst.overviews(i)) for i in dst.indexes])

                # Attempt to recover the resampling method from dataset tags.
                resampling_method = dst.tags(
                    ns='rio_overview').get('resampling') or resampling

                dst.build_overviews(
                    list(factors), OverviewResampling[resampling_method])

        elif build:
            with rasterio.open(input, 'r+') as dst:
                if build == "auto":
                    overview_level = get_maximum_overview_level(dst.width, dst.height)
                    build = [2 ** j for j in range(1, overview_level + 1)]
                dst.build_overviews(build, OverviewResampling[resampling])

                # Save the resampling method to a tag.
                dst.update_tags(ns='rio_overview', resampling=resampling)

        else:
            raise click.UsageError(
                "Please specify --ls, --rebuild, or --build ...")
