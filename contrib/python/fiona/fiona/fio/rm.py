import click
import logging

import fiona
from fiona.fio import with_context_env


logger = logging.getLogger(__name__)


@click.command(help="Remove a datasource or an individual layer.")
@click.argument("input", required=True)
@click.option("--layer", type=str, default=None, required=False, help="Name of layer to remove.")
@click.option("--yes", is_flag=True)
@click.pass_context
@with_context_env
def rm(ctx, input, layer, yes):
    if layer is None:
        kind = "datasource"
    else:
        kind = "layer"

    if not yes:
        click.confirm(f"The {kind} will be removed. Are you sure?", abort=True)

    try:
        fiona.remove(input, layer=layer)
    except Exception:
        logger.exception(f"Failed to remove {kind}.")
        raise click.Abort()
