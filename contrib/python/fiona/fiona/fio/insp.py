"""$ fio insp"""


import code
import sys

import click

import fiona
from fiona.fio import options, with_context_env


@click.command(short_help="Open a dataset and start an interpreter.")
@click.argument("src_path", required=True)
@click.option(
    "--ipython", "interpreter", flag_value="ipython", help="Use IPython as interpreter."
)
@options.open_opt
@click.pass_context
@with_context_env
def insp(ctx, src_path, interpreter, open_options):
    """Open a collection within an interactive interpreter."""
    banner = (
        "Fiona %s Interactive Inspector (Python %s)\n"
        'Type "src.schema", "next(src)", or "help(src)" '
        "for more information."
        % (fiona.__version__, ".".join(map(str, sys.version_info[:3])))
    )

    with fiona.open(src_path, **open_options) as src:
        scope = locals()
        if not interpreter:
            code.interact(banner, local=scope)
        elif interpreter == "ipython":
            import IPython

            IPython.InteractiveShell.banner1 = banner
            IPython.start_ipython(argv=[], user_ns=scope)
        else:
            raise click.ClickException(
                f"Interpreter {interpreter} is unsupported or missing "
                "dependencies"
            )
