import typer

COMMAND = "python -m weasel"
NAME = "weasel"
HELP = """weasel Command-line Interface

DOCS: https://github.com/explosion/weasel
"""

PROJECT_FILE = "project.yml"
PROJECT_LOCK = "project.lock"

# Wrappers for Typer's annotations. Initially created to set defaults and to
# keep the names short, but not needed at the moment.
Arg = typer.Argument
Opt = typer.Option

app = typer.Typer(name=NAME, help=HELP, no_args_is_help=True, add_completion=False)


def _get_parent_command(ctx: typer.Context) -> str:
    parent_command = ""
    ctx_parent = ctx.parent
    while ctx_parent:
        if ctx_parent.info_name:
            parent_command = ctx_parent.info_name + " " + parent_command
            ctx_parent = ctx_parent.parent
        else:
            return COMMAND
    return parent_command.strip()
