import click

from flex.exceptions import ValidationError
from flex.core import load


@click.command()
@click.option(
    '-s', '--source',
    help='Source; a url to a schema or a file path to a schema',
)
def main(source):
    """
    For a given command line supplied argument, negotiate the content, parse
    the schema and then return any issues to stdout or if no schema issues,
    return success exit code.
    """
    if source is None:
        click.echo(
            "You need to supply a file or url to a schema to a swagger schema, for"
            "the validator to work."
        )
        return 1
    try:
        load(source)
        click.echo("Validation passed")
        return 0
    except ValidationError as e:
        raise click.ClickException(str(e))
