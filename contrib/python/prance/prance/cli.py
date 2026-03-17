"""CLI for prance."""

__author__ = "Jens Finkhaeuser"
__copyright__ = "Copyright (c) 2016-2021 Jens Finkhaeuser"
__license__ = "MIT"
__all__ = ()


import click

import prance
from prance.util import default_validation_backend


def __write_to_file(filename, specs):  # noqa: N802
    """
    Write specs to the given filename.

    This takes into account file name extensions as per `fs.write_file`.
    """
    from prance.util import fs, formats

    contents = formats.serialize_spec(specs, filename)
    fs.write_file(filename, contents)


def __parser_for_url(url, resolve, backend, strict, encoding):  # noqa: N802
    """Return a parser instance for the URL and the given parameters."""
    # Try the URL
    formatted = click.format_filename(url)
    click.echo(f'Processing "{formatted}"...')

    from prance.util import fs

    fsurl = fs.abspath(url)
    import os.path

    if os.path.exists(fs.from_posix(fsurl)):
        url = fsurl

    # Create parser to use
    parser = None
    if resolve:
        click.echo(" -> Resolving external references.")
        parser = prance.ResolvingParser(
            url, lazy=True, backend=backend, strict=strict, encoding=encoding
        )
    else:
        click.echo(" -> Not resolving external references.")
        parser = prance.BaseParser(
            url, lazy=True, backend=backend, strict=strict, encoding=encoding
        )

    # XXX maybe enable this in debug mode or something.
    # click.echo(' -> Using backend: {0.backend}'.format(parser))
    return parser, formatted


def __validate(parser, name):  # noqa: N802
    """Validate a spec using this parser."""
    from prance.util.url import ResolutionError
    from prance import ValidationError

    try:
        parser.parse()
    except (ResolutionError, ValidationError) as err:
        msg = f'ERROR in "{name}" [{type(err).__name__}]: {str(err)}'
        click.secho(msg, err=True, fg="red")
        import sys

        sys.exit(1)

    # All good, next file.
    click.echo(f"Validates OK as {parser.version}!")


@click.group()
@click.version_option(version=prance.__version__)
def cli():
    pass  # pragma: no cover


class GroupWithCommandOptions(click.Group):
    """Allow application of options to group with multi command."""

    def add_command(self, cmd, name=None):
        click.Group.add_command(self, cmd, name=name)

        # add the group parameters to the command
        for param in self.params:
            cmd.params.append(param)

        # hook the commands invoke with our own
        cmd.invoke = self.build_command_invoke(cmd.invoke)
        self.invoke_without_command = True

    def build_command_invoke(self, original_invoke):
        def command_invoke(ctx):
            """Insert invocation of group function."""
            # separate the group parameters
            ctx.obj = dict(_params=dict())
            for param in self.params:
                name = param.name
                ctx.obj["_params"][name] = ctx.params[name]
                del ctx.params[name]

            # call the group function with its parameters
            params = ctx.params
            ctx.params = ctx.obj["_params"]
            self.invoke(ctx)
            ctx.params = params

            # now call the original invoke (the command)
            original_invoke(ctx)

        return command_invoke


@click.group(cls=GroupWithCommandOptions)
@click.option(
    "--resolve/--no-resolve",
    default=True,
    help="Resolve external references before validation. The default is to " "do so.",
)
@click.option(
    "--backend",
    default=default_validation_backend(),
    metavar="BACKEND",
    nargs=1,
    help='The validation backend to use. One of "flex", '
    '"swagger-spec-validator" or "openapi-spec-validator". The default'
    "is the best of the installed backends.",
)
@click.option(
    "--strict/--no-strict",
    default=True,
    help="Be strict or lenient in validating specs. Strict validation "
    "rejects non-string spec keys, for example in response codes. "
    'Does not apply to the "flex" backend.',
)
@click.option(
    "--encoding",
    default=None,
    help="If given, override file encoding detection and use the given "
    "encoding for all files. Does not work on remote URLs.",
)
@click.pass_context
def backend_options(ctx, resolve, backend, strict, encoding):
    ctx.obj["resolve"] = resolve
    ctx.obj["backend"] = backend
    ctx.obj["strict"] = strict
    ctx.obj["encoding"] = encoding


@backend_options.command()
@click.option(
    "--output-file",
    "-o",
    type=click.Path(exists=False),
    default=None,
    metavar="FILENAME",
    nargs=1,
    help='[DEPRECATED; see "compile" command] If given, write the '
    "validated specs to this file. Together with the --resolve "
    "option, the output file will be a resolved version of the input "
    "file.",
)
@click.argument(
    "urls",
    type=click.Path(exists=False),
    nargs=-1,
)
@click.pass_context
def validate(ctx, output_file, urls):
    """
    Validate the given spec or specs.

    If the --resolve option is set, references will be resolved before
    validation.

    Note that this merges referenced objects into the main specs. Validation
    backends used by prance cannot validate referenced objects, so resolving
    the references before validation allows for full spec validation.
    """
    # Ensure that when an output file is given, only one input file exists.
    if output_file:
        click.echo(
            "The --output-file parameter is deprecated; use "
            'the "compile" command instead.',
            err=True,
        )
    if output_file and len(urls) > 1:
        raise click.UsageError(
            "If --output-file is given, only one input URL " "is allowed!"
        )

    # Process files
    for url in urls:
        # Create parser to use
        parser, name = __parser_for_url(
            url,
            ctx.obj["resolve"],
            ctx.obj["backend"],
            ctx.obj["strict"],
            ctx.obj["encoding"],
        )

        # Try parsing
        __validate(parser, name)

        # If an output file is given, write the specs to it.
        if output_file:
            __write_to_file(output_file, parser.specification)


@backend_options.command()
@click.argument(
    "url_or_path",
    type=click.Path(exists=False),
    nargs=1,
)
@click.argument(
    "output_file",
    type=click.Path(exists=False),
    nargs=1,
    required=False,
)
@click.pass_context
def compile(ctx, url_or_path, output_file):
    """
    Compile the given spec, resolving references if required.

    Resolves references and uses backends exactly as in the "validate"
    command, but only works on single URLs.

    If an output file name is given, output is written there, otherwise
    it is written to the terminal.
    """
    # Create parser to use
    parser, name = __parser_for_url(
        url_or_path,
        ctx.obj["resolve"],
        ctx.obj["backend"],
        ctx.obj["strict"],
        ctx.obj["encoding"],
    )

    # Try parsing
    __validate(parser, name)

    # Write output
    from prance.util import formats

    contents = formats.serialize_spec(parser.specification, output_file)
    if output_file is None:
        click.echo(contents)
    else:
        from .util import fs

        fs.write_file(output_file, contents)
        click.echo(f'Output written to "{output_file}".')


@cli.command()
@click.argument(
    "url_or_path",
    type=click.Path(exists=False),
    nargs=1,
)
@click.argument(
    "output_file",
    type=click.Path(exists=False),
    nargs=1,
    required=False,
)
def convert(url_or_path, output_file):
    """
    Convert the given spec to OpenAPI 3.x.y.

    The conversion uses the web API provided by mermade.org.uk to perform the
    conversion. As long as that service is kept up-to-date and you have an
    internet connection, conversion should work and should convert to the latest
    version of the specs.
    """
    # Convert call
    from .util import url
    import os

    absurl = url.absurl(url_or_path, os.getcwd())

    from .convert import convert_url

    content, content_type = convert_url(absurl)

    # Write output
    if output_file is None:
        click.echo(content)
    else:
        from .util import fs

        fs.write_file(output_file, content)


cli.add_command(validate)
cli.add_command(compile)
cli.add_command(convert)
