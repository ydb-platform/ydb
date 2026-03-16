import click

from pyhanko.cli.commands.signing import signing
from pyhanko.cli.runtime import pyhanko_exception_manager
from pyhanko.cli.utils import parse_field_location_spec
from pyhanko.pdf_utils.incremental_writer import IncrementalPdfFileWriter
from pyhanko.pdf_utils.reader import PdfFileReader
from pyhanko.pdf_utils.writer import copy_into_new_writer
from pyhanko.sign import fields

__all__ = ['list_sigfields', 'add_sig_field']


@signing.command(name='list', help='list signature fields')
@click.argument('infile', type=click.File('rb'))
@click.option(
    '--skip-status',
    help='do not print status',
    required=False,
    type=bool,
    is_flag=True,
    default=False,
    show_default=True,
)
def list_sigfields(infile, skip_status):
    with pyhanko_exception_manager():
        r = PdfFileReader(infile, strict=False)
        field_info = fields.enumerate_sig_fields(r)
        for ix, (name, value, field_ref) in enumerate(field_info):
            if skip_status:
                click.echo(name)
                continue
            click.echo(f"{name}:{'EMPTY' if value is None else 'FILLED'}")


@signing.command(
    name='addfields', help='add empty signature fields to a PDF field'
)
@click.argument('infile', type=click.File('rb'))
@click.argument('outfile', type=click.File('wb'))
@click.option(
    '--field',
    metavar='PAGE/X1,Y1,X2,Y2/NAME',
    multiple=True,
    required=True,
    help="Field specification (multiple allowed)",
)
@click.option(
    '--resave',
    is_flag=True,
    help="Resave the PDF document instead of creating an incremental update",
)
def add_sig_field(infile, outfile, field, resave):
    with pyhanko_exception_manager():
        if resave:
            writer = copy_into_new_writer(PdfFileReader(infile, strict=False))
        else:
            writer = IncrementalPdfFileWriter(infile, strict=False)

        for s in field:
            name, spec = parse_field_location_spec(s)
            assert spec is not None
            fields.append_signature_field(writer, spec)

        writer.write(outfile)
        infile.close()
        outfile.close()
