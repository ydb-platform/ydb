import click
from pyhanko_certvalidator import ValidationContext

from pyhanko.cli._trust import build_vc_kwargs, trust_options
from pyhanko.cli.commands.signing import signing
from pyhanko.cli.runtime import pyhanko_exception_manager
from pyhanko.pdf_utils.incremental_writer import IncrementalPdfFileWriter
from pyhanko.pdf_utils.reader import PdfFileReader
from pyhanko.sign import signers, validation
from pyhanko.sign.timestamps import HTTPTimeStamper

__all__ = ['ltv_fix', 'lta_update']


@trust_options
@signing.command(name='ltaupdate', help='update LTA timestamp')
@click.argument('infile', type=click.File('r+b'))
@click.option(
    '--timestamp-url',
    help='URL for timestamp server',
    required=True,
    type=str,
    default=None,
)
@click.option(
    '--retroactive-revinfo',
    help='Treat revocation info as retroactively valid '
    '(i.e. ignore thisUpdate timestamp)',
    type=bool,
    is_flag=True,
    default=False,
    show_default=True,
)
@click.pass_context
def lta_update(
    ctx,
    infile,
    validation_context,
    trust,
    trust_replace,
    other_certs,
    timestamp_url,
    retroactive_revinfo,
):
    with pyhanko_exception_manager():
        vc_kwargs = build_vc_kwargs(
            ctx.obj.config,
            validation_context,
            trust,
            trust_replace,
            other_certs,
            retroactive_revinfo,
        )
        timestamper = HTTPTimeStamper(timestamp_url)
        r = PdfFileReader(infile)
        signers.PdfTimeStamper(timestamper).update_archival_timestamp_chain(
            r, ValidationContext(**vc_kwargs)
        )


# TODO perhaps add an option here to fix the lack of a timestamp and/or
#  warn if none is present


@trust_options
@signing.command(
    name='ltvfix', help='add revocation information for a signature to the DSS'
)
@click.argument('infile', type=click.File('r+b'))
@click.option('--field', help='name of the signature field', required=True)
@click.option(
    '--timestamp-url',
    help='URL for timestamp server',
    required=False,
    type=str,
    default=None,
)
@click.option(
    '--apply-lta-timestamp',
    help='Apply a document timestamp after adding revocation info.',
    required=False,
    type=bool,
    default=False,
    is_flag=True,
    show_default=True,
)
@click.pass_context
def ltv_fix(
    ctx,
    infile,
    field,
    timestamp_url,
    apply_lta_timestamp,
    validation_context,
    trust_replace,
    trust,
    other_certs,
):
    if apply_lta_timestamp and not timestamp_url:
        raise click.ClickException(
            "Please specify a timestamp server using --timestamp-url."
        )

    vc_kwargs = build_vc_kwargs(
        ctx.obj.config,
        validation_context,
        trust,
        trust_replace,
        other_certs,
        retroactive_revinfo=False,
        allow_fetching=True,
    )
    vc_kwargs['revocation_mode'] = 'hard-fail'
    r = PdfFileReader(infile)

    try:
        emb_sig = next(
            s for s in r.embedded_regular_signatures if s.field_name == field
        )
    except StopIteration:
        raise click.ClickException(
            f"Could not find a PDF signature labelled {field}."
        )

    output = validation.add_validation_info(
        emb_sig, ValidationContext(**vc_kwargs), in_place=True
    )

    if apply_lta_timestamp:
        timestamper = HTTPTimeStamper(timestamp_url)
        signers.PdfTimeStamper(timestamper).timestamp_pdf(
            IncrementalPdfFileWriter(output),
            signers.DEFAULT_MD,
            ValidationContext(**vc_kwargs),
            in_place=True,
        )
