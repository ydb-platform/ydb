from typing import List, Optional

import click
from pyhanko_certvalidator import ValidationContext

from pyhanko import __version__
from pyhanko.cli._root import cli_root
from pyhanko.cli._trust import (
    _get_key_usage_settings,
    build_vc_kwargs,
    trust_options,
)
from pyhanko.cli.commands.signing.plugin import command_from_plugin
from pyhanko.cli.commands.stamp import select_style
from pyhanko.cli.utils import parse_field_location_spec
from pyhanko.pdf_utils.incremental_writer import IncrementalPdfFileWriter
from pyhanko.sign import DEFAULT_SIGNER_KEY_USAGE, fields, signers
from pyhanko.sign.signers.pdf_byterange import BuildProps
from pyhanko.sign.timestamps import HTTPTimeStamper

from ..._ctx import CLIContext
from ...plugin_api import SigningCommandPlugin

__all__ = ['signing', 'addsig', 'register']

from ...runtime import pyhanko_exception_manager


@cli_root.group(help='sign PDFs and other files', name='sign')
def signing():
    pass


@trust_options
@signing.group(name='addsig', help='add a signature')
@click.option(
    '--field',
    help=(
        'signature field name, or field specification PAGE/X1,Y1,X2,Y2/NAME '
        '(required unless the field contains exactly one signature field)'
    ),
    required=False,
)
@click.option('--name', help='explicitly specify signer name', required=False)
@click.option('--reason', help='reason for signing', required=False)
@click.option('--location', help='location of signing', required=False)
@click.option('--contact-info', help='contact of the signer', required=False)
@click.option(
    '--certify',
    help='add certification signature',
    required=False,
    default=False,
    is_flag=True,
    type=bool,
    show_default=True,
)
@click.option(
    '--existing-only',
    help='never create signature fields',
    required=False,
    default=False,
    is_flag=True,
    type=bool,
    show_default=True,
)
@click.option(
    '--timestamp-url',
    help='URL for timestamp server',
    required=False,
    type=str,
    default=None,
)
@click.option(
    '--use-pades',
    help='sign PAdES-style [level B/B-T/B-LT/B-LTA]',
    required=False,
    default=False,
    is_flag=True,
    type=bool,
    show_default=True,
)
@click.option(
    '--use-pades-lta',
    help='produce PAdES-B-LTA signature',
    required=False,
    default=False,
    is_flag=True,
    type=bool,
    show_default=True,
)
@click.option(
    '--prefer-pss',
    is_flag=True,
    default=False,
    type=bool,
    help='prefer RSASSA-PSS to PKCS#1 v1.5 padding, if available',
)
@click.option(
    '--with-validation-info',
    help='embed revocation info',
    required=False,
    default=False,
    is_flag=True,
    type=bool,
    show_default=True,
)
@click.option(
    '--style-name',
    help='stamp style name for signature appearance',
    required=False,
    type=str,
)
@click.option(
    '--stamp-url',
    help='QR code URL to use in QR stamp style',
    required=False,
    type=str,
)
@click.option(
    '--detach',
    type=bool,
    is_flag=True,
    default=False,
    help=(
        'write only the signature CMS object to the output file; '
        'this can be used to sign non-PDF files'
    ),
)
@click.option(
    '--detach-pem',
    help='output PEM data instead of DER when using --detach',
    type=bool,
    is_flag=True,
    default=False,
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
@click.option(
    '--no-strict-syntax',
    help='Attempt to ignore syntactical problems in the input file '
    'and enable signature creation in hybrid-reference files.'
    '(warning: such documents may behave in unexpected ways)',
    type=bool,
    is_flag=True,
    default=False,
    show_default=True,
)
@click.pass_context
def addsig(
    ctx: click.Context,
    field,
    name,
    reason,
    contact_info,
    location,
    certify,
    existing_only,
    timestamp_url,
    use_pades,
    use_pades_lta,
    with_validation_info,
    validation_context,
    trust_replace,
    trust,
    other_certs,
    style_name,
    stamp_url,
    prefer_pss,
    retroactive_revinfo,
    detach,
    detach_pem,
    no_strict_syntax,
):
    ctx_obj: CLIContext = ctx.obj
    ctx_obj.existing_fields_only = existing_only or field is None
    ctx_obj.timestamp_url = timestamp_url
    ctx_obj.prefer_pss = prefer_pss

    if detach or detach_pem:
        ctx_obj.detach_pem = detach_pem
        ctx_obj.sig_settings = None
        if field:
            raise click.ClickException(
                "--field is not compatible with --detach or --detach-pem"
            )
        return  # everything else doesn't apply

    if use_pades_lta:
        use_pades = with_validation_info = True
        if not timestamp_url:
            raise click.ClickException(
                "--timestamp-url is required for --use-pades-lta"
            )
    if use_pades:
        subfilter = fields.SigSeedSubFilter.PADES
    else:
        subfilter = fields.SigSeedSubFilter.ADOBE_PKCS7_DETACHED

    key_usage = DEFAULT_SIGNER_KEY_USAGE
    if with_validation_info:
        vc_kwargs = build_vc_kwargs(
            ctx.obj.config,
            validation_context,
            trust,
            trust_replace,
            other_certs,
            retroactive_revinfo,
            allow_fetching=True,
        )
        vc = ValidationContext(**vc_kwargs)
        key_usage_sett = _get_key_usage_settings(ctx, validation_context)
        if key_usage_sett is not None and key_usage_sett.key_usage is not None:
            key_usage = key_usage_sett.key_usage
    else:
        vc = None
    field_name: Optional[str]
    if field:
        field_name, new_field_spec = parse_field_location_spec(
            field, require_full_spec=False
        )
    else:
        field_name = new_field_spec = None
    if new_field_spec and existing_only:
        raise click.ClickException(
            "Specifying field coordinates is incompatible with --existing-only"
        )
    ctx_obj.sig_settings = signers.PdfSignatureMetadata(
        field_name=field_name,
        location=location,
        reason=reason,
        contact_info=contact_info,
        name=name,
        certify=certify,
        subfilter=subfilter,
        embed_validation_info=with_validation_info,
        validation_context=vc,
        signer_key_usage=key_usage,
        use_pades_lta=use_pades_lta,
        app_build_props=BuildProps(name='pyHanko CLI', revision=__version__),
    )
    ctx_obj.new_field_spec = new_field_spec
    ctx_obj.stamp_style = select_style(ctx, style_name, stamp_url)
    ctx_obj.stamp_url = stamp_url
    ctx_obj.lenient = no_strict_syntax
    ctx_obj.ux.visible_signature_desired = bool(style_name or new_field_spec)


def register(plugins: List[SigningCommandPlugin]):
    # we reset the command list before (re)populating it, in order to
    # make the tests more consistent
    addsig.commands = {}
    for signer_plugin in plugins:
        if signer_plugin.is_available():
            addsig.add_command(command_from_plugin(signer_plugin))
        else:

            def _unavailable():
                raise click.ClickException(
                    signer_plugin.unavailable_message
                    or "This subcommand is not available"
                )

            addsig.add_command(
                click.Command(
                    name=signer_plugin.subcommand_name,
                    help=signer_plugin.help_summary + " [unavailable]",
                    callback=_unavailable,
                )
            )


readable_file = click.Path(exists=True, readable=True, dir_okay=False)
writable_file = click.Path(writable=True, dir_okay=False)


@trust_options
@signing.command(name='timestamp', help='add timestamp to PDF')
@click.argument('infile', type=readable_file)
@click.argument('outfile', type=writable_file)
@click.option(
    '--timestamp-url',
    help='URL for timestamp server',
    required=True,
    type=str,
    default=None,
)
@click.pass_context
def timestamp(
    ctx,
    infile,
    outfile,
    validation_context,
    trust,
    trust_replace,
    other_certs,
    timestamp_url,
):
    with pyhanko_exception_manager():
        vc_kwargs = build_vc_kwargs(
            ctx.obj.config,
            validation_context,
            trust,
            trust_replace,
            other_certs,
            retroactive_revinfo=True,
        )
        timestamper = HTTPTimeStamper(timestamp_url)
        with open(infile, 'rb') as inf:
            w = IncrementalPdfFileWriter(inf)
            pdf_timestamper = signers.PdfTimeStamper(timestamper)
            with open(outfile, 'wb') as outf:
                pdf_timestamper.timestamp_pdf(
                    w,
                    'sha256',
                    validation_context=ValidationContext(**vc_kwargs),
                    output=outf,
                )
