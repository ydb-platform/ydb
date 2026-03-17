import asyncio
from datetime import datetime
from typing import List, Optional

import click
import tzlocal
from asn1crypto import pem

from pyhanko.cli._ctx import CLIContext
from pyhanko.cli.commands.signing.utils import get_text_params, open_for_signing
from pyhanko.cli.plugin_api import SigningCommandPlugin
from pyhanko.cli.runtime import pyhanko_exception_manager
from pyhanko.cli.utils import readable_file, writable_file
from pyhanko.pdf_utils.rw_common import PdfHandler
from pyhanko.sign import PdfSigner, fields
from pyhanko.sign.signers.pdf_cms import (
    PdfCMSSignedAttributes,
    select_suitable_signing_md,
)
from pyhanko.sign.timestamps import HTTPTimeStamper


def _ensure_field_visible(
    handler: PdfHandler, name: Optional[str], will_create: bool
):
    # verify if the resulting signature will be a visible one
    prefix = "You seem to be trying to create a visible signature"
    try:
        fq_name, _, field_ref = next(
            fields.enumerate_sig_fields(handler, with_name=name)
        )
        sig_annot = fields.get_sig_field_annot(sig_field=field_ref.get_object())
        w, h = fields.annot_width_height(sig_annot)
        if not w or not h:
            raise click.ClickException(
                f"{prefix}, but the field '{fq_name}' in the PDF is not a "
                f"visible one. Please specify another field name if you "
                f"need a visible signature."
            )
    except StopIteration:
        if not name:
            raise click.ClickException(
                f"{prefix}, but the PDF did not contain any signature fields, "
                f"and you did not specify a bounding box. "
                f"Please specify the field as "
                f"--field \"PAGE/X1,Y1,X2,Y2/NAME\" to "
                f"create a visible signature field at the coordinates provided."
            )
        elif not will_create:
            raise click.ClickException(
                f"{prefix}, but the field '{name}' does not exist in the PDF "
                f"file, and you did not specify a bounding box. "
                f"Please specify the field as "
                f"--field \"PAGE/X1,Y1,X2,Y2/{name}\" to "
                f"create a visible signature field at the coordinates provided."
            )


def _callback_logic(
    plugin: SigningCommandPlugin, infile: str, outfile: str, **kwargs
):
    ctx = click.get_current_context()
    cli_ctx: CLIContext = ctx.obj

    timestamp_url: Optional[str] = cli_ctx.timestamp_url
    if timestamp_url is not None:
        timestamper = HTTPTimeStamper(timestamp_url)
    else:
        timestamper = None
    with plugin.create_signer(cli_ctx, **kwargs) as signer:
        pdf_sig_settings = cli_ctx.sig_settings

        if pdf_sig_settings is None:
            # detached sig
            if timestamper is not None:
                # signed TS
                timestamp_attr = None
            else:
                # in this case, embed the signing time as a signed attr
                timestamp_attr = datetime.now(tz=tzlocal.get_localzone())

            with open(infile, 'rb') as inf:
                cert = signer.signing_cert
                assert cert is not None
                signature_job = signer.async_sign_general_data(
                    inf,
                    select_suitable_signing_md(cert.public_key),
                    timestamper=timestamper,
                    signed_attr_settings=PdfCMSSignedAttributes(
                        signing_time=timestamp_attr
                    ),
                )
                signature = asyncio.run(signature_job)

            output_bytes = signature.dump()
            if cli_ctx.detach_pem:
                output_bytes = pem.armor('PKCS7', output_bytes)

            with open(outfile, 'wb') as out:
                out.write(output_bytes)
        else:
            with open_for_signing(
                infile_path=infile, lenient=cli_ctx.lenient
            ) as w:
                if cli_ctx.ux.visible_signature_desired:
                    _ensure_field_visible(
                        w,
                        pdf_sig_settings.field_name,
                        cli_ctx.new_field_spec is not None,
                    )
                result = PdfSigner(
                    pdf_sig_settings,
                    signer=signer,
                    timestamper=timestamper,
                    stamp_style=cli_ctx.stamp_style,
                    new_field_spec=cli_ctx.new_field_spec,
                ).sign_pdf(
                    w,
                    existing_fields_only=cli_ctx.existing_fields_only,
                    appearance_text_params=get_text_params(ctx),
                )

                with open(outfile, 'wb') as outf:
                    buf = result.getbuffer()
                    outf.write(buf)
                    buf.release()


def command_from_plugin(plugin: SigningCommandPlugin) -> click.Command:
    def _callback(*, infile: str, outfile: str, **kwargs):
        with pyhanko_exception_manager():
            _callback_logic(plugin, infile, outfile, **kwargs)

    params: List[click.Parameter] = [
        click.Argument(('infile',), type=readable_file),
        click.Argument(('outfile',), type=writable_file),
    ]
    params.extend(plugin.click_extra_arguments())
    params.extend(plugin.click_options())
    return click.Command(
        name=plugin.subcommand_name,
        callback=_callback,
        help=plugin.help_summary,
        params=params,
    )
