import asyncio
import getpass
from datetime import datetime

import click
from asn1crypto import cms, pem
from pyhanko_certvalidator import ValidationContext

import pyhanko.sign
from pyhanko.cli._trust import (
    _get_key_usage_settings,
    _prepare_vc,
    build_vc_kwargs,
    trust_options,
)
from pyhanko.cli.commands.signing import signing
from pyhanko.cli.runtime import pyhanko_exception_manager
from pyhanko.cli.utils import logger
from pyhanko.pdf_utils import crypt
from pyhanko.pdf_utils.misc import isoparse
from pyhanko.pdf_utils.reader import PdfFileReader
from pyhanko.sign import validation
from pyhanko.sign.validation import RevocationInfoValidationType
from pyhanko.sign.validation.errors import SignatureValidationError

__all__ = ['validate_signatures']


def _signature_status(
    ltv_profile,
    vc_kwargs,
    force_revinfo,
    key_usage_settings,
    embedded_sig,
    skip_diff=False,
):
    if ltv_profile is None:
        vc = ValidationContext(**vc_kwargs)
        status = pyhanko.sign.validation.validate_pdf_signature(
            embedded_sig,
            key_usage_settings=key_usage_settings,
            signer_validation_context=vc,
            skip_diff=skip_diff,
        )
    else:
        status = validation.validate_pdf_ltv_signature(
            embedded_sig,
            ltv_profile,
            key_usage_settings=key_usage_settings,
            force_revinfo=force_revinfo,
            validation_context_kwargs=vc_kwargs,
            skip_diff=skip_diff,
        )
    return status


def _validate_detached(
    infile, sig_infile, validation_context, key_usage_settings
):
    sig_bytes = sig_infile.read()
    try:
        if pem.detect(sig_bytes):
            _, _, sig_bytes = pem.unarmor(sig_bytes)
        content_info = cms.ContentInfo.load(sig_bytes)
        if content_info['content_type'].native != 'signed_data':
            raise click.ClickException("CMS content type is not signedData")
    except ValueError as e:
        raise click.ClickException("Could not parse CMS object") from e

    validation_coro = validation.async_validate_detached_cms(
        infile,
        signed_data=content_info['content'],
        signer_validation_context=validation_context,
        key_usage_settings=key_usage_settings,
    )
    return asyncio.run(validation_coro)


def _signature_status_str(status_callback, pretty_print, executive_summary):
    try:
        status = status_callback()
        if executive_summary and not pretty_print:
            return (
                'VALID' if status.bottom_line else 'INVALID',
                status.bottom_line,
            )
        elif pretty_print:
            return status.pretty_print_details(), status.bottom_line
        else:
            return status.summary(), status.bottom_line
    except validation.ValidationInfoReadingError as e:
        msg = (
            'An error occurred while parsing the revocation information '
            'for this signature: ' + str(e)
        )
        logger.error(msg)
        if pretty_print:
            return msg, False
        else:
            return 'REVINFO_FAILURE', False
    except SignatureValidationError as e:
        msg = 'An error occurred while validating this signature: ' + str(e)
        logger.error(msg, exc_info=e)
        if pretty_print:
            return msg, False
        else:
            return 'INVALID', False


def _attempt_iso_dt_parse(dt_str) -> datetime:
    try:
        dt = isoparse(dt_str)
    except ValueError:
        raise click.ClickException(f"datetime {dt_str!r} could not be parsed")
    return dt


# TODO add an option to do LTV, but guess the profile


@trust_options
@signing.command(name='validate', help='validate signatures')
@click.argument('infile', type=click.File('rb'))
@click.option(
    '--executive-summary',
    help='only print final judgment on signature validity',
    type=bool,
    is_flag=True,
    default=False,
    show_default=True,
)
@click.option(
    '--pretty-print',
    help='render a prettier summary for the signatures in the file',
    type=bool,
    is_flag=True,
    default=False,
    show_default=True,
)
@click.option(
    '--ltv-profile',
    help='LTV signature validation profile',
    type=click.Choice(RevocationInfoValidationType.as_tuple()),
    required=False,
)
@click.option(
    '--force-revinfo',
    help='Fail trust validation if a certificate has no known CRL '
    'or OCSP endpoints.',
    type=bool,
    is_flag=True,
    default=False,
    show_default=True,
)
@click.option(
    '--soft-revocation-check',
    help='Do not fail validation on revocation checking failures '
    '(only applied to on-line revocation checks)',
    type=bool,
    is_flag=True,
    default=False,
    show_default=True,
)
@click.option(
    '--no-revocation-check',
    help='Do not attempt to check revocation status '
    '(meaningless for LTV validation)',
    type=bool,
    is_flag=True,
    default=False,
    show_default=True,
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
    '--validation-time',
    help=(
        'Override the validation time (ISO 8601 date). '
        'The special value \'claimed\' causes the validation time '
        'claimed by the signer to be used. Revocation checking '
        'will be disabled. Option ignored in LTV mode.'
    ),
    type=str,
    required=False,
)
@click.option(
    '--password',
    required=False,
    type=str,
    help='password to access the file (can also be read from stdin)',
)
@click.option(
    '--no-diff-analysis',
    default=False,
    type=bool,
    is_flag=True,
    help='disable incremental update analysis',
)
@click.option(
    '--detached',
    type=click.File('rb'),
    help=(
        'Read signature CMS object from the indicated file; '
        'this can be used to verify signatures on non-PDF files'
    ),
)
@click.option(
    '--no-strict-syntax',
    help='Attempt to ignore syntactical problems in the input file '
    'and enable signature validation in hybrid-reference files.'
    '(warning: this may affect validation results in unexpected '
    'ways.)',
    type=bool,
    is_flag=True,
    default=False,
    show_default=True,
)
@click.pass_context
def validate_signatures(
    ctx: click.Context,
    infile,
    executive_summary,
    pretty_print,
    validation_context,
    trust,
    trust_replace,
    other_certs,
    ltv_profile,
    force_revinfo,
    soft_revocation_check,
    no_revocation_check,
    password,
    retroactive_revinfo,
    detached,
    no_diff_analysis,
    validation_time,
    no_strict_syntax,
):
    no_revocation_check |= validation_time is not None

    if no_revocation_check:
        soft_revocation_check = True

    if pretty_print and executive_summary:
        raise click.ClickException(
            "--pretty-print is incompatible with --executive-summary."
        )

    if ltv_profile is not None:
        if validation_time is not None:
            raise click.ClickException(
                "--validation-time is not compatible with --ltv-profile"
            )
        ltv_profile = RevocationInfoValidationType(ltv_profile)

    vc_kwargs = build_vc_kwargs(
        ctx.obj.config,
        validation_context,
        trust,
        trust_replace,
        other_certs,
        retroactive_revinfo,
        allow_fetching=False if no_revocation_check else None,
    )

    use_claimed_validation_time = False
    if validation_time == 'claimed':
        use_claimed_validation_time = True
    elif validation_time is not None:
        vc_kwargs['moment'] = _attempt_iso_dt_parse(validation_time)

    key_usage_settings = _get_key_usage_settings(ctx, validation_context)
    vc_kwargs = _prepare_vc(
        vc_kwargs,
        soft_revocation_check=soft_revocation_check,
        force_revinfo=force_revinfo,
    )
    with pyhanko_exception_manager():
        if detached is not None:
            (status_str, signature_ok) = _signature_status_str(
                status_callback=lambda: _validate_detached(
                    infile,
                    detached,
                    ValidationContext(**vc_kwargs),
                    key_usage_settings,
                ),
                pretty_print=pretty_print,
                executive_summary=executive_summary,
            )
            if signature_ok:
                click.echo(status_str)
            else:
                raise click.ClickException(status_str)
            return

        if no_strict_syntax:
            logger.info(
                "Strict PDF syntax is disabled; this could impact validation "
                "results. Use caution."
            )
            r = PdfFileReader(infile, strict=False)
        else:
            r = PdfFileReader(infile)
        sh = r.security_handler
        if isinstance(sh, crypt.StandardSecurityHandler):
            if password is None:
                password = getpass.getpass(prompt='File password: ')
            auth_result = r.decrypt(password)
            if auth_result.status == crypt.AuthStatus.FAILED:
                raise click.ClickException("Password didn't match.")
        elif sh is not None:
            raise click.ClickException(
                "The CLI supports only password-based encryption when "
                "validating (for now)"
            )

        all_signatures_ok = True
        for ix, embedded_sig in enumerate(r.embedded_regular_signatures):
            fingerprint: str = embedded_sig.signer_cert.sha256.hex()
            if use_claimed_validation_time:
                vc_kwargs['moment'] = embedded_sig.self_reported_timestamp
            (status_str, signature_ok) = _signature_status_str(
                status_callback=lambda: _signature_status(
                    ltv_profile=ltv_profile,
                    force_revinfo=force_revinfo,
                    vc_kwargs=vc_kwargs,
                    key_usage_settings=key_usage_settings,
                    embedded_sig=embedded_sig,
                    skip_diff=no_diff_analysis,
                ),
                pretty_print=pretty_print,
                executive_summary=executive_summary,
            )
            name = embedded_sig.field_name

            if pretty_print:
                header = f'Field {ix + 1}: {name}'
                line = '=' * len(header)
                click.echo(line)
                click.echo(header)
                click.echo(line)
                click.echo('\n\n' + status_str)
            else:
                click.echo('%s:%s:%s' % (name, fingerprint, status_str))
            all_signatures_ok &= signature_ok

        if not all_signatures_ok:
            raise click.ClickException("Validation failed")
