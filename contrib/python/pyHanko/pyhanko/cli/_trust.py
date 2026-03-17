from typing import Iterable, Optional, TypeVar, Union

import click

from pyhanko.cli.config import CLIConfig
from pyhanko.cli.utils import logger, readable_file
from pyhanko.config.errors import ConfigurationError
from pyhanko.config.trust import init_validation_context_kwargs
from pyhanko.keys import load_certs_from_pemder


def build_vc_kwargs(
    cli_config: Optional[CLIConfig],
    validation_context: Optional[str],
    trust: Union[Iterable[str], str],
    trust_replace: bool,
    other_certs: Union[Iterable[str], str],
    retroactive_revinfo: bool,
    allow_fetching: Optional[bool] = None,
):
    try:
        if validation_context is not None:
            if any((trust, other_certs)):
                raise click.ClickException(
                    "--validation-context is incompatible with --trust "
                    "and --other-certs"
                )
            # load the desired context from config
            if cli_config is None:
                raise click.ClickException("No config file specified.")
            try:
                result = cli_config.get_validation_context(
                    validation_context, as_dict=True
                )
            except ConfigurationError as e:
                msg = (
                    "Configuration problem. Are you sure that the validation "
                    f"context '{validation_context}' is properly defined in the"
                    " configuration file?"
                )
                logger.error(msg, exc_info=e)
                raise click.ClickException(msg)
        elif trust or other_certs:
            # load a validation profile using command line kwargs
            result = init_validation_context_kwargs(
                trust=trust,
                trust_replace=trust_replace,
                other_certs=other_certs,
                retroactive_revinfo=retroactive_revinfo,
            )
        elif cli_config is not None:
            # load the default settings from the CLI config
            try:
                result = cli_config.get_validation_context(as_dict=True)
            except ConfigurationError as e:
                msg = "Failed to load default validation context."
                logger.error(msg, exc_info=e)
                raise click.ClickException(msg)
        else:
            result = {}

        if allow_fetching is not None:
            result['allow_fetching'] = allow_fetching
        else:
            result.setdefault('allow_fetching', True)

        # allow CLI --retroactive-revinfo flag to override settings
        # if necessary
        if retroactive_revinfo:
            result['retroactive_revinfo'] = True
        return result
    except click.ClickException:
        raise
    except IOError as e:
        msg = "I/O problem while reading validation config"
        logger.error(msg, exc_info=e)
        raise click.ClickException(msg)
    except Exception as e:
        msg = "Generic processing problem while reading validation config"
        logger.error(msg, exc_info=e)
        raise click.ClickException(msg)


def _get_key_usage_settings(ctx: click.Context, validation_context: str):
    cli_config: Optional[CLIConfig] = ctx.obj.config
    if cli_config is None:
        return None

    # note: validation_context can be None, this triggers fallback to the
    # default validation context specified in the configuration file
    # If we add support for specifying key usage settings as CLI arguments,
    # using the same fallbacks as _build_cli_kwargs would probably be cleaner
    return cli_config.get_signer_key_usages(name=validation_context)


TRUST_OPTIONS = [
    click.Option(
        ('--validation-context',),
        help='use validation context from config',
        required=False,
        type=str,
    ),
    click.Option(
        ('--trust',),
        help='list trust roots (multiple allowed)',
        required=False,
        multiple=True,
        type=readable_file,
    ),
    click.Option(
        ('--trust-replace',),
        help='listed trust roots supersede OS-provided trust store',
        required=False,
        type=bool,
        is_flag=True,
        default=False,
        show_default=True,
    ),
    click.Option(
        ('--other-certs',),
        help='other certs relevant for validation',
        required=False,
        multiple=True,
        type=readable_file,
    ),
]


FC = TypeVar('FC', bound=click.Command)


def trust_options(f: FC) -> FC:
    f.params.extend(TRUST_OPTIONS)
    return f


def _prepare_vc(vc_kwargs, soft_revocation_check, force_revinfo):
    if soft_revocation_check and force_revinfo:
        raise click.ClickException(
            "--soft-revocation-check is incompatible with " "--force-revinfo"
        )
    if force_revinfo:
        rev_mode = 'require'
    elif soft_revocation_check:
        rev_mode = 'soft-fail'
    else:
        rev_mode = 'hard-fail'
    vc_kwargs['revocation_mode'] = rev_mode
    return vc_kwargs


def grab_certs(files):
    if not files:
        return None
    try:
        return list(load_certs_from_pemder(files))
    except (IOError, ValueError) as e:
        raise click.ClickException(
            f'Could not load certificates from {files}'
        ) from e
