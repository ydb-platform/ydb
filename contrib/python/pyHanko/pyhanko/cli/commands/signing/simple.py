import contextlib
import getpass
from typing import ContextManager, List, Optional

import click

from pyhanko.cli._ctx import CLIContext
from pyhanko.cli._trust import grab_certs
from pyhanko.cli.config import CLIConfig
from pyhanko.cli.plugin_api import SigningCommandPlugin, register_signing_plugin
from pyhanko.cli.utils import _warn_empty_passphrase, logger, readable_file
from pyhanko.config.errors import ConfigurationError
from pyhanko.config.local_keys import (
    PemDerSignatureConfig,
    PKCS12SignatureConfig,
)
from pyhanko.sign.signers.pdf_cms import (
    Signer,
    signer_from_p12_config,
    signer_from_pemder_config,
)

__all__ = ['PemderPlugin', 'PKCS12Plugin']


class KeyFileConfigWrapper:
    def __init__(self, config: CLIConfig):
        config_dict = config.raw_config
        self.pemder_setups = config_dict.get('pemder-setups', {})
        self.pkcs12_setups = config_dict.get('pkcs12-setups', {})

    def get_pkcs12_config(self, name):
        try:
            setup = self.pkcs12_setups[name]
        except KeyError:
            raise ConfigurationError(f"There's no PKCS#12 setup named '{name}'")
        return PKCS12SignatureConfig.from_config(setup)

    def get_pemder_config(self, name):
        try:
            setup = self.pemder_setups[name]
        except KeyError:
            raise ConfigurationError(f"There's no PEM/DER setup named '{name}'")
        return PemDerSignatureConfig.from_config(setup)


class PemderPlugin(SigningCommandPlugin):
    subcommand_name = 'pemder'
    help_summary = 'read key material from PEM/DER files'

    def click_options(self) -> List[click.Option]:
        return [
            click.Option(
                ('--key',),
                help='file containing the private key (PEM/DER)',
                type=readable_file,
                required=False,
            ),
            click.Option(
                ('--cert',),
                help='file containing the signer\'s certificate ' '(PEM/DER)',
                type=readable_file,
                required=False,
            ),
            click.Option(
                ('--chain',),
                type=readable_file,
                multiple=True,
                help='file(s) containing the chain of trust for the '
                'signer\'s certificate (PEM/DER). May be '
                'passed multiple times.',
            ),
            click.Option(
                ('--pemder-setup',),
                type=str,
                required=False,
                help='name of preconfigured PEM/DER profile (overrides all '
                'other options)',
            ),
            # TODO allow reading the passphrase from a specific file descriptor
            #  (for advanced scripting setups)
            click.Option(
                ('--passfile',),
                help='file containing the passphrase ' 'for the private key',
                required=False,
                type=click.File('r'),
                show_default='stdin',
            ),
            click.Option(
                ('--no-pass',),
                help='assume the private key file is unencrypted',
                type=bool,
                is_flag=True,
                default=False,
                show_default=True,
            ),
        ]

    def create_signer(
        self, context: CLIContext, **kwargs
    ) -> ContextManager[Signer]:
        @contextlib.contextmanager
        def _m():
            yield _pemder_signer(context, **kwargs)

        return _m()


def _pemder_signer(
    ctx: CLIContext,
    key,
    cert,
    chain,
    pemder_setup,
    passfile,
    no_pass,
):
    if pemder_setup:
        cli_config = ctx.config
        if cli_config is None:
            raise click.ClickException(
                "The --pemder-setup option requires a configuration file"
            )
        try:
            pemder_config = KeyFileConfigWrapper(cli_config).get_pemder_config(
                pemder_setup
            )
        except ConfigurationError as e:
            msg = f"Error while reading PEM/DER setup {pemder_setup}"
            logger.error(msg, exc_info=e)
            raise click.ClickException(msg)
    elif not (key and cert):
        raise click.ClickException(
            "Either both the --key and --cert options, or the --pemder-setup "
            "option must be provided."
        )
    else:
        pemder_config = PemDerSignatureConfig(
            key_file=key,
            cert_file=cert,
            other_certs=grab_certs(chain),
            prefer_pss=ctx.prefer_pss,
        )

    if pemder_config.key_passphrase is not None:
        passphrase = pemder_config.key_passphrase
    elif passfile is not None:
        passphrase = passfile.readline().strip().encode('utf-8')
        passfile.close()
    elif pemder_config.prompt_passphrase and not no_pass:
        passphrase = getpass.getpass(prompt='Key passphrase: ').encode('utf-8')
        if not passphrase:
            _warn_empty_passphrase()
            passphrase = None
    else:
        passphrase = None

    return signer_from_pemder_config(
        pemder_config, provided_key_passphrase=passphrase
    )


class PKCS12Plugin(SigningCommandPlugin):
    subcommand_name = 'pkcs12'
    help_summary = 'read key material from PKCS#12 files'

    def click_extra_arguments(self) -> List[click.Argument]:
        return [click.Argument(('pfx',), type=readable_file, required=False)]

    def click_options(self) -> List[click.Option]:
        return [
            click.Option(
                ('--p12-setup',),
                type=str,
                required=False,
                help='name of preconfigured PKCS#12 profile (overrides all '
                'other options)',
            ),
            click.Option(
                ('--chain',),
                type=readable_file,
                multiple=True,
                help='PEM/DER file(s) containing extra certificates to embed '
                '(e.g. chain of trust not embedded in the PKCS#12 file)'
                'May be passed multiple times.',
            ),
            click.Option(
                ('--passfile',),
                help='file containing the passphrase for the PKCS#12 file.',
                required=False,
                type=click.File('r'),
                show_default='stdin',
            ),
            click.Option(
                ('--no-pass',),
                help='assume the PKCS#12 file is unencrypted',
                type=bool,
                is_flag=True,
                default=False,
                show_default=True,
            ),
        ]

    def create_signer(
        self, context: CLIContext, **kwargs
    ) -> ContextManager[Signer]:
        @contextlib.contextmanager
        def _m():
            yield _pkcs12_signer(context, **kwargs)

        return _m()


def _pkcs12_signer(ctx: CLIContext, pfx, chain, passfile, p12_setup, no_pass):
    # TODO add sanity check in case the user gets the arg order wrong
    #  (now it fails with a gnarly DER decoding error, which is not very
    #  user-friendly)
    if p12_setup:
        cli_config: Optional[CLIConfig] = ctx.config
        if cli_config is None:
            raise click.ClickException(
                "The --p12-setup option requires a configuration file"
            )
        try:
            pkcs12_config = KeyFileConfigWrapper(cli_config).get_pkcs12_config(
                p12_setup
            )
        except ConfigurationError as e:
            msg = f"Error while reading PKCS#12 config {p12_setup}"
            logger.error(msg, exc_info=e)
            raise click.ClickException(msg)
    elif not pfx:
        raise click.ClickException(
            "Either the PFX argument or the --p12-setup option "
            "must be provided."
        )
    else:
        pkcs12_config = PKCS12SignatureConfig(
            pfx_file=pfx,
            other_certs=grab_certs(chain),
            prefer_pss=ctx.prefer_pss,
        )

    if pkcs12_config.pfx_passphrase is not None:
        passphrase = pkcs12_config.pfx_passphrase
    elif passfile is not None:
        passphrase = passfile.readline().strip().encode('utf-8')
        passfile.close()
    elif pkcs12_config.prompt_passphrase and not no_pass:
        passphrase = getpass.getpass(prompt='PKCS#12 passphrase: ').encode(
            'utf-8'
        )
        if not passphrase:
            _warn_empty_passphrase()
            passphrase = None
    else:
        passphrase = None

    return signer_from_p12_config(
        pkcs12_config, provided_pfx_passphrase=passphrase
    )
