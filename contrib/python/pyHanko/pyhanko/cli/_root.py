import itertools
import logging
from typing import Iterable, Optional

import click

from pyhanko import __version__
from pyhanko.cli._ctx import CLIContext
from pyhanko.cli.config import CLIRootConfig, parse_cli_config
from pyhanko.cli.plugin_api import (
    SIGNING_PLUGIN_ENTRY_POINT_GROUP,
    SIGNING_PLUGIN_REGISTRY,
)
from pyhanko.cli.runtime import DEFAULT_CONFIG_FILE, logging_setup
from pyhanko.config.logging import LogConfig, parse_logging_config

__all__ = ['cli_root']


@click.group()
@click.version_option(prog_name='pyHanko', version=__version__)
@click.option(
    '--config',
    help=(
        'YAML file to load configuration from'
        f'[default: {DEFAULT_CONFIG_FILE}]'
    ),
    required=False,
    type=click.File('r'),
)
@click.option(
    '--verbose',
    help='Run in verbose mode',
    required=False,
    default=False,
    type=bool,
    is_flag=True,
)
@click.option(
    '--no-plugins',
    help='Disable non-builtin plugin loading',
    type=bool,
    is_flag=True,
)
@click.pass_context
def _root(ctx: click.Context, config, verbose, no_plugins):
    config_text = None
    if config is None:
        try:
            with open(DEFAULT_CONFIG_FILE, 'r') as f:
                config_text = f.read()
            config = DEFAULT_CONFIG_FILE
        except FileNotFoundError:
            pass
        except IOError as e:
            raise click.ClickException(
                f"Failed to read {DEFAULT_CONFIG_FILE}: {str(e)}"
            )
    else:
        try:
            config_text = config.read()
        except IOError as e:
            raise click.ClickException(
                f"Failed to read configuration: {str(e)}",
            )

    ctx.ensure_object(CLIContext)
    ctx_obj: CLIContext = ctx.obj
    cfg: Optional[CLIRootConfig] = None
    if config_text is not None:
        cfg = parse_cli_config(config_text)
        ctx_obj.config = cfg.config
        log_config = cfg.log_config
    else:
        # grab the default
        log_config = parse_logging_config({})

    from .commands.signing import register

    plugins_to_register = _load_plugins(cfg, plugins_enabled=not no_plugins)
    register(plugins_to_register)

    if verbose:
        # override the root logger's logging level, but preserve the output
        root_logger_config = log_config[None]
        log_config[None] = LogConfig(
            level=logging.DEBUG, output=root_logger_config.output
        )
    else:
        # use the root logger's output settings to populate the default
        log_output = log_config[None].output
        # Revinfo fetch logs -> filter by default
        log_config['pyhanko_certvalidator.fetchers'] = LogConfig(
            level=logging.WARNING, output=log_output
        )
        if 'fontTools.subset' not in log_config:
            # the fontTools subsetter has a very noisy INFO log, so
            # set that one to WARNING by default
            log_config['fontTools.subset'] = LogConfig(
                level=logging.WARNING, output=log_output
            )

    logging_setup(log_config, verbose)

    if verbose:
        logging.debug("Running with --verbose")
    if config_text is not None:
        logging.debug(f'Finished reading configuration from {config}.')
    else:
        logging.debug('There was no configuration to parse.')


def _load_plugins(root_config: Optional[CLIRootConfig], plugins_enabled: bool):
    import sys
    from importlib import metadata

    # we always load the default ones
    to_load = [
        'pyhanko.cli.commands.signing.pkcs11_cli:PKCS11Plugin',
        'pyhanko.cli.commands.signing.simple:PKCS12Plugin',
        'pyhanko.cli.commands.signing.simple:PemderPlugin',
    ]

    eps_from_metadata: Iterable[metadata.EntryPoint] = []
    if plugins_enabled:
        if root_config is not None:
            to_load += [str(mod) for mod in root_config.plugin_endpoints]

        # need to use dict interface for 3.8 interop
        if sys.version_info < (3, 10):
            eps_from_metadata = metadata.entry_points().get(
                SIGNING_PLUGIN_ENTRY_POINT_GROUP, []
            )
        else:
            eps_from_metadata = metadata.entry_points(
                group=SIGNING_PLUGIN_ENTRY_POINT_GROUP
            )

    # noinspection PyArgumentList
    to_load_as_endpoints: Iterable[metadata.EntryPoint] = [
        metadata.EntryPoint(
            name='',
            value=v,
            group=SIGNING_PLUGIN_ENTRY_POINT_GROUP,
        )
        for v in to_load
    ]
    resulting_plugins = list(SIGNING_PLUGIN_REGISTRY)
    seen = set(type(x) for x in SIGNING_PLUGIN_REGISTRY)
    for ep in itertools.chain(to_load_as_endpoints, eps_from_metadata):
        plugin_cls = ep.load()
        if not isinstance(plugin_cls, type):
            click.echo(
                click.style(
                    f"Plugins must be defined as references to classes with a "
                    f"nullary init function, but '{ep.value}' is "
                    f"a {type(plugin_cls)}. Disregarding...",
                    bold=True,
                )
            )
            continue
        if plugin_cls not in seen:
            seen.add(plugin_cls)
            resulting_plugins.append(plugin_cls())
    return resulting_plugins


cli_root: click.Group = _root
