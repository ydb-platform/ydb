import ydb.apps.dstool.lib.arg_parser as argparse
import asyncio
import logging
import sys
import tempfile

from ydb.tools.mnc.lib import common, config, deploy_ctx
from ydb.tools.mnc.lib.exceptions import CliError, ConfigError

from ydb.tools.mnc.cli.commands import modules
import ydb.tools.mnc.scheme as scheme


def setup_loggers(log_level):
    format = '[%(levelname)s] %(name)s: %(message)s'
    logging.basicConfig(format=format, level=log_level)


async def async_main():
    parser = argparse.ArgumentParser(description='Process some integers.')

    parser.add_argument('--verbose', '-V', dest='verbose', action='store_true', default=False)
    common.add_argument_breaker(parser)

    subparsers = parser.add_subparsers(help='Verbs', dest='verb', required=True)
    actions = {}
    expected_config = {}
    for mod in modules:
        module_name = mod.__name__.split('.')[-1]
        module_parser = subparsers.add_parser(module_name)
        mod.add_arguments(module_parser)
        actions[module_name] = mod.do
        expected_config[module_name] = mod.expected_config

    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.ERROR
    setup_loggers(log_level)

    async def act():
        if expected_config.get(args.verb):
            command_scheme = expected_config[args.verb]
            cfg, msgs = scheme.apply_scheme(config.get_config(scheme.multinode, args), command_scheme)
            if cfg is None:
                print("errors:", *msgs, sep='\n  - ', file=sys.stderr)
                sys.exit(1)
            if 'deploy_flags' in command_scheme:
                cfg['deploy_flags'] = scheme.common.merge_deploy_flags(cfg.get('deploy_flags', []), args.deploy_flags)
            deploy_ctx.apply_cfg(cfg, command_scheme)
            setattr(args, "config", cfg)
        result = await actions[args.verb](args)
        if result is False:
            raise CliError(f"Command '{args.verb}' failed")

    if args.verb:
        if not expected_config[args.verb] or args.work_directory is None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                deploy_ctx.work_directory = tmp_dir
                await act()
        else:
            deploy_ctx.work_directory = args.work_directory
            await act()


def main():
    try:
        deploy_ctx.apply_cfg_mnc(config.get_mnc_config())
        asyncio.run(async_main())
    except ConfigError as error:
        print(f'Error with config \'{error.config_path}\':', file=sys.stderr)
        print(error, file=sys.stderr)
        sys.exit(1)
    except CliError as error:
        print(error, file=sys.stderr)
        sys.exit(1)
