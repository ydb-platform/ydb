import asyncio
import sys
import tempfile

from ydb.tools.mnc.lib import config, deploy_ctx
from ydb.tools.mnc.lib.exceptions import CliError, ConfigError
from ydb.tools.mnc.lib.output import VerbosityMode, init, get_console, get_stderr_console
from ydb.tools.mnc.lib.logging_setup import setup_loggers
from ydb.tools.mnc.lib.progress import MyTraceback

from ydb.tools.mnc.cli import parser_factory
from ydb.tools.mnc.cli.tui import TuiApp, TuiLauncher, should_route_to_launcher
import ydb.tools.mnc.scheme as scheme


async def async_main():
    parser, actions, expected_config, prefer_launcher = parser_factory.build_parser()
    args = parser.parse_args()

    # Initialize output management
    if args.verbose:
        init(VerbosityMode.VERBOSE)
    elif args.quiet:
        init(VerbosityMode.QUIET)
    else:
        init(VerbosityMode.NORMAL)

    # Setup logging with RichHandler
    setup_loggers()

    launcher_used = should_route_to_launcher(args, expected_config, prefer_launcher)
    if launcher_used:
        result = await TuiLauncher(parser, expected_config).run_async(args)
        if result.cancelled:
            return
        args = result.args

    if not args.verb:
        raise CliError("Command wasn't selected")
    if args.verb not in actions:
        raise CliError(f"Unknown command: {args.verb}")

    async def act():
        if expected_config.get(args.verb):
            command_scheme = expected_config[args.verb]
            cfg, msgs = scheme.apply_scheme(config.get_config(scheme.multinode, args), command_scheme)
            if cfg is None:
                raise CliError("Config validation failed:\n  - " + "\n  - ".join(msgs))
            if 'deploy_flags' in command_scheme:
                cfg['deploy_flags'] = scheme.common.merge_deploy_flags(cfg.get('deploy_flags', []), args.deploy_flags)
            deploy_ctx.apply_cfg(cfg, command_scheme)
            setattr(args, "config", cfg)
        result = await actions[args.verb](args)
        if not result:
            raise CliError(f"Command '{args.verb}' failed", result=result)
        return result

    async def run_with_workdir():
        if launcher_used or args.tui:
            app = TuiApp(console=get_console())
            return await app.run_async(lambda pbar: act())
        return await act()

    if not expected_config[args.verb] or args.work_directory is None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            deploy_ctx.work_directory = tmp_dir
            await run_with_workdir()
    else:
        deploy_ctx.work_directory = args.work_directory
        await run_with_workdir()


def main():
    try:
        deploy_ctx.apply_cfg_mnc(config.get_mnc_config())
        asyncio.run(async_main())
    except ConfigError as error:
        if not getattr(error, '_mnc_tui_reported', False):
            print(f'Error with config \'{error.config_path}\':', file=sys.stderr)
            print(error, file=sys.stderr)
        sys.exit(1)
    except CliError as error:
        if not getattr(error, '_mnc_tui_reported', False):
            print(error, file=sys.stderr)
        sys.exit(1)
    except Exception as error:
        if not getattr(error, '_mnc_tui_reported', False):
            init()
            console = get_stderr_console()
            console.print(MyTraceback(error).renderable)
        sys.exit(1)
