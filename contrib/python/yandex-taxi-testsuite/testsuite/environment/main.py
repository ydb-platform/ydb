import argparse
import contextlib
import importlib
import logging
import pathlib
import subprocess
import sys

from testsuite.utils import colors

from . import control, shell, utils

DEFAULT_SERVICE_PLUGINS = [
    'testsuite.databases.mongo.pytest_plugin',
    'testsuite.databases.pgsql.pytest_plugin',
    'testsuite.databases.redis.pytest_plugin',
    'testsuite.databases.mysql.pytest_plugin',
    'testsuite.databases.clickhouse.pytest_plugin',
    'testsuite.databases.rabbitmq.pytest_plugin',
    'testsuite.databases.kafka.pytest_plugin',
]

logger = logging.getLogger(__name__)


class ColoredLevelFormatter(logging.Formatter):
    LEVEL_COLORS = {
        logging.DEBUG: colors.Colors.GRAY,
        logging.INFO: colors.Colors.BRIGHT_GREEN,
        logging.WARNING: colors.Colors.YELLOW,
        logging.ERROR: colors.Colors.RED,
        logging.CRITICAL: colors.Colors.BRIGHT_RED,
    }

    def __init__(self, *, colors_enabled=False):
        super().__init__()
        self._colors_enabled = colors_enabled

    def format(self, record: logging.LogRecord):
        message = super().format(record)
        if not self._colors_enabled:
            return f'{record.levelname} {message}'
        color = self.LEVEL_COLORS.get(record.levelno, colors.Colors.DEFAULT)
        return f'{color}{record.levelname}{colors.Colors.DEFAULT} {message}'


def csv_arg(value: str):
    result = []
    for arg in value.split(','):
        arg = arg.strip()
        if arg:
            result.append(arg)
    return result


def main(args=None, service_plugins=None):
    utils.ensure_non_root_user()
    testsuite_services = _register_services(service_plugins)
    default_services = sorted(testsuite_services.keys())

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--env-dir',
        help='Path environment data directry.',
        type=pathlib.Path,
        default=None,
    )
    parser.add_argument(
        '-f',
        '--force',
        action='store_true',
        help='Force run, ignore failures',
    )
    parser.add_argument(
        '--reuse-services',
        action='store_true',
        help='Do not re-raise already running services',
    )
    services_group = parser.add_mutually_exclusive_group()
    services_group.add_argument(
        '-s',
        '--databases',
        dest='services',
        type=csv_arg,
        help='Comma separated list of services (default: %(default)s)',
        default=default_services,
    )
    services_group.add_argument(
        '--services',
        nargs='+',
        help='Deprecated! List of services (default: %(default)s)',
        default=default_services,
    )
    parser.add_argument(
        '--log-level',
        choices=['debug', 'info', 'warning', 'error', 'critical'],
        default='debug',
    )
    parser.set_defaults(handler=None)

    subparsers = parser.add_subparsers(metavar='command')

    command_parser = subparsers.add_parser('start', help='Start services')
    command_parser.set_defaults(handler=_command_start)

    command_parser = subparsers.add_parser('stop', help='Stop services')
    command_parser.set_defaults(handler=_command_stop)

    command_parser = subparsers.add_parser(
        'run',
        help='Run command with services started',
    )
    command_parser.add_argument('command', nargs='+', help='Command to run')
    command_parser.set_defaults(handler=_command_run)

    args = parser.parse_args(args=args)
    if args.handler is None:
        parser.error('the following arguments are required: command')

    _setup_logging(args.log_level.upper())

    config = control.load_environment_config(
        env_dir=args.env_dir,
        reuse_services=args.reuse_services,
        verbose=2,
    )
    env = control.TestsuiteEnvironment(config)
    for service_name, service_class in testsuite_services.items():
        env.register_service(service_name, service_class)
    args.handler(env, args)


def _setup_logging(log_level):
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(
        ColoredLevelFormatter(
            colors_enabled=sys.stderr.isatty(),
        ),
    )
    root_logger.addHandler(handler)


def _command_start(env, args):
    status = True
    for service_name in args.services:
        try:
            env.ensure_started(service_name)
        except shell.SubprocessFailed as exc:
            logger.error(
                'Failed to start service %s: %s',
                service_name,
                str(exc),
            )
            status = False
            if not args.force:
                break
    if not status:
        sys.exit(1)


def _command_stop(env, args):
    status = True
    for service_name in args.services:
        try:
            env.stop_service(service_name)
        except shell.SubprocessFailed as exc:
            logger.error(
                'Failed to stop service %s: %s',
                service_name,
                str(exc),
            )
            status = False
            if not args.force:
                break
    if not status:
        sys.exit(1)


def _command_run(env, args):
    _command_start(env, args)
    with contextlib.closing(env):
        exit_code = subprocess.call(args.command)
        sys.exit(exit_code)


def _register_services(service_plugins=None):
    services = {}

    def _register_service(name, factory=None):
        def decorator(factory):
            services[name] = factory

        if factory is None:
            return decorator
        return decorator(factory)

    if service_plugins is None:
        service_plugins = DEFAULT_SERVICE_PLUGINS
        strict_check = False
    else:
        strict_check = True

    for modname in service_plugins:
        try:
            mod = importlib.import_module(modname)
        except ImportError:
            if strict_check:
                raise
            continue
        mod.pytest_service_register(register_service=_register_service)
    return services


if __name__ == '__main__':
    main()
