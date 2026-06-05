import ydb.apps.dstool.lib.arg_parser as argparse

from ydb.tools.mnc.lib import common
from ydb.tools.mnc.cli.commands import modules


def build_parser(command_modules=None):
    command_modules = modules if command_modules is None else command_modules
    parser = argparse.ArgumentParser(description='MNC cluster management tool.')

    verbosity_group = parser.add_mutually_exclusive_group()
    verbosity_group.add_argument('--verbose', '-V', dest='verbose', action='store_true', default=False)
    verbosity_group.add_argument('--quiet', '-q', dest='quiet', action='store_true', default=False)
    parser.add_argument('--tui', dest='tui', action='store_true', default=False)
    common.add_argument_breaker(parser)

    subparsers = parser.add_subparsers(help='Verbs', dest='verb', required=False)
    actions = {}
    expected_config = {}
    prefer_launcher = {}
    for mod in command_modules:
        module_name = mod.__name__.split('.')[-1]
        module_parser = subparsers.add_parser(module_name)
        mod.add_arguments(module_parser)
        actions[module_name] = mod.do
        expected_config[module_name] = mod.expected_config
        prefer_launcher[module_name] = getattr(mod, 'prefer_tui_launcher', False)
    return parser, actions, expected_config, prefer_launcher
