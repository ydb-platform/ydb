#!/usr/bin/env python3

from ydb.apps.dstool.lib.arg_parser import ArgumentParser
import ydb.apps.dstool.lib.common as common
import ydb.apps.dstool.lib.commands as commands


def main():
    parser = ArgumentParser(description='YDB Distributed Storage Administration Tool')

    # common options
    common.add_host_access_options(parser)
    parser.add_argument('--dry-run', '-n', action='store_true', help='Run command without side effects')

    subparsers = parser.add_subparsers(help='Subcommands', dest='global_command', required=True)
    command_map = commands.make_command_map_by_structure(subparsers)
    args = parser.parse_args()
    common.apply_args(args)
    commands.run_command(command_map, args)


if __name__ == '__main__':
    main()
