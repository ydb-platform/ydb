# -*- coding: utf-8 -*-
import argparse
from ydb.public.tools.lib import cmds


if __name__ == '__main__':
    help = """
\033[92mTool to setup local Yandex Database (YDB) cluster in the single-node mode (in most cases)\x1b[0m
\033[94m
To deploy the local YDB cluster:

  {prog} deploy --ydb-working-dir /absolute/path/to/working/directory --ydb-binary-path /path/to/kikimr/driver

To cleanup the deployed YDB cluster (this includes removal of working directory, all configuration files, disks and so on):

  {prog} cleanup --ydb-working-dir /absolute/path/to/working/directory --ydb-binary-path /path/to/kikimr/driver

To stop the deployed YDB cluster (no data will be erased):

  {prog} stop --ydb-working-dir /absolute/path/to/working/directory --ydb-binary-path /path/to/kikimr/driver

To start already deployed, but stopped cluster:

  {prog} start --ydb-working-dir /absolute/path/to/working/directory --ydb-binary-path /path/to/kikimr/driver

To update cluster (stop + start):

  {prog} update --ydb-working-dir /absolute/path/to/working/directory --ydb-binary-path /path/to/kikimr/driver
\x1b[0m
"""
    program_name = 'local_ydb'
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description=help.format(
            prog=program_name
        )
    )
    subparsers = parser.add_subparsers(help='sub-command help', required=True)
    deploy = subparsers.add_parser(
        'deploy',
        formatter_class=argparse.RawTextHelpFormatter,
        description="""\033[94mDeploy local YDB cluster\x1b[0m"""
    )

    deploy.set_defaults(command=cmds.deploy)
    cleanup = subparsers.add_parser(
        'cleanup',
        formatter_class=argparse.RawTextHelpFormatter,
        description="""\033[94mCleanup the deployed cluster\x1b[0m"""
    )
    cleanup.set_defaults(command=cmds.cleanup)

    stop = subparsers.add_parser(
        'stop',
        formatter_class=argparse.RawTextHelpFormatter,
        description="""\033[94mStop local YDB cluster\x1b[0m"""
    )
    stop.set_defaults(command=cmds.stop)

    start = subparsers.add_parser(
        'start',
        formatter_class=argparse.RawTextHelpFormatter,
        description="""\033[94mStart already deployed, but stopped local YDB cluster\x1b[0m"""
    )
    start.set_defaults(command=cmds.start)

    update = subparsers.add_parser(
        'update',
        formatter_class=argparse.RawTextHelpFormatter,
        description="""\033[94mUpdate local YDB cluster (start + stop)\x1b[0m"""
    )
    update.set_defaults(command=cmds.update)

    for sub_parser in (deploy, cleanup, start, stop, update):
        sub_parser.add_argument(
            '--erasure', default=None,
            help='Erasure of YDB cluster'
        )
        sub_parser.add_argument(
            '--ydb-working-dir', required=True,
            help='Working directory for YDB cluster (the place to create directories, configuration files, disks)'
        )
        sub_parser.add_argument(
            '--ydb-udfs-dir', default=None,
            help='The directory with YDB udfs'
        )
        sub_parser.add_argument(
            '--auth-config-path', default=None,
            help='The path to auth config'
        )
        sub_parser.add_argument(
            '--streaming-config-path', default=None,
            help='The path to Streaming config'
        )
        sub_parser.add_argument(
            '--fq-config-path', default=None,
            help='The path to Federated Query config'
        )
        sub_parser.add_argument(
            '--enable-feature-flag',
            default=[],
            action='append',
            type=str,
            dest="enabled_feature_flags",
            help='Enable feature flag'
        )
        sub_parser.add_argument(
            '--enable-grpc-service',
            default=[],
            action='append',
            type=str,
            dest="enabled_grpc_services",
            help='Enables GRPC service'
        )
        sub_parser.add_argument(
            '--suppress-version-check', default=False, action='store_true',
            help='Should suppress version check',
        )
        sub_parser.add_argument(
            '--with-s3-internal', default=False,
            action='store_true',
            help='Include s3 internal service'
        )
        sub_parser.add_argument(
            '--enable-session-busy', default=False,
            action='store_true',
            help='Enable session busy status',
        )
        sub_parser.add_argument(
            '--fixed-ports',
            default=False,
            action='store_true',
        )
        sub_parser.add_argument(
            '--base-port-offset',
            type=int,
            default=0,
            action='store',
        )
        sub_parser.add_argument(
            '--ydb-binary-path', required=True,
            help='Path to binary file'
        )
        sub_parser.add_argument(
            '--enable-pq', default=False, action='store_true',
            help='Enable pqv1 service in kikimr'
        )
        sub_parser.add_argument(
            '--enable-datastreams', default=False, action='store_true',
            help='Enable datastreams service'
        )
        sub_parser.add_argument(
            '--public-http-config-path', default=None,
            help='The path to public HTTP config'
        )
        sub_parser.add_argument(
            '--dont-use-log-files', default=False, action='store_true',
            help='Don\'t use log files (only STDOUT and STDERR output)'
        )

    arguments = parser.parse_args()
    arguments.ydb_working_dir = cmds.wrap_path(arguments.ydb_working_dir)
    arguments.ydb_binary_path = cmds.wrap_path(arguments.ydb_binary_path)
    arguments.ydb_udfs_dir = cmds.wrap_path(arguments.ydb_udfs_dir)
    arguments.command(arguments)
