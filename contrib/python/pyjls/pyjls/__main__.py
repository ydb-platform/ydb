#!/usr/bin/env python3
# Copyright 2018-2022 Jetperch LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""jls command-line utility."""


import os
import sys
import argparse
import logging
import traceback
from . import entry_points


_LOG_LEVELS = {
    'OFF': 100,
    'CRITICAL': logging.CRITICAL,
    'ERROR': logging.ERROR,
    'WARNING': logging.WARNING,
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG,
    'ALL': 0,
}

_EPILOG = f"""\
Set the PYJLS_LOG_LEVEL environment variable to change the logging level.
Options are [{', '.join(_LOG_LEVELS.keys())}].
The default is WARNING.
"""


def get_parser():
    parser = argparse.ArgumentParser(
        description='jls command line tools.',
        epilog=_EPILOG,
    )
    subparsers = parser.add_subparsers(
        dest='subparser_name',
        help='The command to execute')

    for entry_point in entry_points.__all__:
        default_name = entry_point.__name__.split('.')[-1]
        name = getattr(entry_point, 'NAME', default_name)
        cfg_fn = entry_point.parser_config
        p = subparsers.add_parser(name, help=cfg_fn.__doc__)
        cmd_fn = cfg_fn(p)
        if not callable(cmd_fn):
            raise ValueError(f'Invalid command function for {name}')
        p.set_defaults(func=cmd_fn)

    subparsers.add_parser('help', help='Display the command help. Use [command] --help to display help for a specific command.')

    return parser


def run():
    log_level = os.environ.get('PYJLS_LOG_LEVEL', 'WARNING').upper()
    log_level = _LOG_LEVELS.get(log_level, logging.WARNING)
    logging.basicConfig(level=log_level,
                        format="%(levelname)s:%(asctime)s:%(filename)s:%(lineno)d:%(name)s:%(message)s")
    parser = get_parser()
    args = parser.parse_args()
    if args.subparser_name is None or args.subparser_name.lower() in ['help']:
        parser.print_help()
        parser.exit()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(run())
