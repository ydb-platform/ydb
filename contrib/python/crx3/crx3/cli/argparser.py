# -*- coding: utf-8 -*-
import argparse

from crx3.__version__ import VERSION


class Subcommand(str):
    CREATE = 'create'
    VERIFY = 'verify'


def parse_args(args):
    parser = argparse.ArgumentParser(
        prog='crx3',
        description='Chrome extension (crx) packaging & parsing tool.')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s ' + str(VERSION))

    subparsers = parser.add_subparsers(title='commands',
                                       description='You can create or parse a crx file using the following commands.',
                                       dest='command')
    create_parser = subparsers.add_parser(Subcommand.CREATE, help='create a crx file from a zip file or a directory')
    create_parser.add_argument('source', type=str, help='zip file or directory to be packed')
    create_parser.add_argument('-pk', '--private-key', type=str, required=False, default='',
                               dest='private_key_file',
                               help='private key file to be used for signing. If not specified, the program '
                                    'automatically creates a new one and saves it to the same directory as the crx '
                                    'file')
    create_parser.add_argument('-o', '--output', type=str, required=False, default='',
                               dest='output_file',
                               help='path to the output crx file')
    create_parser.add_argument('-v', '--verbose', action='store_true', required=False, default=False,
                               dest='verbose',
                               help='print more information')

    verify_parser = subparsers.add_parser(Subcommand.VERIFY,
                                          help='verify that a crx file is a valid chrome extension file')
    verify_parser.add_argument('crx_file', type=str, help='crx file')
    verify_parser.add_argument('-v', '--verbose', action='store_true', required=False, default=False,
                               dest='verbose',
                               help='print more information')

    args = parser.parse_args(args)
    if not args.command:
        parser.print_help()
        return None
    return args
