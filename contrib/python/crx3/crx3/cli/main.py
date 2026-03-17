# -*- coding: utf-8 -*-
import sys

from crx3.cli import argparser, cmd_create, cmd_verify
from crx3.cli.argparser import Subcommand


def main():
    args = argparser.parse_args(sys.argv[1:])
    if args is None:
        return 1
    # print(args)
    subcommand = args.command
    if subcommand == Subcommand.CREATE:
        return cmd_create.create(args.source, args.private_key_file, args.output_file, args.verbose)
    elif subcommand == Subcommand.VERIFY:
        return cmd_verify.verify(args.crx_file, args.verbose)
    return 0


if __name__ == '__main__':
    sys.exit(main())
