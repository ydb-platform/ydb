import sys
import argparse

import openhtf as htf

from openhtf.core.test_descriptor import create_arg_parser

def spintop_create_arg_parser(add_help=False):
    parser = argparse.ArgumentParser(
          'Spintop OpenHTF - The extension to OpenHTF', parents=[create_arg_parser()],
          add_help=add_help)
    parser.add_argument(
        '--coverage', action='store_true',
        help='Print the list of covered components')
    return parser

class Test(htf.Test):
    ARG_PARSER_FACTORY = spintop_create_arg_parser

    def configure(self, **kwargs):
        # This calls known_args_hook
        super(Test, self).configure(**kwargs)

    def known_args_hook(self, known_args):
        """ Check for known args and perform possible effect instead of executing the test.
        Cancel execution using sys.exit(0)
        """
        if known_args.coverage:
            sys.exit(0)
        super(Test, self).known_args_hook(known_args)
