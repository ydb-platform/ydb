import argparse
import logging
import sys

from argparse_addons import Integer  # type: ignore

from .. import database, logreader
from .__utils__ import format_message_by_frame_id

logging.basicConfig(level=logging.WARNING)

def _do_decode(args):
    dbase = database.load_file(args.database,
                               encoding=args.encoding,
                               frame_id_mask=args.frame_id_mask,
                               prune_choices=args.prune,
                               strict=not args.no_strict)
    decode_choices = not args.no_decode_choices
    decode_containers = not args.no_decode_containers
    allow_truncated = args.no_strict
    allow_excess = args.no_strict
    parser = logreader.Parser(sys.stdin)
    for line, frame in parser.iterlines(keep_unknowns=True):
        if frame is not None:
            line += ' ::'
            line += format_message_by_frame_id(dbase,
                                               frame.frame_id,
                                               frame.data,
                                               decode_choices,
                                               args.single_line,
                                               decode_containers,
                                               allow_truncated=allow_truncated,
                                               allow_excess=allow_excess)

        print(line)


def add_subparser(subparsers):
    decode_parser = subparsers.add_parser(
        'decode',
        description=('Decode "candump" CAN frames read from standard input '
                     'and print them in a human readable format.'),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    decode_parser.add_argument(
        '-c', '--no-decode-choices',
        action='store_true',
        help='Do not convert scaled values to choice strings.')
    decode_parser.add_argument(
        '-t', '--no-decode-containers',
        action='store_true',
        help='Do not decode container messages.')
    decode_parser.add_argument(
        '-s', '--single-line',
        action='store_true',
        help='Print the decoded message on a single line.')
    decode_parser.add_argument(
        '-e', '--encoding',
        help='File encoding.')
    decode_parser.add_argument(
        '--prune',
        action='store_true',
        help='Try to shorten the names of named signal choices.')
    decode_parser.add_argument(
        '--no-strict',
        action='store_true',
        help='Skip database consistency checks.')
    decode_parser.add_argument(
        '-m', '--frame-id-mask',
        type=Integer(0),
        help=('Only compare selected frame id bits to find the message in the '
              'database. By default the candump and database frame ids must '
              'be equal for a match.'))
    decode_parser.add_argument(
        'database',
        help='Database file.')
    decode_parser.set_defaults(func=_do_decode)
