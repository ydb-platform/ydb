import argparse
import os
import sys

from ... import database
from ...database.can.database import Database as CanDatabase
from ...database.diagnostics.database import Database as DiagnosticsDatabase
from ...database.utils import format_and
from ...j1939 import frame_id_unpack, is_pdu_format_1, pgn_pack
from . import formatting


def _print_j1939_frame_id(message):
    unpacked = frame_id_unpack(message.frame_id)

    print(f'      Priority:       {unpacked.priority}')

    if is_pdu_format_1(unpacked.pdu_format):
        pdu_format = 'PDU 1'
        pdu_specific = 0
        destination = f'0x{unpacked.pdu_specific:02x}'
    else:
        pdu_format = 'PDU 2'
        pdu_specific = unpacked.pdu_specific
        destination = 'All'

    print('      PGN:            0x{:05x}'.format(
        pgn_pack(unpacked.reserved,
                 unpacked.data_page,
                 unpacked.pdu_format,
                 pdu_specific)))
    print(f'      Source:         0x{unpacked.source_address:02x}')
    print(f'      Destination:    {destination}')
    print(f'      Format:         {pdu_format}')

def _dump_can_message(message, with_comments=False, name_prefix='', WIDTH=None):
    cycle_time = message.cycle_time
    signal_choices_string = formatting.signal_choices_string(message)

    if cycle_time is None:
        cycle_time = '-'

    if len(message.senders) == 0:
        message.senders.append('-')

    print()
    print(f'  Name:           {name_prefix}{message.name}')
    if message.frame_id is not None and not name_prefix:
        # only print the arbitration ID for top-level messages
        print(f'  Id:             0x{message.frame_id:x}')
    if message.header_id is not None and name_prefix:
        # only print the header ID for child messages
        print(f'  Header id:      0x{message._header_id:06x}')

    if message.protocol == 'j1939':
        _print_j1939_frame_id(message)

    if message.is_container:
        print(f'  Maximum length: {message.length} bytes')
    else:
        print(f'  Length:         {message.length} bytes')

    print(f'  Cycle time:     {cycle_time} ms')
    print(f'  Senders:        {format_and(message.senders)}')
    if message.is_container:
        print('  Possibly contained children:')
        print()
        for child in message.contained_messages:
            print(f'      {message.name} :: {child.name}')
        print()
    else:
        print('  Layout:')
        print()
        print('\n'.join([
            ('    ' + line).rstrip()
            for line in formatting.layout_string(message).splitlines()
        ]))
        print()
        print('  Signal tree:')
        print()
        print('\n'.join([
            ('    ' + line).rstrip()
            for line in formatting.signal_tree_string(message, WIDTH, with_comments=with_comments).splitlines()
        ]))
        print()

        if signal_choices_string:
            print('  Signal choices:')
            print('\n'.join([
                ('    ' + line).rstrip()
                for line in signal_choices_string.splitlines()
            ]))
            print()

    print('  ' + 72 * '-')

    if message.is_container:
        # dump the layout of the child messages of the container
        for child in message.contained_messages:
            _dump_can_message(child,
                              with_comments=with_comments,
                              WIDTH=WIDTH,
                              name_prefix=f'{message.name} :: ')

def _dump_can_database(dbase, with_comments=False):
    WIDTH = 80
    try:
        WIDTH, _ = os.get_terminal_size()
    except OSError:
        pass

    print('================================= Messages =================================')
    print()
    print('  ' + 72 * '-')

    for message in dbase.messages:
        _dump_can_message(message,
                          with_comments=with_comments,
                          WIDTH=WIDTH)



def _dump_diagnostics_database(dbase):
    print('=================================== Dids ===================================')
    print()
    print('  ' + 72 * '-')

    for did in dbase.dids:
        print()
        print(f'  Name:       {did.name}')
        print(f'  Length:     {did.length} bytes')
        print('  Layout:')
        print()

        for data in did.datas:
            print(f'    Name:      {data.name}')
            print(f'    Start bit: {data.start}')
            print(f'    Length:    {data.length}')
            print()

        print()
        print('  ' + 72 * '-')


def _do_dump(args):
    dbase = database.load_file(args.database,
                               encoding=args.encoding,
                               prune_choices=args.prune,
                               strict=not args.no_strict)

    if isinstance(dbase, CanDatabase):
        _dump_can_database(dbase, args.with_comments)
    elif isinstance(dbase, DiagnosticsDatabase):
        _dump_diagnostics_database(dbase)
    else:
        sys.exit('Unsupported database type.')


def add_subparser(subparsers):
    dump_parser = subparsers.add_parser(
        'dump',
        description='Dump given database in a human readable format.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    dump_parser.add_argument(
        '-e', '--encoding',
        help='File encoding.')
    dump_parser.add_argument(
        '--prune',
        action='store_true',
        help='Try to shorten the names of named signal choices.')
    dump_parser.add_argument(
        '--no-strict',
        action='store_true',
        help='Skip database consistency checks.')
    dump_parser.add_argument(
        'database',
        help='Database file.')
    dump_parser.add_argument('--with-comments', action='store_true', default=False)
    dump_parser.set_defaults(func=_do_dump)
