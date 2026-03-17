import argparse
from typing import Any

import cantools

from ..database import Bus, Database, Message, Node
from ..database.diagnostics.database import Database as DiagnosticsDatabase
from ..database.namedsignalvalue import NamedSignalValue
from .dump.formatting import signal_tree_string


def _format_val(val: float | int | str | NamedSignalValue | None,
                unit: str,
                value_format_specifier: str) \
        -> str:
    """Returns signal value formatted according to a format specifier

    e.g.
    - ``_format_val(1.234, 'm', '%.2f')`` results in '1.23 m'
    - ``_format_val('IAmAnEnum', 'm', '%.2f')`` results in 'IAmAnEnum'
    - ``_format_val(1.234)`` results in '1.234'
    """
    if val is None:
        return 'None'
    elif not unit or isinstance(val, (str, NamedSignalValue)):
        # physical value does not exhibit a unit or is an enumeration
        return f'{val:{value_format_specifier}}'

    return f'{val:{value_format_specifier}} {unit}'


def _print_message(message: Message,
                   indent: str = '',
                   print_format_specifics: bool = True,
                   values_format_specifier: str = '') \
        -> None:

    # shorten the name for the variable of the format specifier for
    # signal values
    vfs = values_format_specifier

    print(f'{indent}{message.name}:')

    if message.comments:
        for lang in message.comments:
            print(f'{indent}  Comment[{lang}]: {message.comments[lang]}')

    if message.bus_name:
        print(f'{indent}  Bus: {message.bus_name}')

    if message.senders:
        print(f'{indent}  Sending ECUs: {", ".join(sorted(message.senders))}')

    if message.header_id is None:
        print(f'{indent}  Frame ID: 0x{message.frame_id:x} ({message.frame_id})')
        if message.is_container:
            print(f'{indent}  Maximum Size: {message.length} bytes')
        else:
            print(f'{indent}  Size: {message.length} bytes')
        print(f'{indent}  Is extended frame: {message.is_extended_frame}')
        print(f'{indent}  Is CAN-FD frame: {message.is_fd}')
    else:
        print(f'{indent}  Header ID: 0x{message.header_id:x} ({message.header_id})')
        print(f'{indent}  Size: {message.length} bytes')

    if message.cycle_time is not None:
        print(f'{indent}  Cycle time: {_format_val(message.cycle_time, "ms", vfs)}')

    if print_format_specifics and message.autosar:
        print(f'{indent}  Is network management frame: {message.autosar.is_nm}')

        if message.autosar.e2e:
            e2e = message.autosar.e2e
            print(f'{indent}  End-to-end properties:')
            print(f'{indent}    Category: {e2e.category}')
            print(f'{indent}    Data IDs: {e2e.data_ids}')
            print(f'{indent}    Protected size: {e2e.payload_length} bytes')

        print(f'{indent}  Is secured: {message.autosar.is_secured}')
        secoc = message.autosar.secoc
        if secoc:
            print(f'{indent}  Security properties:')
            print(f'{indent}    Authentication algorithm: {secoc.auth_algorithm_name}')
            print(f'{indent}    Freshness algorithm: {secoc.freshness_algorithm_name}')
            print(f'{indent}    Data ID: {secoc.data_id}')
            print(f'{indent}    Authentication transmit bits: {secoc.auth_tx_bit_length}')
            print(f'{indent}    Freshness counter size: {secoc.freshness_bit_length} bits')
            print(f'{indent}    Freshness counter transmit size: {secoc.freshness_tx_bit_length} bits')
            print(f'{indent}    Secured size: {secoc.payload_length} bytes')

    if message.signals:
        print(f'{indent}  Signal tree:')
        st = signal_tree_string(message, console_width=1000*1000)
        print()
        for s in st.split('\n'):
            print(f'{indent}    {s}')
        print()

    if message.contained_messages is not None:
        print(f'{indent}  Potentially contained messages:')
        print()
        for contained_message in message.contained_messages:
            if contained_message.name is not None:
                print(f"{indent}    {contained_message.name} (0x"
                      f"{contained_message.header_id:x})")
            else:
                print(f"{indent}    (0x{contained_message.header_id:x})")
        print()

        print(f'{indent}  Potentially contained message details:')
        for contained_message in message.contained_messages:
            _print_message(contained_message,
                           '    ',
                           print_format_specifics=print_format_specifics)

    if message.signals:
        print(f'{indent}  Signal details:')

    for signal in message.signals:
        signal_type = 'Integer'
        if signal.is_float:
            signal_type = 'Float'
        elif signal.is_multiplexer and \
             signal.name in \
                 [ x.multiplexer_signal for x in message.signals]:
                signal_type = 'Multiplex Selector'

        print(f'{indent}    {signal.name}:')
        if signal.comments is not None:
            for lang in signal.comments:
                print(f'{indent}      Comment[{lang}]: {signal.comments[lang]}')
        if signal.receivers:
            print(f'{indent}      Receiving ECUs: {", ".join(sorted(signal.receivers))}')
        print(f'{indent}      Internal type: {signal_type}')
        if signal.multiplexer_signal is not None:
            print(f'{indent}      Selector signal: {signal.multiplexer_signal}')
            selector_sig = None
            selector_sig = message.get_signal_by_name(signal.multiplexer_signal)
            selector_values = []

            if isinstance(signal.multiplexer_ids, list):
                for x in signal.multiplexer_ids:
                    if selector_sig.choices and x in selector_sig.choices:
                        selector_values.append(f'{selector_sig.choices[x]}')
                    else:
                        selector_values.append(f'{x}')

            print(f'{indent}      Selector values: {", ".join(selector_values)}')

        print(f'{indent}      Start bit: {signal.start}')
        print(f'{indent}      Length: {signal.length} bits')
        print(f'{indent}      Byte order: {signal.byte_order}')
        unit = ''
        if signal.unit:
            print(f'{indent}      Unit: {signal.unit}')
            unit = f'{signal.unit}'
        if signal.initial is not None:
            print(f'{indent}      Initial value: {_format_val(signal.initial, unit, vfs)}')
        if signal.invalid is not None:
            print(f'{indent}      Invalid value: {_format_val(signal.invalid, unit, vfs)}')
        if signal.is_signed is not None:
            print(f'{indent}      Is signed: {signal.is_signed}')
        if signal.minimum is not None:
            print(f'{indent}      Minimum: {_format_val(signal.minimum, unit, vfs)}')
        if signal.maximum is not None:
            print(f'{indent}      Maximum: {_format_val(signal.maximum, unit, vfs)}')

        has_offset = signal.conversion.offset is not None and signal.conversion.offset != 0
        has_scale = \
            signal.conversion.scale is not None \
            and (signal.conversion.scale > 1 + 1e-10 or signal.conversion.scale < 1 - 1e-10)
        if has_offset or has_scale:
            offset = signal.conversion.offset if signal.conversion.offset is not None else 0
            print(f'{indent}      Offset: {_format_val(offset, unit, vfs)}')

            scale = signal.conversion.scale if signal.conversion.scale is not None else 1
            print(f'{indent}      Scaling factor: {_format_val(scale, unit, vfs)}')

        if signal.choices:
            print(f'{indent}      Named values:')
            for value, choice in signal.choices.items():
                print(f'{indent}        {value}: {choice}')
                if isinstance(choice, NamedSignalValue):
                    for lang, description in choice.comments.items():
                        print(f'{indent}          Comment[{lang}]: {description}')

def _print_node(node: Node) -> None:
    print(f'{node.name}:')

    if node.comments:
        for lang in node.comments:
            print(f'  Comment[{lang}]: {node.comments[lang]}')

def _print_bus(bus: Bus) -> None:
    print(f'{bus.name}:')

    if bus.comments:
        for lang in bus.comments:
            print(f'  Comment[{lang}]: {bus.comments[lang]}')

    if bus.baudrate is not None:
        print(f'  Baudrate: {bus.baudrate}')

    if bus.fd_baudrate is not None:
        print(f'  CAN-FD enabled: True')
        print(f'  FD Baudrate: {bus.fd_baudrate}')
    else:
        print(f'  CAN-FD enabled: False')

def _do_list(args: Any, values_format_specifier: str='') -> None:
    input_file_name = args.input_file_name[0]
    prune = args.prune
    no_strict = args.no_strict
    print_buses = args.print_buses
    print_nodes = args.print_nodes

    can_db = cantools.database.load_file(input_file_name,
                                         prune_choices=prune,
                                         strict=not no_strict)

    if isinstance(can_db, DiagnosticsDatabase):
        print('The "list" subcommand only works with non-diagnostic database '
              'files!')
        return

    if print_buses:
        _do_list_buses(can_db, args)
    elif print_nodes:
        _do_list_nodes(can_db, args)
    else:
        _do_list_messages(can_db, args, values_format_specifier)

def _do_list_buses(can_db: Database,
                   args: Any) -> None:
    bus_names = args.items

    for bus in can_db.buses:
        if bus_names and bus.name not in bus_names:
            continue

        _print_bus(bus)

def _do_list_nodes(can_db: Database,
                   args: Any) -> None:
    node_names = args.items

    for node in can_db.nodes:
        if node_names and node.name not in node_names:
            continue

        _print_node(node)

def _do_list_messages(can_db: Database,
                      args: Any,
                      values_format_specifier: str) -> None:
    message_names = args.items
    print_all = args.print_all
    exclude_extended = args.exclude_extended
    exclude_normal = args.exclude_normal
    print_format_specifics = not args.skip_format_specifics

    if print_all:
        # if no messages have been specified, we print the list of
        # messages in the database
        for message in can_db.messages:
            if message.is_extended_frame and exclude_extended:
                continue
            if not message.is_extended_frame and exclude_normal:
                continue
            message_names.append(message.name)

        message_names.sort()

    if not message_names:
        # if no messages have been specified, we print the list of
        # messages in the database
        message_names = []
        for message in can_db.messages:
            if message.is_extended_frame and exclude_extended:
                continue
            if not message.is_extended_frame and exclude_normal:
                continue

            message_names.append(message.name)

        message_names.sort()
        for message_name in message_names:
            print(message_name)

        return
    else:
        # if a list of messages has been specified, the details of these
        # are printed.
        for message_name in message_names:
            try:
                message = can_db.get_message_by_name(message_name)
            except KeyError:
                print(f'No message named "{message_name}" has been found in input file.')
                continue

            _print_message(message,
                           print_format_specifics=print_format_specifics,
                           values_format_specifier=values_format_specifier)



def add_subparser(subparsers: argparse._SubParsersAction) -> None:
    list_parser = subparsers.add_parser(
        'list',
        description=('Print the contents of a bus description file in an easy '
                     'to process and humanly readable format. This is similar '
                     'to "dump" with the output being less pretty but more '
                     'complete and much easier to process by shell scripts.'),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    list_parser.add_argument(
        '-n', '--exclude-normal',
        default=False,
        action='store_const',
        const=True,
        required=False,
        help='Do not print non-extended CAN messages.')
    list_parser.add_argument(
        '-x', '--exclude-extended',
        default=False,
        action='store_const',
        const=True,
        required=False,
        help='Do not print extended CAN messages.')
    list_parser.add_argument(
        '--skip-format-specifics',
        default=False,
        action='store_const',
        const=True,
        required=False,
        help=('Do not print any information which is specific to the format '
              'used by the database file.'))
    list_parser.add_argument(
        '-a', '--all',
        default=False,
        action='store_const',
        dest="print_all",
        const=True,
        required=False,
        help='Print detailed infos for all messages found in the input file.')
    list_parser.add_argument(
        '-b', '--buses',
        default=False,
        action='store_const',
        dest="print_buses",
        const=True,
        required=False,
        help='Print information about the buses described by the input file.')
    list_parser.add_argument(
        '-c', '--nodes',
        default=False,
        action='store_const',
        dest="print_nodes",
        const=True,
        required=False,
        help='Print information about the CAN nodes described by the input file.')
    list_parser.add_argument(
        '--prune',
        action='store_true',
        help='Try to shorten the names of named signal choices.')
    list_parser.add_argument(
        '--no-strict',
        action='store_true',
        help='Skip database consistency checks.')
    list_parser.add_argument('input_file_name', metavar='FILE', nargs=1)
    list_parser.add_argument(
        'items',
        metavar='[BUSES|MESSAGES]',
        nargs='*',
        help='The names of the buses or messages which shall be inspected')

    list_parser.set_defaults(func=_do_list)
