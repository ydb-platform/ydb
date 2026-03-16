# Load and dump a CAN database in SYM format.

import collections
import logging
import re
from collections import OrderedDict as odict
from collections.abc import Callable
from itertools import groupby
from typing import TYPE_CHECKING

import textparser  # type: ignore
from textparser import (
    Any,
    DelimitedList,
    Optional,
    Sequence,
    Token,
    TokenizeError,
    ZeroOrMore,
    ZeroOrMoreDict,
    choice,
    tokenize_init,
)

from ...conversion import BaseConversion
from ...errors import ParseError
from ...namedsignalvalue import NamedSignalValue
from ...utils import (
    SORT_SIGNALS_DEFAULT,
    sort_signals_by_start_bit,
    type_sort_signals,
)
from ..internal_database import InternalDatabase
from ..message import Message
from ..signal import Signal
from .utils import num

if TYPE_CHECKING:
    from collections.abc import Iterator

LOGGER = logging.getLogger(__name__)

# PCAN Symbol Editor will fail to open a SYM File with signals of a longer length
MAX_SIGNAL_NAME_LENGTH = 32
# If a message is in the SEND section of a SYM file, it is sent by the ECU
SEND_MESSAGE_SENDER = 'ECU'
# If a message is in the RECEIVE section of a SYM file, it is sent by the Peripheral devices
RECEIVE_MESSAGE_SENDER = 'Peripherals'


class Parser60(textparser.Parser):
    """Create the SYM 6.0 parser.

    """

    KEYWORDS = {
        'FormatVersion',
        'Title',
        'UniqueVariables',
        'FloatDecimalPlaces',
        'BRS',
        'Enum',
        'Sig',
        'ID',
        'Len',
        'Mux',
        'CycleTime',
        'Timeout',
        'MinInterval',
        'Color',
        'Var',
        'Type'
    }

    def tokenize(self, string):
        names = {
            'LPAREN':      '(',
            'RPAREN':      ')',
            'LBRACE':      '[',
            'RBRACE':      ']',
            'COMMA':       ',',
            'ASSIGN':      '=',
            'ENUMS':       '{ENUMS}',
            'SIGNALS':     '{SIGNALS}',
            'SEND':        '{SEND}',
            'RECEIVE':     '{RECEIVE}',
            'SENDRECEIVE': '{SENDRECEIVE}',
            'U':           '/u:',
            'F':           '/f:',
            'O':           '/o:',
            'MIN':         '/min:',
            'MAX':         '/max:',
            'SPN':         '/spn:',
            'D':           '/d:',
            'LN':          '/ln:',
            'E':           '/e:',
            'P':           '/p:',
            'M':           '-m',
            'H':           '-h',
            'B':           '-b',
            'S':           '-s',
            'T':           '-t',
            'V':           '-v',
            'DP':          '-p'
        }

        re_string = r'"(\\"|[^"])*?"'

        token_specs = [
            ('SKIP',               r'[ \r\n\t]+'),
            ('COMMENT',            r'//.*?\n'),
            ('HEXNUMBER',          r'-?\d+\.?[0-9A-F]*([eE][+-]?\d+)?(h)'),
            ('NUMBER',             r'-?\d+(\.\d+)?([eE][+-]?\d+)?'),
            ('STRING',             re_string),
            ('U',                  fr'/u:({re_string}|\S+)'),
            ('F',                  r'/f:'),
            ('O',                  r'/o:'),
            ('MIN',                r'/min:'),
            ('MAX',                r'/max:'),
            ('SPN',                r'/spn:'),
            ('D',                  r'/d:'),
            ('LN',                 r'/ln:'),
            ('E',                  r'/e:'),
            ('P',                  r'/p:'),
            ('M',                  r'\-m'),
            ('H',                  r'\-h'),
            ('B',                  r'\-b'),
            ('S',                  r'\-s'),
            ('T',                  r'\-t'),
            ('V',                  r'\-v'),
            ('DP',                 r'\-p'),
            ('LPAREN',             r'\('),
            ('RPAREN',             r'\)'),
            ('LBRACE',             r'\['),
            ('RBRACE',             r'\]'),
            ('COMMA',              r','),
            ('ASSIGN',             r'='),
            ('ENUMS',              r'\{ENUMS\}'),
            ('SIGNALS',            r'\{SIGNALS\}'),
            ('SEND',               r'\{SEND\}'),
            ('RECEIVE',            r'\{RECEIVE\}'),
            ('SENDRECEIVE',        r'\{SENDRECEIVE\}'),
            ('WORD',               r'[^\s=\(\]\-]+'),
            ('MISMATCH',           r'.')
        ]

        tokens, token_regex = tokenize_init(token_specs)

        for mo in re.finditer(token_regex, string, re.DOTALL):
            kind = mo.lastgroup

            if kind == 'SKIP':
                pass
            elif kind == 'STRING':
                value = mo.group(kind)[1:-1].replace('\\"', '"')
                tokens.append(Token(kind, value, mo.start()))
            elif kind != 'MISMATCH':
                value = mo.group(kind)

                if value in self.KEYWORDS:
                    kind = value

                if kind in names:
                    kind = names[kind]

                tokens.append(Token(kind, value, mo.start()))
            else:
                raise TokenizeError(string, mo.start())

        return tokens

    def grammar(self):
        word = choice('WORD', *list(self.KEYWORDS))
        version = Sequence('FormatVersion', '=', 'NUMBER', 'COMMENT')
        title = Sequence('Title' , '=', 'STRING')
        unique_variables = Sequence('UniqueVariables' , '=', word)
        float_decimal_places = Sequence('FloatDecimalPlaces' , '=', 'NUMBER')
        bit_rate_switch = Sequence('BRS' , '=', word)

        enum_value = Sequence('NUMBER', '=', 'STRING')
        delim = Sequence(',', Optional('COMMENT'))
        enum = Sequence('Enum', '=', word,
                        '(', Optional(DelimitedList(enum_value, delim=delim)), ')',
                        Optional('COMMENT'))

        sig_unit = '/u:'
        sig_factor = Sequence('/f:', 'NUMBER')
        sig_offset = Sequence('/o:', 'NUMBER')
        sig_min = Sequence('/min:', 'NUMBER')
        sig_max = Sequence('/max:', 'NUMBER')
        sig_spn = Sequence('/spn:', 'NUMBER')
        sig_default = Sequence('/d:', choice('NUMBER', 'WORD'))
        sig_long_name = Sequence('/ln:', 'STRING')
        sig_enum = Sequence('/e:', word)
        sig_places = Sequence('/p:', 'NUMBER')

        signal = Sequence('Sig', '=', Any(), word,
                          Optional('NUMBER'),
                          Optional(choice('-h', '-b')),
                          Optional('-m'),
                          ZeroOrMore(choice(sig_unit,
                                            sig_factor,
                                            sig_offset,
                                            sig_min,
                                            sig_max,
                                            sig_default,
                                            sig_long_name,
                                            sig_enum,
                                            sig_places,
                                            sig_spn)),
                          Optional('COMMENT'))

        variable = Sequence('Var', '=', Any(), word,
                            'NUMBER', ',', 'NUMBER',
                            ZeroOrMore(choice('-v', '-m', '-s', '-h')),
                            ZeroOrMore(choice(sig_unit,
                                              sig_factor,
                                              sig_offset,
                                              sig_min,
                                              sig_max,
                                              sig_default,
                                              sig_long_name,
                                              sig_enum,
                                              sig_places)),
                            Optional('COMMENT'))

        symbol = Sequence('[', Any(), ']',
                          ZeroOrMoreDict(choice(
                              Sequence('ID', '=', 'HEXNUMBER',
                                       Optional('HEXNUMBER'),
                                       Optional('COMMENT')),
                              Sequence('Len', '=', 'NUMBER'),
                              Sequence('Mux', '=', Any(), 'NUMBER', ',',
                                       'NUMBER', choice('NUMBER', 'HEXNUMBER'),
                                       ZeroOrMore(choice('-t', '-m')),
                                       Optional('COMMENT')),
                              Sequence('CycleTime', '=', 'NUMBER', Optional('-p')),
                              Sequence('Timeout', '=', 'NUMBER'),
                              Sequence('MinInterval', '=', 'NUMBER'),
                              Sequence('Color', '=', 'HEXNUMBER'),
                              variable,
                              Sequence('Sig', '=', Any(), 'NUMBER'),
                              Sequence('Type', '=', Any()))))

        enums = Sequence('{ENUMS}', ZeroOrMore(choice(enum, 'COMMENT')))
        signals = Sequence('{SIGNALS}', ZeroOrMore(choice(signal, 'COMMENT')))
        send = Sequence('{SEND}', ZeroOrMore(choice(symbol, 'COMMENT')))
        receive = Sequence('{RECEIVE}', ZeroOrMore(choice(symbol, 'COMMENT')))
        sendreceive = Sequence('{SENDRECEIVE}', ZeroOrMore(choice(symbol, 'COMMENT')))

        section = choice(enums,
                         signals,
                         send,
                         receive,
                         sendreceive)

        grammar = Sequence(Optional('COMMENT'),
                           version,
                           ZeroOrMore(choice(unique_variables,
                                             float_decimal_places,
                                             title,
                                             bit_rate_switch)),
                           ZeroOrMore(section))

        return grammar


def _get_section_tokens(tokens, name):
    rows = []
    for section in tokens[3]:
        if section[0] == name:
            rows.extend([row for row in section[1] if isinstance(row, list)])

    return rows


def _load_comment(tokens):
    return tokens[3:].rstrip('\r\n')


def _get_enum(enums, name):
    try:
        return enums[name]
    except KeyError:
        raise ParseError(f"Enum '{name}' is not defined.") from None


def _load_enums(tokens):
    section = _get_section_tokens(tokens, '{ENUMS}')
    all_enums = {}

    for _, _, name, _, values, _, _ in section:
        if values:
            values = values[0]

        enum = odict()
        for v in values:
            value = num(v[0])
            value_name = v[2]
            enum[value] = NamedSignalValue(value, value_name)

        all_enums[name] = enum

    return all_enums


def _load_signal_type_and_length(type_, tokens, enums):
    # Default values.
    is_signed = False
    is_float = False
    length = 0
    enum = None
    minimum = None
    maximum = None

    if type_ == 'signed':
        is_signed = True
        length = int(tokens[0])
    elif type_ == 'unsigned':
        length = int(tokens[0])
    elif type_ == 'float':
        is_float = True
        length = 32
    elif type_ == 'double':
        is_float = True
        length = 64
    elif type_ == 'bit':
        # As unsigned integer for now.
        length = 1
        minimum = 0
        maximum = 1
    elif type_ == 'char':
        # As unsigned integer for now.
        length = 8
    elif type_ in ['string', 'raw']:
        # As unsigned integer for now.
        length = int(tokens[0])
    else:
        # Enum. As unsigned integer for now.
        length = int(tokens[0])
        enum = _get_enum(enums, type_)

    return is_signed, is_float, length, enum, minimum, maximum


def _load_signal_attributes(tokens, enum, enums, minimum, maximum, spn):
    # Default values.
    factor = 1
    offset = 0
    unit = None

    for item in tokens:
        if isinstance(item, list):
            key, value = item

            if key == '/f:':
                factor = num(value)
            elif key == '/o:':
                offset = num(value)
            elif key == '/min:':
                minimum = num(value)
            elif key == '/max:':
                maximum = num(value)
            elif key == '/e:':
                enum = _get_enum(enums, value)
            elif key == '/spn:':
                spn = int(value)
            else:
                LOGGER.debug("Ignoring unsupported message attribute '%s'.", key)
        elif item.startswith('/u:"'):
            unit = item[4:-1]
        elif item.startswith('/u:'):
            unit = item[3:]
        else:
            raise ParseError(f'Internal error {item}.')

    return unit, factor, offset, enum, minimum, maximum, spn


def _load_signal(tokens, enums):
    # Default values.
    name = tokens[2]
    byte_order = 'little_endian'
    comment = None
    spn = None

    # Type and length.
    (is_signed,
     is_float,
     length,
     enum,
     minimum,
     maximum) = _load_signal_type_and_length(tokens[3],
                                             tokens[4],
                                             enums)

    # Byte order.
    if tokens[6] == ['-m']:
        byte_order = 'big_endian'

    # Comment.
    if tokens[8]:
        comment = _load_comment(tokens[8][0])

    # The rest.
    unit, factor, offset, enum, minimum, maximum, spn = _load_signal_attributes(
        tokens[7],
        enum,
        enums,
        minimum,
        maximum,
        spn)

    conversion = BaseConversion.factory(
        scale=factor,
        offset=offset,
        choices=enum,
        is_float=is_float,
    )

    return Signal(name=name,
                  start=offset,
                  length=length,
                  receivers=[],
                  byte_order=byte_order,
                  is_signed=is_signed,
                  conversion=conversion,
                  minimum=minimum,
                  maximum=maximum,
                  unit=unit,
                  comment=comment,
                  is_multiplexer=False,
                  spn=spn)


def _load_signals(tokens, enums):
    section = _get_section_tokens(tokens, '{SIGNALS}')
    signals = {}

    for signal in section:
        signal = _load_signal(signal, enums)
        signals[signal.name] = signal

    return signals


def _load_message_signal(tokens,
                         signals,
                         multiplexer_signal,
                         multiplexer_ids):
    signal = signals[tokens[2]]
    start = int(tokens[3])
    start = _convert_start(start, signal.byte_order)

    conversion = BaseConversion.factory(
        scale=signal.scale,
        offset=signal.offset,
        choices=signal.choices,
        is_float=signal.is_float,
    )

    return Signal(name=signal.name,
                  start=start,
                  length=signal.length,
                  receivers=signal.receivers,
                  byte_order=signal.byte_order,
                  is_signed=signal.is_signed,
                  conversion=conversion,
                  minimum=signal.minimum,
                  maximum=signal.maximum,
                  unit=signal.unit,
                  comment=signal.comment,
                  is_multiplexer=signal.is_multiplexer,
                  multiplexer_ids=multiplexer_ids,
                  multiplexer_signal=multiplexer_signal,
                  spn=signal.spn)

def _convert_start(start, byte_order):
    if byte_order == 'big_endian':
        start = (8 * (start // 8) + (7 - (start % 8)))
    return start

def _load_message_variable(tokens,
                           enums,
                           multiplexer_signal,
                           multiplexer_ids):
    # Default values.
    name = tokens[2]
    byte_order = 'little_endian'
    start = int(tokens[4])
    comment = None
    spn = None

    # Type and length.
    (is_signed,
     is_float,
     length,
     enum,
     minimum,
     maximum) = _load_signal_type_and_length(tokens[3],
                                             [tokens[6]],
                                             enums)

    # Byte order.
    if '-m' in tokens[7]:
        byte_order = 'big_endian'

    # Comment.
    if tokens[9]:
        comment = _load_comment(tokens[9][0])

    # The rest.
    unit, factor, offset, enum, minimum, maximum, spn = _load_signal_attributes(
        tokens[8],
        enum,
        enums,
        minimum,
        maximum,
        spn)

    start = _convert_start(start, byte_order)

    conversion = BaseConversion.factory(
        scale=factor,
        offset=offset,
        choices=enum,
        is_float=is_float,
    )

    return Signal(name=name,
                  start=start,
                  length=length,
                  receivers=[],
                  byte_order=byte_order,
                  is_signed=is_signed,
                  conversion=conversion,
                  minimum=minimum,
                  maximum=maximum,
                  unit=unit,
                  comment=comment,
                  is_multiplexer=False,
                  multiplexer_ids=multiplexer_ids,
                  multiplexer_signal=multiplexer_signal,
                  spn=spn)


def _load_message_signals_inner(message_tokens,
                                signals,
                                enums,
                                multiplexer_signal=None,
                                multiplexer_ids=None):
    return [
        _load_message_signal(signal,
                             signals,
                             multiplexer_signal,
                             multiplexer_ids)
        for signal in message_tokens[3].get('Sig', [])
    ] + [
        _load_message_variable(variable,
                               enums,
                               multiplexer_signal,
                               multiplexer_ids)
        for variable in message_tokens[3].get('Var', [])
    ]


def _load_muxed_message_signals(message_tokens,
                                message_section_tokens,
                                signals,
                                enums):
    def get_mutliplexer_ids(mux_tokens):
        base = 10
        mux_id = mux_tokens[6]
        if mux_id.endswith('h'):
            base = 16
            mux_id = mux_id[:-1]

        return [int(mux_id, base=base)]

    mux_tokens = message_tokens[3]['Mux'][0]
    multiplexer_signal = mux_tokens[2]
    if '-m' in mux_tokens[7]:
        byte_order = 'big_endian'
    else:
        byte_order = 'little_endian'
    start = int(mux_tokens[3])
    start = _convert_start(start, byte_order)
    if mux_tokens[8]:
        comment = _load_comment(mux_tokens[8][0])
    else:
        comment = None
    result = [
        Signal(name=multiplexer_signal,
               start=start,
               length=int(mux_tokens[5]),
               byte_order=byte_order,
               is_multiplexer=True,
               comment=comment,
        )
    ]

    multiplexer_ids = get_mutliplexer_ids(mux_tokens)
    result += _load_message_signals_inner(message_tokens,
                                          signals,
                                          enums,
                                          multiplexer_signal,
                                          multiplexer_ids)

    for tokens in message_section_tokens:
        if tokens[1] == message_tokens[1] and tokens != message_tokens:
            mux_tokens = tokens[3]['Mux'][0]
            multiplexer_ids = get_mutliplexer_ids(mux_tokens)
            result += _load_message_signals_inner(tokens,
                                                  signals,
                                                  enums,
                                                  multiplexer_signal,
                                                  multiplexer_ids)

    return result


def _is_multiplexed(message_tokens):
    return 'Mux' in message_tokens[3]


def _load_message_signals(message_tokens,
                          message_section_tokens,
                          signals,
                          enums):
    if _is_multiplexed(message_tokens):
        return _load_muxed_message_signals(message_tokens,
                                           message_section_tokens,
                                           signals,
                                           enums)
    else:
        return _load_message_signals_inner(message_tokens,
                                           signals,
                                           enums)


def _get_senders(section_name: str) -> list[str]:
    """Generates a list of senders for a message based on the Send, Receive or Send/Receive
    flag defined in the SYM file. Since the Message object only has a senders property on it,
    it is easiest to translate Send flags into a sender named 'ECU', and translate Receive flags
    into a sender named 'Peripherals'. This is not the cleanest representation of the data,
    however, SYM files are unique in only having a Send, Receive or Send/Receive Direction. Most
    other file formats specify a list of custom-named sending devices
    """
    if section_name == '{SEND}':
        return [SEND_MESSAGE_SENDER]
    elif section_name == '{RECEIVE}':
        return [RECEIVE_MESSAGE_SENDER]
    elif section_name == '{SENDRECEIVE}':
        return [SEND_MESSAGE_SENDER, RECEIVE_MESSAGE_SENDER]
    else:
        raise ValueError(f'Unexpected message section named {section_name}')

def _load_message(frame_id,
                  is_extended_frame,
                  message_tokens,
                  message_section_tokens,
                  signals,
                  enums,
                  strict,
                  sort_signals,
                  section_name):
    #print(message_tokens)
    # Default values.
    name = message_tokens[1]
    length = 8
    cycle_time = None
    comment = None

    if 'Len' in message_tokens[3]:
        length = int(message_tokens[3]['Len'][0][2])

    # Cycle time.
    try:
        cycle_time = num(message_tokens[3]['CycleTime'][0][2])
    except (KeyError, IndexError):
        pass

    # Comment.
    if message_tokens[3]['ID'][0][-1]:
        comment = _load_comment(message_tokens[3]['ID'][0][-1][0])

    return Message(frame_id=frame_id,
                   is_extended_frame=is_extended_frame,
                   name=name,
                   length=length,
                   unused_bit_pattern=0xff,
                   senders=_get_senders(section_name),
                   send_type=None,
                   cycle_time=cycle_time,
                   signals=_load_message_signals(message_tokens,
                                                 message_section_tokens,
                                                 signals,
                                                 enums),
                   comment=comment,
                   bus_name=None,
                   strict=strict,
                   sort_signals=sort_signals)


def _parse_message_frame_ids(message):
    def to_int(string):
        return int(string, 16)

    def is_extended_frame(string, type_str):
        # Length of 9 includes terminating 'h' for hex
        return len(string) == 9 or type_str.lower() in ['extended', 'fdextended']

    message = message[3]

    message_id = message['ID'][0]
    minimum = to_int(message_id[2][:-1])

    if message_id[3]:
        maximum = to_int(message_id[3][0][1:-1])
    else:
        maximum = minimum

    frame_ids = range(minimum, maximum + 1)

    message_type = 'Standard'
    if 'Type' in message:
        message_type = message['Type'][0][2]

    return frame_ids, is_extended_frame(message_id[2], message_type)


def _load_message_section(section_name, tokens, signals, enums, strict, sort_signals):
    def has_frame_id(message):
        return 'ID' in message[3]

    message_section_tokens = _get_section_tokens(tokens, section_name)
    messages = []

    for message_tokens in message_section_tokens:
        if not has_frame_id(message_tokens):
            continue

        frame_ids, is_extended_frame = _parse_message_frame_ids(message_tokens)

        for frame_id in frame_ids:
            message = _load_message(frame_id,
                                    is_extended_frame,
                                    message_tokens,
                                    message_section_tokens,
                                    signals,
                                    enums,
                                    strict,
                                    sort_signals,
                                    section_name)
            messages.append(message)

    return messages


def _load_messages(tokens, signals, enums, strict, sort_signals):
    messages = _load_message_section('{SEND}', tokens, signals, enums, strict, sort_signals)
    messages += _load_message_section('{RECEIVE}', tokens, signals, enums, strict, sort_signals)
    messages += _load_message_section('{SENDRECEIVE}', tokens, signals, enums, strict, sort_signals)

    return messages


def _load_version(tokens):
    return tokens[1][2]


def _get_signal_name(signal: Signal) -> str:
    return signal.name[:MAX_SIGNAL_NAME_LENGTH]

def _get_enum_name(signal: Signal) -> str:
    """Returns the name of an enum for a signal. Returns the shortened
    signal name, plus the letter 'E', since the cantools database doesn't
    store enum names, unlike the SYM file
    """
    return f'{_get_signal_name(signal).replace(" ", "_").replace("/", "_")[:MAX_SIGNAL_NAME_LENGTH - 1]}E'

def _dump_choice(signal: Signal) -> str:
    # Example:
    # Enum=DPF_Actv_Options(0="notActive", 1="active", 2="rgnrtnNddAtmtcllyInttdActvRgnrt", 3="notAvailable")
    if not signal.choices:
        return ''

    enum_str = f'Enum={_get_enum_name(signal)}('
    for choice_count, (choice_number, choice_value) in enumerate(signal.choices.items()):
        if choice_count % 10 == 0 and choice_count != 0:
            enum_str += ',\n'
        elif choice_count > 0:
            enum_str += ", "
        enum_str += f'{choice_number}="{choice_value}"'
    enum_str += ')'
    return enum_str

def _dump_choices(database: InternalDatabase) -> str:
    choices = []
    # SYM requires unique signals
    generated_signals = set()
    for message in database.messages:
        for signal in message.signals:
            if signal.name not in generated_signals:
                generated_signals.add(signal.name)
                new_choice = _dump_choice(signal)
                if new_choice:
                    choices.append(new_choice)

    if choices:
        return '{ENUMS}\n' + '\n'.join(choices)
    else:
        return ''

def _get_signal_type(signal: Signal) -> str:
    if signal.is_float:
        if signal.length == 64:
            return 'double'
        else:
            return 'float'
    elif signal.is_signed:
        return 'signed'
    else:
        if signal.length == 1 and signal.minimum == 0 and signal.maximum == 1:
            return 'bit'

        return 'unsigned'

def _dump_signal(signal: Signal) -> str:
    # Example:
    # Sig=alt_current unsigned 16 /u:A /f:0.05 /o:-1600 /max:1676.75 /d:0 // Alternator Current
    signal_str = f'Sig="{_get_signal_name(signal)}" {_get_signal_type(signal)} {signal.length}'
    if signal.byte_order == 'big_endian':
        signal_str += ' -m'
    if signal.unit:
        signal_str += f' /u:"{signal.unit}"'
    if signal.conversion.scale != 1:
        signal_str += f' /f:{signal.conversion.scale}'
    if signal.conversion.offset != 0:
        signal_str += f' /o:{signal.conversion.offset}'
    if signal.maximum is not None:
        signal_str += f' /max:{signal.maximum}'
    if signal.minimum is not None:
        signal_str += f' /min:{signal.minimum}'
    if signal.spn and signal.spn != 0:
        signal_str += f' /spn:{signal.spn}'
    if signal.choices:
        signal_str += f' /e:{_get_enum_name(signal)}'
    if signal.comment:
        signal_str += f' // {signal.comment}'

    return signal_str

def _dump_signals(database: InternalDatabase, sort_signals: Callable[[list[Signal]], list[Signal]] | None) -> str:
    signal_dumps = []
    # SYM requires unique signals
    generated_signals = set()
    for message in database.messages:
        if sort_signals:
            signals = sort_signals(message.signals)
        else:
            signals = message.signals
        for signal in signals:
            if signal.name not in generated_signals:
                generated_signals.add(signal.name)
                signal_dumps.append(_dump_signal(signal))

    if signals:
        return '{SIGNALS}\n' + '\n'.join(signal_dumps)
    else:
        return ''

def _dump_message(message: Message, signals: list[Signal], min_frame_id: int | None, max_frame_id: int | None = None,
                  multiplexer_id: int | None = None, multiplexer_signal: Signal | None = None) -> str:
    # Example:
    # [TestMessage]
    # ID=14A30000h
    # Type=Extended
    # Len=8
    # Sig=test_signal 0
    extended = ''
    if message.is_extended_frame:
        extended = 'Type=Extended\n'
    frame_id = ''
    frame_id_newline = ''
    comment = ''
    # Frame id should be excluded for multiplexed messages after the first listed message instance
    if min_frame_id is not None:
        if message.is_extended_frame:
            frame_id = f'ID={min_frame_id:08X}h'
        else:
            frame_id = f'ID={min_frame_id:03X}h'
        frame_id_newline = '\n'
        if message.comment is not None:
            comment = f' // {message.comment}'
    frame_id_range = ''
    if max_frame_id is not None:
        if message.is_extended_frame:
            frame_id_range = f'-{max_frame_id:08X}h'
        else:
            frame_id_range = f'-{max_frame_id:03X}h'
    message_str = f'["{message.name}"]\n{frame_id}{frame_id_range}{comment}{frame_id_newline}{extended}Len={message.length}\n'
    if message.cycle_time:
        message_str += f'CycleTime={message.cycle_time}\n'
    if multiplexer_id is not None and multiplexer_signal is not None:
        m_flag = ''
        if multiplexer_signal.byte_order == 'big_endian':
            m_flag = '-m'
        hex_multiplexer_id = format(multiplexer_id, 'x').upper()
        multiplexer_signal_name = multiplexer_signal.name
        if not multiplexer_signal_name:
            raise ValueError(f"The name of the multiplexer signal with ID {hex_multiplexer_id} is empty. The database is corrupt.")
        message_str += f'Mux="{multiplexer_signal_name}" {_convert_start(multiplexer_signal.start, multiplexer_signal.byte_order)},{multiplexer_signal.length} {hex_multiplexer_id}h {m_flag}\n'
    for signal in signals:
        message_str += f'Sig="{_get_signal_name(signal)}" {_convert_start(signal.start, signal.byte_order)}\n'
    return message_str

def _dump_messages(database: InternalDatabase) -> str:
    send_messages = []
    receive_messages = []
    send_receive_messages = []
    message_name: str
    messages_with_name: Iterator[Message]
    for message_name, messages_with_name in groupby(sorted(database.messages, key=lambda m: m.name), key=lambda m: m.name):
        message_dumps = []
        # Cantools represents SYM CAN ID range with multiple messages - need to dedup multiple cantools messages
        # into a single message with a CAN ID range
        messages_with_name_list = list(messages_with_name)
        num_messages_with_name = len(messages_with_name_list)
        if num_messages_with_name == 1:
            message = messages_with_name_list[0]
            min_frame_id = message.frame_id
            max_frame_id = None
        else:
            message = min(messages_with_name_list, key=lambda m: m.frame_id)
            min_frame_id = message.frame_id
            max_frame_id = max(messages_with_name_list, key=lambda m: m.frame_id).frame_id
            frame_id_range = max_frame_id - min_frame_id + 1
            if frame_id_range != num_messages_with_name:
                raise ValueError(f'Expected {frame_id_range} messages with name {message_name} - given {num_messages_with_name}')

        if message.is_multiplexed():
            non_multiplexed_signals = []
            # Store all non-multiplexed signals first
            for signal_tree_signal in message.signal_tree:
                if not isinstance(signal_tree_signal, collections.abc.Mapping):
                    non_multiplexed_signals.append(signal_tree_signal)

            for signal_tree_signal in message.signal_tree:
                if isinstance(signal_tree_signal, collections.abc.Mapping):
                    signal_name, multiplexed_signals = next(iter(signal_tree_signal.items()))
                    is_first_message = True
                    for multiplexer_id, signals_for_multiplexer in multiplexed_signals.items():
                        message_dumps.append(_dump_message(message, [message.get_signal_by_name(s) for s in signals_for_multiplexer] + non_multiplexed_signals,
                                                           min_frame_id if is_first_message else None, max_frame_id, multiplexer_id, message.get_signal_by_name(signal_name)))
                        is_first_message = False
        else:
            message_dumps.append(_dump_message(message, message.signals, min_frame_id, max_frame_id))

        if message.senders == [SEND_MESSAGE_SENDER]:
            send_messages.extend(message_dumps)
        elif message.senders == [RECEIVE_MESSAGE_SENDER]:
            receive_messages.extend(message_dumps)
        else:
            send_receive_messages.extend(message_dumps)

    messages_dump = ''
    if send_messages:
        messages_dump += '{SEND}\n' + '\n'.join(send_messages) + '\n'
    if receive_messages:
        messages_dump += '{RECEIVE}\n' + '\n'.join(receive_messages) + '\n'
    if send_receive_messages:
        messages_dump += '{SENDRECEIVE}\n' + '\n'.join(send_receive_messages) + '\n'
    return messages_dump

def dump_string(database: InternalDatabase, *, sort_signals:type_sort_signals=SORT_SIGNALS_DEFAULT) -> str:
    """Format given database in SYM file format.

    """
    if sort_signals == SORT_SIGNALS_DEFAULT:
        sort_signals = sort_signals_by_start_bit

    sym_str = 'FormatVersion=6.0 // Do not edit this line!\n'
    sym_str += 'Title="SYM Database"\n\n'

    sym_str += _dump_choices(database) + '\n\n'
    sym_str += _dump_signals(database, sort_signals) + '\n\n'
    sym_str += _dump_messages(database)

    return sym_str

def load_string(string:str, strict:bool=True, sort_signals:type_sort_signals=sort_signals_by_start_bit) -> InternalDatabase:
    """Parse given string.

    """

    if not re.search('^FormatVersion=6.0', string, re.MULTILINE):
        raise ParseError('Only SYM version 6.0 is supported.')

    tokens = Parser60().parse(string)

    version = _load_version(tokens)
    enums = _load_enums(tokens)
    signals = _load_signals(tokens, enums)
    messages = _load_messages(tokens, signals, enums, strict, sort_signals)

    return InternalDatabase(messages,
                            [],
                            [],
                            version)
