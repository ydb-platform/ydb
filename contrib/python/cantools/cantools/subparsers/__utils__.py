import re
from collections.abc import Iterable, Sequence

from cantools.database.errors import DecodeError

from ..database.can.database import Database
from ..database.can.message import Message
from ..database.namedsignalvalue import NamedSignalValue
from ..typechecking import (
    ContainerDecodeResultType,
    ContainerUnpackResultType,
    SignalDictType,
    TAdditionalCliArgs,
)

MULTI_LINE_FMT = '''
{message}(
{signals}
)\
'''


def format_signals(message, decoded_signals):
    formatted_signals = []

    for signal in message.signals:
        try:
            value = decoded_signals[signal.name]
        except KeyError:
            continue

        signal_name = signal.name

        if signal.unit is None or \
           isinstance(value, NamedSignalValue) or \
           isinstance(value, str):

            formatted_signal = f'{signal_name}: {value}'

        else:
            formatted_signal = f'{signal_name}: {value} {signal.unit}'

        formatted_signals.append(formatted_signal)

    return formatted_signals


def _format_message_single_line(name : str,
                                formatted_signals : Iterable[str]) -> str:
    return ' {}({})'.format(name,
                            ', '.join(formatted_signals))


def _format_message_multi_line(name: str,
                               formatted_signals : Iterable[str]) -> str:
    indented_signals = [
        '    ' + formatted_signal
        for formatted_signal in formatted_signals
    ]

    return MULTI_LINE_FMT.format(message=name,
                                 signals=',\n'.join(indented_signals))

def _format_container_single_line(message : Message,
                                  unpacked_data : ContainerUnpackResultType,
                                  decoded_data : ContainerDecodeResultType) \
                                  -> str:
    contained_list = []
    for i, (cm, signals) in enumerate(decoded_data):
        if isinstance(cm, Message):
            if isinstance(signals, bytes):
                formatted_cm = f'{cm.name}: Undecodable data: {signals.hex(" ")}'
                contained_list.append(formatted_cm)
            else:
                formatted_cm_signals = format_signals(cm, signals)
                formatted_cm = _format_message_single_line(cm.name, formatted_cm_signals)
            contained_list.append(formatted_cm)
        else:
            header_id = cm
            data = unpacked_data[i][1]
            contained_list.append(
                f'(Unknown contained message: Header ID: 0x{header_id:x}, '
                f'Data: {bytes(data).hex()})')

    return f' {message.name}({", ".join(contained_list)})'


def _format_container_multi_line(message : Message,
                                 unpacked_data : ContainerUnpackResultType,
                                 decoded_data : ContainerDecodeResultType) -> str:
    contained_list = []
    for i, (cm, signals) in enumerate(decoded_data):
        if isinstance(cm, Message):
            if isinstance(signals, bytes):
                formatted_cm = f'    {cm.header_id:06x}##{signals.hex()} ::\n'
                formatted_cm += f'    {cm.name}: Undecodable data'
                contained_list.append(formatted_cm)
            else:
                formatted_cm_signals = format_signals(cm, signals)
                formatted_cm = f'{cm.header_id:06x}##'
                formatted_cm += f'{bytes(unpacked_data[i][1]).hex()} ::'
                formatted_cm += _format_message_multi_line(cm.name, formatted_cm_signals)
                formatted_cm = formatted_cm.replace('\n', '\n    ')
                contained_list.append('    '+formatted_cm.strip())
        else:
            header_id = cm
            data = unpacked_data[i][1]
            contained_list.append(
                f'    {header_id:06x}##{data.hex()} ::\n'
                f'    Unknown contained message')

    return \
        f'\n{message.name}(\n' + \
        ',\n'.join(contained_list) + \
        '\n)'

def format_message_by_frame_id(dbase : Database,
                               frame_id : int,
                               data : bytes,
                               decode_choices : bool,
                               single_line : bool,
                               decode_containers : bool,
                               *,
                               allow_truncated: bool,
                               allow_excess: bool) -> str:
    try:
        message = dbase.get_message_by_frame_id(frame_id)
    except KeyError:
        return f' Unknown frame id {frame_id} (0x{frame_id:x})'

    if message.is_container:
        if decode_containers:
            return format_container_message(message,
                                            data,
                                            decode_choices,
                                            single_line,
                                            allow_truncated=allow_truncated,
                                            allow_excess=allow_excess)
        else:
            return f' Frame 0x{frame_id:x} is a container message'

    try:
        decoded_signals = message.decode_simple(data,
                                        decode_choices,
                                        allow_truncated=allow_truncated,
                                        allow_excess=allow_excess)

        return format_message(message, decoded_signals, single_line)
    except DecodeError as e:
        return f' {e}'

def format_container_message(message : Message,
                             data : bytes,
                             decode_choices : bool,
                             single_line : bool,
                             *,
                             allow_truncated : bool,
                             allow_excess: bool) -> str:
    try:
        unpacked_message = message.unpack_container(data,
                                                    allow_truncated=allow_truncated)
        decoded_message = message.decode_container(data,
                                                   decode_choices=True,
                                                   scaling=True,
                                                   allow_truncated=allow_truncated,
                                                   allow_excess=allow_excess)

    except DecodeError as e:
        return f' {e}'

    if single_line:
        return _format_container_single_line(message,
                                             unpacked_message,
                                             decoded_message)
    else:
        return _format_container_multi_line(message,
                                            unpacked_message,
                                            decoded_message)


def format_message(message : Message,
                   decoded_signals : SignalDictType,
                   single_line : bool) -> str:
    formatted_signals = format_signals(message, decoded_signals)

    if single_line:
        return _format_message_single_line(message.name, formatted_signals)
    else:
        return _format_message_multi_line(message.name, formatted_signals)

def format_multiplexed_name(message : Message,
                            decoded_signals : SignalDictType) -> str:
    # The idea here is that we rely on the sorted order of the Signals, and
    # then simply go through each possible Multiplexer and build a composite
    # key consisting of the Message name prepended to all the possible MUX
    # Signals (and their values). This composite key is therefore unique for
    # all the different possible enumerations of MUX values, which allows us
    # to display each MUXed Message on its own separate line.
    result = [message.name]

    for signal in message.signals:
        if signal.is_multiplexer:
            if signal.name in decoded_signals:
                value = decoded_signals[signal.name]
            elif signal.raw_initial is not None:
                value = signal.raw_initial
            else:
                value = 0
            result.append(f'{signal.name}={value}')

    return ' :: '.join(result)


def cast_from_string(string_val: str) -> str | int | float | bool:
    """Perform trivial type conversion from :class:`str` values.

    :param string_val:
        the string, that shall be converted
    """
    if re.match(r"^[-+]?\d+$", string_val):
        # value is integer
        return int(string_val)

    if re.match(r"^[-+]?\d*\.\d+(?:e[-+]?\d+)?$", string_val):
        # value is float
        return float(string_val)

    if re.match(r"^(?:True|False)$", string_val, re.IGNORECASE):
        # value is bool
        return string_val.lower() == "true"

    # value is string
    return string_val


def parse_additional_config(unknown_args: Sequence[str]) -> TAdditionalCliArgs:
    for arg in unknown_args:
        if not re.match(r"^--[a-zA-Z][a-zA-Z0-9\-]*=\S*?$", arg):
            raise ValueError(f'Parsing argument {arg} failed, use --param=value'
                             ' to pass additional args to CAN Bus.')

    def _split_arg(_arg: str) -> tuple[str, str]:
        left, right = _arg.split("=", 1)
        return left.lstrip("-").replace("-", "_"), right

    args: dict[str, str | int | float | bool] = {}
    for key, string_val in map(_split_arg, unknown_args):
        args[key] = cast_from_string(string_val)
    return args
