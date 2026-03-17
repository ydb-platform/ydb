# Utility functions.

import os.path
import re
from collections import OrderedDict
from collections.abc import Callable, Sequence
from typing import (
    TYPE_CHECKING,
    Final,
    Literal,
    Union,
)

from ..typechecking import (
    ByteOrder,
    Choices,
    Formats,
    SignalDictType,
    SignalMappingType,
    SignalValueType,
)
from .errors import DecodeError, EncodeError
from .namedsignalvalue import NamedSignalValue

if TYPE_CHECKING:
    from ..database import Database
    from ..database.can.attribute import Attribute
    from ..database.can.environment_variable import EnvironmentVariable
    from ..database.can.message import Message
    from ..database.can.node import Node
    from ..database.can.signal import Signal
    from ..database.diagnostics import Data

try:
    import bitstruct.c  # type: ignore
except ImportError:
    import bitstruct  # type: ignore


def format_or(items: list[int | str]) -> str:
    string_items = [str(item) for item in items]

    if len(string_items) == 1:
        return string_items[0]
    else:
        return '{} or {}'.format(', '.join(string_items[:-1]),
                                 string_items[-1])


def format_and(items: list[int | str]) -> str:
    string_items = [str(item) for item in items]

    if len(string_items) == 1:
        return str(string_items[0])
    else:
        return '{} and {}'.format(', '.join(string_items[:-1]),
                                  string_items[-1])


def start_bit(signal: Union["Data", "Signal"]) -> int:
    if signal.byte_order == 'big_endian':
        return 8 * (signal.start // 8) + (7 - (signal.start % 8))
    else:
        return signal.start


def _encode_signal_values(signals: Sequence[Union["Signal", "Data"]],
                          signal_values: SignalMappingType,
                          scaling: bool,
                          ) -> dict[str, int | float]:
    """
    Convert a dictionary of physical signal values into raw ones.
    """
    raw_values = {}
    for signal in signals:
        name = signal.name
        conversion = signal.conversion
        value = signal_values[name]

        if isinstance(value, (int, float)):
            if scaling:
                raw_values[name] = conversion.numeric_scaled_to_raw(value)
                continue

            raw_values[name] = value if conversion.is_float else round(value)
            continue

        if isinstance(value, str):
            raw_values[name] = conversion.choice_to_number(value)
            continue

        if isinstance(value, NamedSignalValue):
            # validate the given NamedSignalValue first
            if value != conversion.raw_to_scaled(value.value, decode_choices=True):
                raise EncodeError(
                    f"Invalid 'NamedSignalValue' name/value pair not found! Name {value.name}, value {value.value}"
                )

            raw_values[name] = value.value
            continue

        raise EncodeError(
            f"Unable to encode signal '{name}' "
            f"with type '{value.__class__.__name__}'."
        )

    return raw_values


def encode_data(signal_values: SignalMappingType,
                signals: Sequence[Union["Signal", "Data"]],
                formats: Formats,
                scaling: bool
                ) -> int:
    if len(signals) == 0:
        return 0

    raw_signal_values = _encode_signal_values(signals, signal_values, scaling)
    big_packed = formats.big_endian.pack(raw_signal_values)
    little_packed = formats.little_endian.pack(raw_signal_values)
    packed_union = int.from_bytes(big_packed, "big") | int.from_bytes(little_packed, "little")

    return packed_union


def decode_data(data: bytes,
                expected_length: int,
                signals: Sequence[Union["Signal", "Data"]],
                formats: Formats,
                decode_choices: bool,
                scaling: bool,
                allow_truncated: bool,
                allow_excess: bool,
                ) -> SignalDictType:

    actual_length = len(data)
    if actual_length != expected_length:
        if allow_truncated:
            # pad the data with 0xff to prevent the codec from
            # raising an exception. Note that all signals
            # that contain garbage will be removed below.
            data = data.ljust(expected_length, b"\xFF")

        if allow_excess:
            # trim the payload data to match the expected size
            data = data[:expected_length]

        if len(data) != expected_length:
            raise DecodeError(f"Wrong data size: {actual_length} instead of "
                              f"{expected_length} bytes")

    try:
        unpacked = {
            **formats.big_endian.unpack(data),
            **formats.little_endian.unpack(data[::-1]),
        }
    except (bitstruct.Error, ValueError) as e:
        # bitstruct returns different errors in PyPy and cpython
        raise DecodeError("unpacking failed") from e

    if actual_length < expected_length and allow_truncated:
        # remove signals that are outside available data bytes
        actual_bit_count = actual_length * 8
        for signal in signals:
            if signal.byte_order == "little_endian":
                sequential_start_bit = signal.start
            else:
                # Calculate start bit with inverted indices.
                # Function body of ``sawtooth_to_network_bitnum()``
                # is inlined for improved performance.
                sequential_start_bit = (8 * (signal.start // 8)) + (7 - (signal.start % 8))

            if sequential_start_bit + signal.length > actual_bit_count:
                del unpacked[signal.name]

    # scale the signal values and decode choices
    decoded: dict[str, SignalValueType] = {}
    for signal in signals:
        if (value := unpacked.get(signal.name)) is None:
            # signal value was removed above...
            continue

        if scaling:
            decoded[signal.name] = signal.conversion.raw_to_scaled(value, decode_choices)
        elif (decode_choices
              and signal.conversion.choices
              and (choice := signal.conversion.choices.get(value, None)) is not None):
            decoded[signal.name] = choice
        else:
            decoded[signal.name] = value

    return decoded


def create_encode_decode_formats(signals: Sequence[Union["Data", "Signal"]], number_of_bytes: int) -> Formats:
    format_length = (8 * number_of_bytes)

    def get_format_string_type(signal: Union["Data", "Signal"]) -> str:
        if signal.conversion.is_float:
            return 'f'
        elif signal.is_signed:
            return 's'
        else:
            return 'u'

    def padding_item(length: int) -> tuple[str, str, None]:
        fmt = f'p{length}'
        padding_mask = '1' * length

        return fmt, padding_mask, None

    def data_item(signal: Union["Data", "Signal"]) -> tuple[str, str, str]:
        fmt = f'{get_format_string_type(signal)}{signal.length}'
        padding_mask = '0' * signal.length

        return fmt, padding_mask, signal.name

    def fmt(items: list[tuple[str, str, str | None]]) -> str:
        return ''.join([item[0] for item in items])

    def names(items:  list[tuple[str, str, str | None]]) -> list[str]:
        return [item[2] for item in items if item[2] is not None]

    def padding_mask(items: list[tuple[str, str, str | None]]) -> int:
        try:
            return int(''.join([item[1] for item in items]), 2)
        except ValueError:
            return 0

    def create_big() -> tuple[str, int, list[str]]:
        items: list[tuple[str, str, str | None]] = []
        start = 0

        # Select BE signals
        be_signals = [signal for signal in signals if signal.byte_order == "big_endian"]

        # Ensure BE signals are sorted in network order
        sorted_signals = sorted(be_signals, key = lambda signal: sawtooth_to_network_bitnum(signal.start))

        for signal in sorted_signals:

            padding_length = (start_bit(signal) - start)

            if padding_length > 0:
                items.append(padding_item(padding_length))

            items.append(data_item(signal))
            start = (start_bit(signal) + signal.length)

        if start < format_length:
            length = format_length - start
            items.append(padding_item(length))

        return fmt(items), padding_mask(items), names(items)

    def create_little() -> tuple[str, int, list[str]]:
        items: list[tuple[str, str, str | None]] = []
        end = format_length

        for signal in signals[::-1]:
            if signal.byte_order == 'big_endian':
                continue

            padding_length = end - (signal.start + signal.length)

            if padding_length > 0:
                items.append(padding_item(padding_length))

            items.append(data_item(signal))
            end = signal.start

        if end > 0:
            items.append(padding_item(end))

        value = padding_mask(items)

        if format_length > 0:
            length = len(''.join([item[1] for item in items]))
            _packed = bitstruct.pack(f'u{length}', value)
            value = int.from_bytes(_packed, "little")

        return fmt(items), value, names(items)

    big_fmt, big_padding_mask, big_names = create_big()
    little_fmt, little_padding_mask, little_names = create_little()

    try:
        big_compiled = bitstruct.c.compile(big_fmt, big_names)
    except Exception:
        big_compiled = bitstruct.compile(big_fmt, big_names)

    try:
        little_compiled = bitstruct.c.compile(little_fmt, little_names)
    except Exception:
        little_compiled = bitstruct.compile(little_fmt, little_names)

    return Formats(big_compiled,
                   little_compiled,
                   big_padding_mask & little_padding_mask)


def sawtooth_to_network_bitnum(sawtooth_bitnum: int) -> int:
    '''Convert SawTooth bit number to Network bit number

    Byte     |   0   |   1   |
    Sawtooth |7 ... 0|15... 8|
    Network  |0 ... 7|8 ...15|
    '''
    return (8 * (sawtooth_bitnum // 8)) + (7 - (sawtooth_bitnum % 8))


def cdd_offset_to_dbc_start_bit(cdd_offset: int, bit_length: int, byte_order: ByteOrder) -> int:
    '''Convert CDD/c-style field bit offset to DBC field start bit convention.

    BigEndian (BE) fields are located by their MSBit's sawtooth index.
    LitteleEndian (LE) fields located by their LSBit's sawtooth index.
    '''
    if byte_order == "big_endian":
        # Note: Allow for BE fields that are smaller or larger than 8 bits.
        return (8 * (cdd_offset // 8)) + min(7, (cdd_offset % 8) + bit_length - 1)
    else:
        return cdd_offset


def prune_signal_choices(signal: "Signal") -> None:
    '''Shorten the names of the signal choices of a single signal

    For signals with multiple named values this means removing the
    longest common prefix that ends with an underscore and for which
    the removal still result the named signal values to be valid
    python identifiers. For signals with a single named choice, this
    means removing all leading segments between underscores which
    occur before a segment that contains a digit.

    Examples:

    ..code:: text

       MyMessage_MySignal_Uint32_Choice1, MyMessage_MySignal_Uint32_Choice2
       -> Choice1, Choice2
       MyMessage_MySignal_Uint32_NotAvailable
       -> NotAvailable

    '''

    if signal.choices is None:
        # no named choices
        return

    if len(signal.choices) == 1:
        # signal exhibits only a single named value: Use the longest
        # postfix starting with an underscore that does not contain
        # digits as the new name. If no such suffix exists, leave the
        # choice alone...
        key = next(iter(signal.choices.keys()))
        choice = next(iter(signal.choices.values()))
        m = re.match(r'^[0-9A-Za-z_]*?_([A-Za-z_]+)$', str(choice))
        val = str(choice)
        if m:
            val = m.group(1)

        if isinstance(choice, str):
            signal.choices[key] = val
        else:
            # assert isinstance(choice, NamedSignalValue)
            choice.name = val
        return

    # if there are multiple choices, remove the longest common prefix
    # that ends with an underscore from all of them provided that the
    # names of the choices stay valid identifiers
    choice_values = [ str(x) for x in signal.choices.values() ]
    full_prefix = os.path.commonprefix(choice_values)
    i = full_prefix.rfind('_')

    if i >= 0:
        full_prefix = full_prefix[0:i]
    else:
        # full_prefix does not contain an underscore
        # but the following algorithm assumes it does
        # and would strip too much
        return

    if not full_prefix:
        # the longest possible prefix is empty, i.e., there is nothing
        # to strip from the names of the signal choices
        return

    full_prefix_segments = full_prefix.split('_')

    # find the longest prefix of the choices which keeps all
    # names valid python identifiers
    prefix = ''
    n = 0
    valid_name_re = re.compile('^[a-zA-Z_][a-zA-Z0-9_]*$')
    for i in range(len(full_prefix_segments), -1, -1):
        if i == 0:
            # there is no such non-empty prefix
            return

        prefix = '_'.join(full_prefix_segments[:i]) + '_'
        n = len(prefix)

        if all(valid_name_re.match(x[n:]) for x in choice_values):
            break

    # remove the prefix from the choice names
    for key, choice in signal.choices.items():
        if isinstance(choice, str):
            signal.choices[key] = choice[n:]
        else:
            # assert isinstance(choice, NamedSignalValue)
            choice.name = choice.name[n:]


def prune_database_choices(database: "Database") -> None:
    '''
    Prune names of all named signal values of all signals of a database
    '''
    for message in database.messages:

        for signal in message.signals:
            prune_signal_choices(signal)

        if message.contained_messages is not None:
            for cm in message.contained_messages:
                for cs in cm.signals:
                    prune_signal_choices(cs)


SORT_SIGNALS_DEFAULT: Final = 'default'
type_sort_signals = Callable[[list["Signal"]], list["Signal"]] | Literal['default'] | None

type_sort_attribute = \
    tuple[Literal['dbc'],     "Attribute", None,   None,      None,     None] | \
    tuple[Literal['node'],    "Attribute", "Node", None,      None,     None] | \
    tuple[Literal['message'], "Attribute", None,   "Message", None,     None] | \
    tuple[Literal['signal'],  "Attribute", None,   "Message", "Signal", None] | \
    tuple[Literal['envvar'],  "Attribute", None,   None,      None,     "EnvironmentVariable"]

type_sort_attributes = Callable[[list[type_sort_attribute]], list[type_sort_attribute]] | Literal['default'] | None

type_sort_choices = Callable[[Choices], Choices] | None

def sort_signals_by_start_bit(signals: list["Signal"]) -> list["Signal"]:
    return sorted(signals, key=start_bit)


def sort_signals_by_start_bit_reversed(signals: list["Signal"]) -> list["Signal"]:
    return sorted(signals, key=start_bit)[::-1]


def sort_signals_by_name(signals: list["Signal"]) -> list["Signal"]:
    return sorted(signals, key=lambda s: s.name)


def sort_signals_by_start_bit_and_mux(signals: list["Signal"]) -> list["Signal"]:
    # sort by start bit
    signals = sorted(signals, key=start_bit)
    # but unmuxed values come first
    signals = sorted(signals, key=lambda s: bool(s.multiplexer_ids))
    # and group by mux... -1 is fine as the "no mux" case because even negative
    # multiplexors get cast to unsigned in the .dbc
    signals = sorted(
        signals, key=lambda s: s.multiplexer_ids[0] if s.multiplexer_ids else -1
    )

    return signals


def sort_choices_by_value(choices: Choices) -> Choices:
    return OrderedDict(sorted(choices.items(), key=lambda x: x[0]))


def sort_choices_by_value_descending(choices: Choices) -> Choices:
    return OrderedDict(sorted(choices.items(), key=lambda x: x[0], reverse=True))
