# A CAN message.

import logging
from copy import deepcopy
from typing import (
    TYPE_CHECKING,
    Optional,
    cast,
)

from ...typechecking import (
    Codec,
    Comments,
    ContainerDecodeResultListType,
    ContainerDecodeResultType,
    ContainerEncodeInputType,
    ContainerHeaderSpecType,
    ContainerUnpackListType,
    ContainerUnpackResultType,
    DecodeResultType,
    EncodeInputType,
    SignalDictType,
    SignalMappingType,
)
from ..errors import DecodeError, EncodeError, Error
from ..namedsignalvalue import NamedSignalValue
from ..utils import (
    SORT_SIGNALS_DEFAULT,
    create_encode_decode_formats,
    decode_data,
    encode_data,
    format_or,
    sort_signals_by_start_bit,
    start_bit,
    type_sort_signals,
)
from .signal import Signal
from .signal_group import SignalGroup

if TYPE_CHECKING:
    from .formats.arxml import AutosarMessageSpecifics
    from .formats.dbc import DbcSpecifics

LOGGER = logging.getLogger(__name__)


class Message:
    """A CAN message with frame id, comment, signals and other
    information.

    If `strict` is ``True`` an exception is raised if any signals are
    overlapping or if they don't fit in the message.

    By default signals are sorted by their start bit when their Message object is created.
    If you don't want them to be sorted pass `sort_signals = None`.
    If you want the signals to be sorted in another way pass something like
    `sort_signals = lambda signals: list(sorted(signals, key=lambda sig: sig.name))`
    """

    def __init__(self,
                 frame_id: int,
                 name: str,
                 length: int,
                 signals: list[Signal],
                 # if the message is a container message, this lists
                 # the messages which it potentially features
                 contained_messages: list['Message'] | None = None,
                 # header ID of message if it is part of a container message
                 header_id: int | None = None,
                 header_byte_order: str = 'big_endian',
                 unused_bit_pattern: int = 0x00,
                 comment: str | Comments | None = None,
                 senders: list[str] | None = None,
                 send_type: str | None = None,
                 cycle_time: int | None = None,
                 dbc_specifics: Optional['DbcSpecifics'] = None,
                 autosar_specifics: Optional['AutosarMessageSpecifics'] = None,
                 is_extended_frame: bool = False,
                 is_fd: bool = False,
                 bus_name: str | None = None,
                 signal_groups: list[SignalGroup] | None = None,
                 strict: bool = True,
                 protocol: str | None = None,
                 sort_signals: type_sort_signals = sort_signals_by_start_bit,
                 ) -> None:
        frame_id_bit_length = frame_id.bit_length()

        if is_extended_frame:
            if frame_id_bit_length > 29:
                raise Error(
                    f'Extended frame id 0x{frame_id:x} is more than 29 bits in '
                    f'message {name}.')
        elif frame_id_bit_length > 11:
            raise Error(
                f'Standard frame id 0x{frame_id:x} is more than 11 bits in '
                f'message {name}.')

        self._frame_id = frame_id
        self._header_id = header_id
        self._header_byte_order = header_byte_order
        self._is_extended_frame = is_extended_frame
        self._is_fd = is_fd
        self._name = name
        self._length = length
        self._unused_bit_pattern = unused_bit_pattern
        if sort_signals == SORT_SIGNALS_DEFAULT:
            self._signals = sort_signals_by_start_bit(signals)
        elif callable(sort_signals):
            self._signals = sort_signals(signals)
        else:
            self._signals = signals
        self._signal_dict: dict[str, Signal] = {}
        self._contained_messages = contained_messages

        # if the 'comment' argument is a string, we assume that is an
        # english comment. this is slightly hacky because the
        # function's behavior depends on the type of the passed
        # argument, but it is quite convenient...
        self._comments: Comments | None
        if isinstance(comment, str):
            # use the first comment in the dictionary as "The" comment
            self._comments = {None: comment}
        else:
            # assume that we have either no comment at all or a
            # multi-lingual dictionary
            self._comments = comment

        self._senders = senders or []
        self._send_type = send_type
        self._cycle_time = cycle_time
        self._dbc = dbc_specifics
        self._autosar = autosar_specifics
        self._bus_name = bus_name
        self._signal_groups = signal_groups
        self._codecs: Codec | None = None
        self._signal_tree: list[str | list[str]] | None = None
        self._strict = strict
        self._protocol = protocol
        self.refresh()

    def _create_codec(self,
                      parent_signal: str | None = None,
                      multiplexer_id: int | None = None,
                      ) -> Codec:
        """Create a codec of all signals with given parent signal. This is a
        recursive function.

        """

        signals = []
        multiplexers: dict[str, dict[int, Codec]] = {}

        # Find all signals matching given parent signal name and given
        # multiplexer id. Root signals' parent and multiplexer id are
        # both None.
        for signal in self._signals:
            if signal.multiplexer_signal != parent_signal:
                continue

            if (
                    multiplexer_id is not None
                    and (signal.multiplexer_ids is None or multiplexer_id not in signal.multiplexer_ids)
            ):
                continue

            if signal.is_multiplexer:
                children_ids: set[int] = set()

                for s in self._signals:
                    if s.multiplexer_signal != signal.name:
                        continue

                    if s.multiplexer_ids is not None:
                        children_ids.update(s.multiplexer_ids)

                # Some CAN messages will have muxes containing only
                # the multiplexer and no additional signals. At Tesla
                # these are indicated in advance by assigning them an
                # enumeration. Here we ensure that any named
                # multiplexer is included, even if it has no child
                # signals.
                if signal.conversion.choices:
                    children_ids.update(signal.conversion.choices.keys())

                for child_id in children_ids:
                    codec = self._create_codec(signal.name, child_id)

                    if signal.name not in multiplexers:
                        multiplexers[signal.name] = {}

                    multiplexers[signal.name][child_id] = codec

            signals.append(signal)

        return {
            'signals': signals,
            'formats': create_encode_decode_formats(signals,
                                                    self._length),
            'multiplexers': multiplexers
        }

    def _create_signal_tree(self, codec):
        """Create a multiplexing tree node of given codec. This is a recursive
        function.

        """

        nodes = []

        for signal in codec['signals']:
            multiplexers = codec['multiplexers']

            if signal.name in multiplexers:
                node = {
                    signal.name: {
                        mux: self._create_signal_tree(mux_codec)
                        for mux, mux_codec in multiplexers[signal.name].items()
                    }
                }
            else:
                node = signal.name

            nodes.append(node)

        return nodes

    @property
    def header_id(self) -> int | None:
        """The header ID of the message if it is part of a container message.

        """

        return self._header_id

    @header_id.setter
    def header_id(self, value: int) -> None:
        self._header_id = value

    @property
    def header_byte_order(self) -> str:
        """The byte order of the header ID of the message if it is part of a
        container message.

        """

        return self._header_byte_order

    @header_byte_order.setter
    def header_byte_order(self, value: str) -> None:
        self._header_byte_order = value

    @property
    def frame_id(self) -> int:
        """The message frame id.

        """

        return self._frame_id

    @frame_id.setter
    def frame_id(self, value: int) -> None:
        self._frame_id = value

    @property
    def is_extended_frame(self) -> bool:
        """``True`` if the message is an extended frame, ``False`` otherwise.

        """

        return self._is_extended_frame

    @is_extended_frame.setter
    def is_extended_frame(self, value: bool) -> None:
        self._is_extended_frame = value

    @property
    def is_fd(self) -> bool:
        """``True`` if the message requires CAN-FD, ``False`` otherwise.

        """

        return self._is_fd

    @is_fd.setter
    def is_fd(self, value: bool) -> None:
        self._is_fd = value

    @property
    def name(self) -> str:
        """The message name as a string.

        """

        return self._name

    @name.setter
    def name(self, value: str) -> None:
        self._name = value

    @property
    def length(self) -> int:
        """The message data length in bytes.

        """

        return self._length

    @length.setter
    def length(self, value: int) -> None:
        self._length = value

    @property
    def signals(self) -> list[Signal]:
        """A list of all signals in the message.

        """

        return self._signals

    @property
    def is_container(self) -> bool:
        """Returns if the message is a container message

        """

        return self._contained_messages is not None

    @property
    def contained_messages(self) -> list['Message'] | None:
        """The list of messages potentially contained within this message

        """

        return self._contained_messages

    @property
    def unused_bit_pattern(self) -> int:
        """The pattern used for unused bits of a message.

        This prevents undefined behaviour and/or information leaks
        when encoding messages.
        """

        return self._unused_bit_pattern

    @unused_bit_pattern.setter
    def unused_bit_pattern(self, value):
        if value < 0 or value > 255:
            LOGGER.info(f'Invalid unused bit pattern "{value}". Must be '
                        f'an integer between 0 and 255')
            self._unused_bit_pattern = 0
            return

        self._unused_bit_pattern = value

    @property
    def signal_groups(self) -> list[SignalGroup] | None:
        """A list of all signal groups in the message.

        """

        return self._signal_groups

    @signal_groups.setter
    def signal_groups(self, value: list[SignalGroup]) -> None:
        self._signal_groups = value

    @property
    def comment(self) -> str | None:
        """The message comment, or ``None`` if unavailable.

        Note that we implicitly try to return the English comment if
        multiple languages were specified.

        """
        if self._comments is None:
            return None
        elif self._comments.get(None) is not None:
            return self._comments.get(None)
        elif self._comments.get('FOR-ALL') is not None:
            return self._comments.get('FOR-ALL')

        return self._comments.get('EN')

    @comment.setter
    def comment(self, value: str | None) -> None:
        if value is None:
            self._comments = None
        else:
            self._comments = {None: value}

    @property
    def comments(self):
        """The dictionary with the descriptions of the message in multiple
        languages. ``None`` if unavailable.

        """
        return self._comments

    @comments.setter
    def comments(self, value):
        self._comments = value

    @property
    def senders(self) -> list[str]:
        """A list of all sender nodes of this message.

        """

        return self._senders

    @property
    def receivers(self) -> set[str]:
        """A set of all receiver nodes of this message.

        This is equivalent to the set of nodes which receive at least
        one of the signals contained in the message.

        """
        result = set()

        for sig in self.signals:
            if sig.receivers is not None:
                result.update(sig.receivers)

        if self.is_container:
            assert self.contained_messages is not None
            for cmsg in self.contained_messages:
                for sig in cmsg.signals:
                    if sig.receivers is not None:
                        result.update(sig.receivers)

        return result

    @property
    def send_type(self) -> str | None:
        """The message send type, or ``None`` if unavailable.

        """

        return self._send_type

    @property
    def cycle_time(self) -> int | None:
        """The message cycle time, or ``None`` if unavailable.

        """

        return self._cycle_time

    @cycle_time.setter
    def cycle_time(self, value: int | None) -> None:
        self._cycle_time = value

    @property
    def dbc(self) -> Optional['DbcSpecifics']:
        """An object containing dbc specific properties like e.g. attributes.

        """

        return self._dbc

    @dbc.setter
    def dbc(self, value: Optional['DbcSpecifics']) -> None:
        self._dbc = value

    @property
    def autosar(self) -> Optional['AutosarMessageSpecifics']:
        """An object containing AUTOSAR specific properties

        e.g. auxiliary data required to implement CRCs, secure on-board
        communication (secOC) or container messages.
        """

        return self._autosar

    @autosar.setter
    def autosar(self, value: Optional['AutosarMessageSpecifics']) -> None:
        self._autosar = value

    @property
    def bus_name(self) -> str | None:
        """The message bus name, or ``None`` if unavailable.

        """

        return self._bus_name

    @bus_name.setter
    def bus_name(self, value: str | None) -> None:
        self._bus_name = value

    @property
    def protocol(self) -> str | None:
        """The message protocol, or ``None`` if unavailable. Only one protocol
        is currently supported; ``'j1939'``.

        """

        return self._protocol

    @protocol.setter
    def protocol(self, value: str | None) -> None:
        self._protocol = value

    @property
    def signal_tree(self):
        """All signal names and multiplexer ids as a tree. Multiplexer signals
        are dictionaries, while other signals are strings.

        >>> foo = db.get_message_by_name('Foo')
        >>> foo.signal_tree
        ['Bar', 'Fum']
        >>> bar = db.get_message_by_name('Bar')
        >>> bar.signal_tree
        [{'A': {0: ['C', 'D'], 1: ['E']}}, 'B']

        """

        return self._signal_tree

    def gather_signals(self,
                       input_data: SignalMappingType,
                       node: Codec | None = None) \
      -> SignalDictType:

        '''Given a superset of all signals required to encode the message,
        return a dictionary containing exactly the ones required.

        If a required signal is missing from the input dictionary, a
        ``EncodeError`` exception is raised.
        '''

        if node is None:
            node = self._codecs
        assert node is not None

        result = {}

        for signal in node['signals']:
            val = input_data.get(signal.name)
            if val is None:
                raise EncodeError(f'The signal "{signal.name}" is '
                                  f'required for encoding.')
            result[signal.name] = val

        for mux_signal_name, mux_nodes in node['multiplexers'].items():
            mux_num = self._get_mux_number(input_data, mux_signal_name)
            mux_node = mux_nodes.get(mux_num)
            if mux_num is None or mux_node is None:
                multiplexers = node['multiplexers']
                try:
                    expected_str = \
                        f'Expected one of {{' \
                        f'{format_or(list(multiplexers[mux_signal_name].keys()))}' \
                        f'}}, but '
                except KeyError:
                    expected_str = ''

                raise EncodeError(f'A valid value for the multiplexer selector '
                                  f'signal "{mux_signal_name}" is required: '
                                  f'{expected_str}'
                                  f'got {input_data[mux_signal_name]}')

            result.update(self.gather_signals(input_data, mux_node))

        return result

    def gather_container(self,
                         contained_messages: list[ContainerHeaderSpecType],
                         signal_values: SignalMappingType) \
      -> ContainerDecodeResultType:

        '''Given a superset of all messages required to encode all messages
        featured by a container message, return a list of (Message,
        SignalDict) tuples that can be passed to ``encode()``.

        If a required signal is missing from the input dictionary, a
        ``EncodeError`` exception is raised.
        '''

        result: ContainerDecodeResultListType = []
        for header in contained_messages:
            contained_message = None
            if isinstance(header, str):
                contained_message = \
                    self.get_contained_message_by_name(header)
            elif isinstance(header, Message):
                # contained message is specified directly. We go once
                # around the circle to ensure that a contained message
                # with the given header ID is there.
                header_id = header.header_id
                assert header_id is not None
                contained_message = \
                    self.get_contained_message_by_header_id(header_id)
            elif isinstance(header, int):
                # contained message is specified directly. We go once
                # around the circle to ensure that a contained message
                # with the given header ID is there.
                contained_message = \
                    self.get_contained_message_by_header_id(header)

            if contained_message is None:
                raise EncodeError(f'Cannot determine contained message '
                                  f'associated with "{header}"')

            contained_signals = contained_message.gather_signals(signal_values)

            result.append( (contained_message, contained_signals) )

        return result

    def assert_signals_encodable(self,
                                 input_data: SignalMappingType,
                                 scaling: bool,
                                 assert_values_valid: bool = True,
                                 assert_all_known: bool = True) \
      -> None:

        '''Given a dictionary of signal name to signal value mappings, ensure
        that all the signals required for encoding are present

        As a minimum, all signals required to encode the message need
        to be specified. If they are not, a ``KeyError`` or an
        ``EncodeError`` exception is raised.

        Depending on the parameters specified, the data of the
        dictionary must adhere to additional requirements:

        :param scaling: If ``False`` no scaling of signals is performed.

        :param assert_values_valid: If ``True``, the values of all
            specified signals must be valid/encodable. If at least one is
            not, an ``EncodeError`` exception is raised. (Note that the
            values of multiplexer selector signals must always be valid!)

        :param assert_all_known: If ``True``, all specified signals must
            be used by the encoding operation or an ``EncodeError``
            exception is raised. This is useful to prevent typos.
        '''

        # this method only deals with ordinary messages
        if self.is_container:
            raise EncodeError(f'Message "{self.name}" is a container')

        # This type checking is not really comprehensive and is
        # superfluous if the type hints are respected by the calling
        # code. That said, it guards against accidentally passing
        # non-dictionary objects such as lists of (Message,
        # SignalDict) tuples expected by container messages...
        if not isinstance(input_data, dict):
            raise EncodeError(f'Input data for encoding message "{self.name}" '
                              f'must be a SignalDict')

        used_signals = self.gather_signals(input_data)
        if assert_all_known and set(used_signals) != set(input_data):
            raise EncodeError(f'The following signals were specified but are '
                              f'not required to encode the message:'
                              f'{set(input_data) - set(used_signals)}')
        if assert_values_valid:
            self._assert_signal_values_valid(used_signals, scaling)

    def assert_container_encodable(self,
                                   input_data: ContainerEncodeInputType,
                                   scaling: bool,
                                   assert_values_valid: bool = True,
                                   assert_all_known: bool = True) \
      -> None:

        """
        This method is identical to ``assert_signals_encodable()``
        except that it is concerned with container messages.
        """

        # this method only deals with container messages
        if not self.is_container:
            raise EncodeError(f'Message "{self.name}" is not a container')

        # This type checking is not really comprehensive and is
        # superfluous if the type hints are respected by the calling
        # code. That said it guards against accidentally passing a
        # SignalDict for normal messages...
        if not isinstance(input_data, list):
            raise EncodeError(f'Input data for encoding message "{self.name}" '
                              f'must be a list of (Message, SignalDict) tuples')

        for header, payload in input_data:
            if isinstance(header, int) and isinstance(payload, bytes):
                # contained message specified as raw data
                continue

            contained_message = None
            if isinstance(header, int):
                contained_message = \
                    self.get_contained_message_by_header_id(header)
            elif isinstance(header, str):
                contained_message = \
                    self.get_contained_message_by_name(header)
            elif isinstance(header, Message):
                hid = header.header_id
                if hid is None:
                    raise EncodeError(f'Message {header.name} cannot be part '
                                      f'of a container because it does not '
                                      f'exhibit a header ID')
                contained_message = self.get_contained_message_by_header_id(hid)

            if contained_message is None:
                raise EncodeError(f'Could not associate "{header}" with any '
                                  f'contained message')

            if isinstance(payload, bytes):
                if len(payload) != contained_message.length:
                    raise EncodeError(f'Payload for contained message '
                                      f'"{contained_message.name}" is '
                                      f'{len(payload)} instead of '
                                      f'{contained_message.length} bytes long')
            else:
                contained_message.assert_signals_encodable(payload,
                                                           scaling,
                                                           assert_values_valid,
                                                           assert_all_known)

    def _get_mux_number(self, decoded: SignalMappingType, signal_name: str) -> int:
        mux = decoded[signal_name]

        if isinstance(mux, str) or isinstance(mux, NamedSignalValue):
            signal = self.get_signal_by_name(signal_name)
            try:
                mux = signal.conversion.choice_to_number(str(mux))
            except KeyError:
                raise EncodeError() from None
        return int(mux)

    def _assert_signal_values_valid(self,
                                    data: SignalMappingType,
                                    scaling: bool) -> None:

        for signal_name, signal_value in data.items():
            signal = self.get_signal_by_name(signal_name)

            if isinstance(signal_value, (str, NamedSignalValue)):
                # Check choices
                signal_value_num = signal.conversion.choice_to_number(str(signal_value))

                if signal_value_num is None:
                    raise EncodeError(f'Invalid value specified for signal '
                                      f'"{signal.name}": "{signal_value}"')
                continue

            # retrieve the signal's scaled value to perform range check against minimum and maximum,
            # retrieve the signal's raw value to check if exists in value table
            if scaling:
                scaled_value = signal_value
                raw_value = signal.conversion.numeric_scaled_to_raw(scaled_value)
            else:
                scaled_value = cast(
                    'int | float',
                    signal.conversion.raw_to_scaled(raw_value=signal_value, decode_choices=False)
                )
                raw_value = signal_value

            if signal.conversion.choices and raw_value in signal.conversion.choices:
                # skip range check if raw value exists in value table
                continue

            if signal.minimum is not None:
                if scaled_value < signal.minimum - abs(signal.conversion.scale)*1e-6:
                    raise EncodeError(
                        f'Expected signal "{signal.name}" value greater than '
                        f'or equal to {signal.minimum} in message "{self.name}", '
                        f'but got {scaled_value}.')

            if signal.maximum is not None:
                if scaled_value > signal.maximum + abs(signal.conversion.scale)*1e-6:
                    raise EncodeError(
                        f'Expected signal "{signal.name}" value smaller than '
                        f'or equal to {signal.maximum} in message "{self.name}", '
                        f'but got {scaled_value}.')

    def _encode(self, node: Codec, data: SignalMappingType, scaling: bool) -> tuple[int, int, list[Signal]]:
        encoded = encode_data(data,
                              node['signals'],
                              node['formats'],
                              scaling)
        padding_mask = node['formats'].padding_mask
        multiplexers = node['multiplexers']

        all_signals = list(node['signals'])
        for signal in multiplexers:
            mux = self._get_mux_number(data, signal)

            try:
                node = multiplexers[signal][mux]
            except KeyError:
                raise EncodeError(f'Expected multiplexer id in '
                                  f'{{{format_or(list(multiplexers[signal].keys()))}}}, '
                                  f'for multiplexer "{signal}" '
                                  f'but got {mux}') from None

            mux_encoded, mux_padding_mask, mux_signals = \
                self._encode(node, data, scaling)
            all_signals.extend(mux_signals)

            encoded |= mux_encoded
            padding_mask &= mux_padding_mask

        return encoded, padding_mask, all_signals

    def _encode_container(self,
                          data: ContainerEncodeInputType,
                          scaling: bool,
                          padding: bool) -> bytes:

        result = b""

        for header, value in data:
            if isinstance(header, str):
                contained_message = \
                    self.get_contained_message_by_name(header)
            elif isinstance(header, Message):
                # contained message is specified directly. We go once
                # around the circle to ensure that a contained message
                # with the given header ID is there.
                contained_message = \
                    self.get_contained_message_by_header_id(header.header_id) # type: ignore
            elif isinstance(header, int):
                # contained message is specified directly. We go once
                # around the circle to ensure that a contained message
                # with the given header ID is there.
                contained_message = \
                    self.get_contained_message_by_header_id(header)
            else:
                raise EncodeError(f'Could not determine message corresponding '
                                  f'to header {header}')

            if contained_message is None:
                if isinstance(value, bytes) and isinstance(header, int):
                    # the contained message was specified as raw data
                    header_id = header
                else:
                    raise EncodeError(f'No message corresponding to header '
                                      f'{header} could be determined')
            else:
                assert contained_message.header_id is not None
                header_id = contained_message.header_id

            if isinstance(value, bytes):
                # raw data

                # produce a message if size of the blob does not
                # correspond to the size specified by the message
                # which it represents.
                if contained_message is not None and \
                    len(value) != contained_message.length:

                    LOGGER.info(f'Specified data for contained message '
                                f'{contained_message.name} is '
                                f'{len(value)} bytes instead of '
                                f'{contained_message.length} bytes')

                contained_payload = value

            elif isinstance(value, dict):
                # signal_name to signal_value dictionary
                assert contained_message is not None
                contained_payload = contained_message.encode(value,
                                                             scaling,
                                                             padding,
                                                             strict=False)

            else:
                assert contained_message is not None
                raise EncodeError(f'Cannot encode payload for contained '
                                  f'message "{contained_message.name}".')

            hbo = 'big' if self.header_byte_order == 'big_endian' else 'little'
            result += int.to_bytes(header_id,
                                   3,
                                   hbo) # type: ignore
            result += int.to_bytes(len(contained_payload), 1, 'big')
            result += bytes(contained_payload)

        return result

    def encode(self,
               data: EncodeInputType,
               scaling: bool = True,
               padding: bool = False,
               strict: bool = True,
               ) -> bytes:

        """Encode given data as a message of this type.

        If the message is an "ordinary" frame, this method expects a
        key-to-value dictionary as `data` which maps the name of every
        required signal to a value that can be encoded by that
        signal. If the current message is a container message, it
        expects a list of `(contained_message, contained_data)` tuples
        where `contained_message` is either an integer with the header
        ID, the name or the message object of the contained
        message. Similarly, the `contained_data` can either be
        specified as raw binary data (`bytes`) or as a key-to-value
        dictionary of every signal needed to encode the featured
        message.

        If `scaling` is ``False`` no scaling of signals is performed.

        If `padding` is ``True`` unused bits are encoded as 1.

        If `strict` is ``True`` the specified signals must exactly be the
        ones expected, and their values must be within their allowed ranges,
        or an `EncodeError` exception is raised.

        >>> foo = db.get_message_by_name('Foo')
        >>> foo.encode({'Bar': 1, 'Fum': 5.0})
        b'\\x01\\x45\\x23\\x00\\x11'

        """

        if self.is_container:
            if strict:
                if not isinstance(data, (list, tuple)):
                    raise EncodeError('Container frames can only encode lists of '
                                      '(message, data) tuples')

                self.assert_container_encodable(data, scaling=scaling)

            return self._encode_container(cast('ContainerEncodeInputType', data),
                                          scaling,
                                          padding)

        if strict:
            # setting 'strict' to True is just a shortcut for calling
            # 'assert_signals_encodable()' using the strictest
            # settings.
            if not isinstance(data, dict):
                raise EncodeError('The payload for encoding non-container '
                                  'messages must be a signal name to '
                                  'signal value dictionary')
            self.assert_signals_encodable(data, scaling=scaling)

        if self._codecs is None:
            raise ValueError('Codec is not initialized.')

        encoded, padding_mask, _ = self._encode(self._codecs,
                                                cast('SignalMappingType', data),
                                                scaling)

        if padding:
            padding_pattern = int.from_bytes([self._unused_bit_pattern] * self._length, "big")
            encoded |= (padding_mask & padding_pattern)

        return encoded.to_bytes(self._length, "big")

    def _decode(self,
                node: Codec,
                data: bytes,
                decode_choices: bool,
                scaling: bool,
                allow_truncated: bool,
                allow_excess: bool) -> SignalDictType:
        decoded = decode_data(data,
                              self.length,
                              node['signals'],
                              node['formats'],
                              decode_choices,
                              scaling,
                              allow_truncated,
                              allow_excess)

        multiplexers = node['multiplexers']

        for signal in multiplexers:
            if allow_truncated and signal not in decoded:
                continue

            mux = self._get_mux_number(decoded, signal)

            try:
                node = multiplexers[signal][mux]
            except KeyError:
                raise DecodeError(f'expected multiplexer id {format_or(sorted(multiplexers[signal].keys()))}, but got {mux}') from None

            decoded.update(self._decode(node,
                                        data,
                                        decode_choices,
                                        scaling,
                                        allow_truncated,
                                        allow_excess))

        return decoded

    def unpack_container(self,
                         data: bytes,
                         allow_truncated: bool = False) \
                         -> ContainerUnpackResultType:
        """Unwrap the contents of a container message.

        This returns a list of ``(contained_message, contained_data)``
        tuples, i.e., the data for the contained message are ``bytes``
        objects, not decoded signal dictionaries. This is required for
        verifying the correctness of the end-to-end protection or the
        authenticity of a contained message.

        Note that ``contained_message`` is the header ID integer value
        if a contained message is unknown. Further, if something goes
        seriously wrong, a ``DecodeError`` is raised.
        """

        if not self.is_container:
            raise DecodeError(f'Cannot unpack non-container message '
                              f'"{self.name}"')

        if len(data) > self.length:
            raise DecodeError(f'Container message "{self.name}" specified '
                              f'as exhibiting at most {self.length} but '
                              f'received a {len(data)} bytes long frame')

        result: ContainerUnpackListType = []
        pos = 0
        while pos < len(data):
            if pos + 4 > len(data):
                # TODO: better throw an exception? only warn in strict mode?
                LOGGER.info(f'Malformed container message '
                            f'"{self.name}" encountered while decoding: '
                            f'No valid header specified for contained '
                            f'message #{len(result)+1} starting at position '
                            f'{pos}. Ignoring.')
                return result

            contained_id = int.from_bytes(data[pos:pos+3], 'big')
            contained_len = data[pos+3]

            if pos + 4 + contained_len > len(data):
                if not allow_truncated:
                    raise DecodeError(f'Malformed container message '
                                      f'"{self.name}": Contained message '
                                      f'{len(result)+1} would exceed total '
                                      f'message size.')
                else:
                    contained_len = len(data) - pos - 4


            contained_data = data[pos+4:pos+4+contained_len]
            contained_msg = \
                self.get_contained_message_by_header_id(contained_id)
            pos += 4+contained_len

            if contained_msg is None:
                result.append((contained_id, bytes(contained_data)))
            else:
                result.append((contained_msg, bytes(contained_data)))

        return result

    def decode(self,
               data: bytes,
               decode_choices: bool = True,
               scaling: bool = True,
               decode_containers: bool = False,
               allow_truncated: bool = False,
               allow_excess: bool = True,
               ) \
               -> DecodeResultType:
        """Decode given data as a message of this type.

        If `decode_choices` is ``False`` scaled values are not
        converted to choice strings (if available).

        If `scaling` is ``False`` no scaling of signals is performed.

        >>> foo = db.get_message_by_name('Foo')
        >>> foo.decode(b'\\x01\\x45\\x23\\x00\\x11')
        {'Bar': 1, 'Fum': 5.0}

        If `decode_containers` is ``True``, the inner messages are
        decoded if the current message is a container frame. The
        reason why this needs to be explicitly enabled is that the
        result of `decode()` for container frames is a list of
        ``(header_id, signals_dict)`` tuples which might cause code
        that does not expect this to misbehave. Trying to decode a
        container message with `decode_containers` set to ``False``
        will raise a `DecodeError`.

        If `allow_truncated` is ``True``, incomplete messages (i.e.,
        ones where the received data is shorter than specified) will
        be partially decoded, i.e., all signals which are fully
        present in the received data will be decoded, and the
        remaining ones will be omitted. If 'allow_truncated` is set to
        ``False``, `DecodeError` will be raised when trying to decode
        incomplete messages.

        If `allow_excess` is ``True``, data that is are longer than
        the expected message length is decoded, else a `ValueError` is
        raised if such data is encountered.
        """

        if decode_containers and self.is_container:
            return self.decode_container(data,
                                         decode_choices,
                                         scaling,
                                         allow_truncated,
                                         allow_excess)

        return self.decode_simple(data,
                                  decode_choices,
                                  scaling,
                                  allow_truncated,
                                  allow_excess)

    def decode_simple(self,
                      data: bytes,
                      decode_choices: bool = True,
                      scaling: bool = True,
                      allow_truncated: bool = False,
                      allow_excess: bool = True) \
                      -> SignalDictType:
        """Decode given data as a container message.

        This method is identical to ``decode()`` except that the
        message **must not** be a container. If the message is a
        container, an exception is raised.
        """

        if self.is_container:
            raise DecodeError(f'Message "{self.name}" is a container')
        elif self._codecs is None:
            raise ValueError('Codec is not initialized.')

        return self._decode(self._codecs,
                            data,
                            decode_choices,
                            scaling,
                            allow_truncated,
                            allow_excess)

    def decode_container(self,
                         data: bytes,
                         decode_choices: bool = True,
                         scaling: bool = True,
                         allow_truncated: bool = False,
                         allow_excess: bool = True) \
                         -> ContainerDecodeResultType:
        """Decode given data as a container message.

        This method is identical to ``decode()`` except that the
        message **must** be a container. If the message is not a
        container, an exception is raised.
        """

        if not self.is_container:
            raise DecodeError(f'Message "{self.name}" is not a container')

        unpacked = self.unpack_container(data, allow_truncated)

        result: ContainerDecodeResultListType = []

        for contained_message, contained_data in unpacked:
            if not isinstance(contained_message, Message):
                result.append((contained_message, bytes(contained_data)))
                continue

            try:
                decoded = contained_message.decode(contained_data,
                                                   decode_choices,
                                                   scaling,
                                                   decode_containers=False,
                                                   allow_truncated=allow_truncated,
                                                   allow_excess=allow_excess)
            except (ValueError, DecodeError):
                result.append((contained_message, bytes(contained_data)))
                continue

            result.append((contained_message, decoded)) # type: ignore

        return result

    def get_contained_message_by_header_id(self, header_id: int) \
        -> Optional['Message']:

        if self.contained_messages is None:
            return None

        tmp = [ x for x in self.contained_messages if x.header_id == header_id ]

        if len(tmp) == 0:
            return None
        elif len(tmp) > 1:
            raise Error(f'Container message "{self.name}" contains multiple '
                        f'contained messages exhibiting id 0x{header_id:x}')

        return tmp[0]

    def get_contained_message_by_name(self, name: str) \
        -> Optional['Message']:

        if self.contained_messages is None:
            return None

        tmp = [ x for x in self.contained_messages if x.name == name ]

        if len(tmp) == 0:
            return None
        elif len(tmp) > 1:
            raise Error(f'Container message "{self.name}" contains multiple '
                        f'contained messages named "{name}"')

        return tmp[0]

    def get_signal_by_name(self, name: str) -> Signal:
        return self._signal_dict[name]

    def is_multiplexed(self) -> bool:
        """Returns ``True`` if the message is multiplexed, otherwise
        ``False``.

        >>> foo = db.get_message_by_name('Foo')
        >>> foo.is_multiplexed()
        False
        >>> bar = db.get_message_by_name('Bar')
        >>> bar.is_multiplexed()
        True

        """
        if self._codecs is None:
            raise ValueError('Codec is not initialized.')

        return bool(self._codecs['multiplexers'])

    def _check_signal(self, message_bits, signal):
        signal_bits = signal.length * [signal.name]

        if signal.byte_order == 'big_endian':
            padding = start_bit(signal) * [None]
            signal_bits = padding + signal_bits
        else:
            signal_bits += signal.start * [None]

            if len(signal_bits) < len(message_bits):
                padding = (len(message_bits) - len(signal_bits)) * [None]
                reversed_signal_bits = padding + signal_bits
            else:
                reversed_signal_bits = signal_bits

            signal_bits = []

            for i in range(0, len(reversed_signal_bits), 8):
                signal_bits = reversed_signal_bits[i:i + 8] + signal_bits

        # Check that the signal fits in the message.
        if len(signal_bits) > len(message_bits):
            raise Error(f'The signal {signal.name} does not fit in message {self.name}.')

        # Check that the signal does not overlap with other
        # signals.
        for offset, signal_bit in enumerate(signal_bits):
            if signal_bit is not None:
                if message_bits[offset] is not None:
                    raise Error(
                        f'The signals {signal.name} and {message_bits[offset]} are overlapping in message {self.name}.')

                message_bits[offset] = signal.name

    def _check_mux(self, message_bits, mux):
        signal_name, children = next(iter(mux.items()))
        self._check_signal(message_bits,
                           self.get_signal_by_name(signal_name))
        children_message_bits = deepcopy(message_bits)

        for multiplexer_id in sorted(children):
            child_tree = children[multiplexer_id]
            child_message_bits = deepcopy(children_message_bits)
            self._check_signal_tree(child_message_bits, child_tree)

            for i, child_bit in enumerate(child_message_bits):
                if child_bit is not None:
                    message_bits[i] = child_bit

    def _check_signal_tree(self, message_bits, signal_tree):
        for signal_name in signal_tree:
            if isinstance(signal_name, dict):
                self._check_mux(message_bits, signal_name)
            else:
                self._check_signal(message_bits,
                                   self.get_signal_by_name(signal_name))

    def _check_signal_lengths(self):
        for signal in self._signals:
            if signal.length <= 0:
                raise Error(
                    f'The signal {signal.name} length {signal.length} is not greater than 0 in '
                    f'message {self.name}.')

    def refresh(self, strict: bool | None = None) -> None:
        """Refresh the internal message state.

        If `strict` is ``True`` an exception is raised if any signals
        are overlapping or if they don't fit in the message. This
        argument overrides the value of the same argument passed to
        the constructor.

        """

        self._check_signal_lengths()
        self._codecs = self._create_codec()
        self._signal_tree = self._create_signal_tree(self._codecs)
        self._signal_dict = {signal.name: signal for signal in self._signals}

        if strict is None:
            strict = self._strict

        if strict:
            message_bits = 8 * self.length * [None]
            self._check_signal_tree(message_bits, self.signal_tree)

    def __repr__(self) -> str:
        return \
            f'message(' \
            f"'{self._name}', " \
            f'0x{self._frame_id:x}, ' \
            f'{self._is_extended_frame}, '\
            f'{self._length}, ' \
            f'{self._comments})'
