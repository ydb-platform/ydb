import logging
from collections import OrderedDict
from typing import (
    Any,
    TextIO,
)

from ...typechecking import DecodeResultType, EncodeInputType, StringPathLike
from ..errors import DecodeError
from ..utils import (
    SORT_SIGNALS_DEFAULT,
    sort_signals_by_start_bit,
    type_sort_attributes,
    type_sort_choices,
    type_sort_signals,
)
from .bus import Bus
from .formats import arxml, dbc, kcd, sym
from .formats.arxml import AutosarDatabaseSpecifics
from .formats.dbc import DbcSpecifics
from .internal_database import InternalDatabase
from .message import Message
from .node import Node

LOGGER = logging.getLogger(__name__)


class Database:
    """This class contains all messages, signals and definitions of a CAN
    network.

    The factory functions :func:`load()<cantools.database.load()>`,
    :func:`load_file()<cantools.database.load_file()>` and
    :func:`load_string()<cantools.database.load_string()>` returns
    instances of this class.

    If `strict` is ``True`` an exception is raised if any signals are
    overlapping or if they don't fit in their message.

    By default signals are sorted by their start bit when their Message object is created.
    If you don't want them to be sorted pass `sort_signals = None`.
    If you want the signals to be sorted in another way pass something like
    `sort_signals = lambda signals: list(sorted(signals, key=lambda sig: sig.name))`
    """

    def __init__(self,
                 messages: list[Message] | None = None,
                 nodes: list[Node] | None = None,
                 buses: list[Bus] | None = None,
                 version: str | None = None,
                 dbc_specifics: DbcSpecifics | None = None,
                 autosar_specifics: AutosarDatabaseSpecifics | None = None,
                 frame_id_mask: int | None = None,
                 strict: bool = True,
                 sort_signals: type_sort_signals = sort_signals_by_start_bit,
                 ) -> None:
        self._messages = messages or []
        self._nodes = nodes or []
        self._buses = buses or []
        self._name_to_message: dict[str, Message] = {}
        self._frame_id_to_message: dict[int, Message] = {}
        self._version = version
        self._dbc = dbc_specifics
        self._autosar = autosar_specifics

        if frame_id_mask is None:
            frame_id_mask = 0xffffffff

        self._frame_id_mask = frame_id_mask
        self._strict = strict
        self._sort_signals = sort_signals
        self.refresh()

    @property
    def messages(self) -> list[Message]:
        """A list of messages in the database.

        Use :meth:`.get_message_by_frame_id()` or
        :meth:`.get_message_by_name()` to find a message by its frame
        id or name.

        """

        return self._messages

    @property
    def nodes(self) -> list[Node]:
        """A list of nodes in the database.

        """

        return self._nodes

    @property
    def buses(self) -> list[Bus]:
        """A list of CAN buses in the database.

        """

        return self._buses

    @property
    def version(self) -> str | None:
        """The database version, or ``None`` if unavailable.

        """

        return self._version

    @version.setter
    def version(self, value: str | None) -> None:
        self._version = value

    @property
    def dbc(self) -> DbcSpecifics | None:
        """An object containing dbc specific properties like e.g. attributes.

        """

        return self._dbc

    @dbc.setter
    def dbc(self, value: DbcSpecifics | None) -> None:
        self._dbc = value

    @property
    def autosar(self) -> AutosarDatabaseSpecifics | None:
        """An object containing AUTOSAR specific properties like e.g. attributes.

        """

        return self._autosar

    @autosar.setter
    def autosar(self, value: AutosarDatabaseSpecifics | None) -> None:
        self._autosar = value


    def is_similar(self,
                   other: "Database",
                   *,
                   tolerance: float = 1e-12,
                   include_format_specifics: bool = True) -> bool:
        """Compare two database objects inexactly

        This means that small discrepanceies stemming from
        e.g. rounding errors are ignored.
        """
        return not self._differences(other,
                                     tolerance=tolerance,
                                     include_format_specifics=include_format_specifics)

    def _differences(self,
                     other: "Database",
                     *,
                     tolerance: float = 1e-12,
                     include_format_specifics: bool = True) -> list[dict[str, str]]:
        """Return a list of differences between this Database and `other`.

        Each difference is a dict with keys: ``path``, ``self``, ``other``, ``reason``.
        """
        diffs: list[dict[str, str]] = []
        seen: set[tuple[int, int]] = set()

        def path_to_str(path_parts: list[str]) -> str:
            if not path_parts:
                return '<root>'
            return ''.join(path_parts)

        def add_diff(path_parts: list[str], a_val: Any, b_val: Any, reason: str) -> None:
            diffs.append({
                'path': path_to_str(path_parts),
                'self': str(a_val),
                'other': str(b_val),
                'reason': reason,
            })

        def compare(a: Any, b: Any, path_parts: list[str]) -> None:
            pair_id = (id(a), id(b))
            if pair_id in seen:
                return
            seen.add(pair_id)

            if type(a) is not type(b):
                add_diff(path_parts, type(a), type(b), 'type-mismatch')
                return

            if callable(a) or a is None:
                # nothing to compare
                return

            if isinstance(a, (int, str, set, bool)):
                if a != b:
                    add_diff(path_parts, a, b, 'value-mismatch')
                return

            if isinstance(a, float):
                if abs(a) > 1:
                    if abs(1.0 - b / a) > tolerance:
                        add_diff(path_parts, a, b, 'float-rel-diff')
                elif abs(b - a) > tolerance:
                    add_diff(path_parts, a, b, 'float-abs-diff')
                return

            if isinstance(a, (list, tuple)):
                if len(a) != len(b):
                    add_diff(path_parts, len(a), len(b), 'length-mismatch')
                min_len = min(len(a), len(b))
                for i in range(min_len):
                    compare(a[i], b[i], [*path_parts, f'[{i}]'])
                return

            if isinstance(a, (dict, OrderedDict)):
                keys_a = set(a.keys())
                keys_b = set(b.keys())
                for k in sorted(keys_a - keys_b, key=str):
                    add_diff([*path_parts, f'[{k!r}]'], a[k], None, 'key-only-in-a')
                for k in sorted(keys_b - keys_a, key=str):
                    add_diff([*path_parts, f'[{k!r}]'], None, b[k], 'key-only-in-b')
                for k in sorted(keys_a & keys_b, key=str):
                    compare(a[k], b[k], [*path_parts, f'[{k!r}]'])
                return

            # get attributes
            a_names = dir(a)
            b_names = dir(b)

            if not include_format_specifics:
                for x in ('dbc', 'autosar'):
                    if x in a_names:
                        a_names.remove(x)
                    if x in b_names:
                        b_names.remove(x)

            if a_names != b_names:
                add_diff(path_parts, sorted(a_names), sorted(b_names), 'attrib-names-mismatch')
                return

            for name in a_names:
                if name == 'messages' and hasattr(a, '_frame_id_to_message'):
                    # compare messages independent of order
                    compare(a._frame_id_to_message, b._frame_id_to_message, [*path_parts, "._frame_id_to_message"])
                    continue

                if name == 'signals' and hasattr(a, '_signal_dict'):
                    # compare messages independent of order
                    compare(a._signal_dict, b._signal_dict, [*path_parts, "._signal_dict"])
                    continue

                if name.startswith('_'):
                    # skip private attributes
                    continue

                a_attr = getattr(a, name)
                b_attr = getattr(b, name)
                compare(a_attr, b_attr, [*path_parts, f'.{name}'])

        # compare root
        compare(self, other, [])

        return diffs

    def add_arxml(self, fp: TextIO) -> None:
        """Read and parse ARXML data from given file-like object and add the
        parsed data to the database.

        """

        self.add_arxml_string(fp.read())

    def add_arxml_file(self,
                       filename: StringPathLike,
                       encoding: str = 'utf-8') -> None:
        """Open, read and parse ARXML data from given file and add the parsed
        data to the database.

        `encoding` specifies the file encoding.

        """

        with open(filename, encoding=encoding, errors='replace') as fin:
            self.add_arxml(fin)

    def add_arxml_string(self, string: str) -> None:
        """Parse given ARXML data string and add the parsed data to the
        database.

        """

        database = arxml.load_string(string, self._strict, sort_signals=self._sort_signals)

        self._messages += database.messages
        self._nodes = database.nodes
        self._buses = database.buses
        self._version = database.version
        self._dbc = database.dbc
        self._autosar = database.autosar
        self.refresh()

    def add_dbc(self, fp: TextIO) -> None:
        """Read and parse DBC data from given file-like object and add the
        parsed data to the database.

        >>> db = cantools.database.Database()
        >>> with open ('foo.dbc', 'r') as fin:
        ...     db.add_dbc(fin)

        """

        self.add_dbc_string(fp.read())

    def add_dbc_file(self,
                     filename: StringPathLike,
                     encoding: str = 'cp1252') -> None:
        """Open, read and parse DBC data from given file and add the parsed
        data to the database.

        `encoding` specifies the file encoding.

        >>> db = cantools.database.Database()
        >>> db.add_dbc_file('foo.dbc')

        """

        with open(filename, encoding=encoding, errors='replace') as fin:
            self.add_dbc(fin)

    def add_dbc_string(self, string: str) -> None:
        """Parse given DBC data string and add the parsed data to the
        database.

        >>> db = cantools.database.Database()
        >>> with open ('foo.dbc', 'r') as fin:
        ...     db.add_dbc_string(fin.read())

        """

        database = dbc.load_string(string, self._strict, sort_signals=self._sort_signals)

        self._messages += database.messages
        self._nodes = database.nodes
        self._buses = database.buses
        self._version = database.version
        self._dbc = database.dbc
        self.refresh()

    def add_kcd(self, fp: TextIO) -> None:
        """Read and parse KCD data from given file-like object and add the
        parsed data to the database.

        """

        self.add_kcd_string(fp.read())

    def add_kcd_file(self,
                     filename: StringPathLike,
                     encoding: str = 'utf-8') -> None:
        """Open, read and parse KCD data from given file and add the parsed
        data to the database.

        `encoding` specifies the file encoding.

        """

        with open(filename, encoding=encoding, errors='replace') as fin:
            self.add_kcd(fin)

    def add_kcd_string(self, string: str) -> None:
        """Parse given KCD data string and add the parsed data to the
        database.

        """

        database = kcd.load_string(string, self._strict, sort_signals=self._sort_signals)

        self._messages += database.messages
        self._nodes = database.nodes
        self._buses = database.buses
        self._version = database.version
        self._dbc = database.dbc
        self.refresh()

    def add_sym(self, fp: TextIO) -> None:
        """Read and parse SYM data from given file-like object and add the
        parsed data to the database.

        """

        self.add_sym_string(fp.read())

    def add_sym_file(self,
                     filename: StringPathLike,
                     encoding: str = 'utf-8') -> None:
        """Open, read and parse SYM data from given file and add the parsed
        data to the database.

        `encoding` specifies the file encoding.

        """

        with open(filename, encoding=encoding, errors='replace') as fin:
            self.add_sym(fin)

    def add_sym_string(self, string: str) -> None:
        """Parse given SYM data string and add the parsed data to the
        database.

        """

        database = sym.load_string(string, self._strict, sort_signals=self._sort_signals)

        self._messages += database.messages
        self._nodes = database.nodes
        self._buses = database.buses
        self._version = database.version
        self._dbc = database.dbc
        self.refresh()

    def _add_message(self, message: Message) -> None:
        """Add given message to the database.

        """

        if message.name in self._name_to_message:
            LOGGER.warning("Overwriting message '%s' with '%s' in the "
                           "name to message dictionary.",
                           self._name_to_message[message.name].name,
                           message.name)

        masked_frame_id = (message.frame_id & self._frame_id_mask)
        if message.is_extended_frame:
            masked_frame_id |= 0x80000000

        if masked_frame_id in self._frame_id_to_message:
            LOGGER.warning(
                "Overwriting message '%s' with '%s' in the frame id to message "
                "dictionary because they have identical masked frame ids 0x%x.",
                self._frame_id_to_message[masked_frame_id].name,
                message.name,
                masked_frame_id)

        self._name_to_message[message.name] = message
        self._frame_id_to_message[masked_frame_id] = message

    def as_dbc_string(self, *,
                      sort_signals:type_sort_signals=SORT_SIGNALS_DEFAULT,
                      sort_attribute_signals:type_sort_signals=SORT_SIGNALS_DEFAULT,
                      sort_attributes:type_sort_attributes=None,
                      sort_choices:type_sort_choices=None,
                      shorten_long_names:bool=True) -> str:
        """Return the database as a string formatted as a DBC file.
           sort_signals defines how to sort signals in message definitions
           sort_attribute_signals defines how to sort signals in metadata -
              comments, value table definitions and attributes

        """
        if not self._sort_signals and sort_signals == SORT_SIGNALS_DEFAULT:
            sort_signals = None

        return dbc.dump_string(InternalDatabase(self._messages,
                                                self._nodes,
                                                self._buses,
                                                self._version,
                                                self._dbc),
                               sort_signals=sort_signals,
                               sort_attribute_signals=sort_attribute_signals,
                               sort_attributes=sort_attributes,
                               sort_choices=sort_choices,
                               shorten_long_names=shorten_long_names)

    def as_kcd_string(self, *, sort_signals:type_sort_signals=SORT_SIGNALS_DEFAULT) -> str:
        """Return the database as a string formatted as a KCD file.

        """
        if not self._sort_signals and sort_signals == SORT_SIGNALS_DEFAULT:
            sort_signals = None

        return kcd.dump_string(InternalDatabase(self._messages,
                                                self._nodes,
                                                self._buses,
                                                self._version,
                                                self._dbc),
                               sort_signals=sort_signals)

    def as_sym_string(self, *, sort_signals:type_sort_signals=SORT_SIGNALS_DEFAULT) -> str:
        """Return the database as a string formatted as a SYM file.

        """
        if not self._sort_signals and sort_signals == SORT_SIGNALS_DEFAULT:
            sort_signals = None

        return sym.dump_string(InternalDatabase(self._messages,
                                                self._nodes,
                                                self._buses,
                                                self._version,
                                                self._dbc),
                               sort_signals=sort_signals)

    def get_message_by_name(self, name: str) -> Message:
        """Find the message object for given name `name`.

        """

        return self._name_to_message[name]

    def get_message_by_frame_id(self, frame_id: int, force_extended_id: bool = False) -> Message:
        """Find the message object for given frame id `frame_id`.

        """

        if force_extended_id or frame_id > 0x7FF:
            frame_id |= 0x80000000

        return self._frame_id_to_message[frame_id & (0x80000000 | self._frame_id_mask)]

    def get_node_by_name(self, name: str) -> Node:
        """Find the node object for given name `name`.

        """

        for node in self._nodes:
            if node.name == name:
                return node

        raise KeyError(name)

    def get_bus_by_name(self, name: str) -> Bus:
        """Find the bus object for given name `name`.

        """

        for bus in self._buses:
            if bus.name == name:
                return bus

        raise KeyError(name)

    def encode_message(self,
                       frame_id_or_name: int | str,
                       data: EncodeInputType,
                       scaling: bool = True,
                       padding: bool = False,
                       strict: bool = True,
                       force_extended_id: bool = False,
                       ) -> bytes:
        """Encode given signal data `data` as a message of given frame id or
        name `frame_id_or_name`. For regular Messages, `data` is a
        dictionary of signal name-value entries, for container
        messages it is a list of (ContainedMessageOrMessageName,
        ContainedMessageSignals) tuples.

        If `scaling` is ``False`` no scaling of signals is performed.

        If `padding` is ``True`` unused bits are encoded as 1.

        If `strict` is ``True`` all signal values must be within their
        allowed ranges, or an exception is raised.

        >>> db.encode_message(158, {'Bar': 1, 'Fum': 5.0})
        b'\\x01\\x45\\x23\\x00\\x11'
        >>> db.encode_message('Foo', {'Bar': 1, 'Fum': 5.0})
        b'\\x01\\x45\\x23\\x00\\x11'

        """

        if isinstance(frame_id_or_name, int):
            if force_extended_id or frame_id_or_name > 0x7FF:
                frame_id_or_name |= 0x80000000
            message = self._frame_id_to_message[frame_id_or_name]
        elif isinstance(frame_id_or_name, str):
            message = self._name_to_message[frame_id_or_name]
        else:
            raise ValueError(f"Invalid frame_id_or_name '{frame_id_or_name}'")

        return message.encode(data, scaling, padding, strict)

    def decode_message(self,
                       frame_id_or_name: int | str,
                       data: bytes,
                       decode_choices: bool = True,
                       scaling: bool = True,
                       decode_containers: bool = False,
                       allow_truncated:  bool = False,
                       force_extended_id: bool = False,
                       ) \
        -> DecodeResultType:

        """Decode given signal data `data` as a message of given frame id or
        name `frame_id_or_name`. Returns a dictionary of signal
        name-value entries.

        If `decode_choices` is ``False`` scaled values are not
        converted to choice strings (if available).

        If `scaling` is ``False`` no scaling of signals is performed.

        >>> db.decode_message(158, b'\\x01\\x45\\x23\\x00\\x11')
        {'Bar': 1, 'Fum': 5.0}
        >>> db.decode_message('Foo', b'\\x01\\x45\\x23\\x00\\x11')
        {'Bar': 1, 'Fum': 5.0}

        If `decode_containers` is ``True``, container frames are
        decoded. The reason why this needs to be explicitly enabled is
        that decoding container frames returns a list of ``(Message,
        SignalsDict)`` tuples which will cause code that does not
        expect this to misbehave. Trying to decode a container message
        with `decode_containers` set to ``False`` will raise a
        `DecodeError`.
        """

        if isinstance(frame_id_or_name, int):
            if force_extended_id or frame_id_or_name > 0x7FF:
                frame_id_or_name |= 0x80000000
            message = self._frame_id_to_message[frame_id_or_name]
        elif isinstance(frame_id_or_name, str):
            message = self._name_to_message[frame_id_or_name]
        else:
            raise ValueError(f"Invalid frame_id_or_name '{frame_id_or_name}'")

        if message.is_container:
            if decode_containers:
                return message.decode(data,
                                      decode_choices,
                                      scaling,
                                      decode_containers=True,
                                      allow_truncated=allow_truncated)
            else:
                raise DecodeError(f'Message "{message.name}" is a container '
                                  f'message, but decoding such messages has '
                                  f'not been enabled!')

        return message.decode(data,
                              decode_choices,
                              scaling,
                              allow_truncated=allow_truncated)

    def refresh(self) -> None:
        """Refresh the internal database state.

        This method must be called after modifying any message in the
        database to refresh the internal lookup tables used when
        encoding and decoding messages.

        """

        self._name_to_message = {}
        self._frame_id_to_message = {}

        for message in self._messages:
            message.refresh(self._strict)
            self._add_message(message)

    def __repr__(self) -> str:
        lines = [f"version('{self._version}')", '']

        if self._nodes:
            for node in self._nodes:
                lines.append(repr(node))

            lines.append('')

        for message in self._messages:
            lines.append(repr(message))

            for signal in message.signals:
                lines.append('  ' + repr(signal))

            lines.append('')

        return '\n'.join(lines)
