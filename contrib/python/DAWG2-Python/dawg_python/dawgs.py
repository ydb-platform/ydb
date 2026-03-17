from __future__ import annotations

import struct
from binascii import a2b_base64
from typing import TYPE_CHECKING

from . import wrapper

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any, Iterator, Mapping

    from typing_extensions import Self, TypeAlias

    Replaces: TypeAlias = Mapping[str, str | list[str]]
    CompiledReplaces: TypeAlias = Mapping[str, list[tuple[bytes, str]]]


class DAWG:
    """
    Base DAWG wrapper.
    """

    dct: wrapper.Dictionary | None

    def __init__(self) -> None:
        self.dct = None

    def __contains__(self, key: str | bytes) -> bool:
        if not isinstance(key, bytes):
            key = key.encode("utf8")
        return self.dct.contains(key)

    def load(self, path: str | Path) -> Self:
        """
        Loads DAWG from a file.
        """
        self.dct = wrapper.Dictionary.load(path)
        return self

    def _has_value(self, index: int) -> bool:
        return self.dct.has_value(index)

    def _similar_keys(self, current_prefix: str, key: str, index: int, replace_chars: CompiledReplaces) -> list[str]:
        res = []
        start_pos = len(current_prefix)
        end_pos = len(key)
        word_pos = start_pos

        while word_pos < end_pos:
            b_step = key[word_pos].encode("utf8")

            if b_step in replace_chars:
                for b_replace_char, u_replace_char in replace_chars[b_step]:
                    next_index = index

                    next_index = self.dct.follow_bytes(b_replace_char, next_index)

                    if next_index:
                        prefix = current_prefix + key[start_pos:word_pos] + u_replace_char
                        extra_keys = self._similar_keys(prefix, key, next_index, replace_chars)
                        res += extra_keys

            index = self.dct.follow_bytes(b_step, index)
            if index is None:
                break
            word_pos += 1

        else:
            if self._has_value(index):
                found_key = current_prefix + key[start_pos:]
                res.insert(0, found_key)

        return res

    def similar_keys(self, key: str, replaces: CompiledReplaces) -> list[str]:
        """
        Returns all variants of ``key`` in this DAWG according to
        ``replaces``.

        ``replaces`` is an object obtained from
        ``DAWG.compile_replaces(mapping)`` where mapping is a dict
        that maps single-char unicode strings to (one or more) single-char
        unicode strings.

        This may be useful e.g. for handling single-character umlauts.
        """
        return self._similar_keys("", key, self.dct.ROOT, replaces)

    @staticmethod
    def compile_replaces(replaces: Replaces) -> CompiledReplaces:
        for k, v in replaces.items():
            if len(k) != 1:
                msg = "Keys must be single-char unicode strings."
                raise ValueError(msg)
            if isinstance(v, str) and len(v) != 1:
                msg = "Values must be single-char unicode strings or non-empty lists of such."
                raise ValueError(msg)
            if isinstance(v, list) and (any(len(v_entry) != 1 for v_entry in v) or len(v) < 1):
                msg = "Values must be single-char unicode strings or non-empty lists of such."
                raise ValueError(msg)

        return {k.encode("utf8"): [(v_entry.encode("utf8"), v_entry) for v_entry in v] for k, v in replaces.items()}

    def prefixes(self, key: str | bytes) -> list[str]:
        """
        Returns a list with keys of this DAWG that are prefixes of the ``key``.
        """
        res = []
        index = self.dct.ROOT
        if not isinstance(key, bytes):
            key = key.encode("utf8")

        pos = 1

        for ch in key:
            index = self.dct.follow_char(ch, index)
            if not index:
                break

            if self._has_value(index):
                res.append(key[:pos].decode("utf8"))
            pos += 1

        return res


class CompletionDAWG(DAWG):
    """
    DAWG with key completion support.
    """

    guide: wrapper.Guide | None

    def __init__(self) -> None:
        super().__init__()
        self.guide = None

    def keys(self, prefix: str = "") -> list[str]:
        return list(self.iterkeys(prefix))

    def iterkeys(self, prefix: str = "") -> Iterator[str]:
        b_prefix = prefix.encode("utf8")
        index = self.dct.follow_bytes(b_prefix, self.dct.ROOT)
        if index is None:
            return

        completer = wrapper.Completer(self.dct, self.guide)
        completer.start(index, b_prefix)

        while completer.next():
            yield completer.key.decode("utf8")

    def load(self, path: str | Path) -> Self:
        """
        Loads DAWG from a file.
        """
        self.dct = wrapper.Dictionary()
        self.guide = wrapper.Guide()

        with open(path, "rb") as f:
            self.dct.read(f)
            self.guide.read(f)

        return self


PAYLOAD_SEPARATOR = b"\x01"
MAX_VALUE_SIZE = 32768


class BytesDAWG(CompletionDAWG):
    """
    DAWG that is able to transparently store extra binary payload in keys;
    there may be several payloads for the same key.

    In other words, this class implements read-only DAWG-based
    {unicode -> list of bytes objects} mapping.
    """

    def __init__(self, payload_separator: bytes = PAYLOAD_SEPARATOR) -> None:
        super().__init__()
        self._payload_separator = payload_separator

    def __contains__(self, key: str | bytes) -> bool:
        if not isinstance(key, bytes):
            key = key.encode("utf8")
        return bool(self._follow_key(key))

    def __getitem__(self, key: str | bytes) -> list[bytes]:
        res = self.get(key)
        if res is None:
            raise KeyError(key)
        return res

    def get(self, key: str | bytes, default: list[bytes] | None = None) -> list[bytes] | None:
        """
        Returns a list of payloads (as byte objects) for a given key
        or ``default`` if the key is not found.
        """
        if not isinstance(key, bytes):
            key = key.encode("utf8")

        return self.b_get_value(key) or default

    def _follow_key(self, b_key: bytes) -> int | None:
        index = self.dct.follow_bytes(b_key, self.dct.ROOT)
        if not index:
            return None

        index = self.dct.follow_bytes(self._payload_separator, index)
        if not index:
            return None

        return index

    def _value_for_index(self, index: int) -> list[bytes]:
        res = []

        completer = wrapper.Completer(self.dct, self.guide)

        completer.start(index)
        while completer.next():
            b64_data = completer.key
            res.append(a2b_base64(b64_data))

        return res

    def b_get_value(self, b_key: bytes) -> list[bytes]:
        index = self._follow_key(b_key)
        if not index:
            return []
        return self._value_for_index(index)

    def keys(self, prefix: str | bytes = "") -> list[str]:
        return list(self.iterkeys(prefix))

    def iterkeys(self, prefix: str | bytes = "") -> Iterator[bytes]:
        if not isinstance(prefix, bytes):
            prefix = prefix.encode("utf8")

        index = self.dct.ROOT

        if prefix:
            index = self.dct.follow_bytes(prefix, index)
            if not index:
                return

        completer = wrapper.Completer(self.dct, self.guide)
        completer.start(index, prefix)

        while completer.next():
            payload_idx = completer.key.index(self._payload_separator)
            u_key = completer.key[:payload_idx].decode("utf8")
            yield u_key

    def items(self, prefix: str | bytes = "") -> list[tuple[str, bytes]]:
        return list(self.iteritems(prefix))

    def iteritems(self, prefix: str | bytes = "") -> Iterator[tuple[str, bytes]]:
        if not isinstance(prefix, bytes):
            prefix = prefix.encode("utf8")

        index = self.dct.ROOT
        if prefix:
            index = self.dct.follow_bytes(prefix, index)
            if not index:
                return

        completer = wrapper.Completer(self.dct, self.guide)
        completer.start(index, prefix)

        while completer.next():
            key, value = completer.key.split(self._payload_separator)
            item = (key.decode("utf8"), a2b_base64(value))
            yield item

    def _has_value(self, index: int) -> int | None:
        return self.dct.follow_bytes(PAYLOAD_SEPARATOR, index)

    def _similar_items(
        self,
        current_prefix: str,
        key: str,
        index: int,
        replace_chars: CompiledReplaces,
    ) -> list[tuple[str, bytes]]:
        res = []
        start_pos = len(current_prefix)
        end_pos = len(key)
        word_pos = start_pos

        while word_pos < end_pos:
            b_step = key[word_pos].encode("utf8")

            if b_step in replace_chars:
                for b_replace_char, u_replace_char in replace_chars[b_step]:
                    next_index = index

                    next_index = self.dct.follow_bytes(b_replace_char, next_index)

                    if next_index:
                        prefix = current_prefix + key[start_pos:word_pos] + u_replace_char
                        extra_items = self._similar_items(prefix, key, next_index, replace_chars)
                        res += extra_items

            index = self.dct.follow_bytes(b_step, index)
            if not index:
                break
            word_pos += 1

        else:
            index = self.dct.follow_bytes(self._payload_separator, index)
            if index:
                found_key = current_prefix + key[start_pos:]
                value = self._value_for_index(index)
                res.insert(0, (found_key, value))

        return res

    def similar_items(self, key: str, replaces: CompiledReplaces) -> list[tuple[str, bytes]]:
        """
        Returns a list of (key, value) tuples for all variants of ``key``
        in this DAWG according to ``replaces``.

        ``replaces`` is an object obtained from
        ``DAWG.compile_replaces(mapping)`` where mapping is a dict
        that maps single-char unicode strings to (one or more) single-char
        unicode strings.
        """
        return self._similar_items("", key, self.dct.ROOT, replaces)

    def _similar_item_values(
        self,
        start_pos: int,
        key: str,
        index: int,
        replace_chars: CompiledReplaces,
    ) -> list[bytes]:
        res = []
        end_pos = len(key)
        word_pos = start_pos

        while word_pos < end_pos:
            b_step = key[word_pos].encode("utf8")

            if b_step in replace_chars:
                for b_replace_char, _u_replace_char in replace_chars[b_step]:
                    next_index = index

                    next_index = self.dct.follow_bytes(b_replace_char, next_index)

                    if next_index:
                        extra_items = self._similar_item_values(word_pos + 1, key, next_index, replace_chars)
                        res += extra_items

            index = self.dct.follow_bytes(b_step, index)
            if not index:
                break
            word_pos += 1

        else:
            index = self.dct.follow_bytes(self._payload_separator, index)
            if index:
                value = self._value_for_index(index)
                res.insert(0, value)

        return res

    def similar_item_values(self, key: str, replaces: CompiledReplaces) -> list[bytes]:
        """
        Returns a list of values for all variants of the ``key``
        in this DAWG according to ``replaces``.

        ``replaces`` is an object obtained from
        ``DAWG.compile_replaces(mapping)`` where mapping is a dict
        that maps single-char unicode strings to (one or more) single-char
        unicode strings.
        """
        return self._similar_item_values(0, key, self.dct.ROOT, replaces)


class RecordDAWG(BytesDAWG):
    def __init__(self, fmt: str | bytes, payload_separator: bytes = PAYLOAD_SEPARATOR) -> None:
        super().__init__(payload_separator)
        self._struct = struct.Struct(fmt)
        self.fmt = fmt

    def _value_for_index(self, index: int) -> list[tuple[Any, ...]]:
        value = super()._value_for_index(index)
        return [self._struct.unpack(val) for val in value]

    def items(self, prefix: str | bytes = "") -> list[tuple[str, tuple[Any, ...]]]:
        return list(self.iteritems(prefix))

    def iteritems(self, prefix: str | bytes = "") -> Iterator[tuple[str, tuple[Any, ...]]]:
        res = super().iteritems(prefix)
        return ((key, self._struct.unpack(val)) for (key, val) in res)


LOOKUP_ERROR = -1


class IntDAWG(DAWG):
    """
    Dict-like class based on DAWG.
    It can store integer values for unicode keys.
    """

    def __getitem__(self, key: str | bytes) -> int | None:
        res = self.get(key, LOOKUP_ERROR)
        if res == LOOKUP_ERROR:
            raise KeyError(key)
        return res

    def get(self, key: str | bytes, default: int | None = None) -> int | None:
        """
        Return value for the given key or ``default`` if the key is not found.
        """
        if not isinstance(key, bytes):
            key = key.encode("utf8")
        res = self.b_get_value(key)
        if res == LOOKUP_ERROR:
            return default
        return res

    def b_get_value(self, key: bytes) -> int:
        return self.dct.find(key)


class IntCompletionDAWG(CompletionDAWG, IntDAWG):
    """
    Dict-like class based on DAWG.
    It can store integer values for unicode keys and support key completion.
    """

    def items(self, prefix: str | bytes = "") -> list[tuple[str, int]]:
        return list(self.iteritems(prefix))

    def iteritems(self, prefix: str | bytes = "") -> Iterator[tuple[str, int]]:
        if not isinstance(prefix, bytes):
            prefix = prefix.encode("utf8")
        index = self.dct.ROOT

        if prefix:
            index = self.dct.follow_bytes(prefix, index)
            if not index:
                return

        completer = wrapper.Completer(self.dct, self.guide)
        completer.start(index, prefix)

        while completer.next():
            yield completer.key.decode("utf8"), completer.value()
