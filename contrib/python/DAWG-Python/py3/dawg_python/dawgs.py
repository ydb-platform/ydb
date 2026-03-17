# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import io
import struct
from binascii import a2b_base64

from library.python.resource import resfs_read

from . import wrapper
from .compat import int_from_byte

class DAWG(object):
    """
    Base DAWG wrapper.
    """
    def __init__(self):
        self.dct = None

    def __contains__(self, key):
        if not isinstance(key, bytes):
            key = key.encode('utf8')
        return self.dct.contains(key)

    def load(self, path):
        """
        Loads DAWG from a file.
        """
        self.dct = wrapper.Dictionary.load(path)
        return self

    def _has_value(self, index):
        return self.dct.has_value(index)

    def _similar_keys(self, current_prefix, key, index, replace_chars):

        res = []
        start_pos = len(current_prefix)
        end_pos = len(key)
        word_pos = start_pos

        while word_pos < end_pos:
            b_step = key[word_pos].encode('utf8')

            if b_step in replace_chars:
                next_index = index
                b_replace_char, u_replace_char = replace_chars[b_step]

                next_index = self.dct.follow_bytes(b_replace_char, next_index)

                if next_index is not None:
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

    def similar_keys(self, key, replaces):
        """
        Returns all variants of ``key`` in this DAWG according to
        ``replaces``.

        ``replaces`` is an object obtained from
        ``DAWG.compile_replaces(mapping)`` where mapping is a dict
        that maps single-char unicode sitrings to another single-char
        unicode strings.

        This may be useful e.g. for handling single-character umlauts.
        """
        return self._similar_keys("", key, self.dct.ROOT, replaces)

    @classmethod
    def compile_replaces(cls, replaces):

        for k,v in replaces.items():
            if len(k) != 1 or len(v) != 1:
                raise ValueError("Keys and values must be single-char unicode strings.")

        return dict(
            (
                k.encode('utf8'),
                (v.encode('utf8'), v)
            )
            for k, v in replaces.items()
        )

    def prefixes(self, key):
        '''
        Returns a list with keys of this DAWG that are prefixes of the ``key``.
        '''
        res = []
        index = self.dct.ROOT
        if not isinstance(key, bytes):
            key = key.encode('utf8')

        pos = 1

        for ch in key:
            index = self.dct.follow_char(int_from_byte(ch), index)
            if not index:
                break

            if self._has_value(index):
                res.append(key[:pos].decode('utf8'))
            pos += 1

        return res



class CompletionDAWG(DAWG):
    """
    DAWG with key completion support.
    """

    def __init__(self):
        super(CompletionDAWG, self).__init__()
        self.guide = None

    def keys(self, prefix=""):
        b_prefix = prefix.encode('utf8')
        res = []

        index = self.dct.follow_bytes(b_prefix, self.dct.ROOT)
        if index is None:
            return res

        completer = wrapper.Completer(self.dct, self.guide)
        completer.start(index, b_prefix)

        while completer.next():
            key = completer.key.decode('utf8')
            res.append(key)

        return res

    def iterkeys(self, prefix=""):
        b_prefix = prefix.encode('utf8')
        index = self.dct.follow_bytes(b_prefix, self.dct.ROOT)
        if index is None:
            return

        completer = wrapper.Completer(self.dct, self.guide)
        completer.start(index, b_prefix)

        while completer.next():
            yield completer.key.decode('utf8')


    def load(self, path):
        """
        Loads DAWG from a file.
        """
        self.dct = wrapper.Dictionary()
        self.guide = wrapper.Guide()

        data = resfs_read(path)
        if data:
            f = io.BytesIO(data)
        else:
            f = open(path, 'rb')

        self.dct.read(f)
        self.guide.read(f)

        f.close()

        return self


PAYLOAD_SEPARATOR = b'\x01'
MAX_VALUE_SIZE = 32768

class BytesDAWG(CompletionDAWG):
    """
    DAWG that is able to transparently store extra binary payload in keys;
    there may be several payloads for the same key.

    In other words, this class implements read-only DAWG-based
    {unicode -> list of bytes objects} mapping.
    """

    def __init__(self, payload_separator=PAYLOAD_SEPARATOR):
        self._payload_separator = payload_separator

    def __contains__(self, key):
        if not isinstance(key, bytes):
            key = key.encode('utf8')
        return bool(self._follow_key(key))

#    def b_has_key(self, key):
#        return bool(self._follow_key(key))

    def __getitem__(self, key):
        res = self.get(key)
        if res is None:
            raise KeyError(key)
        return res

    def get(self, key, default=None):
        """
        Returns a list of payloads (as byte objects) for a given key
        or ``default`` if the key is not found.
        """
        if not isinstance(key, bytes):
            key = key.encode('utf8')

        return self.b_get_value(key) or default

    def _follow_key(self, b_key):
        index = self.dct.follow_bytes(b_key, self.dct.ROOT)
        if not index:
            return False

        index = self.dct.follow_bytes(self._payload_separator, index)
        if not index:
            return False

        return index

    def _value_for_index(self, index):
        res = []

        completer = wrapper.Completer(self.dct, self.guide)

        completer.start(index)
        while completer.next():
            # a2b_base64 doesn't support bytearray in python 2.6
            # so it is converted (and copied) to bytes
            b64_data = bytes(completer.key)
            res.append(a2b_base64(b64_data))

        return res

    def b_get_value(self, b_key):
        index = self._follow_key(b_key)
        if not index:
            return []
        return self._value_for_index(index)

    def keys(self, prefix=""):
        if not isinstance(prefix, bytes):
            prefix = prefix.encode('utf8')
        res = []

        index = self.dct.ROOT

        if prefix:
            index = self.dct.follow_bytes(prefix, index)
            if not index:
                return res

        completer = wrapper.Completer(self.dct, self.guide)
        completer.start(index, prefix)

        while completer.next():
            payload_idx = completer.key.index(self._payload_separator)
            u_key = completer.key[:payload_idx].decode('utf8')
            res.append(u_key)
        return res

    def iterkeys(self, prefix=""):
        if not isinstance(prefix, bytes):
            prefix = prefix.encode('utf8')

        index = self.dct.ROOT

        if prefix:
            index = self.dct.follow_bytes(prefix, index)
            if not index:
                return

        completer = wrapper.Completer(self.dct, self.guide)
        completer.start(index, prefix)

        while completer.next():
            payload_idx = completer.key.index(self._payload_separator)
            u_key = completer.key[:payload_idx].decode('utf8')
            yield u_key

    def items(self, prefix=""):
        if not isinstance(prefix, bytes):
            prefix = prefix.encode('utf8')
        res = []

        index = self.dct.ROOT
        if prefix:
            index = self.dct.follow_bytes(prefix, index)
            if not index:
                return res

        completer = wrapper.Completer(self.dct, self.guide)
        completer.start(index, prefix)

        while completer.next():
            key, value = completer.key.split(self._payload_separator)
            res.append(
                (key.decode('utf8'), a2b_base64(bytes(value))) # bytes() cast is a python 2.6 fix
            )

        return res

    def iteritems(self, prefix=""):
        if not isinstance(prefix, bytes):
            prefix = prefix.encode('utf8')

        index = self.dct.ROOT
        if prefix:
            index = self.dct.follow_bytes(prefix, index)
            if not index:
                return

        completer = wrapper.Completer(self.dct, self.guide)
        completer.start(index, prefix)

        while completer.next():
            key, value = completer.key.split(self._payload_separator)
            item = (key.decode('utf8'), a2b_base64(bytes(value))) # bytes() cast is a python 2.6 fix
            yield item


    def _has_value(self, index):
        return self.dct.follow_bytes(PAYLOAD_SEPARATOR, index)

    def _similar_items(self, current_prefix, key, index, replace_chars):

        res = []
        start_pos = len(current_prefix)
        end_pos = len(key)
        word_pos = start_pos

        while word_pos < end_pos:
            b_step = key[word_pos].encode('utf8')

            if b_step in replace_chars:
                next_index = index
                b_replace_char, u_replace_char = replace_chars[b_step]

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

    def similar_items(self, key, replaces):
        """
        Returns a list of (key, value) tuples for all variants of ``key``
        in this DAWG according to ``replaces``.

        ``replaces`` is an object obtained from
        ``DAWG.compile_replaces(mapping)`` where mapping is a dict
        that maps single-char unicode sitrings to another single-char
        unicode strings.
        """
        return self._similar_items("", key, self.dct.ROOT, replaces)


    def _similar_item_values(self, start_pos, key, index, replace_chars):
        res = []
        end_pos = len(key)
        word_pos = start_pos

        while word_pos < end_pos:
            b_step = key[word_pos].encode('utf8')

            if b_step in replace_chars:
                next_index = index
                b_replace_char, u_replace_char = replace_chars[b_step]

                next_index = self.dct.follow_bytes(b_replace_char, next_index)
                if next_index:
                    extra_items = self._similar_item_values(word_pos+1, key, next_index, replace_chars)
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

    def similar_item_values(self, key, replaces):
        """
        Returns a list of values for all variants of the ``key``
        in this DAWG according to ``replaces``.

        ``replaces`` is an object obtained from
        ``DAWG.compile_replaces(mapping)`` where mapping is a dict
        that maps single-char unicode sitrings to another single-char
        unicode strings.
        """
        return self._similar_item_values(0, key, self.dct.ROOT, replaces)


class RecordDAWG(BytesDAWG):
    def __init__(self, fmt, payload_separator=PAYLOAD_SEPARATOR):
        super(RecordDAWG, self).__init__(payload_separator)
        self._struct = struct.Struct(str(fmt))
        self.fmt = fmt

    def _value_for_index(self, index):
        value = super(RecordDAWG, self)._value_for_index(index)
        return [self._struct.unpack(val) for val in value]

    def items(self, prefix=""):
        res = super(RecordDAWG, self).items(prefix)
        return [(key, self._struct.unpack(val)) for (key, val) in res]

    def iteritems(self, prefix=""):
        res = super(RecordDAWG, self).iteritems(prefix)
        return ((key, self._struct.unpack(val)) for (key, val) in res)


LOOKUP_ERROR = -1

class IntDAWG(DAWG):
    """
    Dict-like class based on DAWG.
    It can store integer values for unicode keys.
    """
    def __getitem__(self, key):
        res = self.get(key, LOOKUP_ERROR)
        if res == LOOKUP_ERROR:
            raise KeyError(key)
        return res

    def get(self, key, default=None):
        """
        Return value for the given key or ``default`` if the key is not found.
        """
        if not isinstance(key, bytes):
            key = key.encode('utf8')
        res = self.b_get_value(key)
        if res == LOOKUP_ERROR:
            return default
        return res

    def b_get_value(self, key):
        return self.dct.find(key)


class IntCompletionDAWG(CompletionDAWG, IntDAWG):
    """
    Dict-like class based on DAWG.
    It can store integer values for unicode keys and support key completion.
    """
    def items(self, prefix=""):
        if not isinstance(prefix, bytes):
            prefix = prefix.encode('utf8')
        res = []
        index = self.dct.ROOT

        if prefix:
            index = self.dct.follow_bytes(prefix, index)
            if not index:
                return res

        completer = wrapper.Completer(self.dct, self.guide)
        completer.start(index, prefix)

        while completer.next():
            res.append(
                (completer.key.decode('utf8'), completer.value())
            )

        return res

    def iteritems(self, prefix=""):
        if not isinstance(prefix, bytes):
            prefix = prefix.encode('utf8')
        index = self.dct.ROOT

        if prefix:
            index = self.dct.follow_bytes(prefix, index)
            if not index:
                return

        completer = wrapper.Completer(self.dct, self.guide)
        completer.start(index, prefix)

        while completer.next():
            yield completer.key.decode('utf8'), completer.value()
