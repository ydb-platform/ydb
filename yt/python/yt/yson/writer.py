# -*- coding: utf-8 -*-

"""``yson`` exposes an API familiar to users of the standard library
:mod:`marshal` and :mod:`pickle` modules.

Serializable types (rules applied top to bottom):
int, long -> yson int
str -> yson string
unicode -> yson string (using specified encoding)
any Mapping -> yson dict
any Iterable -> yson list

Simple examples::

    >>> import yson
    >>> b = [4, 5, 6]
    >>> print yson.dumps({"a" : b, "b" : b}, indent="  ")
    {
      "a" : [
        4;
        5;
        6;
      ];
      "b" : [
        4;
        5;
        6;
      ];
    }
    >>> print yson.dumps(("a", "a"), indent="  ")
    [
      "a";
      "a";
    ]
    >>> print yson.dumps(123456)
    123456
    >>> print yson.dumps(u'"Hello world!" -- "Превед, медвед!"')
    "\"Hello world!\" -- \"\xd0\x9f\xd1\x80\xd0\xb5\xd0\xb2\xd0\xb5\xd0\xb4, \xd0\xbc\xd0\xb5\xd0\xb4\xd0\xb2\xd0\xb5\xd0\xb4!\""
"""

from .common import (YsonError,
                     STRING_MARKER, INT64_MARKER, DOUBLE_MARKER,
                     FALSE_MARKER, TRUE_MARKER, UINT64_MARKER)
from . import yson_types

try:
    from yt.packages.six.moves import map as imap
    from yt.packages.six import (integer_types, text_type, binary_type,
                                 iteritems, iterkeys, iterbytes, PY3)
except ImportError:
    from six.moves import map as imap
    from six import (integer_types, text_type, binary_type,
                     iteritems, iterkeys, iterbytes, PY3)

import math
import struct
# Python3 compatibility
try:
    from collections.abc import Iterable, Mapping
except ImportError:
    from collections import Iterable, Mapping

__all__ = ["dump", "dumps"]


def _is_hex_digit(c):
    return ord(b'0') <= c <= ord(b'9') or ord(b'A') <= c <= ord(b'F') or ord(b'a') <= c <= ord(b'f')


def _is_oct_digit(c):
    return ord(b'0') <= c <= ord(b'7')


def _escape_byte(c, nxt, r):
    if c == ord(b"\""):
        r += b"\\\""
    elif c == ord(b"\\"):
        r += b"\\\\"
    elif 32 <= c <= 126:
        r.append(c)
    elif c == ord(b"\r"):
        r += b"\\r"
    elif c == ord(b"\n"):
        r += b"\\n"
    elif c == ord(b"\t"):
        r += b"\\t"
    elif c < 8 and not _is_oct_digit(nxt):
        r += '\\{}'.format(c).encode("ascii")
    elif not _is_hex_digit(nxt):
        r += '\\x{:02X}'.format(c).encode("ascii")
    else:
        r += '\\{:03o}'.format(c).encode("ascii")


def _escape_bytes(obj):
    if len(obj) == 0:
        return b""

    res = bytearray()
    iterator = iterbytes(obj)
    cur = next(iterator)
    for nxt in iterator:
        _escape_byte(cur, nxt, res)
        cur = nxt
    _escape_byte(cur, ord(b" "), res)
    return bytes(res)


def dump(object, stream, yson_format=None, yson_type=None, indent=None,
         ignore_inner_attributes=False, encoding="utf-8", sort_keys=False,
         check_circular=True):
    """Serializes `object` as a YSON formatted stream to `stream`.

    :param str yson_format: format of YSON, one of ["binary", "text", "pretty"].
    :param str yson_type: type of YSON, one of ["node", "list_fragment", "map_fragment"].
    :param int indent: number of indentation spaces in pretty format.
    :param bool ignore_inner_attributes: skip attributes of non-top-level values.
    :param str encoding: encoding that uses to encode unicode strings.
    :param bool sort_keys: if True, mapping items are printed in sorted order.
    :param bool check_circular: prevent the attempt to serialize an object with reference loop.
    """

    stream.write(dumps(object, yson_format=yson_format, yson_type=yson_type, indent=indent,
                       ignore_inner_attributes=ignore_inner_attributes,
                       encoding=encoding, sort_keys=sort_keys,
                       check_circular=check_circular))


class YsonContext(object):
    def __init__(self):
        self.path_parts = []
        self.row_index = None

    def push(self, key_or_index):
        self.path_parts.append(key_or_index)

    def pop(self):
        self.path_parts.pop()


def _raise_error_with_context(message, context):
    attributes = {}
    if context.row_index is not None:
        attributes["row_index"] = context.row_index

    path_parts = imap(str, context.path_parts)
    if context.path_parts:
        attributes["row_key_path"] = "/" + "/".join(path_parts)
    raise YsonError(message, attributes=attributes)


def _zig_zag_encode(value):
    return (value >> 63) ^ (value << 1)


def _dump_varint(value):
    assert 0 <= value <= 2 ** 64 - 1
    result = bytearray()
    while value >= 0x80:
        result.append(0x80 | (value & 0x7F))
        value >>= 7
    result.append(value)
    return bytes(result)


def dumps(object, yson_format=None, yson_type=None, indent=None,
          ignore_inner_attributes=False, encoding="utf-8", sort_keys=False,
          check_circular=True):
    """Serializes `object` as a YSON formatted stream to string and returns it. See :func:`dump <.dump>`."""
    if indent is None:
        indent = 4
    if isinstance(indent, int):
        indent = b" " * indent
    if yson_format is None:
        yson_format = "text"
    if yson_format not in ("pretty", "text", "binary"):
        raise YsonError("{0} format is not supported".format(yson_format))
    if yson_format in ("text", "binary"):
        indent = None
    if yson_type is not None:
        if yson_type not in ["list_fragment", "map_fragment", "node"]:
            raise YsonError("YSON type {0} is not supported".format(yson_type))
    else:
        yson_type = "node"

    is_text = yson_format in ("text", "pretty")
    d = Dumper(check_circular, encoding, indent, yson_type, sort_keys,
               is_text=is_text, ignore_inner_attributes=ignore_inner_attributes)
    return d.dumps(object, YsonContext())


class Dumper(object):
    def __init__(self, check_circular, encoding, indent, yson_type, sort_keys,
                 is_text, ignore_inner_attributes):
        self.yson_type = yson_type

        self._seen_objects = None
        if check_circular:
            self._seen_objects = {}

        self._encoding = encoding
        self._format = FormatDetails(indent, sort_keys)
        if yson_type == "node":
            self._level = -1
        else:
            self._level = -2  # Stream elements are one level deep, but need not be indented

        self._is_text = is_text
        self._ignore_inner_attributes = ignore_inner_attributes

    def _has_attributes(self, obj):
        if hasattr(obj, "has_attributes"):
            return obj.has_attributes()
        return hasattr(obj, "attributes")

    def dumps(self, obj, context):
        if hasattr(obj, "to_yson_type") and callable(obj.to_yson_type):
            return self.dumps(obj.to_yson_type(), context)
        self._level += 1
        attributes = b""
        if self._has_attributes(obj) and (not self._ignore_inner_attributes or self._level == 0):
            if not isinstance(obj.attributes, dict):
                _raise_error_with_context('Invalid field "attributes": it must be string or None', context)
            if obj.attributes:
                attributes = self._dump_attributes(obj.attributes, context)

        result = None
        if obj is False or (isinstance(obj, yson_types.YsonBoolean) and not obj):
            if self._is_text:
                result = b"%false"
            else:
                result = FALSE_MARKER
        elif obj is True or (isinstance(obj, yson_types.YsonBoolean) and obj):
            if self._is_text:
                result = b"%true"
            else:
                result = TRUE_MARKER
        elif isinstance(obj, integer_types):
            if obj < -2 ** 63 or obj >= 2 ** 64:
                _raise_error_with_context("Integer {0} cannot be represented in YSON "
                                          "since it is out of range [-2^63, 2^64 - 1])".format(obj), context)

            greater_than_max_int64 = obj >= 2 ** 63
            if isinstance(obj, yson_types.YsonUint64) and obj < 0:
                _raise_error_with_context("Can not dump negative integer as YSON uint64", context)
            if isinstance(obj, yson_types.YsonInt64) and greater_than_max_int64:
                _raise_error_with_context("Can not dump integer greater than 2^63-1 as YSON int64", context)

            result = self._dump_integer(obj, greater_than_max_int64)
        elif isinstance(obj, float):
            result = self._dump_float(obj)
        elif isinstance(obj, (text_type, binary_type, yson_types.YsonStringProxy)):
            result = self._dump_string(obj, context)
        elif isinstance(obj, Mapping):
            result = self._dump_map(obj, context)
        elif isinstance(obj, Iterable):
            result = self._dump_list(obj, context)
        elif isinstance(obj, yson_types.YsonEntity) or obj is None:
            result = b"#"
        else:
            _raise_error_with_context("{0!r} is not Yson serializable".format(obj), context)
        self._level -= 1
        return attributes + result

    def _dump_integer(self, obj, force_uint64):
        if self._is_text:
            if isinstance(obj, (yson_types.YsonInt64, yson_types.YsonUint64)):
                obj_str = str(yson_types._YsonIntegerBase(obj))
            else:
                obj_str = str(obj)

            result = obj_str.encode("ascii")
            if not PY3:
                result = result.rstrip(b"L")
            if force_uint64 or isinstance(obj, yson_types.YsonUint64):
                result += b"u"
        else:
            if force_uint64 or isinstance(obj, yson_types.YsonUint64):
                result = UINT64_MARKER + _dump_varint(obj)
            else:
                result = INT64_MARKER + _dump_varint(_zig_zag_encode(obj))
        return result

    def _dump_float(self, obj):
        if self._is_text:
            if math.isnan(obj):
                result = b"%nan"
            elif math.isinf(obj):
                if obj > 0:
                    result = b"%inf"
                else:
                    result = b"%-inf"
            else:
                if type(obj) is yson_types.YsonDouble:
                    obj_str = str(float(obj))
                else:
                    obj_str = str(obj)
                result = obj_str.encode("ascii")
        else:
            result = DOUBLE_MARKER + struct.pack("<d", obj)
        return result

    def _dump_string(self, obj, context):
        if isinstance(obj, binary_type):
            result = obj
        elif isinstance(obj, text_type):
            if self._encoding is None:
                _raise_error_with_context('Cannot encode unicode object {0!r} to bytes since "encoding" '
                                          'parameter is None. Consider using byte strings '
                                          'instead or specify encoding'.format(obj),
                                          context)
            result = obj.encode(self._encoding)
        elif isinstance(obj, yson_types.YsonStringProxy):
            result = obj._bytes
        else:
            assert False
        if self._is_text:
            return b"".join([b'"', _escape_bytes(result), b'"'])
        else:
            encoded_len = _zig_zag_encode(len(result))
            return b"".join([STRING_MARKER, _dump_varint(encoded_len), result])

    def _dump_map(self, obj, context):
        is_stream = self.yson_type == "map_fragment" and self._level == -1
        result = []
        if not is_stream:
            result += [b"{", self._format.nextline()]

        for k, v in self._format.mapping_iter(obj):
            if not isinstance(k, (text_type, binary_type, yson_types.YsonStringProxy)):
                _raise_error_with_context("Only string can be Yson map key. Key: {0!r}".format(k), context)

            @self._circular_check(v)
            def process_item():
                context.push(k)
                item = [self._format.prefix(self._level + 1),
                        self._dump_string(k, context), self._format.space(), b"=",
                        self._format.space(), self.dumps(v, context), b";", self._format.nextline(is_stream)]
                context.pop()
                return item

            result += process_item()

        if not is_stream:
            result += [self._format.prefix(self._level), b"}"]

        return b"".join(result)

    def _dump_list(self, obj, context):
        is_stream = self.yson_type == "list_fragment" and self._level == -1
        result = []
        if not is_stream:
            result += [b"[", self._format.nextline()]

        for index, v in enumerate(obj):
            @self._circular_check(v)
            def process_item():
                if is_stream:
                    context.row_index = index
                else:
                    context.push(index)
                item = [self._format.prefix(self._level + 1),
                        self.dumps(v, context), b";", self._format.nextline(is_stream)]
                if not is_stream:
                    context.pop()
                return item

            result += process_item()

        if not is_stream:
            result += [self._format.prefix(self._level), b"]"]

        return b"".join(result)

    def _dump_attributes(self, obj, context):
        result = [b"<", self._format.nextline()]
        for k, v in obj.items():
            if not isinstance(k, (text_type, binary_type)):
                _raise_error_with_context("Only string can be Yson map key. Key: {0!r}".format(obj), context)

            @self._circular_check(v)
            def process_item():
                context.push("@" + k)
                item = [self._format.prefix(self._level + 1),
                        self._dump_string(k, context), self._format.space(), b"=",
                        self._format.space(), self.dumps(v, context), b";", self._format.nextline()]
                context.pop()
                return item

            result += process_item()
        result += [self._format.prefix(self._level), b">"]
        return b"".join(result)

    def _circular_check(self, obj):
        def decorator(fn):
            def wrapper(*args, **kwargs):
                obj_id = None
                if self._seen_objects is not None:
                    obj_id = id(obj)
                    if obj_id in self._seen_objects:
                        raise YsonError("Circular reference detected. Object: {0!r}".format(obj))
                    else:
                        self._seen_objects[obj_id] = obj

                result = fn(*args, **kwargs)

                if self._seen_objects:
                    del self._seen_objects[obj_id]
                return result

            return wrapper
        return decorator


class FormatDetails(object):
    def __init__(self, indent, sort_keys=False):
        self._indent = indent
        self._sort_keys = sort_keys

    def prefix(self, level):
        if self._indent:
            return b"".join([self._indent] * level)
        else:
            return b""

    def nextline(self, force=False):
        if force or self._indent:
            return b"\n"
        else:
            return b""

    def space(self):
        if self._indent:
            return b" "
        else:
            return b""

    def mapping_iter(self, mapping):
        if self._sort_keys:
            return ((key, mapping[key]) for key in sorted(iterkeys(mapping)))
        else:
            return iteritems(mapping)
