from . import yson_types
from .yson_token import (
    YsonToken,
    TOKEN_STRING,
    TOKEN_INT64,
    TOKEN_UINT64,
    TOKEN_DOUBLE,
    TOKEN_BOOLEAN,
    TOKEN_HASH,
    TOKEN_LEFT_PARENTHESIS,
    TOKEN_RIGHT_PARENTHESIS,
    TOKEN_COMMA,
    TOKEN_COLON,
    TOKEN_SEMICOLON,
    TOKEN_LEFT_ANGLE,
    TOKEN_EQUALS,
    TOKEN_RIGHT_ANGLE,
    TOKEN_LEFT_BRACKET,
    TOKEN_RIGHT_BRACKET,
    TOKEN_LEFT_BRACE,
    TOKEN_RIGHT_BRACE)

from .common import (
    raise_yson_error, _ENCODING_SENTINEL,
    STRING_MARKER, INT64_MARKER, DOUBLE_MARKER,
    FALSE_MARKER, TRUE_MARKER, UINT64_MARKER)

try:
    from yt.packages.six.moves import xrange
    from yt.packages.six import int2byte, iterbytes
except ImportError:
    from six.moves import xrange
    from six import int2byte, iterbytes

import struct

_SEEMS_INT64 = int2byte(0)
_SEEMS_UINT64 = int2byte(1)
_SEEMS_DOUBLE = int2byte(2)

PERCENT_LITERALS = [b"true", b"false", b"nan", b"inf", b"-inf", b"+inf"]
PERCENT_LITERAL_LENGTH = dict((s[0:1], len(s)) for s in PERCENT_LITERALS)
assert len(PERCENT_LITERALS) == len(PERCENT_LITERAL_LENGTH)


def _get_numeric_type(string):
    for code in iterbytes(string):
        ch = int2byte(code)
        if ch == b"E" or ch == b"e" or ch == b".":
            return _SEEMS_DOUBLE
        elif ch == b"u":
            return _SEEMS_UINT64
    return _SEEMS_INT64


def _zig_zag_decode(value):
    return (value >> 1) ^ -(value & 1)


class YsonLexer(object):
    def __init__(self, stream, encoding=None, output_buffer=None):
        assert (encoding is _ENCODING_SENTINEL) != (output_buffer is None), \
            "Exactly one of encoding and output_buffer parameters must be specified"

        self._line_index = 1
        self._position = 1
        self._offset = 0
        self._stream = stream
        self._lookahead = None
        self._encoding = encoding
        self._output_buffer = output_buffer

    def _get_start_state(self, ch):
        tokens = {
            b"#": TOKEN_HASH,
            b"(": TOKEN_LEFT_PARENTHESIS,
            b")": TOKEN_RIGHT_PARENTHESIS,
            b",": TOKEN_COMMA,
            b":": TOKEN_COLON,
            b";": TOKEN_SEMICOLON,
            b"<": TOKEN_LEFT_ANGLE,
            b"=": TOKEN_EQUALS,
            b">": TOKEN_RIGHT_ANGLE,
            b"[": TOKEN_LEFT_BRACKET,
            b"]": TOKEN_RIGHT_BRACKET,
            b"{": TOKEN_LEFT_BRACE,
            b"}": TOKEN_RIGHT_BRACE,
        }
        return tokens.get(ch)

    def get_next_token(self):
        self._skip_whitespaces()
        ch = self._peek_char()
        if not ch:
            return YsonToken()

        if ch == STRING_MARKER:
            return YsonToken(value=self._parse_string(), type=TOKEN_STRING)

        elif ch == b"_" or ch == b'"' or ch.isalpha():
            return YsonToken(value=self._parse_string(), type=TOKEN_STRING)
        elif ch == INT64_MARKER:
            return YsonToken(value=self._parse_binary_int64(), type=TOKEN_INT64)

        elif ch == UINT64_MARKER:
            return YsonToken(value=self._parse_binary_uint64(), type=TOKEN_UINT64)

        elif ch == DOUBLE_MARKER:
            return YsonToken(value=self._parse_binary_double(), type=TOKEN_DOUBLE)

        elif ch == FALSE_MARKER:
            self._expect_char(ch)
            return YsonToken(value=self._maybe_value(False), type=TOKEN_BOOLEAN)

        elif ch == TRUE_MARKER:
            self._expect_char(ch)
            return YsonToken(value=self._maybe_value(True), type=TOKEN_BOOLEAN)

        elif ch == b"%":
            value, token_type = self._parse_percent_literal()
            return YsonToken(value=self._maybe_value(value), type=token_type)

        elif ch == b"#":
            return YsonToken(value=self._parse_entity(), type=TOKEN_HASH)

        elif ch == b"+" or ch == b"-" or ch.isdigit():
            value, token_type = self._parse_numeric()
            return YsonToken(value=self._maybe_value(value), type=token_type)

        state = self._get_start_state(ch)
        self._read_char()
        return YsonToken(value=self._maybe_value(ch), type=state)

    def get_position_info(self):
        return self._line_index, self._position, self._offset

    def _maybe_value(self, value):
        if self._output_buffer is None:
            return value
        return None

    def _read_char(self, binary_input=False):
        if self._lookahead is None:
            result = self._stream.read(1)
        else:
            result = self._lookahead
        self._lookahead = None

        self._offset += 1
        if not binary_input and result == b"\n":
            self._line_index += 1
            self._position = 1
        else:
            self._position += 1

        if self._output_buffer is not None:
            self._output_buffer.append(ord(result))

        return result

    def _peek_char(self):
        if self._lookahead is not None:
            return self._lookahead
        self._lookahead = self._stream.read(1)
        return self._lookahead

    def _read_binary_chars(self, char_count):
        if self._output_buffer is not None:
            string = self._stream.read(char_count)
            self._position += len(string)
            if len(string) != char_count:
                raise_yson_error(
                    "Premature end-of-stream while reading byte {0} out of {1}".format(len(string) + 1, char_count),
                    self.get_position_info())
            self._output_buffer += string
            return string

        result = []
        for i in xrange(char_count):
            ch = self._read_char(True)
            if not ch:
                raise_yson_error(
                    "Premature end-of-stream while reading byte {0} out of {1}".format(i + 1, char_count),
                    self.get_position_info())
            result.append(ch)
        return b"".join(result)

    def _expect_char(self, expected_ch):
        read_ch = self._read_char()
        if not read_ch:
            raise_yson_error(
                'Premature end-of-stream expecting "{0}" in Yson'.format(expected_ch),
                self.get_position_info())
        if read_ch != expected_ch:
            raise_yson_error(
                'Found "{0}" while expecting "{1}" in Yson'.format(read_ch, expected_ch),
                self.get_position_info())

    def _skip_whitespaces(self):
        while self._peek_char().isspace():
            self._read_char()

    def _read_string(self):
        ch = self._peek_char()
        if not ch:
            raise_yson_error(
                "Premature end-of-stream while expecting string literal in Yson",
                self.get_position_info())
        if ch == STRING_MARKER:
            return self._read_binary_string()
        if ch == b'"':
            return self._read_quoted_string()
        if not ch.isalpha() and not ch == b"_" and not ch == b"%":
            raise_yson_error(
                "Expecting string literal but found {0} in Yson".format(ch),
                self.get_position_info())
        return self._read_unquoted_string()

    def _read_binary_string(self):
        self._expect_char(STRING_MARKER)
        length = _zig_zag_decode(self._read_varint())
        string = self._read_binary_chars(length)
        if self._output_buffer is None:
            return self._decode_string(string)

    def _read_varint(self):
        count = 0
        result = 0
        read_next = True
        while read_next:
            ch = self._read_char()
            if not ch:
                raise_yson_error(
                    "Premature end-of-stream while reading varinteger in Yson",
                    self.get_position_info())
            byte = ord(ch)
            result |= (byte & 0x7F) << (7 * count)
            if result > 2 ** 64 - 1:
                raise_yson_error(
                    "Varinteger is too large for Int64 in Yson",
                    self.get_position_info())
            count += 1
            read_next = byte & 0x80 != 0

        return yson_types._YsonIntegerBase(result)

    def _read_quoted_string(self):
        self._expect_char(b'"')
        if self._output_buffer is None:
            result = []
        pending_next_char = False
        while True:
            ch = self._read_char()
            if not ch:
                raise_yson_error(
                    "Premature end-of-stream while reading string literal in Yson",
                    self.get_position_info())
            if ch == b'"' and not pending_next_char:
                break
            if self._output_buffer is None:
                result.append(ch)
            if pending_next_char:
                pending_next_char = False
            elif ch == b"\\":
                pending_next_char = True
        if self._output_buffer is None:
            return self._decode_string(self._unescape(b"".join(result)))

    def _unescape(self, string):
        return string.decode("unicode_escape").encode("latin1")

    def _decode_string(self, string):
        assert self._encoding is not _ENCODING_SENTINEL
        if self._encoding is not None:
            try:
                return string.decode(self._encoding)
            except UnicodeDecodeError:
                proxy = yson_types.YsonStringProxy()
                proxy._bytes = string
                return proxy
        else:
            return string

    def _read_unquoted_string(self):
        if self._output_buffer is None:
            result = []
        while True:
            ch = self._peek_char()
            if ch and (ch.isalpha() or ch.isdigit() or ch in b"_%-."):
                self._read_char()
                if self._output_buffer is None:
                    result.append(ch)
            else:
                break
        if self._output_buffer is None:
            return self._decode_string(b"".join(result))

    def _read_numeric(self):
        result = []
        while True:
            ch = self._peek_char()
            if not ch or not (ch.isdigit() or ch in b"+-.eEu"):
                break
            self._read_char()
            result.append(ch)
        if not result:
            raise_yson_error(
                "Premature end-of-stream while parsing numeric literal in Yson",
                self.get_position_info())
        return b"".join(result)

    def _parse_percent_literal(self):
        def raise_unexpected(string):
            expected = [b"%" + literal for literal in PERCENT_LITERALS]
            raise_yson_error(
                "Incorrect percent-preceded literal %s, expected one of %s" % (b"%" + string, expected),
                self.get_position_info())
        self._expect_char(b"%")
        ch = self._peek_char()
        if ch not in PERCENT_LITERAL_LENGTH:
            raise_unexpected(ch)
        string = self._read_binary_chars(PERCENT_LITERAL_LENGTH[ch])
        if string == b"true":
            return True, TOKEN_BOOLEAN
        elif string == b"false":
            return False, TOKEN_BOOLEAN
        elif string in PERCENT_LITERALS:
            return float(string), TOKEN_DOUBLE
        else:
            raise_unexpected(string)

    def _parse_entity(self):
        self._expect_char(b"#")
        return None

    def _parse_string(self):
        return self._read_string()

    def _parse_binary_int64(self):
        self._expect_char(INT64_MARKER)
        varint = self._read_varint()
        if self._output_buffer is None:
            return _zig_zag_decode(varint)

    def _parse_binary_uint64(self):
        self._expect_char(UINT64_MARKER)
        varint = self._read_varint()
        if self._output_buffer is None:
            return yson_types.YsonUint64(varint)

    def _parse_binary_double(self):
        self._expect_char(DOUBLE_MARKER)
        bytes_ = self._read_binary_chars(struct.calcsize(b"<d"))
        if self._output_buffer is None:
            return struct.unpack(b"<d", bytes_)[0]

    def _parse_numeric(self):
        string = self._read_numeric()
        numeric_type = _get_numeric_type(string)
        if numeric_type == _SEEMS_INT64:
            try:
                result = yson_types._YsonIntegerBase(string)
                token_type = TOKEN_INT64
                if result > 2 ** 63 - 1 or result < -(2 ** 63):
                    raise ValueError()
            except ValueError:
                raise_yson_error(
                    "Failed to parse Int64 literal {0} in Yson".format(string),
                    self.get_position_info())
        elif numeric_type == _SEEMS_UINT64:
            try:
                if string.endswith(b"u"):
                    string = string[:-1]
                else:
                    raise ValueError()
                result = yson_types.YsonUint64(int(string))
                token_type = TOKEN_UINT64
                if result > 2 ** 64 - 1:
                    raise ValueError()
            except ValueError:
                raise_yson_error(
                    "Failed to parse Uint64 literal {0} in Yson".format(string),
                    self.get_position_info())
        else:
            try:
                result = float(string)
                token_type = TOKEN_DOUBLE
            except ValueError:
                raise_yson_error(
                    "Failed to parse Double literal {0} in Yson".format(string),
                    self.get_position_info())
        return result, token_type
