from . import convert
from .common import raise_yson_error, YsonError, StreamWrap, _ENCODING_SENTINEL
from .tokenizer import YsonTokenizer
from .yson_token import (
    TOKEN_STRING,
    TOKEN_INT64,
    TOKEN_UINT64,
    TOKEN_DOUBLE,
    TOKEN_BOOLEAN,
    TOKEN_HASH,
    TOKEN_SEMICOLON,
    TOKEN_LEFT_ANGLE,
    TOKEN_EQUALS,
    TOKEN_RIGHT_ANGLE,
    TOKEN_LEFT_BRACKET,
    TOKEN_RIGHT_BRACKET,
    TOKEN_LEFT_BRACE,
    TOKEN_RIGHT_BRACE,
    TOKEN_START_OF_STREAM,
    TOKEN_END_OF_STREAM,
)

try:
    from yt.packages.six import PY3, BytesIO, text_type
except ImportError:
    from six import PY3, BytesIO, text_type


def _is_text_reader(stream):
    return type(stream.read(0)) is text_type


class YsonParser(object):
    def __init__(self, stream, encoding, always_create_attributes):
        # COMPAT: Before porting YSON to Python 3 it supported parsing from
        # unicode strings.
        if _is_text_reader(stream) and PY3:
            raise TypeError("Only binary streams are supported by YSON parser")
        self._tokenizer = YsonTokenizer(stream, encoding)
        self._always_create_attributes = always_create_attributes
        self._encoding = encoding

    def _has_attributes(self):
        try:
            self._tokenizer.parse_next()
        except YsonError:
            return False
        return self._tokenizer.get_current_type() == TOKEN_LEFT_ANGLE

    def _parse_attributes(self):
        self._tokenizer.get_current_token().expect_type(TOKEN_LEFT_ANGLE)
        result = {}
        while True:
            self._tokenizer.parse_next()
            if self._tokenizer.get_current_type() == TOKEN_RIGHT_ANGLE:
                break
            self._tokenizer.get_current_token().expect_type(TOKEN_STRING)
            key = self._tokenizer.get_current_token().get_value()
            if not key:
                raise_yson_error(
                    "Empty attribute name in Yson",
                    self._tokenizer.get_position_info())
            self._tokenizer.parse_next()
            self._tokenizer.get_current_token().expect_type(TOKEN_EQUALS)
            self._tokenizer.parse_next()
            value = self._parse_any()
            if key in result:
                raise_yson_error(
                    'Repeated attribute "{0}" in Yson'.format(key),
                    self._tokenizer.get_position_info())
            result[key] = value
            self._tokenizer.parse_next()
            if self._tokenizer.get_current_type() == TOKEN_RIGHT_ANGLE:
                break
            self._tokenizer.get_current_token().expect_type(TOKEN_SEMICOLON)
        self._tokenizer.get_current_token().expect_type(TOKEN_RIGHT_ANGLE)
        return result

    def _parse_list(self):
        self._tokenizer.get_current_token().expect_type(TOKEN_LEFT_BRACKET)
        result = []
        while True:
            self._tokenizer.parse_next()
            if self._tokenizer.get_current_type() == TOKEN_RIGHT_BRACKET:
                break
            value = self._parse_any()
            result.append(value)
            self._tokenizer.parse_next()
            if self._tokenizer.get_current_type() == TOKEN_RIGHT_BRACKET:
                break
            self._tokenizer.get_current_token().expect_type(TOKEN_SEMICOLON)
        self._tokenizer.get_current_token().expect_type(TOKEN_RIGHT_BRACKET)
        return result

    def _parse_map(self):
        self._tokenizer.get_current_token().expect_type(TOKEN_LEFT_BRACE)
        result = {}
        while True:
            self._tokenizer.parse_next()
            if self._tokenizer.get_current_type() == TOKEN_RIGHT_BRACE:
                break
            self._tokenizer.get_current_token().expect_type(TOKEN_STRING)
            key = self._tokenizer.get_current_token().get_value()
            self._tokenizer.parse_next()
            self._tokenizer.get_current_token().expect_type(TOKEN_EQUALS)
            self._tokenizer.parse_next()
            value = self._parse_any()
            if key in result:
                raise_yson_error(
                    'Duplicate map key "{0}" in YSON'.format(key),
                    self._tokenizer.get_position_info())
            result[key] = value
            self._tokenizer.parse_next()
            if self._tokenizer.get_current_type() == TOKEN_RIGHT_BRACE:
                break
            self._tokenizer.get_current_token().expect_type(TOKEN_SEMICOLON)
        self._tokenizer.get_current_token().expect_type(TOKEN_RIGHT_BRACE)
        return result

    def _parse_any(self):
        if self._tokenizer.get_current_type() == TOKEN_START_OF_STREAM:
            self._tokenizer.parse_next()
        attributes = None
        if self._tokenizer.get_current_type() == TOKEN_LEFT_ANGLE:
            attributes = self._parse_attributes()
            self._tokenizer.parse_next()

        if self._tokenizer.get_current_type() == TOKEN_END_OF_STREAM:
            raise_yson_error(
                "Premature end-of-stream in Yson",
                self._tokenizer.get_position_info())

        if self._tokenizer.get_current_type() == TOKEN_LEFT_BRACKET:
            result = self._parse_list()

        elif self._tokenizer.get_current_type() == TOKEN_LEFT_BRACE:
            result = self._parse_map()

        elif self._tokenizer.get_current_type() == TOKEN_HASH:
            result = None

        else:
            self._tokenizer.get_current_token().expect_type((TOKEN_BOOLEAN, TOKEN_INT64, TOKEN_UINT64,
                                                             TOKEN_STRING, TOKEN_DOUBLE))
            result = self._tokenizer.get_current_token().get_value()

        return convert.to_yson_type(
            result,
            attributes=attributes,
            always_create_attributes=self._always_create_attributes,
            encoding=self._encoding,
        )

    def parse(self):
        result = self._parse_any()
        self._tokenizer.parse_next()
        self._tokenizer.get_current_token().expect_type(TOKEN_END_OF_STREAM)
        return result


class RawYsonParser(object):
    def __init__(self, stream):
        if _is_text_reader(stream) and PY3:
            raise TypeError("Only binary streams are supported by YSON parser")
        self._buffer = bytearray()
        self._tokenizer = YsonTokenizer(stream, output_buffer=self._buffer)

    def _parse_mapping(self, end_token):
        while True:
            self._tokenizer.parse_next()
            if self._tokenizer.get_current_type() == end_token:
                break
            self._tokenizer.get_current_token().expect_type(TOKEN_STRING)
            self._tokenizer.parse_next()
            self._tokenizer.get_current_token().expect_type(TOKEN_EQUALS)
            self._tokenizer.parse_next()
            self._parse_any()
            self._tokenizer.parse_next()
            if self._tokenizer.get_current_type() == end_token:
                break
            self._tokenizer.get_current_token().expect_type(TOKEN_SEMICOLON)
        self._tokenizer.get_current_token().expect_type(end_token)

    def _parse_attributes(self):
        self._tokenizer.get_current_token().expect_type(TOKEN_LEFT_ANGLE)
        self._parse_mapping(TOKEN_RIGHT_ANGLE)

    def _parse_map(self):
        self._tokenizer.get_current_token().expect_type(TOKEN_LEFT_BRACE)
        self._parse_mapping(TOKEN_RIGHT_BRACE)

    def _parse_list(self):
        self._tokenizer.get_current_token().expect_type(TOKEN_LEFT_BRACKET)
        while True:
            self._tokenizer.parse_next()
            if self._tokenizer.get_current_type() == TOKEN_RIGHT_BRACKET:
                break
            self._parse_any()
            self._tokenizer.parse_next()
            if self._tokenizer.get_current_type() == TOKEN_RIGHT_BRACKET:
                break
            self._tokenizer.get_current_token().expect_type(TOKEN_SEMICOLON)
        self._tokenizer.get_current_token().expect_type(TOKEN_RIGHT_BRACKET)

    def _parse_any(self):
        if self._tokenizer.get_current_type() == TOKEN_START_OF_STREAM:
            self._tokenizer.parse_next()

        if self._tokenizer.get_current_type() == TOKEN_LEFT_ANGLE:
            self._parse_attributes()
            self._tokenizer.parse_next()

        if self._tokenizer.get_current_type() == TOKEN_END_OF_STREAM:
            raise_yson_error(
                "Premature end-of-stream in Yson",
                self._tokenizer.get_position_info())

        if self._tokenizer.get_current_type() == TOKEN_LEFT_BRACKET:
            self._parse_list()

        elif self._tokenizer.get_current_type() == TOKEN_LEFT_BRACE:
            self._parse_map()

        elif self._tokenizer.get_current_type() == TOKEN_HASH:
            pass

        else:
            self._tokenizer.get_current_token().expect_type((TOKEN_BOOLEAN, TOKEN_INT64, TOKEN_UINT64,
                                                             TOKEN_STRING, TOKEN_DOUBLE))

    def _flush_buffer(self):
        res = bytes(self._buffer)
        self._buffer[:] = b''
        return res

    def parse(self):
        while self._tokenizer.get_current_type() != TOKEN_END_OF_STREAM:
            self._parse_any()
            self._tokenizer.parse_next()
            self._tokenizer.get_current_token().expect_type(TOKEN_SEMICOLON)
            yield self._flush_buffer()
            self._tokenizer.parse_next()


def load(stream, yson_type=None, always_create_attributes=True, raw=None,
         encoding=_ENCODING_SENTINEL, lazy=False):
    """Deserializes object from YSON formatted stream `stream`.

    :param str yson_type: type of YSON, one of ["node", "list_fragment", "map_fragment"].
    """
    if lazy:
        raise YsonError("Lazy parsing is not supported in python parser")

    if raw:
        if yson_type != "list_fragment":
            raise YsonError("Raw mode is only supported for list fragments")
        return RawYsonParser(stream).parse()

    if not PY3 and encoding is not _ENCODING_SENTINEL and encoding is not None:
        raise YsonError("Encoding parameter is not supported for Python 2")

    if encoding is _ENCODING_SENTINEL:
        if PY3:
            encoding = "utf-8"
        else:
            encoding = None

    if yson_type == "list_fragment":
        stream = StreamWrap(stream, b"[", b"]")
    elif yson_type == "map_fragment":
        stream = StreamWrap(stream, b"{", b"}")
    else:
        if yson_type is not None:
            raise YsonError("Unexpected yson type: {0!r}".format(yson_type))

    parser = YsonParser(stream, encoding, always_create_attributes)
    return parser.parse()


def loads(string, yson_type=None, always_create_attributes=True, raw=None,
          encoding=_ENCODING_SENTINEL, lazy=False):
    """Deserializes object from YSON formatted string `string`. See :func:`load <.load>`."""
    if type(string) is text_type and PY3:
        raise TypeError("Only binary streams are supported by YSON parser")
    return load(BytesIO(string), yson_type=yson_type,
                always_create_attributes=always_create_attributes,
                raw=raw, encoding=encoding, lazy=lazy)
