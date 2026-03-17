"""Implementation of HjsonDecoder
"""
from __future__ import absolute_import
import re
import sys
import struct
from .compat import fromhex, b, u, text_type, binary_type, PY3, unichr
from .scanner import HjsonDecodeError

# NOTE (3.1.0): HjsonDecodeError may still be imported from this module for
# compatibility, but it was never in the __all__
__all__ = ['HjsonDecoder']

FLAGS = re.VERBOSE | re.MULTILINE | re.DOTALL

def _floatconstants():
    _BYTES = fromhex('7FF80000000000007FF0000000000000')
    # The struct module in Python 2.4 would get frexp() out of range here
    # when an endian is specified in the format string. Fixed in Python 2.5+
    if sys.byteorder != 'big':
        _BYTES = _BYTES[:8][::-1] + _BYTES[8:][::-1]
    nan, inf = struct.unpack('dd', _BYTES)
    return nan, inf, -inf

NaN, PosInf, NegInf = _floatconstants()

WHITESPACE = ' \t\n\r'
PUNCTUATOR = '{}[],:'

NUMBER_RE = re.compile(r'[\t ]*(-?(?:0|[1-9]\d*))(\.\d+)?([eE][-+]?\d+)?[\t ]*')
STRINGCHUNK = re.compile(r'(.*?)([\'"\\\x00-\x1f])', FLAGS)
BACKSLASH = {
    '"': u('"'), '\'': u('\''), '\\': u('\u005c'), '/': u('/'),
    'b': u('\b'), 'f': u('\f'), 'n': u('\n'), 'r': u('\r'), 't': u('\t'),
}

DEFAULT_ENCODING = "utf-8"

def getNext(s, end):
    while 1:
        # Use a slice to prevent IndexError from being raised
        ch = s[end:end + 1]
        # Skip whitespace.
        while ch in WHITESPACE:
            if ch == '': return ch, end
            end += 1
            ch = s[end:end + 1]

        # Hjson allows comments
        ch2 = s[end + 1:end + 2]
        if ch == '#' or ch == '/' and ch2 == '/':
            end = getEol(s, end)
        elif ch == '/' and ch2 == '*':
            end += 2
            ch = s[end]
            while ch != '' and not (ch == '*' and s[end + 1] == '/'):
                end += 1
                ch = s[end]
            if ch != '':
                end += 2
        else:
            break

    return ch, end

def getEol(s, end):
    # skip until eol

    while 1:
        ch = s[end:end + 1]
        if ch == '\r' or ch == '\n' or ch == '':
            return end
        end += 1

def skipIndent(s, end, n):
    ch = s[end:end + 1]
    while ch != '' and ch in " \t\r" and (n > 0 or n < 0):
        end += 1
        n -= 1
        ch = s[end:end + 1]
    return end


def scanstring(s, end, encoding=None, strict=True,
        _b=BACKSLASH, _m=STRINGCHUNK.match, _join=u('').join,
        _PY3=PY3, _maxunicode=sys.maxunicode):
    """Scan the string s for a JSON string. End is the index of the
    character in s after the quote that started the JSON string.
    Unescapes all valid JSON string escape sequences and raises ValueError
    on attempt to decode an invalid string. If strict is False then literal
    control characters are allowed in the string.

    Returns a tuple of the decoded string and the index of the character in s
    after the end quote."""
    if encoding is None:
        encoding = DEFAULT_ENCODING
    chunks = []
    _append = chunks.append
    begin = end - 1
    # callers make sure that string starts with " or '
    exitCh = s[begin]
    while 1:
        chunk = _m(s, end)
        if chunk is None:
            raise HjsonDecodeError(
                "Unterminated string starting at", s, begin)
        end = chunk.end()
        content, terminator = chunk.groups()
        # Content is contains zero or more unescaped string characters
        if content:
            if not _PY3 and not isinstance(content, text_type):
                content = text_type(content, encoding)
            _append(content)
        # Terminator is the end of string, a literal control character,
        # or a backslash denoting that an escape sequence follows
        if terminator == exitCh:
            break
        elif terminator == '"' or terminator == '\'':
            _append(terminator)
            continue
        elif terminator != '\\':
            if strict:
                msg = "Invalid control character %r at"
                raise HjsonDecodeError(msg, s, end)
            else:
                _append(terminator)
                continue
        try:
            esc = s[end]
        except IndexError:
            raise HjsonDecodeError(
                "Unterminated string starting at", s, begin)
        # If not a unicode escape sequence, must be in the lookup table
        if esc != 'u':
            try:
                char = _b[esc]
            except KeyError:
                msg = "Invalid \\X escape sequence %r"
                raise HjsonDecodeError(msg, s, end)
            end += 1
        else:
            # Unicode escape sequence
            msg = "Invalid \\uXXXX escape sequence"
            esc = s[end + 1:end + 5]
            escX = esc[1:2]
            if len(esc) != 4 or escX == 'x' or escX == 'X':
                raise HjsonDecodeError(msg, s, end - 1)
            try:
                uni = int(esc, 16)
            except ValueError:
                raise HjsonDecodeError(msg, s, end - 1)
            end += 5
            # Check for surrogate pair on UCS-4 systems
            # Note that this will join high/low surrogate pairs
            # but will also pass unpaired surrogates through
            if (_maxunicode > 65535 and
                uni & 0xfc00 == 0xd800 and
                s[end:end + 2] == '\\u'):
                esc2 = s[end + 2:end + 6]
                escX = esc2[1:2]
                if len(esc2) == 4 and not (escX == 'x' or escX == 'X'):
                    try:
                        uni2 = int(esc2, 16)
                    except ValueError:
                        raise HjsonDecodeError(msg, s, end)
                    if uni2 & 0xfc00 == 0xdc00:
                        uni = 0x10000 + (((uni - 0xd800) << 10) |
                                         (uni2 - 0xdc00))
                        end += 6
            char = unichr(uni)
        # Append the unescaped character
        _append(char)
    return _join(chunks), end

def mlscanstring(s, end):
    """Scan a multiline string"""

    string = ""
    triple = 0

    # we are at ''' - get indent
    indent = 0
    while 1:
        ch = s[end-indent-1]
        if ch == '\n': break
        indent += 1

    # skip white/to (newline)
    end = skipIndent(s, end + 3, -1)

    ch = s[end]
    if ch == '\n': end = skipIndent(s, end + 1, indent)

    # When parsing multiline string values, we must look for ' characters
    while 1:
        ch = s[end:end + 1]
        if ch == '':
            raise HjsonDecodeError("Bad multiline string", s, end);
        elif ch == '\'':
            triple += 1
            end += 1
            if triple == 3:
                if string and string[-1] == '\n':
                    string = string[:-1] # remove last EOL
                return string, end
            else:
                continue
        else:
            while triple > 0:
                string += '\''
                triple -= 1

        if ch == '\n':
            string += ch
            end = skipIndent(s, end + 1, indent)
        else:
            if ch != '\r':
                string += ch
            end += 1

def scantfnns(context, s, end):
    """Scan s until eol. return string, True, False or None"""

    chf, begin = getNext(s, end)
    end = begin

    if chf in PUNCTUATOR:
        raise HjsonDecodeError("Found a punctuator character when expecting a quoteless string (check your syntax)", s, end);

    while 1:
        ch = s[end:end + 1]

        isEol = ch == '\r' or ch == '\n' or ch == ''
        if isEol or ch == ',' or \
            ch == '}' or ch == ']' or \
            ch == '#' or \
            ch == '/' and (s[end + 1:end + 2] == '/' or s[end + 1:end + 2] == '*'):

            m = None
            mend = end
            if next: mend -= 1

            if chf == 'n' and s[begin:end].strip() == 'null':
                return None, end
            elif chf == 't' and s[begin:end].strip() == 'true':
                return True, end
            elif chf == 'f' and s[begin:end].strip() == 'false':
                return False, end
            elif chf == '-' or chf >= '0' and chf <= '9':
                m = NUMBER_RE.match(s, begin)

            if m is not None and m.end() == end:
                integer, frac, exp = m.groups()
                if frac or exp:
                    res = context.parse_float(integer + (frac or '') + (exp or ''))
                    if int(res) == res and abs(res)<1e10: res = int(res)
                else:
                    res = context.parse_int(integer)
                return res, end

            if isEol:
                return s[begin:end].strip(), end

        end += 1

def scanKeyName(s, end, encoding=None, strict=True):
    """Scan the string s for a JSON/Hjson key. see scanstring"""

    ch, end = getNext(s, end)

    if ch == '"' or ch == '\'':
        return scanstring(s, end + 1, encoding, strict)

    begin = end
    space = -1
    while 1:
        ch = s[end:end + 1]

        if ch == '':
            raise HjsonDecodeError("Bad key name (eof)", s, end);
        elif ch == ':':
            if begin == end:
                raise HjsonDecodeError("Found ':' but no key name (for an empty key name use quotes)", s, begin)
            elif space >= 0:
                if space != end - 1: raise HjsonDecodeError("Found whitespace in your key name (use quotes to include)", s, space)
                return s[begin:end].rstrip(), end
            else:
                return s[begin:end], end
        elif ch in WHITESPACE:
            if space < 0 or space == end - 1: space = end
        elif ch == '{' or ch == '}' or ch == '[' or ch == ']' or ch == ',':
            raise HjsonDecodeError("Found '" + ch + "' where a key name was expected (check your syntax or use quotes if the key name includes {}[],: or whitespace)", s, begin)
        end += 1

def make_scanner(context):
    parse_object = context.parse_object
    parse_array = context.parse_array
    parse_string = context.parse_string
    parse_mlstring = context.parse_mlstring
    parse_tfnns = context.parse_tfnns
    encoding = context.encoding
    strict = context.strict
    object_hook = context.object_hook
    object_pairs_hook = context.object_pairs_hook
    memo = context.memo

    def _scan_once(string, idx):
        try:
            ch = string[idx]
        except IndexError:
            raise HjsonDecodeError('Expecting value', string, idx)

        if ch == '"' or ch == '\'':
            if string[idx:idx + 3] == '\'\'\'':
                return parse_mlstring(string, idx)
            else:
                return parse_string(string, idx + 1, encoding, strict)
        elif ch == '{':
            return parse_object((string, idx + 1), encoding, strict,
                _scan_once, object_hook, object_pairs_hook, memo)
        elif ch == '[':
            return parse_array((string, idx + 1), _scan_once)

        return parse_tfnns(context, string, idx)

    def scan_once(string, idx):
        if idx < 0: raise HjsonDecodeError('Expecting value', string, idx)
        try:
            return _scan_once(string, idx)
        finally:
            memo.clear()

    def scan_object_once(string, idx):
        if idx < 0: raise HjsonDecodeError('Expecting value', string, idx)
        try:
            return parse_object((string, idx), encoding, strict,
                _scan_once, object_hook, object_pairs_hook, memo, True)
        finally:
            memo.clear()

    return scan_once, scan_object_once


def JSONObject(state, encoding, strict, scan_once, object_hook,
        object_pairs_hook, memo=None, objectWithoutBraces=False):
    (s, end) = state
    # Backwards compatibility
    if memo is None:
        memo = {}
    memo_get = memo.setdefault
    pairs = []

    ch, end = getNext(s, end)

    # Trivial empty object
    if not objectWithoutBraces and ch == '}':
        if object_pairs_hook is not None:
            result = object_pairs_hook(pairs)
            return result, end + 1
        pairs = {}
        if object_hook is not None:
            pairs = object_hook(pairs)
        return pairs, end + 1

    while True:
        key, end = scanKeyName(s, end, encoding, strict)
        key = memo_get(key, key)

        ch, end = getNext(s, end)
        if ch != ':':
            raise HjsonDecodeError("Expecting ':' delimiter", s, end)

        ch, end = getNext(s, end + 1)

        value, end = scan_once(s, end)
        pairs.append((key, value))

        ch, end = getNext(s, end)

        if ch == ',':
            ch, end = getNext(s, end + 1)

        if objectWithoutBraces:
            if ch == '': break;
        else:
            if ch == '}':
                end += 1
                break

        ch, end = getNext(s, end)

    if object_pairs_hook is not None:
        result = object_pairs_hook(pairs)
        return result, end
    pairs = dict(pairs)
    if object_hook is not None:
        pairs = object_hook(pairs)
    return pairs, end

def JSONArray(state, scan_once):
    (s, end) = state
    values = []

    ch, end = getNext(s, end)

    # Look-ahead for trivial empty array
    if ch == ']':
        return values, end + 1
    elif ch == '':
        raise HjsonDecodeError("End of input while parsing an array (did you forget a closing ']'?)", s, end)
    _append = values.append
    while True:
        value, end = scan_once(s, end)
        _append(value)

        ch, end = getNext(s, end)
        if ch == ',':
            ch, end = getNext(s, end + 1)

        if ch == ']':
            end += 1
            break

        ch, end = getNext(s, end)

    return values, end


class HjsonDecoder(object):
    """Hjson decoder

    Performs the following translations in decoding by default:

    +---------------+-------------------+
    | JSON          | Python            |
    +===============+===================+
    | object        | dict              |
    +---------------+-------------------+
    | array         | list              |
    +---------------+-------------------+
    | string        | str, unicode      |
    +---------------+-------------------+
    | number (int)  | int, long         |
    +---------------+-------------------+
    | number (real) | float             |
    +---------------+-------------------+
    | true          | True              |
    +---------------+-------------------+
    | false         | False             |
    +---------------+-------------------+
    | null          | None              |
    +---------------+-------------------+

    """

    def __init__(self, encoding=None, object_hook=None, parse_float=None,
            parse_int=None, strict=True,
            object_pairs_hook=None):
        """
        *encoding* determines the encoding used to interpret any
        :class:`str` objects decoded by this instance (``'utf-8'`` by
        default).  It has no effect when decoding :class:`unicode` objects.

        Note that currently only encodings that are a superset of ASCII work,
        strings of other encodings should be passed in as :class:`unicode`.

        *object_hook*, if specified, will be called with the result of every
        JSON object decoded and its return value will be used in place of the
        given :class:`dict`.  This can be used to provide custom
        deserializations (e.g. to support JSON-RPC class hinting).

        *object_pairs_hook* is an optional function that will be called with
        the result of any object literal decode with an ordered list of pairs.
        The return value of *object_pairs_hook* will be used instead of the
        :class:`dict`.  This feature can be used to implement custom decoders
        that rely on the order that the key and value pairs are decoded (for
        example, :func:`collections.OrderedDict` will remember the order of
        insertion). If *object_hook* is also defined, the *object_pairs_hook*
        takes priority.

        *parse_float*, if specified, will be called with the string of every
        JSON float to be decoded.  By default, this is equivalent to
        ``float(num_str)``. This can be used to use another datatype or parser
        for JSON floats (e.g. :class:`decimal.Decimal`).

        *parse_int*, if specified, will be called with the string of every
        JSON int to be decoded.  By default, this is equivalent to
        ``int(num_str)``.  This can be used to use another datatype or parser
        for JSON integers (e.g. :class:`float`).

        *strict* controls the parser's behavior when it encounters an
        invalid control character in a string. The default setting of
        ``True`` means that unescaped control characters are parse errors, if
        ``False`` then control characters will be allowed in strings.

        """
        if encoding is None:
            encoding = DEFAULT_ENCODING
        self.encoding = encoding
        self.object_hook = object_hook
        self.object_pairs_hook = object_pairs_hook
        self.parse_float = parse_float or float
        self.parse_int = parse_int or int
        self.strict = strict
        self.parse_object = JSONObject
        self.parse_array = JSONArray
        self.parse_string = scanstring
        self.parse_mlstring = mlscanstring
        self.parse_tfnns = scantfnns
        self.memo = {}
        (self.scan_once, self.scan_object_once) = make_scanner(self)

    def decode(self, s, _PY3=PY3):
        """Return the Python representation of ``s`` (a ``str`` or ``unicode``
        instance containing a JSON document)

        """
        if _PY3 and isinstance(s, binary_type):
            s = s.decode(self.encoding)
        obj, end = self.raw_decode(s)
        ch, end = getNext(s, end)
        if end != len(s):
            raise HjsonDecodeError("Extra data", s, end, len(s))
        return obj

    def raw_decode(self, s, idx=0, _PY3=PY3):
        """Decode a JSON document from ``s`` (a ``str`` or ``unicode``
        beginning with a JSON document) and return a 2-tuple of the Python
        representation and the index in ``s`` where the document ended.
        Optionally, ``idx`` can be used to specify an offset in ``s`` where
        the JSON document begins.

        This can be used to decode a JSON document from a string that may
        have extraneous data at the end.

        """
        if idx < 0:
            # Ensure that raw_decode bails on negative indexes, the regex
            # would otherwise mask this behavior. #98
            raise HjsonDecodeError('Expecting value', s, idx)
        if _PY3 and not isinstance(s, text_type):
            raise TypeError("Input string must be text")
        # strip UTF-8 bom
        if len(s) > idx:
            ord0 = ord(s[idx])
            if ord0 == 0xfeff:
                idx += 1
            elif ord0 == 0xef and s[idx:idx + 3] == '\xef\xbb\xbf':
                idx += 3

        start_index = idx
        ch, idx = getNext(s, idx)

        # If blank or comment only file, return dict
        if start_index == 0 and ch == '':
            return {}, 0

        if ch == '{' or ch == '[':
            return self.scan_once(s, idx)
        else:
            # assume we have a root object without braces
            try:
                return self.scan_object_once(s, idx)
            except HjsonDecodeError as e:
                # test if we are dealing with a single JSON value instead (true/false/null/num/"")
                try:
                    return self.scan_once(s, idx)
                except:
                    raise e
