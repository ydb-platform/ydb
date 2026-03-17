"""Implementation of HjsonEncoder
"""
from __future__ import absolute_import
import re
from operator import itemgetter
from decimal import Decimal
from .compat import u, unichr, binary_type, string_types, integer_types, PY3
from .decoder import PosInf

# This is required because u() will mangle the string and ur'' isn't valid
# python3 syntax
ESCAPE = re.compile(u'[\\x00-\\x1f\\\\"\\b\\f\\n\\r\\t\u2028\u2029\uffff]')
ESCAPE_ASCII = re.compile(r'([\\"]|[^\ -~])')
HAS_UTF8 = re.compile(r'[\x80-\xff]')
ESCAPE_DCT = {
    '\\': '\\\\',
    '"': '\\"',
    '\b': '\\b',
    '\f': '\\f',
    '\n': '\\n',
    '\r': '\\r',
    '\t': '\\t',
}
for i in range(0x20):
    #ESCAPE_DCT.setdefault(chr(i), '\\u{0:04x}'.format(i))
    ESCAPE_DCT.setdefault(chr(i), '\\u%04x' % (i,))
for i in [0x2028, 0x2029, 0xffff]:
    ESCAPE_DCT.setdefault(unichr(i), '\\u%04x' % (i,))

COMMONRANGE=u'\x7f-\x9f\u00ad\u0600-\u0604\u070f\u17b4\u17b5\u200c-\u200f\u2028-\u202f\u2060-\u206f\ufeff\ufff0-\uffff'

# NEEDSESCAPE tests if the string can be written without escapes
NEEDSESCAPE = re.compile(u'[\\\"\x00-\x1f'+COMMONRANGE+']')
# NEEDSQUOTES tests if the string can be written as a quoteless string (like needsEscape but without \\ and \")
NEEDSQUOTES = re.compile(u'^\\s|^"|^\'|^#|^\\/\\*|^\\/\\/|^\\{|^\\}|^\\[|^\\]|^:|^,|\\s$|[\x00-\x1f'+COMMONRANGE+u']')
# NEEDSESCAPEML tests if the string can be written as a multiline string (like needsEscape but without \n, \r, \\, \", \t)
NEEDSESCAPEML = re.compile(u'\'\'\'|^[\\s]+$|[\x00-\x08\x0b\x0c\x0e-\x1f'+COMMONRANGE+u']')

WHITESPACE = ' \t\n\r'
STARTSWITHNUMBER = re.compile(r'^[\t ]*(-?(?:0|[1-9]\d*))(\.\d+)?([eE][-+]?\d+)?\s*((,|\]|\}|#|\/\/|\/\*).*)?$');
STARTSWITHKEYWORD = re.compile(r'^(true|false|null)\s*((,|\]|\}|#|\/\/|\/\*).*)?$');
NEEDSESCAPENAME = re.compile(r'[,\{\[\}\]\s:#"\']|\/\/|\/\*|'+"'''")

FLOAT_REPR = repr

def encode_basestring(s, _PY3=PY3, _q=u('"')):
    """Return a JSON representation of a Python string

    """
    if _PY3:
        if isinstance(s, binary_type):
            s = s.decode('utf-8')
    else:
        if isinstance(s, str) and HAS_UTF8.search(s) is not None:
            s = s.decode('utf-8')
    def replace(match):
        return ESCAPE_DCT[match.group(0)]
    return _q + ESCAPE.sub(replace, s) + _q


def encode_basestring_ascii(s, _PY3=PY3):
    """Return an ASCII-only JSON representation of a Python string

    """
    if _PY3:
        if isinstance(s, binary_type):
            s = s.decode('utf-8')
    else:
        if isinstance(s, str) and HAS_UTF8.search(s) is not None:
            s = s.decode('utf-8')
    def replace(match):
        s = match.group(0)
        try:
            return ESCAPE_DCT[s]
        except KeyError:
            n = ord(s)
            if n < 0x10000:
                #return '\\u{0:04x}'.format(n)
                return '\\u%04x' % (n,)
            else:
                # surrogate pair
                n -= 0x10000
                s1 = 0xd800 | ((n >> 10) & 0x3ff)
                s2 = 0xdc00 | (n & 0x3ff)
                #return '\\u{0:04x}\\u{1:04x}'.format(s1, s2)
                return '\\u%04x\\u%04x' % (s1, s2)
    return '"' + str(ESCAPE_ASCII.sub(replace, s)) + '"'


class HjsonEncoder(object):
    """Extensible JSON <http://json.org> encoder for Python data structures.

    Supports the following objects and types by default:

    +-------------------+---------------+
    | Python            | JSON          |
    +===================+===============+
    | dict, namedtuple  | object        |
    +-------------------+---------------+
    | list, tuple       | array         |
    +-------------------+---------------+
    | str, unicode      | string        |
    +-------------------+---------------+
    | int, long, float  | number        |
    +-------------------+---------------+
    | True              | true          |
    +-------------------+---------------+
    | False             | false         |
    +-------------------+---------------+
    | None              | null          |
    +-------------------+---------------+

    To extend this to recognize other objects, subclass and implement a
    ``.default()`` method with another method that returns a serializable
    object for ``o`` if possible, otherwise it should call the superclass
    implementation (to raise ``TypeError``).

    """

    def __init__(self, skipkeys=False, ensure_ascii=True,
                 check_circular=True, sort_keys=False,
                 indent='  ', encoding='utf-8', default=None,
                 use_decimal=True, namedtuple_as_object=True,
                 tuple_as_array=True, bigint_as_string=False,
                 item_sort_key=None, for_json=False,
                 int_as_string_bitcount=None):
        """Constructor for HjsonEncoder, with sensible defaults.

        If skipkeys is false, then it is a TypeError to attempt
        encoding of keys that are not str, int, long, float or None.  If
        skipkeys is True, such items are simply skipped.

        If ensure_ascii is true, the output is guaranteed to be str
        objects with all incoming unicode characters escaped.  If
        ensure_ascii is false, the output will be unicode object.

        If check_circular is true, then lists, dicts, and custom encoded
        objects will be checked for circular references during encoding to
        prevent an infinite recursion (which would cause an OverflowError).
        Otherwise, no such check takes place.

        If sort_keys is true, then the output of dictionaries will be
        sorted by key; this is useful for regression tests to ensure
        that JSON serializations can be compared on a day-to-day basis.

        If indent is a string, then JSON array elements and object members
        will be pretty-printed with a newline followed by that string repeated
        for each level of nesting.

        If specified, default is a function that gets called for objects
        that can't otherwise be serialized.  It should return a JSON encodable
        version of the object or raise a ``TypeError``.

        If encoding is not None, then all input strings will be
        transformed into unicode using that encoding prior to JSON-encoding.
        The default is UTF-8.

        If use_decimal is true (not the default), ``decimal.Decimal`` will
        be supported directly by the encoder. For the inverse, decode JSON
        with ``parse_float=decimal.Decimal``.

        If namedtuple_as_object is true (the default), objects with
        ``_asdict()`` methods will be encoded as JSON objects.

        If tuple_as_array is true (the default), tuple (and subclasses) will
        be encoded as JSON arrays.

        If bigint_as_string is true (not the default), ints 2**53 and higher
        or lower than -2**53 will be encoded as strings. This is to avoid the
        rounding that happens in Javascript otherwise.

        If int_as_string_bitcount is a positive number (n), then int of size
        greater than or equal to 2**n or lower than or equal to -2**n will be
        encoded as strings.

        If specified, item_sort_key is a callable used to sort the items in
        each dictionary. This is useful if you want to sort items other than
        in alphabetical order by key.

        If for_json is true (not the default), objects with a ``for_json()``
        method will use the return value of that method for encoding as JSON
        instead of the object.

        """

        self.skipkeys = skipkeys
        self.ensure_ascii = ensure_ascii
        self.check_circular = check_circular
        self.sort_keys = sort_keys
        self.use_decimal = use_decimal
        self.namedtuple_as_object = namedtuple_as_object
        self.tuple_as_array = tuple_as_array
        self.bigint_as_string = bigint_as_string
        self.item_sort_key = item_sort_key
        self.for_json = for_json
        self.int_as_string_bitcount = int_as_string_bitcount
        if indent is not None and not isinstance(indent, string_types):
            indent = indent * ' '
        elif indent is None:
            indent = '  '
        self.indent = indent
        if default is not None:
            self.default = default
        self.encoding = encoding

    def default(self, o):
        """Implement this method in a subclass such that it returns
        a serializable object for ``o``, or calls the base implementation
        (to raise a ``TypeError``).

        For example, to support arbitrary iterators, you could
        implement default like this::

            def default(self, o):
                try:
                    iterable = iter(o)
                except TypeError:
                    pass
                else:
                    return list(iterable)
                return HjsonEncoder.default(self, o)

        """
        raise TypeError(repr(o) + " is not JSON serializable")

    def encode(self, o):
        """Return a JSON string representation of a Python data structure.

        >>> from hjson import HjsonEncoder
        >>> HjsonEncoder().encode({"foo": ["bar", "baz"]})
        '{"foo": ["bar", "baz"]}'

        """
        # This is for extremely simple cases and benchmarks.
        if isinstance(o, binary_type):
            _encoding = self.encoding
            if (_encoding is not None and not (_encoding == 'utf-8')):
                o = o.decode(_encoding)

        # This doesn't pass the iterator directly to ''.join() because the
        # exceptions aren't as detailed.  The list call should be roughly
        # equivalent to the PySequence_Fast that ''.join() would do.
        chunks = self.iterencode(o, _one_shot=True)
        if not isinstance(chunks, (list, tuple)):
            chunks = list(chunks)
        if self.ensure_ascii:
            return ''.join(chunks)
        else:
            return u''.join(chunks)

    def iterencode(self, o, _one_shot=False):
        """Encode the given object and yield each string
        representation as available.

        For example::

            for chunk in HjsonEncoder().iterencode(bigobject):
                mysocket.write(chunk)

        """
        if self.check_circular:
            markers = {}
        else:
            markers = None
        if self.ensure_ascii:
            _encoder = encode_basestring_ascii
        else:
            _encoder = encode_basestring
        if self.encoding != 'utf-8':
            def _encoder(o, _orig_encoder=_encoder, _encoding=self.encoding):
                if isinstance(o, binary_type):
                    o = o.decode(_encoding)
                return _orig_encoder(o)

        def floatstr(o, _repr=FLOAT_REPR, _inf=PosInf, _neginf=-PosInf):
            # Check for specials. Note that this type of test is processor
            # and/or platform-specific, so do tests which don't depend on
            # the internals.

            if o != o or o == _inf or o == _neginf:
                return 'null'
            else:
                return _repr(o)

        key_memo = {}
        int_as_string_bitcount = (
            53 if self.bigint_as_string else self.int_as_string_bitcount)
        _iterencode = _make_iterencode(
            markers, self.default, _encoder, self.indent, floatstr,
            self.sort_keys, self.skipkeys, _one_shot, self.use_decimal,
            self.namedtuple_as_object, self.tuple_as_array,
            int_as_string_bitcount,
            self.item_sort_key, self.encoding, self.for_json,
            Decimal=Decimal)
        try:
            return _iterencode(o, 0, True)
        finally:
            key_memo.clear()


def _make_iterencode(markers, _default, _encoder, _indent, _floatstr,
        _sort_keys, _skipkeys, _one_shot,
        _use_decimal, _namedtuple_as_object, _tuple_as_array,
        _int_as_string_bitcount, _item_sort_key,
        _encoding,_for_json,
        ## HACK: hand-optimized bytecode; turn globals into locals
        _PY3=PY3,
        ValueError=ValueError,
        string_types=string_types,
        Decimal=Decimal,
        dict=dict,
        float=float,
        id=id,
        integer_types=integer_types,
        isinstance=isinstance,
        list=list,
        str=str,
        tuple=tuple,
    ):
    if _item_sort_key and not callable(_item_sort_key):
        raise TypeError("item_sort_key must be None or callable")
    elif _sort_keys and not _item_sort_key:
        _item_sort_key = itemgetter(0)

    if (_int_as_string_bitcount is not None and
        (_int_as_string_bitcount <= 0 or
         not isinstance(_int_as_string_bitcount, integer_types))):
        raise TypeError("int_as_string_bitcount must be a positive integer")

    def _encode_int(value):
        return str(value)

    def _stringify_key(key):
        if isinstance(key, string_types): # pragma: no cover
            pass
        elif isinstance(key, binary_type):
            key = key.decode(_encoding)
        elif isinstance(key, float):
            key = _floatstr(key)
        elif key is True:
            key = 'true'
        elif key is False:
            key = 'false'
        elif key is None:
            key = 'null'
        elif isinstance(key, integer_types):
            key = str(key)
        elif _use_decimal and isinstance(key, Decimal):
            key = str(key)
        elif _skipkeys:
            key = None
        else:
            raise TypeError("key " + repr(key) + " is not a string")
        return key

    def _encoder_key(name):
        if not name: return '""'

        # Check if we can insert this name without quotes
        if NEEDSESCAPENAME.search(name):
            return _encoder(name)
        else:
          # return without quotes
          return name

    def _encoder_str(str, _current_indent_level):
        if not str: return '""'

        # Check if we can insert this string without quotes
        # see hjson syntax (must not parse as true, false, null or number)

        first = str[0]
        isNumber = False
        if first == '-' or first >= '0' and first <= '9':
            isNumber = STARTSWITHNUMBER.match(str) is not None

        if (NEEDSQUOTES.search(str) or
            isNumber or
            STARTSWITHKEYWORD.match(str) is not None):

            # If the string contains no control characters, no quote characters, and no
            # backslash characters, then we can safely slap some quotes around it.
            # Otherwise we first check if the string can be expressed in multiline
            # format or we must replace the offending characters with safe escape
            # sequences.

            if not NEEDSESCAPE.search(str):
                return '"' + str + '"'
            elif not NEEDSESCAPEML.search(str):
                return _encoder_str_ml(str, _current_indent_level + 1)
            else:
                return _encoder(str)
        else:
            # return without quotes
            return str

    def _encoder_str_ml(str, _current_indent_level):

        a = str.replace('\r', '').split('\n')
        # gap += indent;

        if len(a) == 1:
            # The string contains only a single line. We still use the multiline
            # format as it avoids escaping the \ character (e.g. when used in a
            # regex).
            return "'''" + a[0] + "'''"
        else:
            gap = _indent * _current_indent_level
            res = '\n' + gap + "'''"
            for line in a:
                res += '\n'
                if line: res += gap + line
            return res + '\n' + gap + "'''"

    def _iterencode_dict(dct, _current_indent_level, _isRoot=False):
        if not dct:
            yield '{}'
            return
        if markers is not None:
            markerid = id(dct)
            if markerid in markers:
                raise ValueError("Circular reference detected")
            markers[markerid] = dct

        if not _isRoot:
            yield '\n' + (_indent * _current_indent_level)

        _current_indent_level += 1
        newline_indent = '\n' + (_indent * _current_indent_level)

        yield '{'

        if _PY3:
            iteritems = dct.items()
        else:
            iteritems = dct.iteritems()
        if _item_sort_key:
            items = []
            for k, v in dct.items():
                if not isinstance(k, string_types):
                    k = _stringify_key(k)
                    if k is None:
                        continue
                items.append((k, v))
            items.sort(key=_item_sort_key)
        else:
            items = iteritems
        for key, value in items:
            if not (_item_sort_key or isinstance(key, string_types)):
                key = _stringify_key(key)
                if key is None:
                    # _skipkeys must be True
                    continue

            yield newline_indent
            yield _encoder_key(key)

            first = True
            for chunk in _iterencode(value, _current_indent_level):
                if first:
                    first = False
                    if chunk[0 : 1] == '\n': yield ':'
                    else: yield ': '
                yield chunk

        if newline_indent is not None:
            _current_indent_level -= 1
            yield '\n' + (_indent * _current_indent_level)
        yield '}'
        if markers is not None:
            del markers[markerid]


    def _iterencode_list(lst, _current_indent_level, _isRoot=False):
        if not lst:
            yield '[]'
            return
        if markers is not None:
            markerid = id(lst)
            if markerid in markers:
                raise ValueError("Circular reference detected")
            markers[markerid] = lst

        if not _isRoot:
            yield '\n' + (_indent * _current_indent_level)

        _current_indent_level += 1
        newline_indent = '\n' + (_indent * _current_indent_level)
        yield '['

        for value in lst:
            yield newline_indent

            for chunk in _iterencode(value, _current_indent_level, True):
                yield chunk

        if newline_indent is not None:
            _current_indent_level -= 1
            yield '\n' + (_indent * _current_indent_level)
        yield ']'
        if markers is not None:
            del markers[markerid]


    def _iterencode(o, _current_indent_level, _isRoot=False):
        if (isinstance(o, string_types) or
            (_PY3 and isinstance(o, binary_type))):
            yield _encoder_str(o, _current_indent_level)
        elif o is None:
            yield 'null'
        elif o is True:
            yield 'true'
        elif o is False:
            yield 'false'
        elif isinstance(o, integer_types):
            yield _encode_int(o)
        elif isinstance(o, float):
            yield _floatstr(o)
        else:
            for_json = _for_json and getattr(o, 'for_json', None)
            if for_json and callable(for_json):
                for chunk in _iterencode(for_json(), _current_indent_level, _isRoot):
                    yield chunk
            elif isinstance(o, list):
                for chunk in _iterencode_list(o, _current_indent_level, _isRoot):
                    yield chunk
            else:
                _asdict = _namedtuple_as_object and getattr(o, '_asdict', None)
                if _asdict and callable(_asdict):
                    for chunk in _iterencode_dict(_asdict(), _current_indent_level, _isRoot):
                        yield chunk
                elif (_tuple_as_array and isinstance(o, tuple)):
                    for chunk in _iterencode_list(o, _current_indent_level, _isRoot):
                        yield chunk
                elif isinstance(o, dict):
                    for chunk in _iterencode_dict(o, _current_indent_level, _isRoot):
                        yield chunk
                elif _use_decimal and isinstance(o, Decimal):
                    yield str(o)
                else:
                    if markers is not None:
                        markerid = id(o)
                        if markerid in markers:
                            raise ValueError("Circular reference detected")
                        markers[markerid] = o
                    o = _default(o)
                    for chunk in _iterencode(o, _current_indent_level, _isRoot):
                        yield chunk
                    if markers is not None:
                        del markers[markerid]

    return _iterencode
