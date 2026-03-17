r"""Hjson, the Human JSON. A configuration file format that caters to
humans and helps reduce the errors they make.

For details and syntax see <https://hjson.github.io>.

Decoding Hjson::

    >>> import hjson
    >>> text = "{\n  foo: a\n  bar: 1\n}"
    >>> hjson.loads(text)
    OrderedDict([('foo', 'a'), ('bar', 1)])

Encoding Python object hierarchies::

    >>> import hjson
    >>> # hjson.dumps({'foo': 'text', 'bar': (1, 2)})
    >>> hjson.dumps(OrderedDict([('foo', 'text'), ('bar', (1, 2))]))
    '{\n  foo: text\n  bar:\n  [\n    1\n    2\n  ]\n}'

Encoding as JSON::

    Note that this is probably not as performant as the simplejson version.

    >>> import hjson
    >>> hjson.dumpsJSON(['foo', {'bar': ('baz', None, 1.0, 2)}])
    '["foo", {"bar": ["baz", null, 1.0, 2]}]'

Using hjson.tool from the shell to validate and pretty-print::

    $ echo '{"json":"obj"}' | python -m hjson.tool
    {
      json: obj
    }

    Other formats are -c for compact or -j for formatted JSON.

"""
from __future__ import absolute_import
__version__ = '3.1.0'
__all__ = [
    'dump', 'dumps', 'load', 'loads',
    'dumpJSON', 'dumpsJSON',
    'HjsonDecoder', 'HjsonDecodeError', 'HjsonEncoder', 'JSONEncoder',
    'OrderedDict', 'simple_first',
]

# based on simplejson by
# __author__ = 'Bob Ippolito <bob@redivi.com>'
__author__ = 'Christian Zangl <coralllama@gmail.com>'

from decimal import Decimal

from .scanner import HjsonDecodeError
from .decoder import HjsonDecoder
from .encoderH import HjsonEncoder
from .encoder import JSONEncoder
def _import_OrderedDict():
    import collections
    try:
        return collections.OrderedDict
    except AttributeError:
        from . import ordered_dict
        return ordered_dict.OrderedDict
OrderedDict = _import_OrderedDict()


_default_decoder = HjsonDecoder(encoding=None, object_hook=None,
                               object_pairs_hook=OrderedDict)


def load(fp, encoding=None, cls=None, object_hook=None, parse_float=None,
        parse_int=None, object_pairs_hook=OrderedDict,
        use_decimal=False, namedtuple_as_object=True, tuple_as_array=True,
        **kw):
    """Deserialize ``fp`` (a ``.read()``-supporting file-like object containing
    a JSON document) to a Python object.

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

    If *use_decimal* is true (default: ``False``) then it implies
    parse_float=decimal.Decimal for parity with ``dump``.

    To use a custom ``HjsonDecoder`` subclass, specify it with the ``cls``
    kwarg. NOTE: You should use *object_hook* or *object_pairs_hook* instead
    of subclassing whenever possible.

    """
    return loads(fp.read(),
        encoding=encoding, cls=cls, object_hook=object_hook,
        parse_float=parse_float, parse_int=parse_int,
        object_pairs_hook=object_pairs_hook,
        use_decimal=use_decimal, **kw)


def loads(s, encoding=None, cls=None, object_hook=None, parse_float=None,
        parse_int=None, object_pairs_hook=None,
        use_decimal=False, **kw):
    """Deserialize ``s`` (a ``str`` or ``unicode`` instance containing a JSON
    document) to a Python object.

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

    If *use_decimal* is true (default: ``False``) then it implies
    parse_float=decimal.Decimal for parity with ``dump``.

    To use a custom ``HjsonDecoder`` subclass, specify it with the ``cls``
    kwarg. NOTE: You should use *object_hook* or *object_pairs_hook* instead
    of subclassing whenever possible.

    """
    if (cls is None and encoding is None and object_hook is None and
            parse_int is None and parse_float is None and
            object_pairs_hook is None
            and not use_decimal and not kw):
        return _default_decoder.decode(s)
    if cls is None:
        cls = HjsonDecoder
    if object_hook is not None:
        kw['object_hook'] = object_hook
    if object_pairs_hook is not None:
        kw['object_pairs_hook'] = object_pairs_hook
    if parse_float is not None:
        kw['parse_float'] = parse_float
    if parse_int is not None:
        kw['parse_int'] = parse_int
    if use_decimal:
        if parse_float is not None:
            raise TypeError("use_decimal=True implies parse_float=Decimal")
        kw['parse_float'] = Decimal
    return cls(encoding=encoding, **kw).decode(s)


_default_hjson_encoder = HjsonEncoder(
    skipkeys=False,
    ensure_ascii=True,
    check_circular=True,
    indent=None,
    encoding='utf-8',
    default=None,
    use_decimal=True,
    namedtuple_as_object=True,
    tuple_as_array=True,
    bigint_as_string=False,
    item_sort_key=None,
    for_json=False,
    int_as_string_bitcount=None,
)

def dump(obj, fp, skipkeys=False, ensure_ascii=True, check_circular=True,
         cls=None, indent=None,
         encoding='utf-8', default=None, use_decimal=True,
         namedtuple_as_object=True, tuple_as_array=True,
         bigint_as_string=False, sort_keys=False, item_sort_key=None,
         for_json=False, int_as_string_bitcount=None, **kw):
    """Serialize ``obj`` as a JSON formatted stream to ``fp`` (a
    ``.write()``-supporting file-like object).

    If *skipkeys* is true then ``dict`` keys that are not basic types
    (``str``, ``unicode``, ``int``, ``long``, ``float``, ``bool``, ``None``)
    will be skipped instead of raising a ``TypeError``.

    If *ensure_ascii* is false, then the some chunks written to ``fp``
    may be ``unicode`` instances, subject to normal Python ``str`` to
    ``unicode`` coercion rules. Unless ``fp.write()`` explicitly
    understands ``unicode`` (as in ``codecs.getwriter()``) this is likely
    to cause an error.

    If *check_circular* is false, then the circular reference check
    for container types will be skipped and a circular reference will
    result in an ``OverflowError`` (or worse).

    *indent* defines the amount of whitespace that the JSON array elements
    and object members will be indented for each level of nesting.
    The default is two spaces.

    *encoding* is the character encoding for str instances, default is UTF-8.

    *default(obj)* is a function that should return a serializable version
    of obj or raise ``TypeError``. The default simply raises ``TypeError``.

    If *use_decimal* is true (default: ``True``) then decimal.Decimal
    will be natively serialized to JSON with full precision.

    If *namedtuple_as_object* is true (default: ``True``),
    :class:`tuple` subclasses with ``_asdict()`` methods will be encoded
    as JSON objects.

    If *tuple_as_array* is true (default: ``True``),
    :class:`tuple` (and subclasses) will be encoded as JSON arrays.

    If *bigint_as_string* is true (default: ``False``), ints 2**53 and higher
    or lower than -2**53 will be encoded as strings. This is to avoid the
    rounding that happens in Javascript otherwise. Note that this is still a
    lossy operation that will not round-trip correctly and should be used
    sparingly.

    If *int_as_string_bitcount* is a positive number (n), then int of size
    greater than or equal to 2**n or lower than or equal to -2**n will be
    encoded as strings.

    If specified, *item_sort_key* is a callable used to sort the items in
    each dictionary. This is useful if you want to sort items other than
    in alphabetical order by key. This option takes precedence over
    *sort_keys*.

    If *sort_keys* is true (default: ``False``), the output of dictionaries
    will be sorted by item.

    If *for_json* is true (default: ``False``), objects with a ``for_json()``
    method will use the return value of that method for encoding as JSON
    instead of the object.

    To use a custom ``HjsonEncoder`` subclass (e.g. one that overrides the
    ``.default()`` method to serialize additional types), specify it with
    the ``cls`` kwarg. NOTE: You should use *default* or *for_json* instead
    of subclassing whenever possible.

    """
    # cached encoder
    if (not skipkeys and ensure_ascii and
        check_circular and
        cls is None and indent is None and
        encoding == 'utf-8' and default is None and use_decimal
        and namedtuple_as_object and tuple_as_array
        and not bigint_as_string and not sort_keys
        and not item_sort_key and not for_json
        and int_as_string_bitcount is None
        and not kw
    ):
        iterable = _default_hjson_encoder.iterencode(obj)
    else:
        if cls is None:
            cls = HjsonEncoder
        iterable = cls(skipkeys=skipkeys, ensure_ascii=ensure_ascii,
            check_circular=check_circular, indent=indent,
            encoding=encoding,
            default=default, use_decimal=use_decimal,
            namedtuple_as_object=namedtuple_as_object,
            tuple_as_array=tuple_as_array,
            bigint_as_string=bigint_as_string,
            sort_keys=sort_keys,
            item_sort_key=item_sort_key,
            for_json=for_json,
            int_as_string_bitcount=int_as_string_bitcount,
            **kw).iterencode(obj)
    # could accelerate with writelines in some versions of Python, at
    # a debuggability cost
    for chunk in iterable:
        fp.write(chunk)


def dumps(obj, skipkeys=False, ensure_ascii=True, check_circular=True,
          cls=None, indent=None,
          encoding='utf-8', default=None, use_decimal=True,
          namedtuple_as_object=True, tuple_as_array=True,
          bigint_as_string=False, sort_keys=False, item_sort_key=None,
          for_json=False, int_as_string_bitcount=None, **kw):
    """Serialize ``obj`` to a JSON formatted ``str``.

    If ``skipkeys`` is false then ``dict`` keys that are not basic types
    (``str``, ``unicode``, ``int``, ``long``, ``float``, ``bool``, ``None``)
    will be skipped instead of raising a ``TypeError``.

    If ``ensure_ascii`` is false, then the return value will be a
    ``unicode`` instance subject to normal Python ``str`` to ``unicode``
    coercion rules instead of being escaped to an ASCII ``str``.

    If ``check_circular`` is false, then the circular reference check
    for container types will be skipped and a circular reference will
    result in an ``OverflowError`` (or worse).

    *indent* defines the amount of whitespace that the JSON array elements
    and object members will be indented for each level of nesting.
    The default is two spaces.

    ``encoding`` is the character encoding for str instances, default is UTF-8.

    ``default(obj)`` is a function that should return a serializable version
    of obj or raise TypeError. The default simply raises TypeError.

    If *use_decimal* is true (default: ``True``) then decimal.Decimal
    will be natively serialized to JSON with full precision.

    If *namedtuple_as_object* is true (default: ``True``),
    :class:`tuple` subclasses with ``_asdict()`` methods will be encoded
    as JSON objects.

    If *tuple_as_array* is true (default: ``True``),
    :class:`tuple` (and subclasses) will be encoded as JSON arrays.

    If *bigint_as_string* is true (not the default), ints 2**53 and higher
    or lower than -2**53 will be encoded as strings. This is to avoid the
    rounding that happens in Javascript otherwise.

    If *int_as_string_bitcount* is a positive number (n), then int of size
    greater than or equal to 2**n or lower than or equal to -2**n will be
    encoded as strings.

    If specified, *item_sort_key* is a callable used to sort the items in
    each dictionary. This is useful if you want to sort items other than
    in alphabetical order by key. This option takes precendence over
    *sort_keys*.

    If *sort_keys* is true (default: ``False``), the output of dictionaries
    will be sorted by item.

    If *for_json* is true (default: ``False``), objects with a ``for_json()``
    method will use the return value of that method for encoding as JSON
    instead of the object.

    To use a custom ``HjsonEncoder`` subclass (e.g. one that overrides the
    ``.default()`` method to serialize additional types), specify it with
    the ``cls`` kwarg. NOTE: You should use *default* instead of subclassing
    whenever possible.

    """
    # cached encoder
    if (
        not skipkeys and ensure_ascii and
        check_circular and
        cls is None and indent is None and
        encoding == 'utf-8' and default is None and use_decimal
        and namedtuple_as_object and tuple_as_array
        and not bigint_as_string and not sort_keys
        and not item_sort_key and not for_json
        and int_as_string_bitcount is None
        and not kw
    ):
        return _default_hjson_encoder.encode(obj)
    if cls is None:
        cls = HjsonEncoder
    return cls(
        skipkeys=skipkeys, ensure_ascii=ensure_ascii,
        check_circular=check_circular, indent=indent,
        encoding=encoding, default=default,
        use_decimal=use_decimal,
        namedtuple_as_object=namedtuple_as_object,
        tuple_as_array=tuple_as_array,
        bigint_as_string=bigint_as_string,
        sort_keys=sort_keys,
        item_sort_key=item_sort_key,
        for_json=for_json,
        int_as_string_bitcount=int_as_string_bitcount,
        **kw).encode(obj)



_default_json_encoder = JSONEncoder(
    skipkeys=False,
    ensure_ascii=True,
    check_circular=True,
    indent=None,
    separators=None,
    encoding='utf-8',
    default=None,
    use_decimal=True,
    namedtuple_as_object=True,
    tuple_as_array=True,
    bigint_as_string=False,
    item_sort_key=None,
    for_json=False,
    int_as_string_bitcount=None,
)

def dumpJSON(obj, fp, skipkeys=False, ensure_ascii=True, check_circular=True,
         cls=None, indent=None, separators=None,
         encoding='utf-8', default=None, use_decimal=True,
         namedtuple_as_object=True, tuple_as_array=True,
         bigint_as_string=False, sort_keys=False, item_sort_key=None,
         for_json=False, int_as_string_bitcount=None, **kw):
    """Serialize ``obj`` as a JSON formatted stream to ``fp`` (a
    ``.write()``-supporting file-like object).

    If *skipkeys* is true then ``dict`` keys that are not basic types
    (``str``, ``unicode``, ``int``, ``long``, ``float``, ``bool``, ``None``)
    will be skipped instead of raising a ``TypeError``.

    If *ensure_ascii* is false, then the some chunks written to ``fp``
    may be ``unicode`` instances, subject to normal Python ``str`` to
    ``unicode`` coercion rules. Unless ``fp.write()`` explicitly
    understands ``unicode`` (as in ``codecs.getwriter()``) this is likely
    to cause an error.

    If *check_circular* is false, then the circular reference check
    for container types will be skipped and a circular reference will
    result in an ``OverflowError`` (or worse).

    If *indent* is a string, then JSON array elements and object members
    will be pretty-printed with a newline followed by that string repeated
    for each level of nesting. ``None`` (the default) selects the most compact
    representation without any newlines. An integer is also accepted
    and is converted to a string with that many spaces.

    If specified, *separators* should be an
    ``(item_separator, key_separator)`` tuple.  The default is ``(', ', ': ')``
    if *indent* is ``None`` and ``(',', ': ')`` otherwise.  To get the most
    compact JSON representation, you should specify ``(',', ':')`` to eliminate
    whitespace.

    *encoding* is the character encoding for str instances, default is UTF-8.

    *default(obj)* is a function that should return a serializable version
    of obj or raise ``TypeError``. The default simply raises ``TypeError``.

    If *use_decimal* is true (default: ``True``) then decimal.Decimal
    will be natively serialized to JSON with full precision.

    If *namedtuple_as_object* is true (default: ``True``),
    :class:`tuple` subclasses with ``_asdict()`` methods will be encoded
    as JSON objects.

    If *tuple_as_array* is true (default: ``True``),
    :class:`tuple` (and subclasses) will be encoded as JSON arrays.

    If *bigint_as_string* is true (default: ``False``), ints 2**53 and higher
    or lower than -2**53 will be encoded as strings. This is to avoid the
    rounding that happens in Javascript otherwise. Note that this is still a
    lossy operation that will not round-trip correctly and should be used
    sparingly.

    If *int_as_string_bitcount* is a positive number (n), then int of size
    greater than or equal to 2**n or lower than or equal to -2**n will be
    encoded as strings.

    If specified, *item_sort_key* is a callable used to sort the items in
    each dictionary. This is useful if you want to sort items other than
    in alphabetical order by key. This option takes precedence over
    *sort_keys*.

    If *sort_keys* is true (default: ``False``), the output of dictionaries
    will be sorted by item.

    If *for_json* is true (default: ``False``), objects with a ``for_json()``
    method will use the return value of that method for encoding as JSON
    instead of the object.

    To use a custom ``JSONEncoder`` subclass (e.g. one that overrides the
    ``.default()`` method to serialize additional types), specify it with
    the ``cls`` kwarg. NOTE: You should use *default* or *for_json* instead
    of subclassing whenever possible.

    """
    # cached encoder
    if (not skipkeys and ensure_ascii and
        check_circular and
        cls is None and indent is None and separators is None and
        encoding == 'utf-8' and default is None and use_decimal
        and namedtuple_as_object and tuple_as_array
        and not bigint_as_string and not sort_keys
        and not item_sort_key and not for_json
        and int_as_string_bitcount is None
        and not kw
    ):
        iterable = _default_json_encoder.iterencode(obj)
    else:
        if cls is None:
            cls = JSONEncoder
        iterable = cls(skipkeys=skipkeys, ensure_ascii=ensure_ascii,
            check_circular=check_circular, indent=indent,
            separators=separators, encoding=encoding,
            default=default, use_decimal=use_decimal,
            namedtuple_as_object=namedtuple_as_object,
            tuple_as_array=tuple_as_array,
            bigint_as_string=bigint_as_string,
            sort_keys=sort_keys,
            item_sort_key=item_sort_key,
            for_json=for_json,
            int_as_string_bitcount=int_as_string_bitcount,
            **kw).iterencode(obj)
    # could accelerate with writelines in some versions of Python, at
    # a debuggability cost
    for chunk in iterable:
        fp.write(chunk)


def dumpsJSON(obj, skipkeys=False, ensure_ascii=True, check_circular=True,
          cls=None, indent=None, separators=None,
          encoding='utf-8', default=None, use_decimal=True,
          namedtuple_as_object=True, tuple_as_array=True,
          bigint_as_string=False, sort_keys=False, item_sort_key=None,
          for_json=False, int_as_string_bitcount=None, **kw):
    """Serialize ``obj`` to a JSON formatted ``str``.

    If ``skipkeys`` is false then ``dict`` keys that are not basic types
    (``str``, ``unicode``, ``int``, ``long``, ``float``, ``bool``, ``None``)
    will be skipped instead of raising a ``TypeError``.

    If ``ensure_ascii`` is false, then the return value will be a
    ``unicode`` instance subject to normal Python ``str`` to ``unicode``
    coercion rules instead of being escaped to an ASCII ``str``.

    If ``check_circular`` is false, then the circular reference check
    for container types will be skipped and a circular reference will
    result in an ``OverflowError`` (or worse).

    If ``indent`` is a string, then JSON array elements and object members
    will be pretty-printed with a newline followed by that string repeated
    for each level of nesting. ``None`` (the default) selects the most compact
    representation without any newlines. An integer is also accepted
    and is converted to a string with that many spaces.

    If specified, ``separators`` should be an
    ``(item_separator, key_separator)`` tuple.  The default is ``(', ', ': ')``
    if *indent* is ``None`` and ``(',', ': ')`` otherwise.  To get the most
    compact JSON representation, you should specify ``(',', ':')`` to eliminate
    whitespace.

    ``encoding`` is the character encoding for str instances, default is UTF-8.

    ``default(obj)`` is a function that should return a serializable version
    of obj or raise TypeError. The default simply raises TypeError.

    If *use_decimal* is true (default: ``True``) then decimal.Decimal
    will be natively serialized to JSON with full precision.

    If *namedtuple_as_object* is true (default: ``True``),
    :class:`tuple` subclasses with ``_asdict()`` methods will be encoded
    as JSON objects.

    If *tuple_as_array* is true (default: ``True``),
    :class:`tuple` (and subclasses) will be encoded as JSON arrays.

    If *bigint_as_string* is true (not the default), ints 2**53 and higher
    or lower than -2**53 will be encoded as strings. This is to avoid the
    rounding that happens in Javascript otherwise.

    If *int_as_string_bitcount* is a positive number (n), then int of size
    greater than or equal to 2**n or lower than or equal to -2**n will be
    encoded as strings.

    If specified, *item_sort_key* is a callable used to sort the items in
    each dictionary. This is useful if you want to sort items other than
    in alphabetical order by key. This option takes precendence over
    *sort_keys*.

    If *sort_keys* is true (default: ``False``), the output of dictionaries
    will be sorted by item.

    If *for_json* is true (default: ``False``), objects with a ``for_json()``
    method will use the return value of that method for encoding as JSON
    instead of the object.

    To use a custom ``JSONEncoder`` subclass (e.g. one that overrides the
    ``.default()`` method to serialize additional types), specify it with
    the ``cls`` kwarg. NOTE: You should use *default* instead of subclassing
    whenever possible.

    """
    # cached encoder
    if (
        not skipkeys and ensure_ascii and
        check_circular and
        cls is None and indent is None and separators is None and
        encoding == 'utf-8' and default is None and use_decimal
        and namedtuple_as_object and tuple_as_array
        and not bigint_as_string and not sort_keys
        and not item_sort_key and not for_json
        and int_as_string_bitcount is None
        and not kw
    ):
        return _default_json_encoder.encode(obj)
    if cls is None:
        cls = JSONEncoder
    return cls(
        skipkeys=skipkeys, ensure_ascii=ensure_ascii,
        check_circular=check_circular, indent=indent,
        separators=separators, encoding=encoding, default=default,
        use_decimal=use_decimal,
        namedtuple_as_object=namedtuple_as_object,
        tuple_as_array=tuple_as_array,
        bigint_as_string=bigint_as_string,
        sort_keys=sort_keys,
        item_sort_key=item_sort_key,
        for_json=for_json,
        int_as_string_bitcount=int_as_string_bitcount,
        **kw).encode(obj)



def simple_first(kv):
    """Helper function to pass to item_sort_key to sort simple
    elements to the top, then container elements.
    """
    return (isinstance(kv[1], (list, dict, tuple)), kv[0])
