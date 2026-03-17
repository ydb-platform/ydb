# -*- coding: utf-8 -*-
r"""
    phpserialize
    ~~~~~~~~~~~~

    a port of the ``serialize`` and ``unserialize`` functions of
    php to python.  This module implements the python serialization
    interface (eg: provides `dumps`, `loads` and similar functions).

    Usage
    =====

    >>> from phpserialize import *
    >>> obj = dumps("Hello World")
    >>> loads(obj)
    'Hello World'

    Due to the fact that PHP doesn't know the concept of lists, lists
    are serialized like hash-maps in PHP.  As a matter of fact the
    reverse value of a serialized list is a dict:

    >>> loads(dumps(range(2)))
    {0: 0, 1: 1}

    If you want to have a list again, you can use the `dict_to_list`
    helper function:

    >>> dict_to_list(loads(dumps(range(2))))
    [0, 1]

    It's also possible to convert into a tuple by using the `dict_to_tuple`
    function:

    >>> dict_to_tuple(loads(dumps((1, 2, 3))))
    (1, 2, 3)

    Another problem are unicode strings.  By default unicode strings are
    encoded to 'utf-8' but not decoded on `unserialize`.  The reason for
    this is that phpserialize can't guess if you have binary or text data
    in the strings:

    >>> loads(dumps(u'Hello W\xf6rld'))
    'Hello W\xc3\xb6rld'

    If you know that you have only text data of a known charset in the result
    you can decode strings by setting `decode_strings` to True when calling
    loads:

    >>> loads(dumps(u'Hello W\xf6rld'), decode_strings=True)
    u'Hello W\xf6rld'

    Dictionary keys are limited to strings and integers.  `None` is converted
    into an empty string and floats and booleans into integers for PHP
    compatibility:

    >>> loads(dumps({None: 14, 42.23: 'foo', True: [1, 2, 3]}))
    {'': 14, 1: {0: 1, 1: 2, 2: 3}, 42: 'foo'}

    It also provides functions to read from file-like objects:

    >>> from StringIO import StringIO
    >>> stream = StringIO('a:2:{i:0;i:1;i:1;i:2;}')
    >>> dict_to_list(load(stream))
    [1, 2]

    And to write to those:

    >>> stream = StringIO()
    >>> dump([1, 2], stream)
    >>> stream.getvalue()
    'a:2:{i:0;i:1;i:1;i:2;}'

    Like `pickle` chaining of objects is supported:

    >>> stream = StringIO()
    >>> dump([1, 2], stream)
    >>> dump("foo", stream)
    >>> stream.seek(0)
    >>> load(stream)
    {0: 1, 1: 2}
    >>> load(stream)
    'foo'

    This feature however is not supported in PHP.  PHP will only unserialize
    the first object.

    Array Serialization
    ===================

    Starting with 1.2 you can provide an array hook to the unserialization
    functions that are invoked with a list of pairs to return a real array
    object.  By default `dict` is used as array object which however means
    that the information about the order is lost for associative arrays.

    For example you can pass the ordered dictionary to the unserilization
    functions:

    >>> from collections import OrderedDict
    >>> loads('a:2:{s:3:"foo";i:1;s:3:"bar";i:2;}',
    ...       array_hook=OrderedDict)
    collections.OrderedDict([('foo', 1), ('bar', 2)])

    Object Serialization
    ====================

    PHP supports serialization of objects.  Starting with 1.2 of phpserialize
    it is possible to both serialize and unserialize objects.  Because class
    names in PHP and Python usually do not map, there is a separate
    `object_hook` parameter that is responsible for creating these classes.

    For a simple test example the `phpserialize.phpobject` class can be used:

    >>> data = 'O:7:"WP_User":1:{s:8:"username";s:5:"admin";}'
    >>> user = loads(data, object_hook=phpobject)
    >>> user.username
    'admin'
    >>> user.__name__
    'WP_User'

    An object hook is a function that takes the name of the class and a dict
    with the instance data as arguments.  The instance data keys are in PHP
    format which usually is not what you want.  To convert it into Python
    identifiers you can use the `convert_member_dict` function.  For more
    information about that, have a look at the next section.  Here an
    example for a simple object hook:

    >>> class User(object):
    ...     def __init__(self, username):
    ...         self.username = username
    ...
    >>> def object_hook(name, d):
    ...     cls = {'WP_User': User}[name]
    ...     return cls(**d)
    ...
    >>> user = loads(data, object_hook=object_hook)
    >>> user.username
    'admin'

    To serialize objects you can use the `object_hook` of the dump functions
    and return instances of `phpobject`:

    >>> def object_hook(obj):
    ...     if isinstance(obj, User):
    ...         return phpobject('WP_User', {'username': obj.username})
    ...     raise LookupError('unknown object')
    ...
    >>> dumps(user, object_hook=object_hook)
    'O:7:"WP_User":1:{s:8:"username";s:5:"admin";}'

    PHP's Object System
    ===================

    The PHP object system is derived from compiled languages such as Java
    and C#.  Attributes can be protected from external access by setting
    them to `protected` or `private`.  This does not only serve the purpose
    to encapsulate internals but also to avoid name clashes.

    In PHP each class in the inheritance chain can have a private variable
    with the same name, without causing clashes.  (This is similar to the
    Python `__var` name mangling system).

    This PHP class::

        class WP_UserBase {
            protected $username;

            public function __construct($username) {
                $this->username = $username;
            }
        }

        class WP_User extends WP_UserBase {
            private $password;
            public $flag;

            public function __construct($username, $password) {
                parent::__construct($username);
                $this->password = $password;
                $this->flag = 0;
            }
        }

    Is serialized with a member data dict that looks like this:

    >>> data = {
    ...     ' * username':          'the username',
    ...     ' WP_User password':    'the password',
    ...     'flag':                 'the flag'
    ... }

    Because this access system does not exist in Python, the
    `convert_member_dict` can convert this dict:

    >>> d = convert_member_dict(data)
    >>> d['username']
    'the username'
    >>> d['password']
    'the password'

    The `phpobject` class does this conversion on the fly.  What is
    serialized is the special `__php_vars__` dict of the class:

    >>> user = phpobject('WP_User', data)
    >>> user.username
    'the username'
    >>> user.username = 'admin'
    >>> user.__php_vars__[' * username']
    'admin'

    As you can see, reassigning attributes on a php object will try
    to change a private or protected attribute with the same name.
    Setting an unknown one will create a new public attribute:

    >>> user.is_admin = True
    >>> user.__php_vars__['is_admin']
    True

    To convert the phpobject into a dict, you can use the `_asdict`
    method:

    >>> d = user._asdict()
    >>> d['username']
    'admin'

    Python 3 Notes
    ==============

    Because the unicode support in Python 3 no longer transparently
    handles bytes and unicode objects we had to change the way the
    decoding works.  On Python 3 you most likely want to always
    decode strings.  Because this would totally fail on binary data
    phpserialize uses the "surrogateescape" method to not fail on
    invalid data.  See the documentation in Python 3 for more
    information.

    Changelog
    =========

    1.3
        -   added support for Python 3

    1.2
        -   added support for object serialization
        -   added support for array hooks

    1.1
        -   added `dict_to_list` and `dict_to_tuple`
        -   added support for unicode
        -   allowed chaining of objects like pickle does


    :copyright: 2007-2012 by Armin Ronacher.
    license: BSD
"""
import codecs
try:
    codecs.lookup_error('surrogateescape')
    default_errors = 'surrogateescape'
except LookupError:
    default_errors = 'strict'

try:
    from StringIO import StringIO as BytesIO
except ImportError:
    from io import BytesIO as BytesIO

try:
    unicode
except NameError:
    # Python 3
    unicode = str
    basestring = (bytes, str)

try:
    long
except NameError:
    # Python 3
    long = int

try:
    xrange
except NameError:
    xrange = range

__author__ = 'Armin Ronacher <armin.ronacher@active-4.com>'
__version__ = '1.3'
__all__ = ('phpobject', 'convert_member_dict', 'dict_to_list', 'dict_to_tuple',
           'load', 'loads', 'dump', 'dumps', 'serialize', 'unserialize')


def _translate_member_name(name):
    if name[:1] == ' ':
        name = name.split(None, 2)[-1]
    return name


class phpobject(object):
    """Simple representation for PHP objects.  This is used """
    __slots__ = ('__name__', '__php_vars__')

    def __init__(self, name, d=None):
        if d is None:
            d = {}
        object.__setattr__(self, '__name__', name)
        object.__setattr__(self, '__php_vars__', d)

    def _asdict(self):
        """Returns a new dictionary from the data with Python identifiers."""
        return convert_member_dict(self.__php_vars__)

    def _lookup_php_var(self, name):
        for key, value in self.__php_vars__.items():
            if _translate_member_name(key) == name:
                return key, value

    def __getattr__(self, name):
        rv = self._lookup_php_var(name)
        if rv is not None:
            return rv[1]
        raise AttributeError(name)

    def __setattr__(self, name, value):
        rv = self._lookup_php_var(name)
        if rv is not None:
            name = rv[0]
        self.__php_vars__[name] = value

    def __repr__(self):
        return '<phpobject %r>' % (self.__name__,)


def convert_member_dict(d):
    """Converts the names of a member dict to Python syntax.  PHP class data
    member names are not the plain identifiers but might be prefixed by the
    class name if private or a star if protected.  This function converts them
    into standard Python identifiers:

    >>> convert_member_dict({"username": "user1", " User password":
    ...                      "default", " * is_active": True})
    {'username': 'user1', 'password': 'default', 'is_active': True}
    """
    return dict((_translate_member_name(k), v) for k, v in d.items())


def dumps(data, charset='utf-8', errors=default_errors, object_hook=None):
    """Return the PHP-serialized representation of the object as a string,
    instead of writing it to a file like `dump` does.  On Python 3
    this returns bytes objects, on Python 3 this returns bytestrings.
    """
    def _serialize(obj, keypos):
        if keypos:
            if isinstance(obj, (int, long, float, bool)):
                return ('i:%i;' % obj).encode('latin1')
            if isinstance(obj, basestring):
                encoded_obj = obj
                if isinstance(obj, unicode):
                    encoded_obj = obj.encode(charset, errors)
                s = BytesIO()
                s.write(b's:')
                s.write(str(len(encoded_obj)).encode('latin1'))
                s.write(b':"')
                s.write(encoded_obj)
                s.write(b'";')
                return s.getvalue()
            if obj is None:
                return b's:0:"";'
            raise TypeError('can\'t serialize %r as key' % type(obj))
        else:
            if obj is None:
                return b'N;'
            if isinstance(obj, bool):
                return ('b:%i;' % obj).encode('latin1')
            if isinstance(obj, (int, long)):
                return ('i:%s;' % obj).encode('latin1')
            if isinstance(obj, float):
                return ('d:%s;' % obj).encode('latin1')
            if isinstance(obj, basestring):
                encoded_obj = obj
                if isinstance(obj, unicode):
                    encoded_obj = obj.encode(charset, errors)
                s = BytesIO()
                s.write(b's:')
                s.write(str(len(encoded_obj)).encode('latin1'))
                s.write(b':"')
                s.write(encoded_obj)
                s.write(b'";')
                return s.getvalue()
            if isinstance(obj, (list, tuple, dict)):
                out = []
                if isinstance(obj, dict):
                    iterable = obj.items()
                else:
                    iterable = enumerate(obj)
                for key, value in iterable:
                    out.append(_serialize(key, True))
                    out.append(_serialize(value, False))
                return b''.join([
                    b'a:',
                    str(len(obj)).encode('latin1'),
                    b':{',
                    b''.join(out),
                    b'}'
                ])
            if isinstance(obj, phpobject):
                return b'O' + _serialize(obj.__name__, True)[1:-1] + \
                       _serialize(obj.__php_vars__, False)[1:]
            if object_hook is not None:
                return _serialize(object_hook(obj), False)
            raise TypeError('can\'t serialize %r' % type(obj))

    return _serialize(data, False)


def load(fp, charset='utf-8', errors=default_errors, decode_strings=False,
         object_hook=None, array_hook=None):
    """Read a string from the open file object `fp` and interpret it as a
    data stream of PHP-serialized objects, reconstructing and returning
    the original object hierarchy.

    `fp` must provide a `read()` method that takes an integer argument.  Both
    method should return strings.  Thus `fp` can be a file object opened for
    reading, a `StringIO` object (`BytesIO` on Python 3), or any other custom
    object that meets this interface.

    `load` will read exactly one object from the stream.  See the docstring of
    the module for this chained behavior.

    If an object hook is given object-opcodes are supported in the serilization
    format.  The function is called with the class name and a dict of the
    class data members.  The data member names are in PHP format which is
    usually not what you want.  The `simple_object_hook` function can convert
    them to Python identifier names.

    If an `array_hook` is given that function is called with a list of pairs
    for all array items.  This can for example be set to
    `collections.OrderedDict` for an ordered, hashed dictionary.
    """
    if array_hook is None:
        array_hook = dict

    def _expect(e):
        v = fp.read(len(e))
        if v != e:
            raise ValueError('failed expectation, expected %r got %r' % (e, v))

    def _read_until(delim):
        buf = []
        while 1:
            char = fp.read(1)
            if char == delim:
                break
            elif not char:
                raise ValueError('unexpected end of stream')
            buf.append(char)
        return b''.join(buf)

    def _load_array():
        items = int(_read_until(b':')) * 2
        _expect(b'{')
        result = []
        last_item = Ellipsis
        for idx in xrange(items):
            item = _unserialize()
            if last_item is Ellipsis:
                last_item = item
            else:
                result.append((last_item, item))
                last_item = Ellipsis
        _expect(b'}')
        return result

    def _unserialize():
        type_ = fp.read(1).lower()
        if type_ == b'n':
            _expect(b';')
            return None
        if type_ in b'idb':
            _expect(b':')
            data = _read_until(b';')
            if type_ == b'i':
                return int(data)
            if type_ == b'd':
                return float(data)
            return int(data) != 0
        if type_ == b's':
            _expect(b':')
            length = int(_read_until(b':'))
            _expect(b'"')
            data = fp.read(length)
            _expect(b'"')
            if decode_strings:
                data = data.decode(charset, errors)
            _expect(b';')
            return data
        if type_ == b'a':
            _expect(b':')
            return array_hook(_load_array())
        if type_ == b'o':
            if object_hook is None:
                raise ValueError('object in serialization dump but '
                                 'object_hook not given.')
            _expect(b':')
            name_length = int(_read_until(b':'))
            _expect(b'"')
            name = fp.read(name_length)
            _expect(b'":')
            if decode_strings:
                name = name.decode(charset, errors)
            return object_hook(name, dict(_load_array()))
        raise ValueError('unexpected opcode')

    return _unserialize()


def loads(data, charset='utf-8', errors=default_errors, decode_strings=False,
          object_hook=None, array_hook=None):
    """Read a PHP-serialized object hierarchy from a string.  Characters in the
    string past the object's representation are ignored.  On Python 3 the
    string must be a bytestring.
    """
    return load(BytesIO(data), charset, errors, decode_strings,
                object_hook, array_hook)


def dump(data, fp, charset='utf-8', errors=default_errors, object_hook=None):
    """Write a PHP-serialized representation of obj to the open file object
    `fp`.  Unicode strings are encoded to `charset` with the error handling
    of `errors`.

    `fp` must have a `write()` method that accepts a single string argument.
    It can thus be a file object opened for writing, a `StringIO` object
    (or a `BytesIO` object on Python 3), or any other custom object that meets
    this interface.

    The `object_hook` is called for each unknown object and has to either
    raise an exception if it's unable to convert the object or return a
    value that is serializable (such as a `phpobject`).
    """
    fp.write(dumps(data, charset, errors, object_hook))


def dict_to_list(d):
    """Converts an ordered dict into a list."""
    # make sure it's a dict, that way dict_to_list can be used as an
    # array_hook.
    d = dict(d)
    try:
        return [d[x] for x in xrange(len(d))]
    except KeyError:
        raise ValueError('dict is not a sequence')


def dict_to_tuple(d):
    """Converts an ordered dict into a tuple."""
    return tuple(dict_to_list(d))


serialize = dumps
unserialize = loads
