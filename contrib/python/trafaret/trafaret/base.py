# -*- coding: utf-8 -*-

import functools
import itertools
import warnings
from datetime import date, datetime
from .lib import (
    py3,
    py36,
    py3metafix,
    getargspec,
    get_callable_args,
    with_context_caller,
    _empty,
    STR_TYPES,
    AbcMapping,
    Iterable as AbcIterable,
)
from .dataerror import DataError
from . import codes


if py36:
    from .async_mixins import (
        TrafaretAsyncMixin,
        OrAsyncMixin,
        AndAsyncMixin,
        ListAsyncMixin,
        TupleAsyncMixin,
        MappingAsyncMixin,
        CallAsyncMixin,
        ForwardAsyncMixin,
        DictAsyncMixin,
        KeyAsyncMixin,
    )
else:  # pragma: no cover
    class EmptyMixin(object):
        pass
    TrafaretAsyncMixin = EmptyMixin
    OrAsyncMixin = EmptyMixin
    AndAsyncMixin = EmptyMixin
    ListAsyncMixin = EmptyMixin
    TupleAsyncMixin = EmptyMixin
    MappingAsyncMixin = EmptyMixin
    CallAsyncMixin = EmptyMixin
    ForwardAsyncMixin = EmptyMixin
    DictAsyncMixin = EmptyMixin
    KeyAsyncMixin = EmptyMixin


# Python3 support
if py3:
    unicode = str
    BYTES_TYPE = bytes
    STR_TYPE = str
else:  # pragma: no cover
    BYTES_TYPE = str
    STR_TYPE = unicode


def deprecated(message):
    warnings.warn(message, DeprecationWarning)


"""
Trafaret is tiny library for data validation
It provides several primitives to validate complex data structures
Look at doctests for usage examples
"""


class TrafaretMeta(type):
    """
    Metaclass for trafarets to make using "|" operator possible not only
    on instances but on classes

    >>> Int | String
    <Or(<Int>, <String>)>
    >>> Int | String | Null
    <Or(<Int>, <String>, <Null>)>
    >>> (Int >> (lambda v: v if v ** 2 > 15 else 0)).check(5)
    5
    """

    def __or__(cls, other):
        return cls() | other

    def __and__(cls, other):
        return cls() & other

    def __rshift__(cls, other):
        return cls() & other


@py3metafix
class Trafaret(TrafaretAsyncMixin):
    """
    Base class for trafarets, provides only one method for
    trafaret validation failure reporting
    """

    __metaclass__ = TrafaretMeta

    def check(self, value, context=None):
        """
        Common logic. In subclasses you need to implement check_value or
        check_and_return.
        """
        if hasattr(self, 'transform'):
            return self.transform(value, context=context)
        elif hasattr(self, 'check_value'):
            self.check_value(value)
            return value
        elif hasattr(self, 'check_and_return'):
            return self.check_and_return(value)
        else:
            cls = "{}.{}".format(
                type(self).__module__,
                type(self).__name__,
            )
            raise NotImplementedError(
                "You must implement check_value or"
                " check_and_return methods '%s'" % cls
            )

    def is_valid(self, value):
        """
        Allows to check value and get bool, is value valid or not.
        """
        try:
            self.check(value)
            return True
        except DataError:
            return False

    def _failure(self, error=None, value=_empty, code=None):
        """
        Shortcut method for raising validation error
        """
        raise DataError(error=error, value=value, trafaret=self, code=code)

    def __or__(self, other):
        return Or(self, other)

    def __and__(self, other):
        return And(self, other)

    def __rshift__(self, other):
        return And(self, other)

    def __call__(self, val, context=None):
        return self.check(val, context=context)


class OnError(Trafaret):
    def __init__(self, trafaret, message, code=None):
        self.trafaret = ensure_trafaret(trafaret)
        self.message = message
        self.code = code

    def transform(self, value, context=None):
        try:
            return self.trafaret(value, context=context)
        except DataError as de:
            raise DataError(
                self.message,
                value=value,
                trafaret=self.trafaret,
                code=self.code or de.code,
            )


class WithRepr(Trafaret):
    def __init__(self, trafaret, representation):
        self.trafaret = ensure_trafaret(trafaret)
        self.representation = representation

    def transform(self, value, context=None):
        return self.trafaret(value, context=context)

    def __repr__(self):
        return self.representation


def ensure_trafaret(trafaret):
    """
    Helper for complex trafarets, takes trafaret instance or class
    and returns trafaret instance
    """
    if isinstance(trafaret, Trafaret):
        return trafaret
    elif isinstance(trafaret, type):
        if issubclass(trafaret, Trafaret):
            return trafaret()
        # str, int, float are classes, but its appropriate to use them
        # as trafaret functions
        return Call(lambda val: trafaret(val))
    elif callable(trafaret):
        return Call(trafaret)
    else:
        raise RuntimeError("%r should be instance or subclass"
                           " of Trafaret" % trafaret)


class TypeMeta(TrafaretMeta):
    def __getitem__(self, type_):
        return self(type_)


@py3metafix
class TypingTrafaret(Trafaret):
    """A trafaret used for instance type and class inheritance checks."""

    __metaclass__ = TypeMeta

    def __init__(self, type_):
        self.type_ = type_

    def check_value(self, value):
        if not self.typing_checker(value, self.type_):
            self._failure(
                self.failure_message % self.type_.__name__,
                value=value,
                code=self.code,
            )

    def __repr__(self):
        return "<%s(%s)>" % (self.__class__.__name__, self.type_.__name__)


class Subclass(TypingTrafaret):
    """
    >>> Subclass(type)
    <Subclass(type)>
    >>> Subclass[type]
    <Subclass(type)>
    >>> s = Subclass[type]
    >>> s.check(type)
    <type 'type'>
    >>> extract_error(s, object)
    'value is not subclass of type'
    """

    typing_checker = issubclass
    failure_message = "value is not subclass of %s"
    code = "is_not_subclass"


class Type(TypingTrafaret):
    """
    >>> Type(int)
    <Type(int)>
    >>> Type[int]
    <Type(int)>
    >>> c = Type[int]
    >>> c.check(1)
    1
    >>> extract_error(c, "foo")
    'value is not int'
    >>> c.is_valid("foo")
    False
    """

    typing_checker = isinstance
    failure_message = "value is not %s"
    code = "is_not_instance"


class Any(Trafaret):
    """
    >>> Any()
    <Any>
    >>> (Any() >> ignore).check(object())
    """

    def check_value(self, value):
        pass

    def __repr__(self):
        return "<Any>"


@py3metafix
class Or(Trafaret, OrAsyncMixin):
    """
    >>> nullString = Or(String, Null)
    >>> nullString
    <Or(<String>, <Null>)>
    >>> nullString.check(None)
    >>> nullString.check("test")
    'test'
    >>> extract_error(nullString, 1)
    {0: 'value is not a string', 1: 'value should be None'}
    >>> nullString.is_valid(None)
    True
    >>> nullString.is_valid("test")
    True
    >>> nullString.is_valid(1)
    False
    """

    __slots__ = ['trafarets']

    def __init__(self, *trafarets):
        self.trafarets = [ensure_trafaret(t) for t in trafarets]

    def transform(self, value, context=None):
        errors = []
        for trafaret in self.trafarets:
            try:
                return trafaret(value, context=context)
            except DataError as e:
                errors.append(e)
        raise self._failure(dict(enumerate(errors)), code=codes.NOTHING_MATCH)

    def __repr__(self):
        return "<Or(%s)>" % (", ".join(repr(t) for t in self.trafarets))


class And(Trafaret, AndAsyncMixin):
    """
    Will work over trafarets sequentially
    """
    __slots__ = ('trafaret', 'other', 'disable_old_check_convert')

    def __init__(self, trafaret, other):
        self.trafaret = ensure_trafaret(trafaret)
        self.other = ensure_trafaret(other)

    def transform(self, value, context=None):
        # it will raise in case of error
        res = self.trafaret(value, context=context)
        return self.other(res, context=context)

    def __repr__(self):
        return "<And(%s, %s)>" % (
            repr(self.trafaret),
            repr(self.other),
        )


class Null(Trafaret):
    """
    >>> Null()
    <Null>
    >>> Null().check(None)
    >>> extract_error(Null(), 1)
    'value should be None'
    >>> Null().is_valid(None)
    True
    >>> Null().is_valid(1)
    False
    """

    def check_value(self, value):
        if value is not None:
            self._failure("value should be None", value=value, code=codes.IS_NOT_NULL)

    def __repr__(self):
        return "<Null>"


class Bool(Trafaret):
    """
    >>> Bool()
    <Bool>
    >>> Bool().check(True)
    True
    >>> Bool().check(False)
    False
    >>> extract_error(Bool(), 1)
    'value should be True or False'
    >>> Null().is_valid(True)
    True
    >>> Null().is_valid(False)
    True
    >>> Null().is_valid(1)
    False
    """

    def check_value(self, value):
        if not isinstance(value, bool):
            self._failure("value should be True or False", value=value, code=codes.IS_NOT_BOOL)

    def __repr__(self):
        return "<Bool>"


class ToBool(Trafaret):
    """
    >>> extract_error(ToBool(), 'aloha')
    "value can't be converted to Bool"
    >>> ToBool().check(1)
    True
    >>> ToBool().check(0)
    False
    >>> ToBool().check('y')
    True
    >>> ToBool().check('n')
    False
    >>> ToBool().check(None)
    False
    >>> ToBool().check('1')
    True
    >>> ToBool().check('0')
    False
    >>> ToBool().check('YeS')
    True
    >>> ToBool().check('No')
    False
    >>> ToBool().check(True)
    True
    >>> ToBool().check(False)
    False
    """

    true_values = ('t', 'true', 'y', 'yes', 'on', '1', '1.0')
    false_values = ('false', 'n', 'no', 'off', '0', 'none', '0.0')
    convertable = true_values + false_values

    def check_and_return(self, value):
        _value = str(value).strip().lower()
        if _value not in self.convertable:
            self._failure(
                'value can\'t be converted to Bool',
                value=value,
                code=codes.IS_NOT_CONVERTIBLE_TO_BOOL,
            )
        return _value in ('t', 'true', 'y', 'yes', 'on', '1', '1.0')

    def __repr__(self):
        return "<ToBool>"


class Atom(Trafaret):
    """
    >>> Atom('atom').check('atom')
    'atom'
    >>> extract_error(Atom('atom'), 'molecule')
    "value is not exactly 'atom'"
    >>> Atom('atom').is_valid('atom')
    True
    >>> Atom('atom').is_valid('molecule')
    False
    """
    __slots__ = ['value']

    def __init__(self, value):
        self.value = value

    def check_value(self, value):
        if self.value != value:
            self._failure(
                "value is not exactly '%s'" % self.value,
                value=value,
                code=codes.IS_NOT_EXACTLY,
            )


class String(Trafaret):
    """
    >>> String()
    <String>
    >>> String(allow_blank=True)
    <String(blank)>
    >>> String().check("foo")
    'foo'
    >>> extract_error(String(), "")
    'blank value is not allowed'
    >>> String(allow_blank=True).check("")
    ''
    >>> extract_error(String(), 1)
    'value is not a string'
    >>> String(min_length=2, max_length=3).check('123')
    '123'
    >>> extract_error(String(min_length=2, max_length=6), '1')
    'String is shorter than 2 characters'
    >>> extract_error(String(min_length=2, max_length=6), '1234567')
    'String is longer than 6 characters'
    >>> String(min_length=2, max_length=6, allow_blank=True)
    Traceback (most recent call last):
    ...
    AssertionError: Either allow_blank or min_length should be specified, not both
    >>> String(min_length=0, max_length=6, allow_blank=True).check('123')
    '123'
    >>> String().is_valid("foo")
    True
    >>> String().is_valid("")
    False
    >>> String().is_valid(1)
    False
    """
    str_type = STR_TYPE

    TYPE_ERROR_MESSAGE = "value is not a string"
    TYPE_ERROR_CODE = codes.IS_NOT_A_STRING

    def __init__(self, allow_blank=False, min_length=None, max_length=None):
        assert not (allow_blank and min_length), \
            "Either allow_blank or min_length should be specified, not both"
        self.allow_blank = allow_blank
        self.min_length = min_length
        self.max_length = max_length

    def check_and_return(self, value):
        if not isinstance(value, self.str_type):
            self._failure(self.TYPE_ERROR_MESSAGE, value=value, code=self.TYPE_ERROR_CODE)
        if not self.allow_blank and len(value) == 0:
            self._failure("blank value is not allowed", value=value, code=codes.EMPTY_STRING)
        elif self.allow_blank and len(value) == 0:
            return value
        if self.min_length is not None and len(value) < self.min_length:
            self._failure(
                'String is shorter than %s characters' % self.min_length,
                value=value,
                code=codes.SHORT_STRING,
            )
        if self.max_length is not None and len(value) > self.max_length:
            self._failure(
                'String is longer than %s characters' % self.max_length,
                value=value,
                code=codes.LONG_STRING,
            )
        return value

    def __repr__(self):
        return "<String(blank)>" if self.allow_blank else "<String>"


class Date(Trafaret):
    """
    Checks that value is a `datetime.date` & `datetime.datetime` instances or a string
    that is convertable to `datetime.date` object.

    >>> Date()
    <Date %Y-%m-%d>
    >>> Date('%y-%m-%d')
    <Date %y-%m-%d>
    >>> Date().check(date.today())
    datetime.date(2019, 7, 25)
    >>> Date().check(datetime.now())
    datetime.datetime(2019, 10, 6, 14, 42, 52, 431348)
    >>> Date().check("2019-07-25")
    '2019, 7, 25'
    >>> Date(format='%y-%m-%d').check('00-01-01')
    '00-01-01'
    >>> extract_error(Date(), "25-07-2019")
    'value does not match format %Y-%m-%d'
    >>> extract_error(Date(), 1564077758)
    'value cannot be converted to date'
    >>> Date().is_valid(date.today())
    True
    >>> Date().is_valid(1564077758)
    False
    """

    def __init__(self, format='%Y-%m-%d'):
        self._format = format

    def _check(self, value):
        if isinstance(value, datetime):
            return value.date()
        elif isinstance(value, date):
            return value

        try:
            extracted_date = datetime.strptime(value, self._format).date()
        except ValueError:
            self._failure(
                'value does not match format %s' % self._format,
                value=value,
                code=codes.DOES_NOT_MATCH_FORMAT
            )
        except TypeError:
            self._failure(
                'value cannot be converted to date',
                value=value,
                code=codes.IS_NOT_CONVERTIBLE_TO_DATE
            )
        else:
            return extracted_date

    def check_and_return(self, value):
        self._check(value)
        return value

    def __repr__(self):
        return '<Date {}>'.format(self._format)


class ToDate(Date):
    """
    Returns instance of `datetime.date` object if value is a string or `datetime.date` & `datetime.datetime` instances.

    >>> ToDate().check(datetime.now())
    datetime.date(2019, 10, 6)
    >>> ToDate().check("2019-07-25")
    datetime.date(2019, 7, 25)
    >>> ToDate(format='%y-%m-%d').check('00-01-01')
    datetime.date(2000, 1, 1)
    """

    def check_and_return(self, data):
        return self._check(data)

    def __repr__(self):
        return '<ToDate {}>'.format(self._format)


class DateTime(Trafaret):
    """
    Checks that value is a `datetime.datetime` instance or a string that is convertable to `datetime.datetime` object.

    >>> DateTime()
    <DateTime %Y-%m-%d %H:%M:%S>
    >>> DateTime('%Y-%m-%d %H:%M')
    <DateTime %Y-%m-%d %H:%M>
    >>> DateTime().check(datetime.now())
    datetime.datetime(2019, 7, 25, 21, 45, 37, 319284)
    >>> DateTime('%Y-%m-%d %H:%M').check("2019-07-25 21:45")
    '2019-07-25 21:45'
    >>> extract_error(DateTime(), "2019-07-25")
    'value does not match format %Y-%m-%d %H:%M:%S'
    >>> extract_error(DateTime(), date.today())
    'value cannot be converted to datetime'
    >>> DateTime('%Y-%m-%d %H:%M').is_valid("2019-07-25 21:45")
    True
    >>> extract_error(DateTime(), "2019-07-25")
    False

    """

    def __init__(self, format='%Y-%m-%d %H:%M:%S'):
        self._format = format

    def _check(self, value):
        if isinstance(value, datetime):
            return value

        try:
            extracted_datetime = datetime.strptime(value, self._format)
        except ValueError:
            self._failure(
                'value does not match format %s' % self._format,
                value=value,
                code=codes.DOES_NOT_MATCH_FORMAT
            )
        except TypeError:
            self._failure(
                'value cannot be converted to datetime',
                value=value,
                code=codes.IS_NOT_CONVERTIBLE_TO_DATETIME
            )
        else:
            return extracted_datetime

    def check_and_return(self, value):
        self._check(value)
        return value

    def __repr__(self):
        return '<DateTime {}>'.format(self._format)


class ToDateTime(DateTime):
    """
    Returns instance of `datetime.datetime` object if value is a string or `datetime.datetime` instance.

    >>> DateTime('%Y-%m-%d %H:%M').check("2019-07-25 21:45")
    datetime.datetime(2019, 7, 25, 21, 45)
    """

    def check_and_return(self, value):
        return self._check(value)

    def __repr__(self):
        return '<ToDateTime {}>'.format(self._format)


class Bytes(String):
    str_type = (BYTES_TYPE,)

    TYPE_ERROR_MESSAGE = "value is not a bytes string"
    TYPE_ERROR_CODE = codes.IS_NOT_A_BYTES_STRING


class ToBytes(Trafaret):
    """Get str and try to encode it with given encoding, utf-8 by default."""
    def __init__(self, encoding='utf-8'):
        self.encoding = encoding

    def check_and_return(self, value):
        if isinstance(value, BYTES_TYPE):
            return value
        elif isinstance(value, STR_TYPE):
            try:
                return value.encode(self.encoding)
            except UnicodeError:
                raise self._failure(
                    'value cannot be encoded with %s encoding' % self.encoding,
                    value=value,
                    code=codes.CANNOT_BE_ENCODED,
                )
        else:
            self._failure(
                'value is not str/bytes type',
                value=value,
                code=codes.IS_NOT_A_STRING,
            )

    def __repr__(self):
        return '<ToBytes>'


class AnyString(String):
    str_type = (BYTES_TYPE, STR_TYPE)


class FromBytes(Trafaret):
    """ Get bytes and try to decode it with given encoding, utf-8 by default.
    It can be used like ``unicode_or_koi8r = String | FromBytes(encoding='koi8r')``
    """
    def __init__(self, encoding='utf-8'):
        self.encoding = encoding

    def check_and_return(self, value):
        if not isinstance(value, BYTES_TYPE):
            self._failure(
                'value is not a bytes',
                value=value,
                code=codes.IS_NOT_BYTES,
            )
        try:
            return value.decode(self.encoding)
        except UnicodeError:
            raise self._failure(
                'value cannot be decoded with %s encoding' % self.encoding,
                value=value,
                code=codes.CANNOT_BE_DECODED,
            )

    def __repr__(self):
        return "<FromBytes>"


class SquareBracketsMeta(TrafaretMeta):
    """
    Allows usage of square brackets for List initialization

    >>> List[Int]
    <List(<Int>)>
    >>> List[Int, 1:]
    <List(min_length=1 | <Int>)>
    >>> List[:10, Int]
    <List(max_length=10 | <Int>)>
    >>> List[1:10]
    Traceback (most recent call last):
    ...
    RuntimeError: Trafaret is required for List initialization
    """

    def __getitem__(self, args):
        slice_ = None
        trafaret = None
        if not isinstance(args, tuple):
            args = (args, )
        for arg in args:
            if isinstance(arg, slice):
                slice_ = arg
            elif (
                isinstance(arg, Trafaret)
                or issubclass(arg, Trafaret)
                or isinstance(arg, type)
            ):
                trafaret = arg
        if not trafaret:
            raise RuntimeError("Trafaret is required for List initialization")
        if slice_:
            return self(
                trafaret,
                min_length=slice_.start or 0,
                max_length=slice_.stop,
            )
        return self(trafaret)


@py3metafix
class Iterable(Trafaret, ListAsyncMixin):
    """
    >>> List(Int)
    <List(<Int>)>
    >>> List(Int, min_length=1)
    <List(min_length=1 | <Int>)>
    >>> List(Int, min_length=1, max_length=10)
    <List(min_length=1, max_length=10 | <Int>)>
    >>> extract_error(List(Int), 1)
    'value is not a list'
    >>> List(Int).check([1, 2, 3])
    [1, 2, 3]
    >>> List(String).check(["foo", "bar", "spam"])
    ['foo', 'bar', 'spam']
    >>> extract_error(List(Int), [1, 2, 1 + 3j])
    {2: 'value is not int'}
    >>> List(Int, min_length=1).check([1, 2, 3])
    [1, 2, 3]
    >>> extract_error(List(Int, min_length=1), [])
    'list length is less than 1'
    >>> List(Int, max_length=2).check([1, 2])
    [1, 2]
    >>> extract_error(List(Int, max_length=2), [1, 2, 3])
    'list length is greater than 2'
    >>> extract_error(List(Int), ["a"])
    {0: "value can't be converted to int"}
    >>> List(Int).is_valid([1, 2, 3])
    True
    >>> List(Int).is_valid(1)
    False
    """

    __metaclass__ = SquareBracketsMeta
    __slots__ = ['trafaret', 'min_length', 'max_length']

    def __init__(self, trafaret, min_length=0, max_length=None):
        self.trafaret = ensure_trafaret(trafaret)
        self.min_length = min_length
        self.max_length = max_length

    def check_common(self, value):
        if not isinstance(value, AbcIterable):
            self._failure(
                "value is not iterable",
                value=value,
                code=codes.IS_NOT_A_LIST,
            )
        if len(value) < self.min_length:
            self._failure(
                "list length is less than %s" % self.min_length,
                value=value,
                code=codes.TOO_SHORT,
            )
        if self.max_length is not None and len(value) > self.max_length:
            self._failure(
                "list length is greater than %s" % self.max_length,
                value=value,
                code=codes.TOO_LONG,
            )

    def transform(self, value, context=None):
        self.check_common(value)
        lst = []
        errors = {}
        for index, item in enumerate(value):
            try:
                lst.append(self.trafaret(item, context=context))
            except DataError as err:
                errors[index] = err
        if errors:
            raise self._failure(errors, code=codes.SOME_ELEMENTS_DID_NOT_MATCH)
        return lst

    def __repr__(self):
        r = "<List("
        options = []
        if self.min_length:
            options.append("min_length=%s" % self.min_length)
        if self.max_length:
            options.append("max_length=%s" % self.max_length)
        r += ", ".join(options)
        if options:
            r += " | "
        r += repr(self.trafaret)
        r += ")>"
        return r


class List(Iterable):
    def check_common(self, value):
        if not isinstance(value, list):
            self._failure(
                "value is not a list",
                value=value,
                code=codes.IS_NOT_A_LIST,
            )
        super(List, self).check_common(value)


class Tuple(Trafaret, TupleAsyncMixin):
    """
    Tuple checker can be used to check fixed tuples, like (Int, Int, String).

    >>> t = Tuple(Int, Int, String)
    >>> t.check([3, 4, '5'])
    (3, 4, '5')
    >>> extract_error(t, [3, 4, 5])
    {2: 'value is not a string'}
    >>> t
    <Tuple(<Int>, <Int>, <String>)>
    """
    __slots__ = ['trafarets', 'length']

    def __init__(self, *args):
        self.trafarets = [ensure_trafaret(t) for t in args]
        self.length = len(self.trafarets)

    def check_common(self, value):
        try:
            value = tuple(value)
        except TypeError:
            self._failure(
                'value must be convertable to tuple',
                value=value,
                code=codes.TUPLE_LIKE,
            )
        if len(value) != self.length:
            self._failure(
                'value must contain %s items' % self.length,
                value=value,
                code=codes.LOT_ELEMENTS,
            )

    def transform(self, value, context=None):
        self.check_common(value)
        result = []
        errors = {}
        for idx, (item, trafaret) in enumerate(zip(value, self.trafarets)):
            try:
                result.append(trafaret(item, context=context))
            except DataError as err:
                errors[idx] = err
        if errors:
            self._failure(errors, value=value, code=codes.SOME_ELEMENTS_DID_NOT_MATCH)
        return tuple(result)

    def __repr__(self):
        return '<Tuple(' + ', '.join(repr(t) for t in self.trafarets) + ')>'


class Key(KeyAsyncMixin):
    """
    Helper class for Dict.

    It gets ``name``, and provides method ``extract(data)`` that extract key value
    from data through mapping ``get`` method.
    Key `__call__` method yields ``(key name, result or DataError, [touched keys])`` triples.

    You can redefine ``get_data(data, default)`` method in subclassed ``Key`` if you want to use something other
    then ``.get(...)`` method.

    Like this for the aiohttp MultiDict::

        class MDKey(t.Key):
            def get_data(self, data, default):
                return data.getall(self.name, default)
    """
    __slots__ = ['name', 'to_name', 'default', 'optional', 'trafaret']

    def __init__(self, name, default=_empty, optional=False, to_name=None, trafaret=None):
        self.name = name
        self.to_name = to_name
        self.default = default
        self.optional = optional
        self.trafaret = ensure_trafaret(trafaret) if trafaret else Any()

    def __call__(self, data, context=None):
        if self.name in data or self.default is not _empty:
            if callable(self.default):
                default = self.default()
            else:
                default = self.default
            error = None
            try:
                result = self.trafaret(self.get_data(data, default), context=context)
            except DataError as de:
                error = de
            if error:
                yield self.name, error, (self.name,)
            else:
                yield self.get_name(), result, (self.name,)
            return

        if not self.optional:
            yield self.name, DataError(error='is required', code=codes.REQUIRED), (self.name,)

    def get_data(self, data, default):
        return data.get(self.name, default)

    def set_trafaret(self, trafaret):
        self.trafaret = ensure_trafaret(trafaret)
        return self

    def __rshift__(self, name):
        self.to_name = name
        return self

    def get_name(self):
        return self.to_name or self.name

    def __repr__(self):
        return '<%s "%s"%s %s>' % (
            self.__class__.__name__,
            self.name,
            ' to "%s"' % self.to_name if getattr(self, 'to_name', False) else '',
            self.trafaret,
        )


class Dict(Trafaret, DictAsyncMixin):
    """ Dict is a most complex trafaret that going with this library.
    The main difference from other common validation libs is that `Dict`
    trafaret does not know anything about actual keys. Every key that Dict works
    with knows itself how to get value from given mapping and if this will be one value
    or two or multi values.

    So `Dict` cannot make any assumptions about whats going on other then contract with
    a `key` implementation. And we need to note, that any callable can be `key`, not
    only `Key` subclasses.
    So we need to look at the `key` contract:

    .. code-block:: python

        key_instance(data: Mapping) -> Sequence[
            name_to_store,
            result or DataError,
            Sequence[touched keys],
        ]

    It is a bit complex, so let me explain it to you. Every key instance get this data that
    `Dict` trying to check. Then `key` will return one or multiple results and it is
    common for `key` to be generator.

    Every result is three component tuple
        1. Key for the result dict. For standard `Key` it is result key in case of successful check
        or original key name if there was an error
        2. Result if keys trafaret check was successful or DataError instance otherwise
        3. An iterable with all keys of original mapping that this `key` touched

    With this tricky interface `key` in our lib can do anything you can imagine. Like work
    with MultiDicts, compare keys, get subdicts and check them independently from main one.

    Why we need this third extra iterable with touched names? Because our Dict can check that
    all keys were consumed and what to do with extras.

    Arguments:

    * Dict accepts keys as `*args`
    * if first argument to Dict is a `dict` then its keys will be merged with args keys and
      this `dict` values must be trafarets. If key of this `dict` is a str, then Dict will create
      `Key` instance with this key as Key name and value as its trafaret. If `key` is a `Key` instance
      then Dict will call this key `set_trafaret` method.

    `allow_extra` argument can be a list of keys, or `'*'` for any, that will be checked against
    `allow_extra_trafaret` or `Any`.

    `ignore_extra` argument can be a list of keys, or `'*'` for any, that will be ignored.
    """
    __slots__ = ['extras', 'extras_trafaret', 'allow_any', 'ignore', 'ignore_any', 'keys', '_keys']

    def __init__(self, *args, **trafarets):
        if args and isinstance(args[0], AbcMapping):
            keys = args[0]
            args = args[1:]
        else:
            keys = {}
        if any(not callable(key) for key in args):
            raise RuntimeError('Keys in single attributes must be callables')

        # extra
        allow_extra = trafarets.pop('allow_extra', [])
        allow_extra_trafaret = trafarets.pop('allow_extra_trafaret', Any)
        self.extras_trafaret = ensure_trafaret(allow_extra_trafaret)
        self.allow_any = '*' in allow_extra
        self.extras = [name for name in allow_extra if name != '*']
        # ignore
        ignore_extra = trafarets.pop('ignore_extra', [])
        self.ignore_any = '*' in ignore_extra
        self.ignore = [name for name in ignore_extra if name != '*']

        self.keys = list(args)
        for key, trafaret in itertools.chain(trafarets.items(), keys.items()):
            key_ = Key(key) if isinstance(key, STR_TYPES) else key
            if not callable(key_) and not hasattr(key_, 'async_call'):
                raise RuntimeError('Non callable Keys are not supported')
            key_.set_trafaret(ensure_trafaret(trafaret))
            self.keys.append(key_)
        # optimized version without runtime check for context arg
        self._keys = []
        for key in self.keys:
            self._keys.append(with_context_caller(key))

    def _clone_args(self):
        """ return args to create new Dict clone
        """
        keys = list(self.keys)
        kw = {}
        if self.allow_any or self.extras:
            kw['allow_extra'] = list(self.extras)
            if self.allow_any:
                kw['allow_extra'].append('*')
            kw['allow_extra_trafaret'] = self.extras_trafaret
        if self.ignore_any or self.ignore:
            kw['ignore_extra'] = list(self.ignore)
            if self.ignore_any:
                kw['ignore_extra'].append('*')
        return keys, kw

    def allow_extra(self, *names, **kw):
        """ multi arguments that represents attribute names or `*`.
        Will allow unconsumed by other keys attributes for given names
        or all if includes `*`.
        Also you can pass `trafaret` keyword argument to set `Trafaret`
        instance for this extra args, or it will be `Any`.
        Method creates `Dict` clone.
        """
        keys, dictkw = self._clone_args()
        allow_extra = dictkw.setdefault('allow_extra', [])
        allow_extra.extend(names)
        if 'trafaret' in kw:
            dictkw['allow_extra_trafaret'] = kw['trafaret']
        return self.__class__(*keys, **dictkw)

    def ignore_extra(self, *names):
        """ multi arguments that represents attribute names or `*`.
        Will ignore unconsumed by other keys attribute names for given names
        or all if includes `*`.
        Method creates `Dict` clone.
        """
        keys, kw = self._clone_args()
        ignore_extra = kw.setdefault('ignore_extra', [])
        ignore_extra.extend(names)
        return self.__class__(*keys, **kw)

    def transform(self, value, context=None):
        if not isinstance(value, AbcMapping):
            self._failure(
                "value is not a dict",
                value=value,
                code=codes.IS_NOT_A_DICT,
            )
        collect = {}
        errors = {}
        touched_names = []
        for key in self._keys:
            for k, v, names in key(value, context=context):
                if isinstance(v, DataError):
                    errors[k] = v
                else:
                    collect[k] = v
                touched_names.extend(names)

        if not self.ignore_any:
            for key in value:
                if key in touched_names:
                    continue
                if key in self.ignore:
                    continue
                if not self.allow_any and key not in self.extras:
                    if key in collect:
                        errors[key] = DataError(
                            "%s key was shadowed" % key,
                            code=codes.SHADOWED,
                        )
                    else:
                        errors[key] = DataError(
                            "%s is not allowed key" % key,
                            code=codes.NOT_ALLOWED,
                        )
                elif key in collect:
                    errors[key] = DataError(
                        "%s key was shadowed" % key,
                        code=codes.SHADOWED,
                    )
                else:
                    try:
                        collect[key] = self.extras_trafaret(value[key])
                    except DataError as de:
                        errors[key] = de
        if errors:
            self._failure(error=errors, code=codes.SOME_ELEMENTS_DID_NOT_MATCH)
        return collect

    def __repr__(self):
        r = "<Dict("
        options = []
        if self.allow_any:
            options.append("any")
        if self.ignore:
            options.append("ignore=(%s)" % (", ".join(self.ignore)))
        if self.extras:
            options.append("extras=(%s)" % (", ".join(self.extras)))
        r += ", ".join(options)
        if options:
            r += " | "
        options = []
        for key in sorted(self.keys, key=lambda k: repr(k)):
            options.append(repr(key))
        r += ", ".join(options)
        r += ")>"
        return r

    def merge(self, other):
        """
        Extends one Dict with other Dict Key`s or Key`s list,
        or dict instance supposed for Dict
        """
        extra = self.extras
        if isinstance(other, Dict):
            other_keys = other.keys
            extra += other.extras
            ignore = '*' if (self.ignore_any or other.ignore_any) else self.ignore + other.ignore
        elif isinstance(other, (list, tuple)):
            other_keys = list(other)
            ignore = self.ignore
        elif isinstance(other, dict):
            return self.__class__(other, *self.keys)
        else:
            raise TypeError('You must merge Dict only with Dict'
                            ' or list of Keys')
        return self.__class__(*(self.keys + other_keys), ignore_extra=ignore,
                              allow_extra=extra)

    __add__ = merge


def DictKeys(keys):
    """
    Checks if dict has all given keys

    :param keys:
    :type keys:
    """
    req = [(Key(key), Any) for key in keys]
    return Dict(dict(req))


class Mapping(Trafaret, MappingAsyncMixin):
    """
    Mapping gets two trafarets as arguments, one for key and one for value,
    like `Mapping(t.Int, t.List(t.Str))`.
    """
    __slots__ = ['key', 'value']

    def __init__(self, key, value):
        self.key = ensure_trafaret(key)
        self.value = ensure_trafaret(value)

    def transform(self, mapping, context=None):
        if not isinstance(mapping, AbcMapping):
            self._failure(
                "value is not a dict",
                value=mapping,
                code=codes.IS_NOT_A_DICT,
            )
        checked_mapping = {}
        errors = {}
        for key, value in mapping.items():
            pair_errors = {}
            try:
                checked_key = self.key(key, context=context)
            except DataError as err:
                pair_errors['key'] = err
            try:
                checked_value = self.value(value, context=context)
            except DataError as err:
                pair_errors['value'] = err
            if pair_errors:
                errors[key] = DataError(error=pair_errors, code=codes.PAIR_MEMBERS_DID_NOT_MATCH)
            else:
                checked_mapping[checked_key] = checked_value
        if errors:
            self._failure(errors, code=codes.SOME_ELEMENTS_DID_NOT_MATCH)
        return checked_mapping

    def __repr__(self):
        return "<Mapping(%r => %r)>" % (self.key, self.value)


class Enum(Trafaret):
    """
    >>> trafaret = Enum("foo", "bar", 1) >> ignore
    >>> trafaret
    <Enum('foo', 'bar', 1)>
    >>> trafaret.check("foo")
    >>> trafaret.check(1)
    >>> extract_error(trafaret, 2)
    "value doesn't match any variant"
    >>> trafaret.is_valid(1)
    True
    >>> trafaret.is_valid(2)
    False
    """
    __slots__ = ['variants']

    def __init__(self, *variants):
        self.variants = variants[:]

    def check_value(self, value):
        if value not in self.variants:
            self._failure(
                "value doesn't match any variant",
                value=value,
                code=codes.DOES_NOT_MATCH_ANY,
            )

    def __repr__(self):
        return "<Enum(%s)>" % (", ".join(repr(v) for v in self.variants))


class Callable(Trafaret):
    """
    >>> (Callable() >> ignore).check(lambda: 1)
    >>> extract_error(Callable(), 1)
    'value is not callable'
    >>> (Callable() >> ignore).is_valid(lambda: 1)
    True
    >>> Callable().is_valid(1)
    False
    """

    def check_value(self, value):
        if not callable(value):
            self._failure(
                "value is not callable",
                value=value,
                code=codes.IS_NOT_CALLABLE,
            )

    def __repr__(self):
        return "<Callable>"


class Call(Trafaret, CallAsyncMixin):
    """
    >>> def validator(value):
    ...     if value != "foo":
    ...         return DataError("I want only foo!")
    ...     return 'foo'
    ...
    >>> trafaret = Call(validator)
    >>> trafaret
    <Call(validator)>
    >>> trafaret.check("foo")
    'foo'
    >>> extract_error(trafaret, "bar")
    'I want only foo!'
    """
    __slots__ = ['fn']

    def __init__(self, fn):
        if not callable(fn):
            raise RuntimeError("Call argument should be callable")
        args = set(get_callable_args(fn))
        self.supports_context = 'context' in args
        self.fn = fn

    def transform(self, value, context=None):
        if self.supports_context:
            res = self.fn(value, context=context)
        else:
            res = self.fn(value)
        if isinstance(res, DataError):
            raise res
        else:
            return res

    def __repr__(self):
        return "<Call(%s)>" % self.fn.__name__


class Forward(Trafaret, ForwardAsyncMixin):
    """
    >>> node = Forward()
    >>> node << Dict(name=String, children=List[node])
    >>> node
    <Forward(<Dict(children=<List(<recur>)>, name=<String>)>)>
    >>> node.check({"name": "foo", "children": []}) == {'children': [], 'name': 'foo'}
    True
    >>> extract_error(node, {"name": "foo", "children": [1]})
    {'children': {0: 'value is not a dict'}}
    >>> node.check({"name": "foo", "children": [ \
                        {"name": "bar", "children": []} \
                     ]}) == {'children': [{'children': [], 'name': 'bar'}], 'name': 'foo'}
    True
    >>> empty_node = Forward()
    >>> empty_node
    <Forward(None)>
    >>> extract_error(empty_node, 'something')
    'trafaret not set yet'
    """

    def __init__(self):
        self.trafaret = None
        self._recur_repr = False

    def __lshift__(self, trafaret):
        self.provide(trafaret)

    def provide(self, trafaret):
        if self.trafaret:
            raise RuntimeError("trafaret for Forward is already specified")
        self.trafaret = ensure_trafaret(trafaret)

    def transform(self, value, context=None):
        if self.trafaret is None:
            self._failure(
                'trafaret not set yet',
                value=value,
                code=codes.TRAFARET_IS_NOT_SET,
            )
        return self.trafaret(value, context=context)

    def __repr__(self):
        # XXX not threadsafe
        if self._recur_repr:
            return "<recur>"
        self._recur_repr = True
        r = "<Forward(%r)>" % self.trafaret
        self._recur_repr = False
        return r


class GuardError(DataError):
    """
    Raised when guarded function gets invalid arguments,
    inherits error message from corresponding DataError
    """

    pass


def guard(trafaret=None, **kwargs):
    """
    Decorator for protecting function with trafarets

    >>> @guard(a=String, b=Int, c=String)
    ... def fn(a, b, c="default"):
    ...     '''docstring'''
    ...     return (a, b, c)
    ...
    >>> fn.__module__ = None
    >>> help(fn)
    Help on function fn:
    <BLANKLINE>
    fn(*args, **kwargs)
        guarded with <Dict(a=<String>, b=<Int>, c=<String>)>
    <BLANKLINE>
        docstring
    <BLANKLINE>
    >>> fn("foo", 1)
    ('foo', 1, 'default')
    >>> extract_error(fn, "foo", 1, 2)
    {'c': 'value is not a string'}
    >>> extract_error(fn, "foo")
    {'b': 'is required'}
    >>> g = guard(Dict())
    >>> c = Forward()
    >>> c << Dict(name=str, children=List[c])
    >>> g = guard(c)
    >>> g = guard(Int())
    Traceback (most recent call last):
    ...
    RuntimeError: trafaret should be instance of Dict or Forward
    """
    if (
        trafaret
        and not isinstance(trafaret, Dict)
        and not isinstance(trafaret, Forward)
    ):
        raise RuntimeError("trafaret should be instance of Dict or Forward")
    elif trafaret and kwargs:
        raise RuntimeError("choose one way of initialization,"
                           " trafaret or kwargs")
    if not trafaret:
        trafaret = Dict(**kwargs)

    def wrapper(fn):
        argspec = getargspec(fn)

        @functools.wraps(fn)
        def decor(*args, **kwargs):
            fnargs = argspec.args
            if fnargs and fnargs[0] in ['self', 'cls']:
                obj = args[0]
                fnargs = fnargs[1:]
                checkargs = args[1:]
            else:
                obj = None
                checkargs = args

            defaults = zip(reversed(fnargs), reversed(argspec.defaults or ()))
            kw_only_defaults = (argspec.kwonlydefaults or {}).items()

            try:
                call_args = dict(
                    itertools.chain(defaults, kw_only_defaults, zip(fnargs, checkargs), kwargs.items())
                )
                converted = trafaret(call_args)
            except DataError as err:
                raise GuardError(error=err.error)
            return fn(obj, **converted) if obj else fn(**converted)
        decor.__doc__ = "guarded with %r\n\n" % trafaret + (decor.__doc__ or "")
        return decor
    return wrapper


def ignore(val):
    """
    Stub to ignore value from trafaret
    Use it like:

    >>> a = Int >> ignore
    >>> a.check(7)
    """
    pass


def catch(checker, *a, **kw):
    """
    Helper for tests - catch error and return it as dict
    """
    try:
        return checker(*a, **kw)
    except DataError as error:
        return error


catch_error = catch


def extract_error(checker, *a, **kw):
    """
    Helper for tests - catch error and return it as dict
    """

    res = catch_error(checker, *a, **kw)
    if isinstance(res, DataError):
        return res.as_dict()
    return res
