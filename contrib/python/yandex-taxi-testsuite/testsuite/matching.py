import collections.abc
import datetime
import itertools
import operator
import re
import typing

import dateutil.parser


class BaseError(Exception):
    pass


class NoValueCapturedError(BaseError):
    pass


_Sentinel = object()


class Any:
    """Matches any value."""

    def __repr__(self):
        return '<Any>'

    def __eq__(self, other):
        return True


class AnyString:
    """Matches any string."""

    __testsuite_types__ = (str,)

    def __repr__(self):
        return '<AnyString>'

    def __eq__(self, other):
        if isinstance(other, str):
            return True
        return any(issubclass(type, str) for type in _resolve_types(other))


class RegexString:
    """Match string with regular expression.

    .. code-block:: python

       assert response.json() == {
          'order_id': matching.RegexString('^[0-9a-f]*$'),
          ...
       }
    """

    __testsuite_types__ = (str,)

    def __init__(self, pattern):
        self._pattern = re.compile(pattern)

    def __repr__(self):
        return f'<{self.__class__.__name__} pattern={self._pattern!r}>'

    def __eq__(self, other):
        if isinstance(other, str):
            return self._pattern.match(other) is not None
        if isinstance(other, RegexString):
            return other._pattern == self._pattern
        return False


class UuidString(RegexString):
    """Matches lower-case hexadecimal uuid string."""

    def __init__(self):
        super().__init__('^[0-9a-f]{32}$')


class ObjectIdString(RegexString):
    """Matches lower-case hexadecimal objectid string."""

    def __init__(self):
        super().__init__('^[0-9a-f]{24}$')


class DatetimeString:
    """Matches datetime string in any format."""

    __testsuite_types__ = (str,)

    def __repr__(self):
        return '<DatetimeString>'

    def __eq__(self, other):
        if isinstance(other, str):
            try:
                dateutil.parser.parse(other)
                return True
            except ValueError:
                return False
        return isinstance(other, DatetimeString)


class IsInstance:
    """Match value by its type.

    Use this class when you only need to check value type.

    .. code-block:: python

       assert response.json() == {
          # order_id must be a string
          'order_id': matching.IsInstance(str),
          # int or float is acceptable here
          'weight': matching.IsInstance([int, float]),
          ...
       }
    """

    def __init__(self, types):
        self._types = types

    def __repr__(self):
        if isinstance(self._types, (list, tuple)):
            type_names = [t.__name__ for t in self._types]
        else:
            type_names = [self._types.__name__]
        return f'<IsInstance {", ".join(type_names)}>'

    def __eq__(self, other):
        if isinstance(other, self._types):
            return True
        if isinstance(other, IsInstance):
            return self._types == other._types
        return False


class And:
    """Logical AND on conditions.

    .. code-block:: python

       # match integer is in range [10, 100)
       assert num == matching.And([matching.Ge(10), matching.Lt(100)])
    """

    def __init__(self, *conditions):
        self._conditions = conditions

    def __repr__(self):
        conditions = [repr(cond) for cond in self._conditions]
        return f'<And {", ".join(conditions)}>'

    def __eq__(self, other):
        if isinstance(other, And):
            return self._conditions == other._conditions
        for condition in self._conditions:
            if condition != other:
                return False
        return True

    def __testsuite_visit__(self, visit):
        return And(*[visit(condition) for condition in self._conditions])


class Or:
    """Logical OR on conditions.

    .. code-block:: python

       # match integers abs(num) >= 10
       assert num == matching.Or([matching.Ge(10), matching.Le(-10)])
    """

    def __init__(self, *conditions):
        self._conditions = conditions

    def __repr__(self):
        conditions = [repr(cond) for cond in self._conditions]
        return f'<Or {", ".join(conditions)}>'

    def __eq__(self, other):
        if isinstance(other, Or):
            return self._conditions == other._conditions
        for condition in self._conditions:
            if condition == other:
                return True
        return False

    def __testsuite_visit__(self, visit):
        return Or(*[visit(condition) for condition in self._conditions])


class Not:
    """Condition inversion.

    Example:

    .. code-block:: python

       # check value is not 1
       assert value == matching.Not(1)
    """

    def __init__(self, condition):
        self._condition = condition

    def __repr__(self):
        return f'<Not {self._condition!r}>'

    def __eq__(self, other):
        if isinstance(other, Not):
            return self._condition == other._condition
        return self._condition != other

    def __testsuite_visit__(self, visit):
        return Not(visit(self._condition))


class Comparator:
    op: typing.Callable[[typing.Any, typing.Any], bool] = operator.eq

    def __init__(self, value):
        self._value = value

    def __repr__(self):
        return f'<{self.op.__name__} {self._value}>'

    def __eq__(self, other):
        if isinstance(other, Comparator):
            return self.op == other.op and self._value == other._value
        try:
            return self.op(other, self._value)
        except TypeError:
            return False

    def __testsuite_visit__(self, visit):
        return self.__class__(visit(self._value))


class Gt(Comparator):
    """Value is greater than.

    Example:

    .. code-block:: python

       # Value must be > 10
       assert value == matching.Gt(10)
    """

    op = operator.gt


class Ge(Comparator):
    """Value is greater or equal.

    Example:

    .. code-block:: python

       # Value must be >= 10
       assert value == matching.Ge(10)
    """

    op = operator.ge


class Lt(Comparator):
    """Value is less than.

    Example:

    .. code-block:: python

       # Value must be < 10
       assert value == matching.Lt(10)
    """

    op = operator.lt


class Le(Comparator):
    """Value is less or equal.

    Example:

    .. code-block:: python

       # Value must be <= 10
       assert value == matching.Le(10)
    """

    op = operator.le


class PartialDict(collections.abc.Mapping):
    """Partial dictionary matching.

    It might be useful to only check specific keys of a dictionary.
    :py:class:`PartialDict` serves to solve this task.

    :py:class:`PartialDict` is wrapper around regular `dict()` when instantiated
    all arguments are passed as is to internal dict object.

    Example:

    .. code-block:: python

       assert {'foo': 1, 'bar': 2} == matching.PartialDict({
           # Only check for foo >= 1 ignoring other keys
           'foo': matching.Ge(1),
       })
    """

    __testsuite_types__ = (dict,)

    def __init__(self, *args, **kwargs):
        self._dict = dict(*args, **kwargs)

    def __contains__(self, item):
        return True

    def __getitem__(self, item):
        return self._dict.get(item, any_value)

    def __iter__(self):
        return iter(self._dict)

    def __len__(self):
        return len(self._dict)

    def __repr__(self):
        return f'<PartialDict {self._dict!r}>'

    def __eq__(self, other):
        if not isinstance(other, collections.abc.Mapping):
            return False

        for key in self:
            if other.get(key) != self.get(key):
                return False

        return True

    def __testsuite_visit__(self, visit):
        return PartialDict(visit(self._dict))

    def __testsuite_resolve_value__(self, other, report_error):
        if not isinstance(other, collections.abc.Mapping):
            return self
        return {**other, **self._dict}

    def __testsuite_adjust_values__(
        self, other, report_error
    ) -> tuple[typing.Any, typing.Any]:
        if not isinstance(other, collections.abc.Mapping):
            return self, other
        return self._dict, {
            key: value for key, value in other.items() if key in self._dict
        }


class UnorderedList:
    def __init__(self, sequence, key):
        self._value = sorted(sequence, key=key)
        self._key = key

    def __repr__(self):
        return f'<UnorderedList: {self._value}>'

    def __eq__(self, other):
        if isinstance(other, list):
            return sorted(other, key=self._key) == self._value
        if isinstance(other, UnorderedList):
            return self._value == other._value and self._key == other._key
        return False

    def __testsuite_visit__(self, visit):
        return UnorderedList(visit(self._value), self._key)

    def __testsuite_resolve_value__(self, other, report_error):
        if not isinstance(other, list):
            return self

        sort_key = self._key or (lambda x: x)
        other_sorted = sorted(
            enumerate(other), key=lambda x: (sort_key(x[1]), x[0])
        )

        idx_seq = itertools.count(len(other_sorted))
        it_self = iter(self._value)

        def doit():
            item_self = next(it_self, _Sentinel)
            for idx_other, item_other in other_sorted:
                if item_self is _Sentinel:
                    return
                while sort_key(item_other) > sort_key(item_self):
                    yield next(idx_seq), item_self
                    item_self = next(it_self, _Sentinel)
                    if item_self is _Sentinel:
                        return
                if sort_key(item_other) < sort_key(item_self):
                    continue
                if item_other == item_self:
                    yield idx_other, item_other
                else:
                    yield next(idx_seq), item_self
                item_self = next(it_self, _Sentinel)
            if item_self is not _Sentinel:
                yield next(idx_seq), item_self
            yield from zip(idx_seq, it_self)

        return [item for _, item in sorted(doit(), key=operator.itemgetter(0))]

    def __testsuite_adjust_values__(
        self, other, report_error
    ) -> tuple[typing.Any, typing.Any]:
        if not isinstance(other, list):
            return self, other
        return self._value, list(sorted(other, key=self._key))


class AnyList:
    """Value is a list.

    Example:

    .. code-block:: python

       assert ['foo', 'bar']  == matching.any_list
    """

    def __repr__(self):
        return '<AnyList>'

    def __eq__(self, other):
        return isinstance(other, (list, AnyList))

    def __testsuite_resolve_value__(self, other, report_error):
        if not isinstance(other, list):
            return self
        return other

    def __testsuite_adjust_values__(
        self, other, report_error
    ) -> tuple[typing.Any, typing.Any]:
        if not isinstance(other, list):
            return self, other
        return other, other


class ListOf:
    """Value is a list of values.

    Example:

    .. code-block:: python

       assert ['foo', 'bar']  == matching.ListOf(matching.any_string)
       assert [1, 2]  != matching.ListOf(matching.any_string)
    """

    def __init__(self, value=Any()):
        self._value = value

    def __repr__(self):
        return f'<ListOf value={self._value}>'

    def __eq__(self, other):
        if isinstance(other, list):
            for value in other:
                if self._value != value:
                    return False
            return True
        if isinstance(other, ListOf):
            return self._value == other._value
        return False

    def __testsuite_visit__(self, visit):
        return ListOf(visit(self._value))

    def __testsuite_resolve_value__(self, other, report_error):
        if not isinstance(other, list):
            return self
        return [self._value] * len(other)

    def __testsuite_adjust_values__(
        self, other, report_error
    ) -> tuple[typing.Any, typing.Any]:
        if not isinstance(other, list):
            return self, other
        return [self._value] * len(other), other


class AnyDict:
    """Value is a dictionary.

    Example:

    .. code-block:: python

       assert {'foo': 'bar'} == matching.any_dict
    """

    def __repr__(self):
        return '<AnyDict>'

    def __eq__(self, other):
        return isinstance(other, (dict, AnyDict))

    def __testsuite_resolve_value__(self, other, report_error):
        if not isinstance(other, collections.abc.Mapping):
            return self
        return other

    def __testsuite_adjust_values__(
        self, other, report_error
    ) -> tuple[typing.Any, typing.Any]:
        if not isinstance(other, collections.abc.Mapping):
            return self, other
        return other, other


class DictOf:
    """Value is a dictionary of (key, value) pairs.

    Example:

    .. code-block:: python

       pred = matching.DictOf(key=matching.any_string, value=matching.any_string)
       assert pred == {'foo': 'bar'}
       assert pred != {'foo': 1}
       assert pred != {1: 'bar'}
    """

    def __init__(self, key=Any(), value=Any()):
        self._key = key
        self._value = value

    def __repr__(self):
        return f'<DictOf key={self._key} value={self._value}>'

    def __eq__(self, other):
        if isinstance(other, dict):
            for key, value in other.items():
                if self._key != key:
                    return False
                if self._value != value:
                    return False
            return True
        if isinstance(other, DictOf):
            return self._key == other._key and self._value == other._value
        return False

    def __testsuite_visit__(self, visit):
        return DictOf(visit(self._key), visit(self._value))

    def __testsuite_resolve_value__(self, other, report_error):
        if not isinstance(other, collections.abc.Mapping):
            return self

        result = {}
        for key, value in other.items():
            if key != self._key:
                report_error(
                    f'dict key must match {self._key} expression',
                    path=f'[{key!r}]',
                )
            result[key] = self._value
        return result

    def __testsuite_adjust_values__(
        self, other, report_error
    ) -> tuple[typing.Any, typing.Any]:
        if not isinstance(other, collections.abc.Mapping):
            return self, other

        adjusted_self = {}

        for key, value in other.items():
            if key != self._key:
                report_error(
                    f'dict key must match {self._key} expression',
                    path=f'[{key!r}]',
                )
            adjusted_self[key] = self._value

        return adjusted_self, other


class Capture:
    """Capture matched value(s).

    Example:

    .. code-block:: python

       # You can define matching rule out of pattern
       capture_foo = matching.Capture(matching.any_string)
       pattern = {'foo': capture_foo}
       assert pattern == {'foo': 'bar'}
       assert capture_foo.value == 'bar'
       assert capture_foo.values_list == ['bar']

       # Or do it later
       capture_foo = matching.Capture()
       pattern = {'foo': capture_foo(matching.any_string)}
       assert pattern == {'foo': 'bar'}
       assert capture_foo.value == 'bar'
       assert capture_foo.values_list == ['bar']
    """

    def __init__(self, value=Any(), _link_captured=None):
        self._value = value
        if _link_captured is None:
            self._captured = []
        else:
            self._captured = _link_captured

    @property
    def value(self):
        if self._captured:
            return self._captured[0]
        raise NoValueCapturedError(f'No value captured for value {self._value}')

    @property
    def values_list(self):
        return self._captured

    def __eq__(self, other):
        if self._value != other:
            return False
        self._captured.append(other)
        return True

    def __call__(self, value):
        return Capture(value, _link_captured=self._captured)

    def __testsuite_visit__(self, visit):
        return Capture(visit(self._value), self._captured)

    def __testsuite_resolve_value__(self, other, report_error):
        return _resolve_value(self._value, other, report_error)


def unordered_list(sequence, *, key=None):
    """Unordered list comparison.

    You may want to compare lists without respect to order. For instance,
    when your service is serializing std::unordered_map to array.

    `unordered_list` can help you with that. It sorts both array before
    comparison.

    :param sequence: Initial sequence
    :param key: Sorting key function

    Example:

    .. code-block:: python

       assert [3, 2, 1] == matching.unordered_list([1, 2, 3])
    """
    return UnorderedList(sequence, key)


class _ObjectTransform:
    def visit(self, value):
        if isinstance(value, dict):
            return self.visit_dict(value)
        if isinstance(value, list):
            return self.visit_list(value)
        visit = getattr(value, '__testsuite_visit__', None)
        if visit:
            return visit(self.visit)
        return value

    def visit_dict(self, value):
        return {key: self.visit(value) for key, value in value.items()}

    def visit_list(self, value):
        return [self.visit(item) for item in value]


def recursive_partial_dict(*args, **kwargs):
    """Creates recursive partial dict.

        Traverse input dict and create `PartialDict` for nested dicts.
        Supports visiting `testsuite.matching` predicates. Skips inner
        :py:class:`PartialDict` nodes in order to allow user to customize
        behavior.

    l    Example:

        .. code-block:: python

           assert {
               'foo': {'bar': 123, 'extra'}, 'extra'
           } == matching.recursive_partial_dict({
                    'foo: {'bar': 123}
                })
    """

    class Transform(_ObjectTransform):
        def visit(self, value):
            if isinstance(value, PartialDict):
                return value
            return super().visit(value)

        def visit_dict(self, value):
            value = super().visit_dict(value)
            return PartialDict(value)

    root = dict(*args, **kwargs)
    return Transform().visit(root)


def _resolve_types(value):
    return getattr(value, '__testsuite_types__', ())


def _resolve_value(obj, other, report_error):
    if hasattr(obj, '__testsuite_resolve_value__'):
        return obj.__testsuite_resolve_value__(other, report_error)
    return obj


any_value = Any()
any_float = IsInstance(float)
any_integer = IsInstance(int)
any_numeric = IsInstance((int, float))
any_datetime = IsInstance(datetime.datetime)
any_timedelta = IsInstance(datetime.timedelta)
positive_float = And(any_float, Gt(0))
positive_integer = And(any_integer, Gt(0))
positive_numeric = And(any_numeric, Gt(0))
negative_float = And(any_float, Lt(0))
negative_integer = And(any_integer, Lt(0))
negative_numeric = And(any_numeric, Lt(0))
non_negative_float = And(any_float, Ge(0))
non_negative_integer = And(any_integer, Ge(0))
non_negative_numeric = And(any_numeric, Ge(0))
any_string = AnyString()
datetime_string = DatetimeString()
objectid_string = ObjectIdString()
uuid_string = UuidString()

any_dict = AnyDict()
any_list = AnyList()
