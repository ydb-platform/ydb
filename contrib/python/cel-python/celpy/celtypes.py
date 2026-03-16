# SPDX-Copyright: Copyright (c) Capital One Services, LLC
# SPDX-License-Identifier: Apache-2.0
# Copyright 2020 Capital One Services, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

"""
CEL Types: wrappers on Python types to provide CEL semantics.

This can be used by a Python module to work with CEL-friendly values and CEL results.

Examples of distinctions between CEL and Python:

-   Unlike Python ``bool``, CEL :py:class:`BoolType` won't do some math.

-   CEL has ``int64`` and ``uint64`` subclasses of integer. These have specific ranges and
    raise :exc:`ValueError` errors on overflow.

CEL types will raise :exc:`ValueError` for out-of-range values and :exc:`TypeError`
for operations they refuse.
The :py:mod:`evaluation` module can capture these exceptions and turn them into result values.
This can permit the logic operators to quietly silence them via "short-circuiting".

In the normal course of events, CEL's evaluator may attempt operations between a
CEL exception result and an instance of one of CEL types.
We rely on this leading to an ordinary Python :exc:`TypeError` to be raised to propogate
the error. Or. A logic operator may discard the error object.

The :py:mod:`evaluation` module extends these types with it's own :exc:`CELEvalError` exception.
We try to keep that as a separate concern from the core operator implementations here.
We leverage Python features, which means raising exceptions when there is a problem.

Types
=============

See https://github.com/google/cel-go/tree/master/common/types

These are the Go type definitions that are used by CEL:

-   BoolType
-   BytesType
-   DoubleType
-   DurationType
-   IntType
-   ListType
-   MapType
-   NullType
-   StringType
-   TimestampType
-   TypeType
-   UintType

The above types are handled directly byt CEL syntax.
e.g., ``42`` vs. ``42u`` vs. ``"42"`` vs. ``b"42"`` vs. ``42.``.

We provide matching Python class names for each of these types. The Python type names
are subclasses of Python native types, allowing a client to transparently work with
CEL results. A Python host should be able to provide values to CEL that will be tolerated.

A type hint of ``Value`` unifies these into a common hint.

The CEL Go implementation also supports protobuf types:

-   dpb.Duration
-   tpb.Timestamp
-   structpb.ListValue
-   structpb.NullValue
-   structpb.Struct
-   structpb.Value
-   wrapperspb.BoolValue
-   wrapperspb.BytesValue
-   wrapperspb.DoubleValue
-   wrapperspb.FloatValue
-   wrapperspb.Int32Value
-   wrapperspb.Int64Value
-   wrapperspb.StringValue
-   wrapperspb.UInt32Value
-   wrapperspb.UInt64Value

These types involve expressions like the following::

    google.protobuf.UInt32Value{value: 123u}

In this case, the well-known protobuf name is directly visible as CEL syntax.
There's a ``google`` package with the needed definitions.

Type Provider
==============================

A type provider can be bound to the environment, this will support additional types.
This appears to be a factory to map names of types to type classes.

Run-time type binding is shown by a CEL expression like the following::

    TestAllTypes{single_uint32_wrapper: 432u}

The ``TestAllTypes`` is a protobuf type added to the CEL run-time. The syntax
is defined by this syntax rule::

    member_object  : member "{" [fieldinits] "}"

The ``member`` is part of a type provider library,
either a standard protobuf definition or an extension. The field inits build
values for the protobuf object.

See https://github.com/google/cel-go/blob/master/test/proto3pb/test_all_types.proto
for the ``TestAllTypes`` protobuf definition that is registered as a type provider.

This expression will describes a Protobuf ``uint32`` object.

Type Adapter
=============

So far, it appears that a type adapter wraps existing Go or C++ types
with CEL-required methods. This seems like it does not need to be implemented
in Python.

Numeric Details
===============

Integer division truncates toward zero.

The Go definition of modulus::

    // Mod returns the floating-point remainder of x/y.
    // The magnitude of the result is less than y and its
    // sign agrees with that of x.

https://golang.org/ref/spec#Arithmetic_operators

"Go has the nice property that -a/b == -(a/b)."

::

     x     y     x / y     x % y
     5     3       1         2
    -5     3      -1        -2
     5    -3      -1         2
    -5    -3       1        -2

Python definition::

    The modulo operator always yields a result
    with the same sign as its second operand (or zero);
    the absolute value of the result is strictly smaller than
    the absolute value of the second operand.

Here's the essential rule::

    x//y * y + x%y == x

However. Python ``//`` truncates toward negative infinity. Go ``/`` truncates toward zero.

To get Go-like behavior, we need to use absolute values and restore the signs later.

::

    x_sign = -1 if x < 0 else +1
    go_mod = x_sign * (abs(x) % abs(y))
    return go_mod

Timzone Details
===============

An implementation may have additional timezone names that must be injected into
the ``pendulum`` processing. (Formerly ``dateutil.gettz()``.)

For example, there may be the following sequence:

1. A lowercase match for an alias or an existing timezone.

2. A titlecase match for an existing timezone.

3. The fallback, which is a +/-HH:MM string.

..  TODO: Permit an extension into the timezone lookup.

"""

import datetime
import logging
import re
from functools import reduce, wraps
from math import fsum
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    NoReturn,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

import pendulum
from pendulum import timezone
import pendulum.tz.exceptions


logger = logging.getLogger(f"celpy.{__name__}")


Value = Union[
    "BoolType",
    "BytesType",
    "DoubleType",
    "DurationType",
    "IntType",
    "ListType",
    "MapType",
    None,  # Used instead of NullType
    "StringType",
    "TimestampType",
    "UintType",
]

# The domain of types used to build Annotations.
CELType = Union[
    Type["BoolType"],
    Type["BytesType"],
    Type["DoubleType"],
    Type["DurationType"],
    Type["IntType"],
    Type["ListType"],
    Type["MapType"],
    Callable[..., None],  # Used instead of NullType
    Type["StringType"],
    Type["TimestampType"],
    Type["TypeType"],  # Used to mark Protobuf Type values
    Type["UintType"],
    Type["PackageType"],
    Type["MessageType"],
]


def type_matched(method: Callable[[Any, Any], Any]) -> Callable[[Any, Any], Any]:
    """Decorates a method to assure the "other" value has the same type."""

    @wraps(method)
    def type_matching_method(self: Any, other: Any) -> Any:
        if not (
            issubclass(type(other), type(self)) or issubclass(type(self), type(other))
        ):
            raise TypeError(
                f"no such overload: {self!r} {type(self)} != {other!r} {type(other)}"
            )
        return method(self, other)

    return type_matching_method


def logical_condition(e: Value, x: Value, y: Value) -> Value:
    """
    CEL e ? x : y operator.
    Choose one of x or y. Exceptions in the unchosen expression are ignored.

    Example::

        2 / 0 > 4 ? 'baz' : 'quux'

    is a "division by zero" error.

    ::

        >>> logical_condition(
        ... BoolType(True), StringType("this"), StringType("Not That"))
        StringType('this')
        >>> logical_condition(
        ... BoolType(False), StringType("Not This"), StringType("that"))
        StringType('that')
    """
    if not isinstance(e, BoolType):
        raise TypeError(f"Unexpected {type(e)} ? {type(x)} : {type(y)}")
    result = x if e else y
    logger.debug("logical_condition(%r, %r, %r) = %r", e, x, y, result)
    return result


def logical_and(x: Value, y: Value) -> Value:
    """
    Native Python has a left-to-right rule.
    CEL && is commutative with non-Boolean values, including error objects.
    """
    if not isinstance(x, BoolType) and not isinstance(y, BoolType):
        raise TypeError(f"{type(x)} {x!r} and {type(y)} {y!r}")
    elif not isinstance(x, BoolType) and isinstance(y, BoolType):
        if y:
            return x  # whatever && true == whatever
        else:
            return y  # whatever && false == false
    elif isinstance(x, BoolType) and not isinstance(y, BoolType):
        if x:
            return y  # true && whatever == whatever
        else:
            return x  # false && whatever == false
    else:
        return BoolType(cast(BoolType, x) and cast(BoolType, y))


def logical_not(x: Value) -> Value:
    """
    Native python `not` isn't fully exposed for CEL types.
    """
    if isinstance(x, BoolType):
        result = BoolType(not x)
    else:
        raise TypeError(f"not {type(x)}")
    logger.debug("logical_not(%r) = %r", x, result)
    return result


def logical_or(x: Value, y: Value) -> Value:
    """
    Native Python has a left-to-right rule: (True or y) is True, (False or y) is y.
    CEL || is commutative with non-Boolean values, including errors.
    ``(x || false)`` is ``x``, and ``(false || y)`` is ``y``.

    Example 1::

        false || 1/0 != 0

    is a "no matching overload" error.

    Example 2::

        (2 / 0 > 3 ? false : true) || true

    is a "True"

    If the operand(s) are not BoolType, we'll create an TypeError that will become a CELEvalError.
    """
    if not isinstance(x, BoolType) and not isinstance(y, BoolType):
        raise TypeError(f"{type(x)} {x!r} or {type(y)} {y!r}")
    elif not isinstance(x, BoolType) and isinstance(y, BoolType):
        if y:
            return y  # whatever || true == true
        else:
            return x  # whatever || false == whatever
    elif isinstance(x, BoolType) and not isinstance(y, BoolType):
        if x:
            return x  # true || whatever == true
        else:
            return y  # false || whatever == whatever
    else:
        return BoolType(cast(BoolType, x) or cast(BoolType, y))


class BoolType(int):
    """
    Native Python permits unary operators on Booleans.

    For CEL, We need to prevent -false from working.
    """

    def __new__(cls: Type["BoolType"], source: Any) -> "BoolType":
        if source is None:
            return super().__new__(cls, 0)
        elif isinstance(source, BoolType):
            return source
        elif isinstance(source, MessageType):
            return super().__new__(cls, cast(int, source.get(StringType("value"))))
        else:
            return super().__new__(cls, source)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({bool(self)})"

    def __str__(self) -> str:
        return str(bool(self))

    def __neg__(self) -> NoReturn:
        raise TypeError("no such overload")

    def __hash__(self) -> int:
        return super().__hash__()


class BytesType(bytes):
    """Python's bytes semantics are close to CEL."""

    def __new__(
        cls: Type["BytesType"],
        source: Union[str, bytes, Iterable[int], "BytesType", "StringType"],
        *args: Any,
        **kwargs: Any,
    ) -> "BytesType":
        if source is None:
            return super().__new__(cls, b"")
        elif isinstance(source, (bytes, BytesType)):
            return super().__new__(cls, source)
        elif isinstance(source, (str, StringType)):
            return super().__new__(cls, source.encode("utf-8"))
        elif isinstance(source, MessageType):
            return super().__new__(
                cls,
                cast(bytes, source.get(StringType("value"))),  # type: ignore [attr-defined]
            )
        elif isinstance(source, Iterable):
            return super().__new__(cls, cast(Iterable[int], source))
        else:
            raise TypeError(f"Invalid initial value type: {type(source)}")

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({super().__repr__()})"


class DoubleType(float):
    """
    Native Python permits mixed type comparisons, doing conversions as needed.

    For CEL, we need to prevent mixed-type comparisons from working.

    TODO: Conversions from string? IntType? UintType? DoubleType?
    """

    def __new__(cls: Type["DoubleType"], source: Any) -> "DoubleType":
        if source is None:
            return super().__new__(cls, 0)
        elif isinstance(source, MessageType):
            return super().__new__(cls, cast(float, source.get(StringType("value"))))
        else:
            return super().__new__(cls, source)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({super().__repr__()})"

    def __str__(self) -> str:
        text = str(float(self))
        return text

    def __neg__(self) -> "DoubleType":
        return DoubleType(super().__neg__())

    def __mod__(self, other: Any) -> NoReturn:
        raise TypeError(
            f"found no matching overload for '_%_' applied to '(double, {type(other)})'"
        )

    def __truediv__(self, other: Any) -> "DoubleType":
        if cast(float, other) == 0.0:
            return DoubleType("inf")
        else:
            return DoubleType(super().__truediv__(other))

    def __rmod__(self, other: Any) -> NoReturn:
        raise TypeError(
            f"found no matching overload for '_%_' applied to '({type(other)}, double)'"
        )

    def __rtruediv__(self, other: Any) -> "DoubleType":
        if self == 0.0:
            return DoubleType("inf")
        else:
            return DoubleType(super().__rtruediv__(other))

    @type_matched
    def __eq__(self, other: Any) -> bool:
        return super().__eq__(other)

    @type_matched
    def __ne__(self, other: Any) -> bool:
        return super().__ne__(other)

    def __hash__(self) -> int:
        return super().__hash__()


IntOperator = TypeVar("IntOperator", bound=Callable[..., int])


def int64(operator: IntOperator) -> IntOperator:
    """Apply an operation, but assure the value is within the int64 range."""

    @wraps(operator)
    def clamped_operator(*args: Any, **kwargs: Any) -> int:
        result: int = operator(*args, **kwargs)
        if -(2**63) <= result < 2**63:
            return result
        raise ValueError("overflow")

    return cast(IntOperator, clamped_operator)


class IntType(int):
    """
    A version of int with overflow errors outside int64 range.

    features/integer_math.feature:277  "int64_overflow_positive"

    >>> IntType(9223372036854775807) + IntType(1)
    Traceback (most recent call last):
    ...
    ValueError: overflow

    >>> 2**63
    9223372036854775808

    features/integer_math.feature:285  "int64_overflow_negative"

    >>> -IntType(9223372036854775808) - IntType(1)
    Traceback (most recent call last):
    ...
    ValueError: overflow

    >>> IntType(DoubleType(1.9))
    IntType(2)
    >>> IntType(DoubleType(-123.456))
    IntType(-123)
    """

    def __new__(
        cls: Type["IntType"], source: Any, *args: Any, **kwargs: Any
    ) -> "IntType":
        convert: Callable[..., int]
        if source is None:
            return super().__new__(cls, 0)
        elif isinstance(source, IntType):
            return source
        elif isinstance(source, MessageType):
            # Used by protobuf.
            return super().__new__(cls, cast(int, source.get(StringType("value"))))
        elif isinstance(source, (float, DoubleType)):
            convert = int64(round)
        elif isinstance(source, TimestampType):
            convert = int64(lambda src: src.timestamp())
        elif isinstance(source, (str, StringType)) and source[:2] in {"0x", "0X"}:
            convert = int64(lambda src: int(src[2:], 16))
        elif isinstance(source, (str, StringType)) and source[:3] in {"-0x", "-0X"}:
            convert = int64(lambda src: -int(src[3:], 16))
        else:
            # Must tolerate "-" as part of the literal.
            # See https://github.com/google/cel-spec/issues/126
            convert = int64(int)
        return super().__new__(cls, convert(source))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({super().__repr__()})"

    def __str__(self) -> str:
        text = str(int(self))
        return text

    @int64
    def __neg__(self) -> "IntType":
        return IntType(super().__neg__())

    @int64
    def __add__(self, other: Any) -> "IntType":
        return IntType(super().__add__(cast(IntType, other)))

    @int64
    def __sub__(self, other: Any) -> "IntType":
        return IntType(super().__sub__(cast(IntType, other)))

    @int64
    def __mul__(self, other: Any) -> "IntType":
        return IntType(super().__mul__(cast(IntType, other)))

    @int64
    def __truediv__(self, other: Any) -> "IntType":
        other = cast(IntType, other)
        self_sign = -1 if self < IntType(0) else +1
        other_sign = -1 if other < IntType(0) else +1
        go_div = self_sign * other_sign * (abs(self) // abs(other))
        return IntType(go_div)

    __floordiv__ = __truediv__

    @int64
    def __mod__(self, other: Any) -> "IntType":
        self_sign = -1 if self < IntType(0) else +1
        go_mod = self_sign * (abs(self) % abs(cast(IntType, other)))
        return IntType(go_mod)

    @int64
    def __radd__(self, other: Any) -> "IntType":
        return IntType(super().__radd__(cast(IntType, other)))

    @int64
    def __rsub__(self, other: Any) -> "IntType":
        return IntType(super().__rsub__(cast(IntType, other)))

    @int64
    def __rmul__(self, other: Any) -> "IntType":
        return IntType(super().__rmul__(cast(IntType, other)))

    @int64
    def __rtruediv__(self, other: Any) -> "IntType":
        other = cast(IntType, other)
        self_sign = -1 if self < IntType(0) else +1
        other_sign = -1 if other < IntType(0) else +1
        go_div = self_sign * other_sign * (abs(other) // abs(self))
        return IntType(go_div)

    __rfloordiv__ = __rtruediv__

    @int64
    def __rmod__(self, other: Any) -> "IntType":
        left_sign = -1 if other < IntType(0) else +1
        go_mod = left_sign * (abs(other) % abs(self))
        return IntType(go_mod)

    @type_matched
    def __eq__(self, other: Any) -> bool:
        return super().__eq__(other)

    @type_matched
    def __ne__(self, other: Any) -> bool:
        return super().__ne__(other)

    @type_matched
    def __lt__(self, other: Any) -> bool:
        return super().__lt__(other)

    @type_matched
    def __le__(self, other: Any) -> bool:
        return super().__le__(other)

    @type_matched
    def __gt__(self, other: Any) -> bool:
        return super().__gt__(other)

    @type_matched
    def __ge__(self, other: Any) -> bool:
        return super().__ge__(other)

    def __hash__(self) -> int:
        return super().__hash__()


def uint64(operator: IntOperator) -> IntOperator:
    """Apply an operation, but assure the value is within the uint64 range."""

    @wraps(operator)
    def clamped_operator(*args: Any, **kwargs: Any) -> int:
        result = operator(*args, **kwargs)
        if 0 <= result < 2**64:
            return result
        raise ValueError("overflow")

    return cast(IntOperator, clamped_operator)


class UintType(int):
    """
    A version of int with overflow errors outside uint64 range.

    Alternatives:

        Option 1 - Use https://pypi.org/project/fixedint/

        Option 2 - use array or struct modules to access an unsigned object.

    Test Cases:

    features/integer_math.feature:149  "unary_minus_no_overload"

    >>> -UintType(42)
    Traceback (most recent call last):
    ...
    TypeError: no such overload

    uint64_overflow_positive

    >>> UintType(18446744073709551615) + UintType(1)
    Traceback (most recent call last):
    ...
    ValueError: overflow

    uint64_overflow_negative

    >>> UintType(0) - UintType(1)
    Traceback (most recent call last):
    ...
    ValueError: overflow

    >>> - UintType(5)
    Traceback (most recent call last):
    ...
    TypeError: no such overload
    """

    def __new__(
        cls: Type["UintType"], source: Any, *args: Any, **kwargs: Any
    ) -> "UintType":
        convert: Callable[..., int]
        if isinstance(source, UintType):
            return source
        elif isinstance(source, (float, DoubleType)):
            convert = uint64(round)
        elif isinstance(source, TimestampType):
            convert = uint64(lambda src: src.timestamp())
        elif isinstance(source, (str, StringType)) and source[:2] in {"0x", "0X"}:
            convert = uint64(lambda src: int(src[2:], 16))
        elif isinstance(source, MessageType):
            # Used by protobuf.
            convert = uint64(
                lambda src: src["value"] if src["value"] is not None else 0
            )
        elif source is None:
            convert = uint64(lambda src: 0)
        else:
            convert = uint64(int)
        return super().__new__(cls, convert(source))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({super().__repr__()})"

    def __str__(self) -> str:
        text = str(int(self))
        return text

    def __neg__(self) -> NoReturn:
        raise TypeError("no such overload")

    @uint64
    def __add__(self, other: Any) -> "UintType":
        return UintType(super().__add__(cast(IntType, other)))

    @uint64
    def __sub__(self, other: Any) -> "UintType":
        return UintType(super().__sub__(cast(IntType, other)))

    @uint64
    def __mul__(self, other: Any) -> "UintType":
        return UintType(super().__mul__(cast(IntType, other)))

    @uint64
    def __truediv__(self, other: Any) -> "UintType":
        return UintType(super().__floordiv__(cast(IntType, other)))

    __floordiv__ = __truediv__

    @uint64
    def __mod__(self, other: Any) -> "UintType":
        return UintType(super().__mod__(cast(IntType, other)))

    @uint64
    def __radd__(self, other: Any) -> "UintType":
        return UintType(super().__radd__(cast(IntType, other)))

    @uint64
    def __rsub__(self, other: Any) -> "UintType":
        return UintType(super().__rsub__(cast(IntType, other)))

    @uint64
    def __rmul__(self, other: Any) -> "UintType":
        return UintType(super().__rmul__(cast(IntType, other)))

    @uint64
    def __rtruediv__(self, other: Any) -> "UintType":
        return UintType(super().__rfloordiv__(cast(IntType, other)))

    __rfloordiv__ = __rtruediv__

    @uint64
    def __rmod__(self, other: Any) -> "UintType":
        return UintType(super().__rmod__(cast(IntType, other)))

    @type_matched
    def __eq__(self, other: Any) -> bool:
        return super().__eq__(other)

    @type_matched
    def __ne__(self, other: Any) -> bool:
        return super().__ne__(other)

    def __hash__(self) -> int:
        return super().__hash__()


class ListType(List[Value]):
    """
    Native Python implements comparison operations between list objects.

    For CEL, we prevent list comparison operators from working.

    We provide an :py:meth:`__eq__` and :py:meth:`__ne__` that
    gracefully ignore type mismatch problems, calling them not equal.

    See https://github.com/google/cel-spec/issues/127

    An implied logical And means a singleton behaves in a distinct way from a non-singleton list.
    """

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({super().__repr__()})"

    def __lt__(self, other: Any) -> NoReturn:
        raise TypeError("no such overload")

    def __le__(self, other: Any) -> NoReturn:
        raise TypeError("no such overload")

    def __gt__(self, other: Any) -> NoReturn:
        raise TypeError("no such overload")

    def __ge__(self, other: Any) -> NoReturn:
        raise TypeError("no such overload")

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, (list, ListType)):
            raise TypeError(f"no such overload: ListType == {type(other)}")

        def equal(s: Any, o: Any) -> Value:
            try:
                return BoolType(s == o)
            except TypeError as ex:
                return cast(BoolType, ex)  # Instead of Union[BoolType, TypeError]

        result = len(self) == len(other) and reduce(  # noqa: W503
            logical_and,  # type: ignore [arg-type]
            (equal(item_s, item_o) for item_s, item_o in zip(self, other)),
            BoolType(True),  # type: ignore [arg-type]
        )
        if isinstance(result, TypeError):
            raise result
        return bool(result)

    def __ne__(self, other: Any) -> bool:
        if not isinstance(other, (list, ListType)):
            raise TypeError(f"no such overload: ListType != {type(other)}")

        def not_equal(s: Any, o: Any) -> Value:
            try:
                return BoolType(s != o)
            except TypeError as ex:
                return cast(BoolType, ex)  # Instead of Union[BoolType, TypeError]

        result = len(self) != len(other) or reduce(  # noqa: W503
            logical_or,  # type: ignore [arg-type]
            (not_equal(item_s, item_o) for item_s, item_o in zip(self, other)),
            BoolType(False),  # type: ignore [arg-type]
        )
        if isinstance(result, TypeError):
            raise result
        return bool(result)


BaseMapTypes = Union[Mapping[Any, Any], Sequence[Tuple[Any, Any]], None]


MapKeyTypes = Union["IntType", "UintType", "BoolType", "StringType", str]


class MapType(Dict[Value, Value]):
    """
    Native Python allows mapping updates and any hashable type as a kay.

    CEL prevents mapping updates and has a limited domain of key types.
        int, uint, bool, or string keys

    We provide an :py:meth:`__eq__` and :py:meth:`__ne__` that
    gracefully ignore type mismatch problems for the values, calling them not equal.

    See https://github.com/google/cel-spec/issues/127

    An implied logical And means a singleton behaves in a distinct way from a non-singleton mapping.
    """

    def __init__(self, items: BaseMapTypes = None) -> None:
        super().__init__()
        if items is None:
            pass
        elif isinstance(items, Sequence):
            for name, value in items:
                self[name] = value
        elif isinstance(items, Mapping):
            for name, value in items.items():
                self[name] = value
        else:
            raise TypeError(f"Invalid initial value type: {type(items)}")

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({super().__repr__()})"

    def __getitem__(self, key: Any) -> Any:
        if not MapType.valid_key_type(key):
            raise TypeError(f"unsupported key type: {type(key)}")
        return super().__getitem__(key)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, (Mapping, MapType)):
            raise TypeError(f"no such overload: MapType == {type(other)}")

        def equal(s: Any, o: Any) -> BoolType:
            try:
                return BoolType(s == o)
            except TypeError as ex:
                return cast(BoolType, ex)  # Instead of Union[BoolType, TypeError]

        keys_s = self.keys()
        keys_o = other.keys()
        result = keys_s == keys_o and reduce(  # noqa: W503
            logical_and,  # type: ignore [arg-type]
            (equal(self[k], other[k]) for k in keys_s),
            BoolType(True),  # type: ignore [arg-type]
        )
        if isinstance(result, TypeError):
            raise result
        return bool(result)

    def __ne__(self, other: Any) -> bool:
        if not isinstance(other, (Mapping, MapType)):
            raise TypeError(f"no such overload: MapType != {type(other)}")

        # Singleton special case, may return no-such overload.
        if len(self) == 1 and len(other) == 1 and self.keys() == other.keys():
            k = next(iter(self.keys()))
            return cast(
                bool, self[k] != other[k]
            )  # Instead of Union[BoolType, TypeError]

        def not_equal(s: Any, o: Any) -> BoolType:
            try:
                return BoolType(s != o)
            except TypeError as ex:
                return cast(BoolType, ex)  # Instead of Union[BoolType, TypeError]

        keys_s = self.keys()
        keys_o = other.keys()
        result = keys_s != keys_o or reduce(  # noqa: W503
            logical_or,  # type: ignore [arg-type]
            (not_equal(self[k], other[k]) for k in keys_s),
            BoolType(False),  # type: ignore [arg-type]
        )
        if isinstance(result, TypeError):
            raise result
        return bool(result)

    @staticmethod
    def valid_key_type(key: Any) -> bool:
        """Valid CEL key types. Plus native str for tokens in the source when evaluating ``e.f``"""
        return isinstance(key, (IntType, UintType, BoolType, StringType, str))


class NullType:
    """Python's None semantics aren't quite right for CEL."""

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, NullType)

    def __ne__(self, other: Any) -> bool:
        return not isinstance(other, NullType)


class StringType(str):
    """Python's str semantics are very, very close to CEL.

    We rely on the overlap between ``"/u270c"`` and ``"/U0001f431"`` in CEL and Python.
    """

    def __new__(
        cls: Type["StringType"],
        source: Union[str, bytes, "BytesType", "StringType"],
        *args: Any,
        **kwargs: Any,
    ) -> "StringType":
        if isinstance(source, (bytes, BytesType)):
            return super().__new__(cls, source.decode("utf"))
        elif isinstance(source, (str, StringType)):
            # TODO: Consider returning the original StringType object.
            return super().__new__(cls, source)
        else:
            return cast(StringType, super().__new__(cls, source))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({super().__repr__()})"

    def __eq__(self, other: Any) -> bool:
        return super().__eq__(other)

    def __ne__(self, other: Any) -> bool:
        return super().__ne__(other)

    def __hash__(self) -> int:
        return super().__hash__()


class TimestampType(datetime.datetime):
    """
    Implements google.protobuf.Timestamp

    See https://developers.google.com/protocol-buffers/docs/reference/google.protobuf

    Also see https://www.ietf.org/rfc/rfc3339.txt.

    The protobuf implementation is an ordered pair of int64 seconds and int32 nanos.

    Instead of a Tuple[int, int] we use a wrapper for :py:class:`datetime.datetime`.

    From protobuf documentation for making a Timestamp in Python::

        now = time.time()
        seconds = int(now)
        nanos = int((now - seconds) * 10**9)
        timestamp = Timestamp(seconds=seconds, nanos=nanos)

    Also::

        >>> t = TimestampType("2009-02-13T23:31:30Z")
        >>> repr(t)
        "TimestampType('2009-02-13T23:31:30Z')"
        >>> t.timestamp()
        1234567890.0
        >>> str(t)
        '2009-02-13T23:31:30Z'

    :strong:`Timezones`

    Timezones are expressed in the following grammar:

    ::

        TimeZone = "UTC" | LongTZ | FixedTZ ;
        LongTZ = ? list available at
                   http://joda-time.sourceforge.net/timezones.html ? ;
        FixedTZ = ( "+" | "-" ) Digit Digit ":" Digit Digit ;
        Digit = "0" | "1" | ... | "9" ;

    Fixed timezones are explicit hour and minute offsets from UTC.
    Long timezone names are like Europe/Paris, CET, or US/Central.

    The Joda project (https://www.joda.org/joda-time/timezones.html)
    says "Time zone data is provided by the public IANA time zone database."

    TZ handling and timestamp parsing is doine with
    the ``pendulum`` (https://pendulum.eustace.io) project.

    Additionally, there is a ``TZ_ALIASES`` mapping available in this class to permit additional
    timezone names. By default, the mapping is empty, and the only names
    available are those recognized by :mod:`pendulum.timezone`.
    """

    TZ_ALIASES: Dict[str, str] = {}

    def __new__(
        cls: Type["TimestampType"],
        source: Union[int, str, datetime.datetime],
        *args: Any,
        **kwargs: Any,
    ) -> "TimestampType":
        if isinstance(source, datetime.datetime):
            # Wrap a datetime.datetime
            return super().__new__(
                cls,
                year=source.year,
                month=source.month,
                day=source.day,
                hour=source.hour,
                minute=source.minute,
                second=source.second,
                microsecond=source.microsecond,
                tzinfo=source.tzinfo or datetime.timezone.utc,
            )

        elif isinstance(source, int) and len(args) >= 2:
            # Wrap a sequence of integers that datetime.datetime might accept.
            ts: TimestampType = super().__new__(cls, source, *args, **kwargs)
            if not ts.tzinfo:
                ts = ts.replace(tzinfo=datetime.timezone.utc)
            return ts

        elif isinstance(source, str):
            # Use dateutil to try a variety of text formats.
            parsed_datetime = cast(datetime.datetime, pendulum.parse(source))
            return super().__new__(
                cls,
                year=parsed_datetime.year,
                month=parsed_datetime.month,
                day=parsed_datetime.day,
                hour=parsed_datetime.hour,
                minute=parsed_datetime.minute,
                second=parsed_datetime.second,
                microsecond=parsed_datetime.microsecond,
                tzinfo=parsed_datetime.tzinfo,
            )

        else:
            raise TypeError(f"Cannot create {cls} from {source!r}")

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({str(self)!r})"

    def __str__(self) -> str:
        text = self.strftime("%Y-%m-%dT%H:%M:%S%z")
        if text.endswith("+0000"):
            return f"{text[:-5]}Z"
        return f"{text[:-2]}:{text[-2:]}"

    def __add__(self, other: Any) -> "TimestampType":
        """Timestamp + Duration -> Timestamp"""
        result = super().__add__(other)
        if result == NotImplemented:
            return NotImplemented
        return TimestampType(result)

    def __radd__(self, other: Any) -> "TimestampType":
        """Duration + Timestamp -> Timestamp"""
        result = super().__radd__(other)
        if result == NotImplemented:
            return NotImplemented
        return TimestampType(result)

    # For more information, check the typeshed definition
    # https://github.com/python/typeshed/blob/master/stdlib/2and3/datetime.pyi

    @overload  # type: ignore
    def __sub__(self, other: "TimestampType") -> "DurationType": ...  # pragma: no cover

    @overload
    def __sub__(self, other: "DurationType") -> "TimestampType": ...  # pragma: no cover

    def __sub__(
        self, other: Union["TimestampType", "DurationType"]
    ) -> Union["TimestampType", "DurationType"]:
        result = super().__sub__(other)
        if result == NotImplemented:
            return cast(DurationType, result)
        if isinstance(result, datetime.timedelta):
            return DurationType(result)
        return TimestampType(result)

    @classmethod
    def tz_name_lookup(cls, tz_name: str) -> Optional[datetime.tzinfo]:
        """
        The :py:func:`dateutil.tz.gettz` may be extended with additional aliases.

        ..  TODO: Permit an extension into the timezone lookup.
            Tweak ``celpy.celtypes.TimestampType.TZ_ALIASES``.
        """
        tz_lookup = str(tz_name)
        tz: Optional[datetime.tzinfo]
        if tz_lookup in cls.TZ_ALIASES:
            tz = timezone(cls.TZ_ALIASES[tz_lookup])
        else:
            try:
                tz = cast(datetime.tzinfo, timezone(tz_lookup))
            except pendulum.tz.exceptions.InvalidTimezone:
                # ±hh:mm format...
                tz = cls.tz_offset_parse(tz_name)
        return tz

    @classmethod
    def tz_offset_parse(cls, tz_name: str) -> Optional[datetime.tzinfo]:
        tz_pat = re.compile(r"^([+-]?)(\d\d?):(\d\d)$")
        tz_match = tz_pat.match(tz_name)
        if not tz_match:
            raise ValueError(f"Unparsable timezone: {tz_name!r}")
        sign, hh, mm = tz_match.groups()
        offset_min = (int(hh) * 60 + int(mm)) * (-1 if sign == "-" else +1)
        offset = datetime.timedelta(seconds=offset_min * 60)
        tz = datetime.timezone(offset)
        return tz

    @staticmethod
    def tz_parse(tz_name: Optional[str]) -> Optional[datetime.tzinfo]:
        if tz_name:
            tz = TimestampType.tz_name_lookup(tz_name)
            return tz
        else:
            return timezone("UTC")

    def getDate(self, tz_name: Optional[StringType] = None) -> IntType:
        new_tz = self.tz_parse(tz_name)
        return IntType(self.astimezone(new_tz).day)

    def getDayOfMonth(self, tz_name: Optional[StringType] = None) -> IntType:
        new_tz = self.tz_parse(tz_name)
        return IntType(self.astimezone(new_tz).day - 1)

    def getDayOfWeek(self, tz_name: Optional[StringType] = None) -> IntType:
        new_tz = self.tz_parse(tz_name)
        return IntType(self.astimezone(new_tz).isoweekday() % 7)

    def getDayOfYear(self, tz_name: Optional[StringType] = None) -> IntType:
        new_tz = self.tz_parse(tz_name)
        working_date = self.astimezone(new_tz)
        jan1 = datetime.datetime(working_date.year, 1, 1, tzinfo=new_tz)
        days = working_date.toordinal() - jan1.toordinal()
        return IntType(days)

    def getMonth(self, tz_name: Optional[StringType] = None) -> IntType:
        new_tz = self.tz_parse(tz_name)
        return IntType(self.astimezone(new_tz).month - 1)

    def getFullYear(self, tz_name: Optional[StringType] = None) -> IntType:
        new_tz = self.tz_parse(tz_name)
        return IntType(self.astimezone(new_tz).year)

    def getHours(self, tz_name: Optional[StringType] = None) -> IntType:
        new_tz = self.tz_parse(tz_name)
        return IntType(self.astimezone(new_tz).hour)

    def getMilliseconds(self, tz_name: Optional[StringType] = None) -> IntType:
        new_tz = self.tz_parse(tz_name)
        return IntType(self.astimezone(new_tz).microsecond // 1000)

    def getMinutes(self, tz_name: Optional[StringType] = None) -> IntType:
        new_tz = self.tz_parse(tz_name)
        return IntType(self.astimezone(new_tz).minute)

    def getSeconds(self, tz_name: Optional[StringType] = None) -> IntType:
        new_tz = self.tz_parse(tz_name)
        return IntType(self.astimezone(new_tz).second)


class DurationType(datetime.timedelta):
    """
    Implements google.protobuf.Duration

    https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#duration

    The protobuf implementation is an ordered pair of int64 seconds and int32 nanos.
    Instead of a Tuple[int, int] we use a wrapper for :py:class:`datetime.timedelta`.

    The definition once said this::

        "type conversion, duration should be end with "s", which stands for seconds"

    This is obsolete, however, considering the following issue.

    See https://github.com/google/cel-spec/issues/138

    This refers to the following implementation detail
    ::

        // A duration string is a possibly signed sequence of
        // decimal numbers, each with optional fraction and a unit suffix,
        // such as "300ms", "-1.5h" or "2h45m".
        // Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".

    The real regex, then is this::

        [-+]?([0-9]*(\\.[0-9]*)?[a-z]+)+

    """

    MaxSeconds = 315576000000
    MinSeconds = -315576000000
    NanosecondsPerSecond = 1000000000

    scale: Dict[str, float] = {
        "ns": 1e-9,
        "us": 1e-6,
        "µs": 1e-6,
        "ms": 1e-3,
        "s": 1.0,
        "m": 60.0,
        "h": 60.0 * 60.0,
        "d": 24.0 * 60.0 * 60.0,
    }

    def __new__(
        cls: Type["DurationType"], seconds: Any, nanos: int = 0, **kwargs: Any
    ) -> "DurationType":
        if isinstance(seconds, datetime.timedelta):
            if not (cls.MinSeconds <= seconds.total_seconds() <= cls.MaxSeconds):
                raise ValueError("range error: {seconds}")
            return super().__new__(
                cls,
                days=seconds.days,
                seconds=seconds.seconds,
                microseconds=seconds.microseconds,
            )
        elif isinstance(seconds, int):
            if not (cls.MinSeconds <= seconds <= cls.MaxSeconds):
                raise ValueError("range error: {seconds}")
            return super().__new__(cls, seconds=seconds, microseconds=nanos // 1000)
        elif isinstance(seconds, str):
            duration_pat = re.compile(r"^[-+]?([0-9]*(\.[0-9]*)?[a-z]+)+$")

            duration_match = duration_pat.match(seconds)
            if not duration_match:
                raise ValueError(f"Invalid duration {seconds!r}")

            # Consume the sign.
            sign: float
            if seconds.startswith("+"):
                seconds = seconds[1:]
                sign = +1
            elif seconds.startswith("-"):
                seconds = seconds[1:]
                sign = -1
            else:
                sign = +1

            # Sum the remaining time components: number * unit
            try:
                seconds = sign * fsum(
                    map(
                        lambda n_u: float(n_u.group(1)) * cls.scale[n_u.group(3)],
                        re.finditer(r"([0-9]*(\.[0-9]*)?)([a-z]+)", seconds),
                    )
                )
            except KeyError:
                raise ValueError(f"Invalid duration {seconds!r}")

            if not (cls.MinSeconds <= seconds <= cls.MaxSeconds):
                raise ValueError("range error: {seconds}")
            return super().__new__(cls, seconds=seconds)
        else:
            raise TypeError(f"Invalid initial value type: {type(seconds)}")

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({str(self)!r})"

    def __str__(self) -> str:
        return "{0}s".format(int(self.total_seconds()))

    def __add__(self, other: Any) -> "DurationType":
        """
        This doesn't need to handle the rich variety of TimestampType overloadds.
        This class only needs to handle results of duration + duration.
        A duration + timestamp is not implemented by the timedelta superclass;
        it is handled by the datetime superclass that implementes timestamp + duration.
        """
        result = super().__add__(other)
        if result == NotImplemented:
            return cast(DurationType, result)
        # This is handled by TimestampType; this is here for completeness, but isn't used.
        if isinstance(result, (datetime.datetime, TimestampType)):
            return TimestampType(result)  # pragma: no cover
        return DurationType(result)

    def __radd__(self, other: Any) -> "DurationType":  # pragma: no cover
        """
        This doesn't need to handle the rich variety of TimestampType overloadds.

        Most cases are handled by TimeStamp.
        """
        result = super().__radd__(other)
        if result == NotImplemented:
            return cast(DurationType, result)
        # This is handled by TimestampType; this is here for completeness, but isn't used.
        if isinstance(result, (datetime.datetime, TimestampType)):
            return TimestampType(result)
        return DurationType(result)

    def getHours(self, tz_name: Optional[str] = None) -> IntType:
        assert tz_name is None
        return IntType(int(self.total_seconds() / 60 / 60))

    def getMilliseconds(self, tz_name: Optional[str] = None) -> IntType:
        assert tz_name is None
        return IntType(int(self.total_seconds() * 1000))

    def getMinutes(self, tz_name: Optional[str] = None) -> IntType:
        assert tz_name is None
        return IntType(int(self.total_seconds() / 60))

    def getSeconds(self, tz_name: Optional[str] = None) -> IntType:
        assert tz_name is None
        return IntType(int(self.total_seconds()))


class FunctionType:
    """
    We need a concrete Annotation object to describe callables to celpy.
    We need to describe functions as well as callable objects.
    The description would tend to shadow ``typing.Callable``.

    An ``__isinstance__()`` method, for example, may be helpful for run-time type-checking.

    Superclass for CEL extension functions that are defined at run-time.
    This permits a formal annotation in the environment construction that creates
    an intended type for a given name.

    This allows for some run-time type checking to see if the actual object binding
    matches the declared type binding.

    Also used to define protobuf classes provided as an annotation.

    We *could* define this as three overloads to cover unary, binary, and tertiary cases.
    """

    def __call__(self, *args: Value, **kwargs: Value) -> Value:
        raise NotImplementedError


class PackageType(MapType):
    """
    A package of message types, usually protobuf.

    TODO: This may not be needed.
    """

    pass


class MessageType(MapType):
    """
    An individual protobuf message definition. A mapping from field name to field value.

    See Scenario: "message_literal" in the parse.feature. This is a very deeply-nested
    message (30? levels), but the navigation to "payload" field seems to create a default
    value at the top level.
    """

    def __init__(self, *args: Value, **fields: Value) -> None:
        if args and len(args) == 1:
            super().__init__(cast(Mapping[Value, Value], args[0]))
        elif args and len(args) > 1:
            raise TypeError(r"Expected dictionary or fields, not {args!r}")
        else:
            super().__init__({StringType(k): v for k, v in fields.items()})

    # def get(self, field: Any, default: Optional[Value] = None) -> Value:
    #     """
    #     Alternative implementation with descent to locate a deeply-buried field.
    #     It seemed like this was the defined behavior. It turns it, it isn't.
    #     The code is here in case we're wrong and it really is the defined behavior.
    #
    #     Note. There is no default provision in CEL.
    #     """
    #     if field in self:
    #         return super().get(field)
    #
    #     def descend(message: MessageType, field: Value) -> MessageType:
    #         if field in message:
    #             return message
    #         for k in message.keys():
    #             found = descend(message[k], field)
    #             if found is not None:
    #                 return found
    #         return None
    #
    #     sub_message = descend(self, field)
    #     if sub_message is None:
    #         return default
    #     return sub_message.get(field)


class TypeType:
    """
    Annotation used to mark protobuf type objects.
    We map these to CELTypes so that type name testing works.
    """

    type_name_mapping = {
        "google.protobuf.Duration": DurationType,
        "google.protobuf.Timestamp": TimestampType,
        "google.protobuf.Int32Value": IntType,
        "google.protobuf.Int64Value": IntType,
        "google.protobuf.UInt32Value": UintType,
        "google.protobuf.UInt64Value": UintType,
        "google.protobuf.FloatValue": DoubleType,
        "google.protobuf.DoubleValue": DoubleType,
        "google.protobuf.Value": MessageType,
        "google.protubuf.Any": MessageType,  # Weird.
        "google.protobuf.Any": MessageType,
        "list_type": ListType,
        "map_type": MapType,
        "map": MapType,
        "list": ListType,
        "string": StringType,
        "bytes": BytesType,
        "bool": BoolType,
        "int": IntType,
        "uint": UintType,
        "double": DoubleType,
        "null_type": type(None),
        "STRING": StringType,
        "BOOL": BoolType,
        "INT64": IntType,
        "UINT64": UintType,
        "INT32": IntType,
        "UINT32": UintType,
        "BYTES": BytesType,
        "DOUBLE": DoubleType,
    }

    def __init__(self, value: Any = "") -> None:
        if isinstance(value, str) and value in self.type_name_mapping:
            self.type_reference = self.type_name_mapping[value]
        elif isinstance(value, str):
            try:
                self.type_reference = eval(value)
            except (NameError, SyntaxError):
                raise TypeError(f"Unknown type {value!r}")
        else:
            self.type_reference = value.__class__

    def __eq__(self, other: Any) -> bool:
        return (
            other == self.type_reference or isinstance(other, self.type_reference)  # noqa: W503
        )
