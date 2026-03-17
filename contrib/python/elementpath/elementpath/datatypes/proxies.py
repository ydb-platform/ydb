#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
"""
Type proxies for using builtin Python datatypes as primitive XSD datatypes.
A proxy class creates and validates a Python datatype and can be used for
checking instances of XSD primitive and derived types registered as virtual
subclasses.
"""
import math
from decimal import Decimal
from typing import Any, cast, NoReturn

from elementpath.aliases import XPath2ParserType
from elementpath.helpers import BOOLEAN_VALUES, FloatArgType, LazyPattern, \
    collapse_white_spaces, get_double
from .any_types import AnyAtomicType
from .untyped import UntypedAtomic
from .numeric import Float, Integer
from .datetime import AbstractDateTime, Duration

__all__ = ['BooleanProxy', 'DecimalProxy', 'DoubleProxy', 'DoubleProxy10',
           'StringProxy', 'NumericProxy', 'ArithmeticProxy', 'ErrorProxy']


class BooleanProxy(AnyAtomicType):
    """
    Proxy class for xs:boolean builtin datatype. Builds instances as <class 'bool'>.`.
    """
    name = 'boolean'
    pattern = LazyPattern(r'^(?:true|false|1|0)$')

    def __new__(cls, value: object) -> bool:  # type: ignore[misc]
        if isinstance(value, bool):
            return value
        elif isinstance(value, (int, float, Decimal)):
            if math.isnan(value):
                return False
            return bool(value)
        elif isinstance(value, UntypedAtomic):
            value = value.value
        elif not isinstance(value, str):
            raise TypeError('invalid type {!r} for xs:{}'.format(type(value), cls.name))

        if value.strip() not in BOOLEAN_VALUES:
            raise ValueError('invalid value {!r} for xs:{}'.format(value, cls.name))
        return 't' in value or '1' in value

    def __init__(self, value: object) -> None:
        bool.__init__(self)

    @classmethod
    def __subclasshook__(cls, subclass: type) -> bool:
        return issubclass(subclass, bool)

    @classmethod
    def validate(cls, value: object) -> None:
        if isinstance(value, bool):
            return
        elif isinstance(value, str):
            if cls.pattern.match(value) is None:
                raise cls._invalid_value(value)
        else:
            raise cls._invalid_type(value)


class DecimalProxy(AnyAtomicType):
    """
    Proxy class for xs:decimal builtin datatype. Builds instances as `decimal.Decimal`.
    """
    name = 'decimal'
    pattern = LazyPattern(r'^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)$')

    def __new__(cls, value: Any) -> Decimal:  # type: ignore[misc]
        if isinstance(value, (str, UntypedAtomic)):
            value = collapse_white_spaces(str(value)).replace(' ', '')
            if cls.pattern.match(value) is None:
                raise cls._invalid_value(value)
        elif isinstance(value, (float, Float, Decimal)):
            if math.isinf(value) or math.isnan(value):
                raise cls._invalid_value(value)
        try:
            return Decimal(value)
        except (ValueError, ArithmeticError):
            msg = 'invalid value {!r} for xs:{}'
            raise ArithmeticError(msg.format(value, cls.name)) from None

    def __init__(self, value: Any) -> None:
        pass

    @classmethod
    def __subclasshook__(cls, subclass: type) -> bool:
        return issubclass(subclass, (int, Decimal, Integer)) and not issubclass(subclass, bool)

    @classmethod
    def validate(cls, value: object) -> None:
        match value:
            case Decimal():
                if math.isnan(value) or math.isinf(value):
                    raise cls._invalid_value(value)
            case bool():
                raise cls._invalid_type(value)
            case int() | Integer():
                return
            case str():
                if cls.pattern.match(value) is None:
                    raise cls._invalid_value(value)
            case _:
                raise cls._invalid_type(value)


class DoubleProxy(AnyAtomicType):
    """
    Proxy class for xs:double builtin datatype. Builds instances as <class 'float'>.`.
    """
    name = 'double'
    pattern = LazyPattern(
        r'^(?:[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[Ee][+-]?[0-9]+)?|[+-]?INF|NaN)$'
    )

    @classmethod
    def make(cls, value: Any,
             parser: XPath2ParserType | None = None,
             **kwargs: Any) -> 'float':
        if cls.__name__.endswith('10'):
            return get_double(value, '1.0')
        elif parser is not None:
            return get_double(value, parser.xsd_version)
        else:
            return get_double(value, kwargs.get('xsd_version'))

    def __new__(cls, value: FloatArgType) -> float:  # type: ignore[misc]
        return get_double(value)

    def __init__(self, value: FloatArgType) -> None:
        float.__init__(self)

    @classmethod
    def __subclasshook__(cls, subclass: type) -> bool:
        return issubclass(subclass, float) and not issubclass(subclass, Float)

    @classmethod
    def validate(cls, value: object) -> None:
        if isinstance(value, float) and not isinstance(value, Float):
            return
        elif isinstance(value, str):
            if cls.pattern.match(value) is None:
                raise cls._invalid_value(value)
        else:
            raise cls._invalid_type(value)


class DoubleProxy10(DoubleProxy):
    def __new__(cls, value: FloatArgType) -> float:  # type: ignore[misc]
        return get_double(value, '1.0')


class StringProxy(AnyAtomicType):
    """
    Proxy class for xs:string builtin datatype. Builds instances as <class 'str'>.`.
    """
    name = 'string'

    def __new__(cls, *args: object, **kwargs: object) -> str:  # type: ignore[misc]
        return str(*args, **kwargs)

    def __init__(self, *args: object, **kwargs: object) -> None:
        str.__init__(self)

    @classmethod
    def __subclasshook__(cls, subclass: type) -> bool:
        return issubclass(subclass, str)

    @classmethod
    def validate(cls, value: object) -> None:
        if not isinstance(value, str):
            raise cls._invalid_type(value)


####
# type proxies for multiple type-checking in XPath expressions
class NumericTypeMeta(type):
    """Metaclass for checking numeric classes and instances."""

    def __instancecheck__(cls, instance: object) -> bool:
        return isinstance(instance, (int, float, Decimal)) and not isinstance(instance, bool)

    def __subclasscheck__(cls, subclass: type) -> bool:
        if issubclass(subclass, bool):
            return False
        return issubclass(subclass, int) or issubclass(subclass, float) \
            or issubclass(subclass, Decimal)


class NumericProxy(metaclass=NumericTypeMeta):
    """Proxy for xs:numeric related types. Builds xs:float instances."""

    def __new__(cls, *args: FloatArgType, **kwargs: FloatArgType) -> float:  # type: ignore[misc]
        return float(*args, **kwargs)


class ArithmeticTypeMeta(type):
    """Metaclass for checking numeric, datetime and duration classes/instances."""

    def __instancecheck__(cls, instance: object) -> bool:
        return isinstance(
            instance, (int, float, Decimal, AbstractDateTime, Duration, UntypedAtomic)
        ) and not isinstance(instance, bool)

    def __subclasscheck__(cls, subclass: type) -> bool:
        if issubclass(subclass, bool):
            return False
        return issubclass(subclass, int) or issubclass(subclass, float) or \
            issubclass(subclass, Decimal) or issubclass(subclass, Duration) \
            or issubclass(subclass, AbstractDateTime) or issubclass(subclass, UntypedAtomic)


class ArithmeticProxy(metaclass=ArithmeticTypeMeta):
    """Proxy for arithmetic related types. Builds xs:float instances."""

    def __new__(cls, *args: FloatArgType, **kwargs: FloatArgType) -> float:  # type: ignore[misc]
        return float(*args, **kwargs)


class ErrorProxy(AnyAtomicType):
    """Class for XPath xs:error datatype. Accepts `None` or an empty sequence."""
    name = 'error'

    def __new__(cls, value: Any) -> list[NoReturn] | None:  # type: ignore[misc]
        if value is None or value == []:
            return cast(list[NoReturn] | None, value)
        msg = f"Cast {value!r} to xs:error is not possible"
        if isinstance(value, list):
            raise cls._invalid_value(msg)
        raise cls._invalid_type(msg)

    def __init__(self, value: Any) -> None:
        pass
