#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
import operator
from collections.abc import Callable
from decimal import Decimal
from typing import Any, cast, Union

from elementpath.aliases import XPath2ParserType
from elementpath.helpers import reversed_sub, reversed_truediv, BOOLEAN_VALUES, get_double
from .any_types import AnyAtomicType

UntypedArgType = Union[str, bytes, bool, float, Decimal, 'UntypedAtomic', AnyAtomicType]

__all__ = ['UntypedAtomic']


class UntypedAtomic(AnyAtomicType):
    """
    Class for xs:untypedAtomic data. Provides special methods for comparing
    and converting to basic data types.

    :param value: the untyped value, usually a string.
    :param parser: the XPath parser that creates the instance, if any.
    """
    name = 'untypedAtomic'
    value: str

    __slots__ = ('value', '_xsd_version', 'parser')

    @classmethod
    def make(cls, value: Any, **kwargs: Any) -> 'UntypedAtomic':
        return cls(value, kwargs.get('parser'))

    @classmethod
    def validate(cls, value: object) -> None:
        if not isinstance(value, cls):
            raise cls._invalid_type(value)

    def __init__(self, value: UntypedArgType,
                 parser: XPath2ParserType | None = None) -> None:
        match value:
            case str():
                self.value = value
            case bytes():
                self.value = value.decode('utf-8')
            case bool():
                self.value = 'true' if value else 'false'
            case float():
                self.value = str(value).rstrip('0').rstrip('.')
            case Decimal():
                self.value = str(value.normalize())
            case UntypedAtomic():
                self.value = value.value
            case None:
                raise TypeError("{!r} is not an atomic value".format(value))
            case AnyAtomicType():
                self.value = str(value)
            case _:
                raise TypeError("{!r} is not an atomic value".format(value))

        self.parser = parser
        self._xsd_version = parser.xsd_version if parser is not None else None

    def __repr__(self) -> str:
        return '%s(%r)' % (self.__class__.__name__, self.value)

    def _operator(self, op: Callable[[Any, Any], bool],
                  other: object, force_float: bool = True) -> bool:
        """
        Returns a couple of operands, applying a cast to the instance value based on
        the type of the *other* argument.

        :param other: The other operand, that determines the cast for the untyped instance.
        :param force_float: Force a conversion to float if *other* is an UntypedAtomic instance.
        :return: A couple of values.
        """
        match other:
            case UntypedAtomic():
                if force_float:
                    return op(get_double(self.value, self._xsd_version),
                              get_double(other.value, self._xsd_version))
                return op(self.value, other.value)
            case bool():
                # Cast to xs:boolean
                value = self.value.strip()
                if value not in BOOLEAN_VALUES:
                    raise ValueError("{!r} cannot be cast to xs:boolean".format(self.value))
                return op(value in ('1', 'true'), other)
            case int():
                return op(get_double(self.value, self._xsd_version), other)
            case None | str() | list():
                return op(self.value, other)
            case AnyAtomicType():
                if hasattr(other, 'make'):
                    return op(type(other).make(self.value, parser=self.parser), other)
                else:
                    return op(type(other)(self.value), other)
            case _:
                return cast(bool, NotImplemented)

    def __hash__(self) -> int:
        return hash(self.value)

    def __eq__(self, other: Any) -> bool:
        return self._operator(operator.eq, other, force_float=False)

    def __ne__(self, other: Any) -> bool:
        return not self._operator(operator.eq, other, force_float=False)

    def __lt__(self, other: Any) -> bool:
        return self._operator(operator.lt, other)

    def __le__(self, other: Any) -> bool:
        return self._operator(operator.le, other)

    def __gt__(self, other: Any) -> bool:
        return self._operator(operator.gt, other)

    def __ge__(self, other: Any) -> bool:
        return self._operator(operator.ge, other)

    def __add__(self, other: Any) -> bool:
        return self._operator(operator.add, other)
    __radd__ = __add__

    def __sub__(self, other: Any) -> Any:
        return self._operator(operator.sub, other)

    def __rsub__(self, other: Any) -> Any:
        return self._operator(reversed_sub, other)

    def __mul__(self, other: Any) -> Any:
        return self._operator(operator.mul, other)
    __rmul__ = __mul__

    def __truediv__(self, other: Any) -> Any:
        return self._operator(operator.truediv, other)

    def __rtruediv__(self, other: Any) -> Any:
        return self._operator(reversed_truediv, other)

    def __int__(self) -> int:
        return int(self.value)

    def __float__(self) -> float:
        return get_double(self.value, self._xsd_version)

    def __bool__(self) -> bool:
        return bool(self.value)  # For effective boolean value, not for cast to xs:boolean.

    def __abs__(self) -> Decimal:
        return abs(Decimal(self.value))

    def __mod__(self, other: Any) -> Any:
        return self._operator(operator.mod, other)

    def __round__(self, n: int | None = None) -> float:
        return round(float(self.value), ndigits=n)

    def __str__(self) -> str:
        return self.value

    def __bytes__(self) -> bytes:
        return bytes(self.value, encoding='utf-8')
