#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
import math
from typing import Any, Optional, SupportsInt, Union

from elementpath.aliases import XPath2ParserType
from elementpath.helpers import FloatArgType, NUMERIC_INF_OR_NAN, INVALID_NUMERIC, \
    LazyPattern, collapse_white_spaces
from .any_types import AnyAtomicType

__all__ = ['Float', 'Float10', 'Integer', 'Int', 'Long',
           'NegativeInteger', 'PositiveInteger', 'NonNegativeInteger',
           'NonPositiveInteger', 'Short', 'Byte', 'UnsignedByte',
           'UnsignedInt', 'UnsignedLong', 'UnsignedShort']


class Float(float, AnyAtomicType):
    name = 'float'
    pattern = LazyPattern(
        r'^(?:[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[Ee][+-]?[0-9]+)? |[+-]?INF|NaN)$'
    )

    @classmethod
    def make(cls, value: Any,
             parser: XPath2ParserType | None = None,
             xsd_version: str | None = None,
             **kwargs: Any) -> 'Float':
        if cls.__name__.endswith('10'):
            return Float(value, '1.0')
        elif parser is not None:
            return Float(value, parser.xsd_version)
        else:
            return Float(value, xsd_version)

    def __new__(cls, value: FloatArgType | str, xsd_version: str | None = None) -> 'Float':
        if isinstance(value, str):
            value = collapse_white_spaces(value)
            if value in NUMERIC_INF_OR_NAN:
                if xsd_version == '1.0' and value == '+INF':
                    raise cls._invalid_value(value)
                elif value == 'NaN':
                    try:
                        return float_nan
                    except NameError:
                        pass
            elif value.lower() in INVALID_NUMERIC:
                raise cls._invalid_value(value)
        elif math.isnan(value):
            try:
                return float_nan
            except NameError:  # pragma: no cover
                pass

        _value = super().__new__(cls, value)
        if _value > 3.4028235E38:
            return super().__new__(cls, 'INF')
        elif _value < -3.4028235E38:
            return super().__new__(cls, '-INF')
        elif -1e-37 < _value < 1e-37:
            return super().__new__(cls, -0.0 if str(_value).startswith('-') else 0.0)
        return _value

    def __init__(self, value: FloatArgType, xsd_version: str | None = None) -> None:
        float.__init__(self)

    def __hash__(self) -> int:
        return super(Float, self).__hash__()

    def __eq__(self, other: object) -> bool:
        if isinstance(other, self.__class__):
            if super(Float, self).__eq__(other):
                return True
            return math.isclose(self, other, rel_tol=1e-7, abs_tol=0.0)
        return super(Float, self).__eq__(other)

    def __ne__(self, other: object) -> bool:
        if isinstance(other, self.__class__):
            if super(Float, self).__eq__(other):
                return False
            return not math.isclose(self, other, rel_tol=1e-7, abs_tol=0.0)
        return super(Float, self).__ne__(other)

    def __add__(self, other: object) -> Union[float, 'Float']:
        if isinstance(other, (self.__class__, int)) and not isinstance(other, bool):
            return self.__class__(super(Float, self).__add__(other))
        elif isinstance(other, float):
            return super(Float, self).__add__(other)
        return NotImplemented

    def __radd__(self, other: object) -> Union[float, 'Float']:
        if isinstance(other, (self.__class__, int)) and not isinstance(other, bool):
            return self.__class__(super(Float, self).__radd__(other))
        elif isinstance(other, float):
            return super(Float, self).__radd__(other)
        return NotImplemented

    def __sub__(self, other: object) -> Union[float, 'Float']:
        if isinstance(other, (self.__class__, int)) and not isinstance(other, bool):
            return self.__class__(super(Float, self).__sub__(other))
        elif isinstance(other, float):
            return super(Float, self).__sub__(other)
        return NotImplemented

    def __rsub__(self, other: object) -> Union[float, 'Float']:
        if isinstance(other, (self.__class__, int)) and not isinstance(other, bool):
            return self.__class__(super(Float, self).__rsub__(other))
        elif isinstance(other, float):
            return super(Float, self).__rsub__(other)
        return NotImplemented

    def __mul__(self, other: object) -> Union[float, 'Float']:
        if isinstance(other, (self.__class__, int)) and not isinstance(other, bool):
            return self.__class__(super(Float, self).__mul__(other))
        elif isinstance(other, float):
            return super(Float, self).__mul__(other)
        return NotImplemented

    def __rmul__(self, other: object) -> Union[float, 'Float']:
        if isinstance(other, (self.__class__, int)) and not isinstance(other, bool):
            return self.__class__(super(Float, self).__rmul__(other))
        elif isinstance(other, float):
            return super(Float, self).__rmul__(other)
        return NotImplemented

    def __truediv__(self, other: object) -> Union[float, 'Float']:
        if isinstance(other, (self.__class__, int)) and not isinstance(other, bool):
            return self.__class__(super(Float, self).__truediv__(other))
        elif isinstance(other, float):
            return super(Float, self).__truediv__(other)
        return NotImplemented

    def __rtruediv__(self, other: object) -> Union[float, 'Float']:
        if isinstance(other, (self.__class__, int)) and not isinstance(other, bool):
            return self.__class__(super(Float, self).__rtruediv__(other))
        elif isinstance(other, float):
            return super(Float, self).__rtruediv__(other)
        return NotImplemented

    def __mod__(self, other: object) -> Union[float, 'Float']:
        if isinstance(other, (self.__class__, int)) and not isinstance(other, bool):
            return self.__class__(super(Float, self).__mod__(other))
        elif isinstance(other, float):
            return super(Float, self).__mod__(other)
        return NotImplemented

    def __rmod__(self, other: object) -> Union[float, 'Float']:
        if isinstance(other, (self.__class__, int)) and not isinstance(other, bool):
            return self.__class__(super(Float, self).__rmod__(other))
        elif isinstance(other, float):
            return super(Float, self).__rmod__(other)
        return NotImplemented

    def __abs__(self) -> Union['Float']:
        return self.__class__(super(Float, self).__abs__())


class Float10(Float):
    """xs:float for XSD 1.0"""
    def __new__(cls, value: FloatArgType | str,  # type: ignore[misc]
                xsd_version: str | None = '1.0') -> 'Float':
        return Float(value, xsd_version=xsd_version)


# The instance used for xs:float NaN values in order to keep identity
float_nan = Float('NaN')


class Integer(int, AnyAtomicType):
    name = 'integer'
    pattern = LazyPattern(r'^[\-+]?[0-9]+$')

    _lower_bound: Optional[int] = None
    _higher_bound: Optional[int] = None

    def __init__(self, value: Union[str, SupportsInt]) -> None:
        """
        :param value: a string or an integer compatible value.
        """
        if self._lower_bound is not None and self < self._lower_bound:
            raise ValueError("value {} is too low for {!r}".format(value, self.__class__))
        elif self._higher_bound is not None and self >= self._higher_bound:
            raise ValueError("value {} is too high for {!r}".format(value, self.__class__))
        int.__init__(self)

    @classmethod
    def __subclasshook__(cls, subclass: type[Any]) -> bool:
        if cls is Integer:
            return issubclass(subclass, int) and not issubclass(subclass, bool)
        return NotImplemented  # type: ignore[no-any-return,unused-ignore]

    @classmethod
    def validate(cls, value: object) -> None:
        if isinstance(value, cls):
            return
        elif isinstance(value, str):
            if cls.pattern.match(value) is None:
                raise cls._invalid_value(value)
        else:
            raise cls._invalid_type(value)


class NonPositiveInteger(Integer):
    name = 'nonPositiveInteger'
    _lower_bound, _higher_bound = None, 1


class NegativeInteger(NonPositiveInteger):
    name = 'negativeInteger'
    _lower_bound, _higher_bound = None, 0


class Long(Integer):
    name = 'long'
    _lower_bound, _higher_bound = -2 ** 63, 2 ** 63


class Int(Long):
    name = 'int'
    _lower_bound, _higher_bound = -2 ** 31, 2 ** 31


class Short(Int):
    name = 'short'
    _lower_bound, _higher_bound = -2 ** 15, 2 ** 15


class Byte(Short):
    name = 'byte'
    _lower_bound, _higher_bound = -2 ** 7, 2 ** 7


class NonNegativeInteger(Integer):
    name = 'nonNegativeInteger'
    _lower_bound = 0
    _higher_bound: Optional[int] = None


class PositiveInteger(NonNegativeInteger):
    name = 'positiveInteger'
    _lower_bound, _higher_bound = 1, None


class UnsignedLong(NonNegativeInteger):
    name = 'unsignedLong'
    _lower_bound, _higher_bound = 0, 2 ** 64


class UnsignedInt(UnsignedLong):
    name = 'unsignedInt'
    _lower_bound, _higher_bound = 0, 2 ** 32


class UnsignedShort(UnsignedInt):
    name = 'unsignedShort'
    _lower_bound, _higher_bound = 0, 2 ** 16


class UnsignedByte(UnsignedShort):
    name = 'unsignedByte'
    _lower_bound, _higher_bound = 0, 2 ** 8
