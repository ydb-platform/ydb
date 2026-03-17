import decimal
import numbers
from .base import Trafaret, TrafaretMeta
from .lib import (
    py3metafix,
    STR_TYPES,
)
from . import codes


class NumberMeta(TrafaretMeta):
    """
    Allows slicing syntax for min and max arguments for
    number trafarets

    >>> Int[1:]
    <Int(gte=1)>
    >>> Int[1:10]
    <Int(gte=1, lte=10)>
    >>> Int[:10]
    <Int(lte=10)>
    >>> Float[1:]
    <Float(gte=1)>
    >>> Int > 3
    <Int(gt=3)>
    >>> 1 < (Float < 10)
    <Float(gt=1, lt=10)>
    >>> (Int > 5).check(10)
    10
    >>> extract_error(Int > 5, 1)
    'value should be greater than 5'
    >>> (Int < 3).check(1)
    1
    >>> extract_error(Int < 3, 3)
    'value should be less than 3'
    """

    def __getitem__(cls, slice_):
        return cls(gte=slice_.start, lte=slice_.stop)

    def __lt__(cls, lt):
        return cls(lt=lt)

    def __le__(cls, lte):
        return cls(lte=lte)

    def __gt__(cls, gt):
        return cls(gt=gt)

    def __ge__(cls, gte):
        return cls(gte=gte)


@py3metafix
class Float(Trafaret):
    """
    Tests that value is a float or a string that is convertable to float.

    >>> Float()
    <Float>
    >>> Float(gte=1)
    <Float(gte=1)>
    >>> Float(lte=10)
    <Float(lte=10)>
    >>> Float(gte=1, lte=10)
    <Float(gte=1, lte=10)>
    >>> Float().check(1.0)
    1.0
    >>> extract_error(Float(), 1 + 3j)
    'value is not float'
    >>> extract_error(Float(), 1)
    1.0
    >>> Float(gte=2).check(3.0)
    3.0
    >>> extract_error(Float(gte=2), 1.0)
    'value is less than 2'
    >>> Float(lte=10).check(5.0)
    5.0
    >>> extract_error(Float(lte=3), 5.0)
    'value is greater than 3'
    >>> Float().check("5.0")
    5.0
    """

    __metaclass__ = NumberMeta

    convertable = STR_TYPES + (numbers.Real,)
    value_type = float

    def __init__(self, gte=None, lte=None, gt=None, lt=None):
        self.gte = gte
        self.lte = lte
        self.gt = gt
        self.lt = lt

    def _converter(self, value):
        if not isinstance(value, self.convertable):
            self._failure(
                'value is not %s' % self.value_type.__name__,
                value=value,
                code=codes.WRONG_TYPE,
            )
        try:
            return self.value_type(value)
        except ValueError:
            self._failure(
                "value can't be converted to %s" % self.value_type.__name__,
                value=value,
                code=codes.IS_NOT_A_NUMBER,
            )

    def _check(self, data):
        if not isinstance(data, self.value_type):
            value = self._converter(data)
        else:
            value = data
        if self.gte is not None and value < self.gte:
            self._failure("value is less than %s" % self.gte, value=data, code=codes.TOO_SMALL)
        if self.lte is not None and value > self.lte:
            self._failure("value is greater than %s" % self.lte, value=data, code=codes.TOO_BIG)
        if self.lt is not None and value >= self.lt:
            self._failure("value should be less than %s" % self.lt, value=data, code=codes.TOO_BIG)
        if self.gt is not None and value <= self.gt:
            self._failure("value should be greater than %s" % self.gt, value=data, code=codes.TOO_SMALL)
        return value

    def check_and_return(self, data):
        self._check(data)
        return data

    def __lt__(self, lt):
        return type(self)(gte=self.gte, lte=self.lte, gt=self.gt, lt=lt)

    def __le__(self, lte):
        return type(self)(gte=self.gte, lte=lte, gt=self.gt, lt=self.lt)

    def __gt__(self, gt):
        return type(self)(gte=self.gte, lte=self.lte, gt=gt, lt=self.lt)

    def __ge__(self, gte):
        return type(self)(gte=gte, lte=self.lte, gt=self.gt, lt=self.lt)

    def __repr__(self):
        r = "<%s" % type(self).__name__
        options = []
        for param in ("gte", "lte", "gt", "lt"):
            if getattr(self, param) is not None:
                options.append("%s=%s" % (param, getattr(self, param)))
        if options:
            r += "(%s)" % (", ".join(options))
        r += ">"
        return r


class ToFloat(Float):
    """Checks that value is a float.
    Or if value is a string converts this string to float
    """
    def check_and_return(self, data):
        return self._check(data)


class Int(Float):
    """
    >>> Int()
    <Int>
    >>> Int().check(5)
    5
    >>> extract_error(Int(), 1.1)
    'value is not int'
    >>> extract_error(Int(), 1 + 1j)
    'value is not int'
    """

    value_type = int

    def _converter(self, value):
        if isinstance(value, float):
            if not value.is_integer():
                self._failure('value is not int', value=value, code=codes.IS_NOT_INT)
        return super(Int, self)._converter(value)


class ToInt(Int):
    def check_and_return(self, data):
        return self._check(data)


class ToDecimal(Float):
    value_type = decimal.Decimal

    def check_and_return(self, data):
        return self._check(data)

    def _converter(self, value):
        try:
            return self.value_type(value)
        except (ValueError, decimal.InvalidOperation):
            self._failure(
                'value can\'t be converted to Decimal',
                value=value,
                code=codes.INVALID_DECIMAL,
            )
