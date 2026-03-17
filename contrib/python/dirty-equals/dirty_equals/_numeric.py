import math
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Optional, TypeVar, Union

from ._base import DirtyEquals

__all__ = (
    'IsApprox',
    'IsNumeric',
    'IsNumber',
    'IsPositive',
    'IsNegative',
    'IsNonPositive',
    'IsNonNegative',
    'IsInt',
    'IsPositiveInt',
    'IsNegativeInt',
    'IsFloat',
    'IsPositiveFloat',
    'IsNegativeFloat',
)

from ._utils import Omit

AnyNumber = Union[int, float, Decimal]
N = TypeVar('N', int, float, Decimal, date, datetime, AnyNumber)


class IsNumeric(DirtyEquals[N]):
    """
    Base class for all numeric types, `IsNumeric` implements approximate and inequality comparisons,
    as well as the type checks.

    This class can be used directly or via any of its subclasses.
    """

    allowed_types: Union[type[N], tuple[type, ...]] = (int, float, Decimal, date, datetime)
    """It allows any of the types supported in its subclasses."""

    def __init__(
        self,
        *,
        exactly: Optional[N] = None,
        approx: Optional[N] = None,
        delta: Optional[N] = None,
        gt: Optional[N] = None,
        lt: Optional[N] = None,
        ge: Optional[N] = None,
        le: Optional[N] = None,
    ):
        """
        Args:
            exactly: A value to exactly compare to - useful when you want to make sure a value is an `int` or `float`,
                while also checking its value.
            approx: A value to approximately compare to.
            delta: The allowable different when comparing to the value to `approx`,
                if omitted `value / 100` is used except for datetimes where 2 seconds is used.
            gt: Value which the compared value should be greater than.
            lt: Value which the compared value should be less than.
            ge: Value which the compared value should be greater than or equal to.
            le: Value which the compared value should be less than or equal to.

        If not values are provided, only the type is checked.

        If `approx` is provided as well a `gt`, `lt`, `ge`, or `le`, a `TypeError` is raised.

        Example of direct usage:

        ```py title="IsNumeric"
        from datetime import datetime

        from dirty_equals import IsNumeric

        assert 1.0 == IsNumeric
        assert 4 == IsNumeric(gt=3)
        d = datetime(2020, 1, 1, 12, 0, 0)
        assert d == IsNumeric(approx=datetime(2020, 1, 1, 12, 0, 1))
        ```
        """
        self.exactly: Optional[N] = exactly
        if self.exactly is not None and (gt, lt, ge, le) != (None, None, None, None):
            raise TypeError('"exactly" cannot be combined with "gt", "lt", "ge", or "le"')
        if self.exactly is not None and approx is not None:
            raise TypeError('"exactly" cannot be combined with "approx"')
        self.approx: Optional[N] = approx
        if self.approx is not None and (gt, lt, ge, le) != (None, None, None, None):
            raise TypeError('"approx" cannot be combined with "gt", "lt", "ge", or "le"')
        self.delta: Optional[N] = delta
        self.gt: Optional[N] = gt
        self.lt: Optional[N] = lt
        self.ge: Optional[N] = ge
        self.le: Optional[N] = le
        self.has_bounds_checks = not all(f is None for f in (exactly, approx, delta, gt, lt, ge, le))
        kwargs = {
            'exactly': Omit if exactly is None else exactly,
            'approx': Omit if approx is None else approx,
            'delta': Omit if delta is None else delta,
            'gt': Omit if gt is None else gt,
            'lt': Omit if lt is None else lt,
            'ge': Omit if ge is None else ge,
            'le': Omit if le is None else le,
        }
        super().__init__(**kwargs)

    def prepare(self, other: Any) -> N:
        if other is True or other is False:
            raise TypeError('booleans are not numbers')
        elif not isinstance(other, self.allowed_types):
            raise TypeError(f'not a {self.allowed_types}')
        else:
            return other

    def equals(self, other: Any) -> bool:
        other = self.prepare(other)

        if self.has_bounds_checks:
            return self.bounds_checks(other)
        else:
            return True

    def bounds_checks(self, other: N) -> bool:
        if self.exactly is not None:
            return self.exactly == other
        elif self.approx is not None:
            if self.delta is None:
                if isinstance(other, date):
                    delta: Any = timedelta(seconds=2)
                else:
                    delta = abs(other / 100)
            else:
                delta = self.delta
            return self.approx_equals(other, delta)
        elif self.gt is not None and not other > self.gt:
            return False
        elif self.lt is not None and not other < self.lt:
            return False
        elif self.ge is not None and not other >= self.ge:
            return False
        elif self.le is not None and not other <= self.le:
            return False
        else:
            return True

    def approx_equals(self, other: Any, delta: Any) -> bool:
        return abs(self.approx - other) <= delta


class IsNumber(IsNumeric[AnyNumber]):
    """
    Base class for all types that can be used with all number types, e.g. numeric but not `date` or `datetime`.

    Inherits from [`IsNumeric`][dirty_equals.IsNumeric] and can therefore be initialised with any of its arguments.
    """

    allowed_types = int, float, Decimal
    """
    It allows any of the number types.
    """


Num = TypeVar('Num', int, float, Decimal)


class IsApprox(IsNumber):
    """
    Simplified subclass of [`IsNumber`][dirty_equals.IsNumber] that only allows approximate comparisons.
    """

    def __init__(self, approx: Num, *, delta: Optional[Num] = None):
        """
        Args:
            approx: A value to approximately compare to.
            delta: The allowable different when comparing to the value to `approx`, if omitted `value / 100` is used.

        ```py title="IsApprox"
        from dirty_equals import IsApprox

        assert 1.0 == IsApprox(1)
        assert 123 == IsApprox(120, delta=4)
        assert 201 == IsApprox(200)
        assert 201 != IsApprox(200, delta=0.1)
        ```
        """
        super().__init__(approx=approx, delta=delta)


class IsPositive(IsNumber):
    """
    Check that a value is positive (`> 0`), can be an `int`, a `float` or a `Decimal`
    (or indeed any value which implements `__gt__` for `0`).

    ```py title="IsPositive"
    from decimal import Decimal

    from dirty_equals import IsPositive

    assert 1.0 == IsPositive
    assert 1 == IsPositive
    assert Decimal('3.14') == IsPositive
    assert 0 != IsPositive
    assert -1 != IsPositive
    ```
    """

    def __init__(self) -> None:
        super().__init__(gt=0)
        self._repr_kwargs = {}


class IsNegative(IsNumber):
    """
    Check that a value is negative (`< 0`), can be an `int`, a `float` or a `Decimal`
    (or indeed any value which implements `__lt__` for `0`).

    ```py title="IsNegative"
    from decimal import Decimal

    from dirty_equals import IsNegative

    assert -1.0 == IsNegative
    assert -1 == IsNegative
    assert Decimal('-3.14') == IsNegative
    assert 0 != IsNegative
    assert 1 != IsNegative
    ```
    """

    def __init__(self) -> None:
        super().__init__(lt=0)
        self._repr_kwargs = {}


class IsNonNegative(IsNumber):
    """
    Check that a value is positive or zero (`>= 0`), can be an `int`, a `float` or a `Decimal`
    (or indeed any value which implements `__ge__` for `0`).

    ```py title="IsNonNegative"
    from decimal import Decimal

    from dirty_equals import IsNonNegative

    assert 1.0 == IsNonNegative
    assert 1 == IsNonNegative
    assert Decimal('3.14') == IsNonNegative
    assert 0 == IsNonNegative
    assert -1 != IsNonNegative
    assert Decimal('0') == IsNonNegative
    ```
    """

    def __init__(self) -> None:
        super().__init__(ge=0)
        self._repr_kwargs = {}


class IsNonPositive(IsNumber):
    """
    Check that a value is negative or zero (`<=0`), can be an `int`, a `float` or a `Decimal`
    (or indeed any value which implements `__le__` for `0`).

    ```py title="IsNonPositive"
    from decimal import Decimal

    from dirty_equals import IsNonPositive

    assert -1.0 == IsNonPositive
    assert -1 == IsNonPositive
    assert Decimal('-3.14') == IsNonPositive
    assert 0 == IsNonPositive
    assert 1 != IsNonPositive
    assert Decimal('-0') == IsNonPositive
    assert Decimal('0') == IsNonPositive
    ```
    """

    def __init__(self) -> None:
        super().__init__(le=0)
        self._repr_kwargs = {}


class IsInt(IsNumeric[int]):
    """
    Checks that a value is an integer.

    Inherits from [`IsNumeric`][dirty_equals.IsNumeric] and can therefore be initialised with any of its arguments.

    ```py title="IsInt"
    from dirty_equals import IsInt

    assert 1 == IsInt
    assert -2 == IsInt
    assert 1.0 != IsInt
    assert 'foobar' != IsInt
    assert True != IsInt
    assert 1 == IsInt(exactly=1)
    assert -2 != IsInt(exactly=1)
    ```
    """

    allowed_types = int
    """
    As the name suggests, only integers are allowed, booleans (`True` are `False`) are explicitly excluded although
    technically they are sub-types of `int`.
    """


class IsPositiveInt(IsInt):
    """
    Like [`IsPositive`][dirty_equals.IsPositive] but only for `int`s.

    ```py title="IsPositiveInt"
    from decimal import Decimal

    from dirty_equals import IsPositiveInt

    assert 1 == IsPositiveInt
    assert 1.0 != IsPositiveInt
    assert Decimal('3.14') != IsPositiveInt
    assert 0 != IsPositiveInt
    assert -1 != IsPositiveInt
    ```
    """

    def __init__(self) -> None:
        super().__init__(gt=0)
        self._repr_kwargs = {}


class IsNegativeInt(IsInt):
    """
    Like [`IsNegative`][dirty_equals.IsNegative] but only for `int`s.

    ```py title="IsNegativeInt"
    from decimal import Decimal

    from dirty_equals import IsNegativeInt

    assert -1 == IsNegativeInt
    assert -1.0 != IsNegativeInt
    assert Decimal('-3.14') != IsNegativeInt
    assert 0 != IsNegativeInt
    assert 1 != IsNegativeInt
    ```
    """

    def __init__(self) -> None:
        super().__init__(lt=0)
        self._repr_kwargs = {}


class IsFloat(IsNumeric[float]):
    """
    Checks that a value is a float.

    Inherits from [`IsNumeric`][dirty_equals.IsNumeric] and can therefore be initialised with any of its arguments.

    ```py title="IsFloat"
    from dirty_equals import IsFloat

    assert 1.0 == IsFloat
    assert 1 != IsFloat
    assert 1.0 == IsFloat(exactly=1.0)
    assert 1.001 != IsFloat(exactly=1.0)
    ```
    """

    allowed_types = float
    """
    As the name suggests, only floats are allowed.
    """


class IsPositiveFloat(IsFloat):
    """
    Like [`IsPositive`][dirty_equals.IsPositive] but only for `float`s.

    ```py title="IsPositiveFloat"
    from decimal import Decimal

    from dirty_equals import IsPositiveFloat

    assert 1.0 == IsPositiveFloat
    assert 1 != IsPositiveFloat
    assert Decimal('3.14') != IsPositiveFloat
    assert 0.0 != IsPositiveFloat
    assert -1.0 != IsPositiveFloat
    ```
    """

    def __init__(self) -> None:
        super().__init__(gt=0)
        self._repr_kwargs = {}


class IsNegativeFloat(IsFloat):
    """
    Like [`IsNegative`][dirty_equals.IsNegative] but only for `float`s.

    ```py title="IsNegativeFloat"
    from decimal import Decimal

    from dirty_equals import IsNegativeFloat

    assert -1.0 == IsNegativeFloat
    assert -1 != IsNegativeFloat
    assert Decimal('-3.14') != IsNegativeFloat
    assert 0.0 != IsNegativeFloat
    assert 1.0 != IsNegativeFloat
    ```
    """

    def __init__(self) -> None:
        super().__init__(lt=0)
        self._repr_kwargs = {}


class IsFloatInf(IsFloat):
    """
    Checks that a value is float and infinite (positive or negative).

    Inherits from [`IsFloat`][dirty_equals.IsFloat].

    ```py title="IsFloatInf"
    from dirty_equals import IsFloatInf

    assert float('inf') == IsFloatInf
    assert float('-inf') == IsFloatInf
    assert 1.0 != IsFloatInf
    ```
    """

    def equals(self, other: Any) -> bool:
        other = self.prepare(other)
        return math.isinf(other)


class IsFloatInfPos(IsFloatInf):
    """
    Checks that a value is float and positive infinite.

    Inherits from [`IsFloatInf`][dirty_equals.IsFloatInf].

    ```py title="IsFloatInfPos"
    from dirty_equals import IsFloatInfPos

    assert float('inf') == IsFloatInfPos
    assert -float('-inf') == IsFloatInfPos
    assert -float('inf') != IsFloatInfPos
    assert float('-inf') != IsFloatInfPos
    ```
    """

    def __init__(self) -> None:
        super().__init__(gt=0)
        self._repr_kwargs = {}

    def equals(self, other: Any) -> bool:
        return self.bounds_checks(other) and super().equals(other)


class IsFloatInfNeg(IsFloatInf):
    """
    Checks that a value is float and negative infinite.

    Inherits from [`IsFloatInf`][dirty_equals.IsFloatInf].

    ```py title="IsFloatInfNeg"
    from dirty_equals import IsFloatInfNeg

    assert -float('inf') == IsFloatInfNeg
    assert float('-inf') == IsFloatInfNeg
    assert float('inf') != IsFloatInfNeg
    assert -float('-inf') != IsFloatInfNeg
    ```
    """

    def __init__(self) -> None:
        super().__init__(lt=0)
        self._repr_kwargs = {}

    def equals(self, other: Any) -> bool:
        return self.bounds_checks(other) and super().equals(other)


class IsFloatNan(IsFloat):
    """
    Checks that a value is float and nan (not a number).

    Inherits from [`IsFloat`][dirty_equals.IsFloat].

    ```py title="IsFloatNan"
    from dirty_equals import IsFloatNan

    assert float('nan') == IsFloatNan
    assert 1.0 != IsFloatNan
    ```
    """

    def equals(self, other: Any) -> bool:
        other = self.prepare(other)
        return math.isnan(other)
