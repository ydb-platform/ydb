from __future__ import annotations

import abc
from datetime import datetime
from datetime import time
from datetime import timedelta
from decimal import Decimal
from typing import Any
from typing import cast
from typing import Dict
from typing import Generic
from typing import List
from typing import Optional
from typing import Tuple
from typing import TypeVar
from typing import Union

PythonTemporalType = TypeVar("PythonTemporalType", bound=Union[time, datetime])
POWERS_OF_TEN: Dict[int, Decimal] = {i: Decimal(10**i) for i in range(0, 13)}
MAX_PYTHON_TEMPORAL_PRECISION_POWER = 6
MAX_PYTHON_TEMPORAL_PRECISION = POWERS_OF_TEN[MAX_PYTHON_TEMPORAL_PRECISION_POWER]


class TemporalType(Generic[PythonTemporalType], metaclass=abc.ABCMeta):
    def __init__(self, whole_python_temporal_value: PythonTemporalType, remaining_fractional_seconds: Decimal):
        self._whole_python_temporal_value = whole_python_temporal_value
        self._remaining_fractional_seconds = remaining_fractional_seconds

    @abc.abstractmethod
    def new_instance(self, value: PythonTemporalType, fraction: Decimal) -> TemporalType[PythonTemporalType]:
        pass

    @abc.abstractmethod
    def to_python_type(self) -> PythonTemporalType:
        pass

    def round_to(self, precision: int) -> TemporalType[PythonTemporalType]:
        """
            Python datetime and time only support up to microsecond precision
            In case the supplied value exceeds the specified precision,
            the value needs to be rounded.
        """
        precision = min(precision, MAX_PYTHON_TEMPORAL_PRECISION_POWER)
        remaining_fractional_seconds = self._remaining_fractional_seconds
        # exponent can return `n`, `N`, `F` too if the value is a NaN for example
        digits = abs(remaining_fractional_seconds.as_tuple().exponent)  # type: ignore
        if digits > precision:
            rounding_factor = POWERS_OF_TEN[precision]
            rounded = remaining_fractional_seconds.quantize(Decimal(1 / rounding_factor))
            return self.new_instance(self._whole_python_temporal_value, rounded)
        return self

    @abc.abstractmethod
    def add_time_delta(self, time_delta: timedelta) -> PythonTemporalType:
        """
            This method shall be overriden to implement fraction arithmetics.
        """
        pass


class Time(TemporalType[time]):
    def new_instance(self, value: time, fraction: Decimal) -> TemporalType[time]:
        return Time(value, fraction)

    def to_python_type(self) -> time:
        if self._remaining_fractional_seconds > 0:
            time_delta = timedelta(microseconds=int(self._remaining_fractional_seconds * MAX_PYTHON_TEMPORAL_PRECISION))
            return self.add_time_delta(time_delta)
        return self._whole_python_temporal_value

    def add_time_delta(self, time_delta: timedelta) -> time:
        time_delta_added = datetime.combine(datetime(1, 1, 1), self._whole_python_temporal_value) + time_delta
        return time_delta_added.time().replace(tzinfo=self._whole_python_temporal_value.tzinfo)


class TimeWithTimeZone(Time, TemporalType[time]):
    def new_instance(self, value: time, fraction: Decimal) -> TemporalType[time]:
        return TimeWithTimeZone(value, fraction)


class Timestamp(TemporalType[datetime]):
    def new_instance(self, value: datetime, fraction: Decimal) -> Timestamp:
        return Timestamp(value, fraction)

    def to_python_type(self) -> datetime:
        if self._remaining_fractional_seconds > 0:
            time_delta = timedelta(microseconds=int(self._remaining_fractional_seconds * MAX_PYTHON_TEMPORAL_PRECISION))
            return self.add_time_delta(time_delta)
        return self._whole_python_temporal_value

    def add_time_delta(self, time_delta: timedelta) -> datetime:
        return self._whole_python_temporal_value + time_delta


class TimestampWithTimeZone(Timestamp, TemporalType[datetime]):
    def new_instance(self, value: datetime, fraction: Decimal) -> TimestampWithTimeZone:
        return TimestampWithTimeZone(value, fraction)


class NamedRowTuple(Tuple[Any, ...]):
    """Custom tuple class as namedtuple doesn't support missing or duplicate names"""
    def __new__(cls, values: List[Any], names: List[str], types: List[str]) -> NamedRowTuple:
        return cast(NamedRowTuple, super().__new__(cls, values))

    def __init__(self, values: List[Any], names: List[Optional[str]], types: List[str]):
        self._names = names
        # With names and types users can retrieve the name and Trino data type of a row
        self.__annotations__ = dict()
        self.__annotations__["names"] = names
        self.__annotations__["types"] = types
        elements: List[Any] = []
        for name, value in zip(names, values):
            if name is not None and names.count(name) == 1:
                setattr(self, name, value)
                elements.append(f"{name}: {repr(value)}")
            else:
                elements.append(repr(value))
        self._repr = "(" + ", ".join(elements) + ")"

    def __getattr__(self, name: str) -> Any:
        if self._names.count(name):
            raise ValueError("Ambiguous row field reference: " + name)

    def __getnewargs__(self) -> Any:
        return (tuple(self), (), ())

    def __getstate__(self) -> Any:
        return vars(self)

    def __setstate__(self, state: Any) -> None:
        vars(self).update(state)

    def __repr__(self) -> str:
        return self._repr
