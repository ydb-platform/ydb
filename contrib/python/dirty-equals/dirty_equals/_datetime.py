from __future__ import annotations as _annotations

from datetime import date, datetime, timedelta, timezone, tzinfo
from typing import Any
from zoneinfo import ZoneInfo

from ._numeric import IsNumeric
from ._utils import Omit


class IsDatetime(IsNumeric[datetime]):
    """
    Check if the value is a datetime, and matches the given conditions.
    """

    allowed_types = datetime

    def __init__(
        self,
        *,
        approx: datetime | None = None,
        delta: timedelta | int | float | None = None,
        gt: datetime | None = None,
        lt: datetime | None = None,
        ge: datetime | None = None,
        le: datetime | None = None,
        unix_number: bool = False,
        iso_string: bool = False,
        format_string: str | None = None,
        enforce_tz: bool = True,
    ):
        """
        Args:
            approx: A value to approximately compare to.
            delta: The allowable different when comparing to the value to `approx`, if omitted 2 seconds is used,
                ints and floats are assumed to represent seconds and converted to `timedelta`s.
            gt: Value which the compared value should be greater than (after).
            lt: Value which the compared value should be less than (before).
            ge: Value which the compared value should be greater than (after) or equal to.
            le: Value which the compared value should be less than (before) or equal to.
            unix_number: whether to allow unix timestamp numbers in comparison
            iso_string: whether to allow iso formatted strings in comparison
            format_string: if provided, `format_string` is used with `datetime.strptime` to parse strings
            enforce_tz: whether timezone should be enforced in comparison, see below for more details

        Examples of basic usage:

        ```py title="IsDatetime"
        from datetime import datetime

        from dirty_equals import IsDatetime

        y2k = datetime(2000, 1, 1)
        assert datetime(2000, 1, 1) == IsDatetime(approx=y2k)
        # Note: this requires the system timezone to be UTC
        assert 946684800.123 == IsDatetime(approx=y2k, unix_number=True)
        assert datetime(2000, 1, 1, 0, 0, 9) == IsDatetime(approx=y2k, delta=10)
        assert '2000-01-01T00:00' == IsDatetime(approx=y2k, iso_string=True)

        assert datetime(2000, 1, 2) == IsDatetime(gt=y2k)
        assert datetime(1999, 1, 2) != IsDatetime(gt=y2k)
        ```
        """
        if isinstance(delta, (int, float)):
            delta = timedelta(seconds=delta)

        super().__init__(
            approx=approx,
            delta=delta,  # type: ignore[arg-type]
            gt=gt,
            lt=lt,
            ge=ge,
            le=le,
        )
        self.unix_number = unix_number
        self.iso_string = iso_string
        self.format_string = format_string
        self.enforce_tz = enforce_tz
        self._repr_kwargs.update(
            unix_number=Omit if unix_number is False else unix_number,
            iso_string=Omit if iso_string is False else iso_string,
            format_string=Omit if format_string is None else format_string,
            enforce_tz=Omit if enforce_tz is True else format_string,
        )

    def prepare(self, other: Any) -> datetime:
        if isinstance(other, datetime):
            dt = other
        elif isinstance(other, (float, int)):
            if self.unix_number:
                dt = datetime.fromtimestamp(other)
            else:
                raise TypeError('numbers not allowed')
        elif isinstance(other, str):
            if self.iso_string:
                dt = datetime.fromisoformat(other)
            elif self.format_string:
                dt = datetime.strptime(other, self.format_string)
            else:
                raise ValueError('not a valid datetime string')
        else:
            raise ValueError(f'{type(other)} not valid as datetime')

        if self.approx is not None and not self.enforce_tz and self.approx.tzinfo is None and dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        return dt

    def approx_equals(self, other: datetime, delta: timedelta) -> bool:
        if not super().approx_equals(other, delta):
            return False

        if self.enforce_tz:
            if self.approx.tzinfo is None:  # type: ignore[union-attr]
                return other.tzinfo is None
            else:
                approx_offset = self.approx.tzinfo.utcoffset(self.approx)  # type: ignore[union-attr]
                other_offset = other.tzinfo.utcoffset(other)  # type: ignore[union-attr]
                return approx_offset == other_offset
        else:
            return True


class IsNow(IsDatetime):
    """
    Check if a datetime is close to now, this is similar to `IsDatetime(approx=datetime.now())`,
    but slightly more powerful.
    """

    def __init__(
        self,
        *,
        delta: timedelta | int | float = 2,
        unix_number: bool = False,
        iso_string: bool = False,
        format_string: str | None = None,
        enforce_tz: bool = True,
        tz: str | tzinfo | None = None,
    ):
        """
        Args:
            delta: The allowable different when comparing to the value to now, if omitted 2 seconds is used,
                ints and floats are assumed to represent seconds and converted to `timedelta`s.
            unix_number: whether to allow unix timestamp numbers in comparison
            iso_string: whether to allow iso formatted strings in comparison
            format_string: if provided, `format_string` is used with `datetime.strptime` to parse strings
            enforce_tz: whether timezone should be enforced in comparison, see below for more details
            tz: either a `ZoneInfo`, a `datetime.timezone` or a string which will be passed to `ZoneInfo`,
                to get a timezone, if provided now will be converted to this timezone.

        ```py title="IsNow"
        from datetime import datetime, timezone

        from dirty_equals import IsNow

        now = datetime.now()
        assert now == IsNow
        assert now.timestamp() == IsNow(unix_number=True)
        assert now.timestamp() != IsNow
        assert now.isoformat() == IsNow(iso_string=True)
        assert now.isoformat() != IsNow

        utc_now = datetime.utcnow().replace(tzinfo=timezone.utc)
        assert utc_now == IsNow(tz=timezone.utc)
        ```
        """
        if isinstance(tz, str):
            tz = ZoneInfo(tz)

        self.tz = tz

        approx = self._get_now()

        super().__init__(
            approx=approx,
            delta=delta,
            unix_number=unix_number,
            iso_string=iso_string,
            format_string=format_string,
            enforce_tz=enforce_tz,
        )
        if tz is not None:
            self._repr_kwargs['tz'] = tz

    def _get_now(self) -> datetime:
        if self.tz is None:
            return datetime.now()
        else:
            utc_now = datetime.now(tz=timezone.utc).replace(tzinfo=timezone.utc)
            return utc_now.astimezone(self.tz)

    def prepare(self, other: Any) -> datetime:
        # update approx for every comparing, to check if other value is dirty equal
        # to current moment of time
        self.approx = self._get_now()

        return super().prepare(other)


class IsDate(IsNumeric[date]):
    """
    Check if the value is a date, and matches the given conditions.
    """

    allowed_types = date

    def __init__(
        self,
        *,
        approx: date | None = None,
        delta: timedelta | int | float | None = None,
        gt: date | None = None,
        lt: date | None = None,
        ge: date | None = None,
        le: date | None = None,
        iso_string: bool = False,
        format_string: str | None = None,
    ):
        """
        Args:
            approx: A value to approximately compare to.
            delta: The allowable different when comparing to the value to now, if omitted 2 seconds is used,
                ints and floats are assumed to represent seconds and converted to `timedelta`s.
            gt: Value which the compared value should be greater than (after).
            lt: Value which the compared value should be less than (before).
            ge: Value which the compared value should be greater than (after) or equal to.
            le: Value which the compared value should be less than (before) or equal to.
            iso_string: whether to allow iso formatted strings in comparison
            format_string: if provided, `format_string` is used with `datetime.strptime` to parse strings

        Examples of basic usage:

        ```py title="IsDate"
        from datetime import date

        from dirty_equals import IsDate

        y2k = date(2000, 1, 1)
        assert date(2000, 1, 1) == IsDate(approx=y2k)
        assert '2000-01-01' == IsDate(approx=y2k, iso_string=True)

        assert date(2000, 1, 2) == IsDate(gt=y2k)
        assert date(1999, 1, 2) != IsDate(gt=y2k)
        ```
        """

        if delta is None:
            delta = timedelta()
        elif isinstance(delta, (int, float)):
            delta = timedelta(seconds=delta)

        super().__init__(approx=approx, gt=gt, lt=lt, ge=ge, le=le, delta=delta)  # type: ignore[arg-type]

        self.iso_string = iso_string
        self.format_string = format_string
        self._repr_kwargs.update(
            iso_string=Omit if iso_string is False else iso_string,
            format_string=Omit if format_string is None else format_string,
        )

    def prepare(self, other: Any) -> date:
        if type(other) is date:
            dt = other
        elif isinstance(other, str):
            if self.iso_string:
                dt = date.fromisoformat(other)
            elif self.format_string:
                dt = datetime.strptime(other, self.format_string).date()
            else:
                raise ValueError('not a valid date string')
        else:
            raise ValueError(f'{type(other)} not valid as date')

        return dt


class IsToday(IsDate):
    """
    Check if a date is today, this is similar to `IsDate(approx=date.today())`, but slightly more powerful.
    """

    def __init__(
        self,
        *,
        iso_string: bool = False,
        format_string: str | None = None,
    ):
        """
        Args:
            iso_string: whether to allow iso formatted strings in comparison
            format_string: if provided, `format_string` is used with `datetime.strptime` to parse strings
        ```py title="IsToday"
        from datetime import date, timedelta

        from dirty_equals import IsToday

        today = date.today()
        assert today == IsToday
        assert today.isoformat() == IsToday(iso_string=True)
        assert today.isoformat() != IsToday
        assert today + timedelta(days=1) != IsToday
        assert today.strftime('%Y/%m/%d') == IsToday(format_string='%Y/%m/%d')
        assert today.strftime('%Y/%m/%d') != IsToday()
        ```
        """

        super().__init__(approx=date.today(), iso_string=iso_string, format_string=format_string)
