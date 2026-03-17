from datetime import date, datetime, timedelta, timezone
from unittest.mock import Mock
from zoneinfo import ZoneInfo

import pytest

from dirty_equals import IsDate, IsDatetime, IsNow, IsToday


@pytest.mark.parametrize(
    'value,dirty,expect_match',
    [
        pytest.param(datetime(2000, 1, 1), IsDatetime(approx=datetime(2000, 1, 1)), True, id='same'),
        # Note: this requires the system timezone to be UTC
        pytest.param(946684800, IsDatetime(approx=datetime(2000, 1, 1), unix_number=True), True, id='unix-int'),
        # Note: this requires the system timezone to be UTC
        pytest.param(946684800.123, IsDatetime(approx=datetime(2000, 1, 1), unix_number=True), True, id='unix-float'),
        pytest.param(946684800, IsDatetime(approx=datetime(2000, 1, 1)), False, id='unix-different'),
        pytest.param(
            '2000-01-01T00:00', IsDatetime(approx=datetime(2000, 1, 1), iso_string=True), True, id='iso-string-true'
        ),
        pytest.param('2000-01-01T00:00', IsDatetime(approx=datetime(2000, 1, 1)), False, id='iso-string-different'),
        pytest.param('broken', IsDatetime(approx=datetime(2000, 1, 1)), False, id='iso-string-wrong'),
        pytest.param(
            '28/01/87', IsDatetime(approx=datetime(1987, 1, 28), format_string='%d/%m/%y'), True, id='string-format'
        ),
        pytest.param('28/01/87', IsDatetime(approx=datetime(2000, 1, 1)), False, id='string-format-different'),
        pytest.param('foobar', IsDatetime(approx=datetime(2000, 1, 1)), False, id='string-format-wrong'),
        pytest.param(datetime(2000, 1, 1).isoformat(), IsNow(iso_string=True), False, id='isnow-str-different'),
        pytest.param([1, 2, 3], IsDatetime(approx=datetime(2000, 1, 1)), False, id='wrong-type'),
        pytest.param(
            datetime(2020, 1, 1, 12, 13, 14), IsDatetime(approx=datetime(2020, 1, 1, 12, 13, 14)), True, id='tz-same'
        ),
        pytest.param(
            datetime(2020, 1, 1, 12, 13, 14, tzinfo=timezone.utc),
            IsDatetime(approx=datetime(2020, 1, 1, 12, 13, 14), enforce_tz=False),
            True,
            id='tz-utc',
        ),
        pytest.param(
            datetime(2020, 1, 1, 12, 13, 14, tzinfo=timezone.utc),
            IsDatetime(approx=datetime(2020, 1, 1, 12, 13, 14)),
            False,
            id='tz-utc-different',
        ),
        pytest.param(
            datetime(2020, 1, 1, 12, 13, 14),
            IsDatetime(approx=datetime(2020, 1, 1, 12, 13, 14, tzinfo=timezone.utc), enforce_tz=False),
            False,
            id='tz-approx-tz',
        ),
        pytest.param(
            datetime(2020, 1, 1, 12, 13, 14, tzinfo=timezone(offset=timedelta(hours=1))),
            IsDatetime(approx=datetime(2020, 1, 1, 12, 13, 14), enforce_tz=False),
            True,
            id='tz-1-hour',
        ),
        pytest.param(
            datetime(2022, 2, 15, 15, 15, tzinfo=ZoneInfo('Europe/London')),
            IsDatetime(approx=datetime(2022, 2, 15, 10, 15, tzinfo=ZoneInfo('America/New_York')), enforce_tz=False),
            True,
            id='tz-both-tz',
        ),
        pytest.param(
            datetime(2022, 2, 15, 15, 15, tzinfo=ZoneInfo('Europe/London')),
            IsDatetime(approx=datetime(2022, 2, 15, 10, 15, tzinfo=ZoneInfo('America/New_York'))),
            False,
            id='tz-both-tz-different',
        ),
        pytest.param(datetime(2000, 1, 1), IsDatetime(ge=datetime(2000, 1, 1)), True, id='ge'),
        pytest.param(datetime(1999, 1, 1), IsDatetime(ge=datetime(2000, 1, 1)), False, id='ge-not'),
        pytest.param(datetime(2000, 1, 2), IsDatetime(gt=datetime(2000, 1, 1)), True, id='gt'),
        pytest.param(datetime(2000, 1, 1), IsDatetime(gt=datetime(2000, 1, 1)), False, id='gt-not'),
    ],
)
def test_is_datetime(value, dirty, expect_match):
    if expect_match:
        assert value == dirty
    else:
        assert value != dirty


@pytest.mark.skipif(ZoneInfo is None, reason='requires zoneinfo')
def test_is_datetime_zoneinfo():
    london = datetime(2022, 2, 15, 15, 15, tzinfo=ZoneInfo('Europe/London'))
    ny = datetime(2022, 2, 15, 10, 15, tzinfo=ZoneInfo('America/New_York'))
    assert london != IsDatetime(approx=ny)
    assert london == IsDatetime(approx=ny, enforce_tz=False)


def test_is_now_dt():
    is_now = IsNow()
    dt = datetime.now()
    assert dt == is_now
    assert str(is_now) == repr(dt)


def test_is_now_str():
    assert datetime.now().isoformat() == IsNow(iso_string=True)


def test_repr():
    v = IsDatetime(approx=datetime(2032, 1, 2, 3, 4, 5), iso_string=True)
    assert str(v) == 'IsDatetime(approx=datetime.datetime(2032, 1, 2, 3, 4, 5), iso_string=True)'


@pytest.mark.skipif(ZoneInfo is None, reason='requires zoneinfo')
def test_is_now_tz():
    utc_now = datetime.now(timezone.utc)
    now_ny = utc_now.astimezone(ZoneInfo('America/New_York'))
    assert now_ny == IsNow(tz='America/New_York')
    # depends on the time of year and DST
    assert now_ny == IsNow(tz=timezone(timedelta(hours=-5))) | IsNow(tz=timezone(timedelta(hours=-4)))

    now = datetime.now()
    assert now == IsNow
    assert now.timestamp() == IsNow(unix_number=True)
    assert now.timestamp() != IsNow
    assert now.isoformat() == IsNow(iso_string=True)
    assert now.isoformat() != IsNow

    assert utc_now == IsNow(tz=timezone.utc)


def test_delta():
    assert IsNow(delta=timedelta(hours=2)).delta == timedelta(seconds=7200)
    assert IsNow(delta=3600).delta == timedelta(seconds=3600)
    assert IsNow(delta=3600.1).delta == timedelta(seconds=3600, microseconds=100000)


def test_is_now_relative(monkeypatch):
    mock = Mock(return_value=datetime(2020, 1, 1, 12, 13, 14))
    monkeypatch.setattr(IsNow, '_get_now', mock)
    assert IsNow() == datetime(2020, 1, 1, 12, 13, 14)


@pytest.mark.skipif(ZoneInfo is None, reason='requires zoneinfo')
def test_tz():
    new_year_london = datetime(2000, 1, 1, tzinfo=ZoneInfo('Europe/London'))

    new_year_eve_nyc = datetime(1999, 12, 31, 19, 0, 0, tzinfo=ZoneInfo('America/New_York'))

    assert new_year_eve_nyc == IsDatetime(approx=new_year_london, enforce_tz=False)
    assert new_year_eve_nyc != IsDatetime(approx=new_year_london, enforce_tz=True)

    new_year_naive = datetime(2000, 1, 1)

    assert new_year_naive != IsDatetime(approx=new_year_london, enforce_tz=False)
    assert new_year_naive != IsDatetime(approx=new_year_eve_nyc, enforce_tz=False)
    assert new_year_london == IsDatetime(approx=new_year_naive, enforce_tz=False)
    assert new_year_eve_nyc != IsDatetime(approx=new_year_naive, enforce_tz=False)


@pytest.mark.parametrize(
    'value,dirty,expect_match',
    [
        pytest.param(date(2000, 1, 1), IsDate(approx=date(2000, 1, 1)), True, id='same'),
        pytest.param('2000-01-01', IsDate(approx=date(2000, 1, 1), iso_string=True), True, id='iso-string-true'),
        pytest.param('2000-01-01', IsDate(approx=date(2000, 1, 1)), False, id='iso-string-different'),
        pytest.param('2000-01-01T00:00', IsDate(approx=date(2000, 1, 1)), False, id='iso-string-different'),
        pytest.param('broken', IsDate(approx=date(2000, 1, 1)), False, id='iso-string-wrong'),
        pytest.param('28/01/87', IsDate(approx=date(1987, 1, 28), format_string='%d/%m/%y'), True, id='string-format'),
        pytest.param('28/01/87', IsDate(approx=date(2000, 1, 1)), False, id='string-format-different'),
        pytest.param('foobar', IsDate(approx=date(2000, 1, 1)), False, id='string-format-wrong'),
        pytest.param([1, 2, 3], IsDate(approx=date(2000, 1, 1)), False, id='wrong-type'),
        pytest.param(
            datetime(2000, 1, 1, 10, 11, 12), IsDate(approx=date(2000, 1, 1)), False, id='wrong-type-datetime'
        ),
        pytest.param(date(2020, 1, 1), IsDate(approx=date(2020, 1, 1)), True, id='tz-same'),
        pytest.param(date(2000, 1, 1), IsDate(ge=date(2000, 1, 1)), True, id='ge'),
        pytest.param(date(1999, 1, 1), IsDate(ge=date(2000, 1, 1)), False, id='ge-not'),
        pytest.param(date(2000, 1, 2), IsDate(gt=date(2000, 1, 1)), True, id='gt'),
        pytest.param(date(2000, 1, 1), IsDate(gt=date(2000, 1, 1)), False, id='gt-not'),
        pytest.param(date(2000, 1, 1), IsDate(gt=date(2000, 1, 1), delta=10), False, id='delta-int'),
        pytest.param(date(2000, 1, 1), IsDate(gt=date(2000, 1, 1), delta=10.5), False, id='delta-float'),
        pytest.param(
            date(2000, 1, 1), IsDate(gt=date(2000, 1, 1), delta=timedelta(seconds=10)), False, id='delta-timedelta'
        ),
    ],
)
def test_is_date(value, dirty, expect_match):
    if expect_match:
        assert value == dirty
    else:
        assert value != dirty


def test_is_today():
    today = date.today()
    assert today == IsToday
    assert today + timedelta(days=2) != IsToday
    assert today.isoformat() == IsToday(iso_string=True)
    assert today.isoformat() != IsToday()
    assert today.strftime('%Y/%m/%d') == IsToday(format_string='%Y/%m/%d')
    assert today.strftime('%Y/%m/%d') != IsToday()
