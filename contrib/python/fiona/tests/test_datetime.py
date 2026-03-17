"""
See also test_rfc3339.py for datetime parser tests.
"""

import fiona
from fiona._env import get_gdal_version_num, calc_gdal_version_num
import pytest
from fiona.errors import DriverSupportError
from fiona.rfc3339 import parse_time, parse_datetime
from .conftest import get_temp_filename
from fiona.env import GDALVersion
from fiona.model import Feature
import datetime
from fiona.drvsupport import (
    supported_drivers,
    driver_mode_mingdal,
    _driver_converts_field_type_silently_to_str,
    _driver_supports_field,
    _driver_converts_to_str,
    _driver_supports_timezones,
    _driver_supports_milliseconds,
    _driver_supports_mode,
)
import pytz
from pytz import timezone

gdal_version = GDALVersion.runtime()


def get_schema(driver, field_type):
    if driver == "GPX":
        return {
            "properties": {"ele": "float", "time": field_type},
            "geometry": "Point",
        }
    if driver == "GPSTrackMaker":
        return {
            "properties": {
                "name": "str",
                "comment": "str",
                "icon": "int",
                "time": field_type,
            },
            "geometry": "Point",
        }
    if driver == "CSV":
        return {"properties": {"datefield": field_type}}
    return {"geometry": "Point", "properties": {"datefield": field_type}}


def get_records(driver, values):
    if driver == "GPX":
        return [
            Feature.from_dict(
                **{
                    "geometry": {"type": "Point", "coordinates": [1, 2]},
                    "properties": {"ele": 0, "time": val},
                }
            )
            for val in values
        ]
    if driver == "GPSTrackMaker":
        return [
            Feature.from_dict(
                **{
                    "geometry": {"type": "Point", "coordinates": [1, 2]},
                    "properties": {"name": "", "comment": "", "icon": 48, "time": val},
                }
            )
            for val in values
        ]
    if driver == "CSV":
        return [
            Feature.from_dict(**{"properties": {"datefield": val}}) for val in values
        ]

    return [
        Feature.from_dict(
            **{
                "geometry": {"type": "Point", "coordinates": [1, 2]},
                "properties": {"datefield": val},
            }
        )
        for val in values
    ]


def get_schema_field(driver, schema):
    if driver in {"GPX", "GPSTrackMaker"}:
        return schema["properties"]["time"]
    return schema["properties"]["datefield"]


def get_field(driver, f):
    if driver in {"GPX", "GPSTrackMaker"}:
        return f["properties"]["time"]
    return f.properties["datefield"]


class TZ(datetime.tzinfo):
    def __init__(self, minutes):
        self.minutes = minutes

    def utcoffset(self, dt):
        return datetime.timedelta(minutes=self.minutes)


def generate_testdata(field_type, driver):
    """Generate test cases for test_datefield

    Each test case has the format [(in_value1, true_value as datetime.*object),
                                   (in_value2, true_value as datetime.*object), ...]
    """

    # Test data for 'date' data type
    if field_type == "date":
        return [
            ("2018-03-25", datetime.date(2018, 3, 25)),
            (datetime.date(2018, 3, 25), datetime.date(2018, 3, 25)),
        ]

    # Test data for 'datetime' data type
    if field_type == "datetime":
        return [
            ("2018-03-25T22:49:05", datetime.datetime(2018, 3, 25, 22, 49, 5)),
            (
                datetime.datetime(2018, 3, 25, 22, 49, 5),
                datetime.datetime(2018, 3, 25, 22, 49, 5),
            ),
            (
                "2018-03-25T22:49:05.23",
                datetime.datetime(2018, 3, 25, 22, 49, 5, 230000),
            ),
            (
                datetime.datetime(2018, 3, 25, 22, 49, 5, 230000),
                datetime.datetime(2018, 3, 25, 22, 49, 5, 230000),
            ),
            (
                "2018-03-25T22:49:05.123456",
                datetime.datetime(2018, 3, 25, 22, 49, 5, 123000),
            ),
            (
                datetime.datetime(2018, 3, 25, 22, 49, 5, 123456),
                datetime.datetime(2018, 3, 25, 22, 49, 5, 123000),
            ),
            (
                "2018-03-25T22:49:05+01:30",
                datetime.datetime(2018, 3, 25, 22, 49, 5, tzinfo=TZ(90)),
            ),
            (
                "2018-03-25T22:49:05-01:30",
                datetime.datetime(2018, 3, 25, 22, 49, 5, tzinfo=TZ(-90)),
            ),
            (
                datetime.datetime(2018, 3, 25, 22, 49, 5, tzinfo=TZ(90)),
                datetime.datetime(2018, 3, 25, 22, 49, 5, tzinfo=TZ(90)),
            ),
            (
                datetime.datetime(2018, 3, 25, 22, 49, 5, tzinfo=TZ(-90)),
                datetime.datetime(2018, 3, 25, 22, 49, 5, tzinfo=TZ(-90)),
            ),
            (
                datetime.datetime(2020, 1, 21, 12, 0, 0, tzinfo=pytz.utc).astimezone(
                    timezone("Europe/Zurich")
                ),
                datetime.datetime(2020, 1, 21, 12, 0, 0, tzinfo=pytz.utc).astimezone(
                    timezone("Europe/Zurich")
                ),
            ),
            (
                datetime.datetime(2020, 1, 21, 12, 0, 0, tzinfo=pytz.utc).astimezone(
                    timezone("America/Denver")
                ),
                datetime.datetime(2020, 1, 21, 12, 0, 0, tzinfo=pytz.utc).astimezone(
                    timezone("America/Denver")
                ),
            ),
            (
                datetime.datetime(2018, 3, 25, 22, 49, 5, tzinfo=TZ(60 * 24 - 15)),
                datetime.datetime(2018, 3, 25, 22, 49, 5, tzinfo=TZ(60 * 24 - 15)),
            ),
            (
                datetime.datetime(2018, 3, 25, 22, 49, 5, tzinfo=TZ(-60 * 24 + 15)),
                datetime.datetime(2018, 3, 25, 22, 49, 5, tzinfo=TZ(-60 * 24 + 15)),
            ),
            (
                "2018-03-25T22:49:05-23:45",
                datetime.datetime(2018, 3, 25, 22, 49, 5, tzinfo=TZ(-60 * 24 + 15)),
            ),
            (
                "2018-03-25T22:49:05+23:45",
                datetime.datetime(2018, 3, 25, 22, 49, 5, tzinfo=TZ(60 * 24 - 15)),
            ),
        ]

    # Test data for 'time' data type
    elif field_type == "time":
        return [
            ("22:49:05", datetime.time(22, 49, 5)),
            (datetime.time(22, 49, 5), datetime.time(22, 49, 5)),
            ("22:49:05.23", datetime.time(22, 49, 5, 230000)),
            (datetime.time(22, 49, 5, 230000), datetime.time(22, 49, 5, 230000)),
            ("22:49:05.123456", datetime.time(22, 49, 5, 123000)),
            (datetime.time(22, 49, 5, 123456), datetime.time(22, 49, 5, 123000)),
            ("22:49:05+01:30", datetime.time(22, 49, 5, tzinfo=TZ(90))),
            ("22:49:05-01:30", datetime.time(22, 49, 5, tzinfo=TZ(-90))),
            (
                datetime.time(22, 49, 5, tzinfo=TZ(90)),
                datetime.time(22, 49, 5, tzinfo=TZ(90)),
            ),
            (
                datetime.time(22, 49, 5, tzinfo=TZ(-90)),
                datetime.time(22, 49, 5, tzinfo=TZ(-90)),
            ),
            (
                datetime.time(22, 49, 5, tzinfo=TZ(60 * 24 - 15)),
                datetime.time(22, 49, 5, tzinfo=TZ(60 * 24 - 15)),
            ),
            (
                datetime.time(22, 49, 5, tzinfo=TZ(-60 * 24 + 15)),
                datetime.time(22, 49, 5, tzinfo=TZ(-60 * 24 + 15)),
            ),
            ("22:49:05-23:45", datetime.time(22, 49, 5, tzinfo=TZ(-60 * 24 + 15))),
            ("22:49:05+23:45", datetime.time(22, 49, 5, tzinfo=TZ(60 * 24 - 15))),
        ]


def compare_datetimes_utc(d1, d2):
    """Test if two time objects are the same. Native times are assumed to be UTC"""

    if d1.tzinfo is None:
        d1 = d1.replace(tzinfo=TZ(0))

    if d2.tzinfo is None:
        d2 = d2.replace(tzinfo=TZ(0))

    return d1 == d2


def test_compare_datetimes_utc():
    """Test compare_datetimes_utc"""
    d1 = datetime.datetime(2020, 1, 21, 12, 30, 0, tzinfo=TZ(60))
    d2 = datetime.datetime(2020, 1, 21, 11, 30, 0, tzinfo=TZ(0))
    assert d1 == d2
    assert compare_datetimes_utc(d1, d2)

    d1 = datetime.datetime(2020, 1, 21, 12, 30, 0, tzinfo=TZ(-60))
    d2 = datetime.datetime(2020, 1, 21, 11, 30, 0, tzinfo=TZ(0))
    assert not d1 == d2
    assert not compare_datetimes_utc(d1, d2)

    d1 = datetime.datetime(2020, 1, 21, 13, 0, 0, tzinfo=TZ(60))
    d2 = datetime.datetime(2020, 1, 21, 5, 0, 0, tzinfo=TZ(-60 * 7))
    assert d1 == d2
    assert compare_datetimes_utc(d1, d2)

    d1 = datetime.datetime(2020, 1, 21, 12, 0, 0, tzinfo=pytz.utc).astimezone(
        timezone("Europe/Zurich")
    )
    d2 = datetime.datetime(2020, 1, 21, 12, 0, 0, tzinfo=pytz.utc)
    assert d1 == d2
    assert compare_datetimes_utc(d1, d2)

    d1 = datetime.datetime(2020, 1, 21, 12, 0, 0, tzinfo=pytz.utc).astimezone(
        timezone("Europe/Zurich")
    )
    d2 = datetime.datetime(2020, 1, 21, 12, 0, 0, tzinfo=pytz.utc).astimezone(
        timezone("America/Denver")
    )
    assert d1 == d2
    assert compare_datetimes_utc(d1, d2)

    d1 = datetime.datetime(2020, 6, 21, 12, 0, 0, tzinfo=pytz.utc).astimezone(
        timezone("Europe/Zurich")
    )
    d2 = datetime.datetime(2020, 6, 21, 12, 0, 0, tzinfo=pytz.utc).astimezone(
        timezone("America/Denver")
    )
    assert d1 == d2
    assert compare_datetimes_utc(d1, d2)


def convert_time_to_utc(d):
    """Convert datetime.time object to UTC"""
    d = datetime.datetime(
        1900, 1, 1, d.hour, d.minute, d.second, d.microsecond, d.tzinfo
    )
    d -= d.utcoffset()
    return d.time()


def compare_times_utc(d1, d2):
    """Test if two datetime.time objects with fixed timezones have the same UTC time"""
    if d1.tzinfo is not None:
        d1 = convert_time_to_utc(d1)

    if d2.tzinfo is not None:
        d2 = convert_time_to_utc(d2)

    return d1.replace(tzinfo=None) == d2.replace(tzinfo=None)


def test_compare_times_utc():
    """
    Test compare_times_utc
    """
    d1 = datetime.time(12, 30, 0, tzinfo=TZ(60))
    d2 = datetime.time(11, 30, 0, tzinfo=TZ(0))
    assert compare_times_utc(d1, d2)

    d1 = datetime.time(12, 30, 0, tzinfo=TZ(-60))
    d2 = datetime.time(11, 30, 0, tzinfo=TZ(0))
    assert not compare_times_utc(d1, d2)

    d1 = datetime.time(13, 0, 0, tzinfo=TZ(60))
    d2 = datetime.time(5, 0, 0, tzinfo=TZ(-60 * 7))
    assert compare_times_utc(d1, d2)

    d1 = (
        datetime.datetime(2020, 6, 21, 12, 0, 0, tzinfo=pytz.utc)
        .astimezone(timezone("MET"))
        .timetz()
    )
    d2 = (
        datetime.datetime(2020, 6, 21, 12, 0, 0, tzinfo=pytz.utc)
        .astimezone(timezone("EST"))
        .timetz()
    )
    assert compare_times_utc(d1, d2)


def get_tz_offset(d):
    """Returns a Timezone (sign, hours, minutes) tuples

    E.g.: for '2020-01-21T12:30:00+01:30' ('+', 1, 30) is returned

    """
    offset_minutes = d.utcoffset().total_seconds() / 60
    if offset_minutes < 0:
        sign = "-"
    else:
        sign = "+"
    hours = int(abs(offset_minutes) / 60)
    minutes = int(abs(offset_minutes) % 60)
    return sign, hours, minutes


def test_get_tz_offset():
    """Test get_tz_offset"""
    d = datetime.datetime(2020, 1, 21, 12, 30, 0, tzinfo=TZ(90))
    assert get_tz_offset(d) == ("+", 1, 30)

    d = datetime.datetime(2020, 1, 21, 12, 30, 0, tzinfo=TZ(-90))
    assert get_tz_offset(d) == ("-", 1, 30)

    d = datetime.datetime(2020, 1, 21, 12, 30, 0, tzinfo=TZ(60 * 24 - 15))
    assert get_tz_offset(d) == ("+", 23, 45)

    d = datetime.datetime(2020, 1, 21, 12, 30, 0, tzinfo=TZ(-60 * 24 + 15))
    assert get_tz_offset(d) == ("-", 23, 45)


def generate_testcases():
    """Generate test cases for drivers that support date fields, convert date fields to string or do not support
    date fields"""
    _test_cases_datefield = []
    _test_cases_datefield_to_str = []
    _test_cases_datefield_not_supported = []

    for field_type in ["time", "datetime", "date"]:
        # Select only driver that are capable of writing fields
        for driver, raw in supported_drivers.items():
            if _driver_supports_mode(driver, "w"):
                if _driver_supports_field(driver, field_type):
                    if _driver_converts_field_type_silently_to_str(driver, field_type):
                        _test_cases_datefield_to_str.append((driver, field_type))
                    else:
                        _test_cases_datefield.append((driver, field_type))
                else:
                    _test_cases_datefield_not_supported.append((driver, field_type))

    return (
        _test_cases_datefield,
        _test_cases_datefield_to_str,
        _test_cases_datefield_not_supported,
    )


(
    test_cases_datefield,
    test_cases_datefield_to_str,
    test_cases_datefield_not_supported,
) = generate_testcases()


@pytest.mark.parametrize("driver, field_type", test_cases_datefield)
@pytest.mark.gdal
def test_datefield(tmpdir, driver, field_type):
    """
    Test date, time, datetime field types.
    """

    def _validate(val, val_exp, field_type, driver):

        if field_type == "date":
            return val == val_exp.isoformat()

        elif field_type == "datetime":

            # some drivers do not support timezones. In this case, Fiona converts datetime fields with a timezone other
            # than UTC to UTC. Thus, both the datetime read by Fiona, as well as expected value are first converted to
            # UTC before compared.

            # Milliseconds
            if _driver_supports_milliseconds(driver):
                y, m, d, hh, mm, ss, ms, tz = parse_datetime(val)
                if tz is not None:
                    tz = TZ(tz)
                val_d = datetime.datetime(y, m, d, hh, mm, ss, ms, tz)
                return compare_datetimes_utc(val_d, val_exp)
            else:
                # No Milliseconds
                y, m, d, hh, mm, ss, ms, tz = parse_datetime(val)
                if tz is not None:
                    tz = TZ(tz)
                val_d = datetime.datetime(y, m, d, hh, mm, ss, ms, tz)
                return compare_datetimes_utc(val_d, val_exp.replace(microsecond=0))

        elif field_type == "time":

            # some drivers do not support timezones. In this case, Fiona converts datetime fields with a timezone other
            # than UTC to UTC. Thus, both the time read by Fiona, as well as expected value are first converted to UTC
            # before compared.

            # Milliseconds
            if _driver_supports_milliseconds(driver):
                y, m, d, hh, mm, ss, ms, tz = parse_time(val)
                if tz is not None:
                    tz = TZ(tz)
                val_d = datetime.time(hh, mm, ss, ms, tz)
                return compare_times_utc(val_d, val_exp)
            else:
                # No Milliseconds
                y, m, d, hh, mm, ss, ms, tz = parse_time(val)
                if tz is not None:
                    tz = TZ(tz)
                val_d = datetime.time(hh, mm, ss, ms, tz)
                return compare_times_utc(val_d, val_exp.replace(microsecond=0))
        return False

    schema = get_schema(driver, field_type)
    path = str(tmpdir.join(get_temp_filename(driver)))
    values_in, values_exp = zip(*generate_testdata(field_type, driver))
    records = get_records(driver, values_in)

    with fiona.open(path, "w", driver=driver, schema=schema) as c:
        c.writerecords(records)

    with fiona.open(path, "r") as c:
        assert get_schema_field(driver, c.schema) == field_type, f"Returned field type is {get_schema_field(driver, c.schema)}, expected {field_type}"
        items = [get_field(driver, f) for f in c]
        assert len(items) == len(values_in)
        for val, val_exp in zip(items, values_exp):
            assert _validate(
                val, val_exp, field_type, driver
            ), f"{val} does not match {val_exp.isoformat()}"


@pytest.mark.parametrize("driver, field_type", test_cases_datefield_to_str)
@pytest.mark.gdal
def test_datefield_driver_converts_to_string(tmpdir, driver, field_type):
    """
    Test handling of date, time, datetime for drivers that convert these types to string.

    As the formatting can be arbitrary, we only test if the elements of a date / datetime / time object
    is included in the string. E.g. for the PCIDSK driver if hour 22 from date.time(22:49:05) is in
    '0000/00/00 22:49:05'.

    """

    def _validate(val, val_exp, field_type, driver):

        if field_type == "date":
            if (
                str(val_exp.year) in val
                and str(val_exp.month) in val
                and str(val_exp.day) in val
            ):
                return True
        elif field_type == "datetime":

            if (
                not _driver_supports_timezones(driver, field_type)
                and val_exp.utcoffset() is not None
            ):
                val_exp = convert_time_to_utc(val_exp)

            # datetime fields can, depending on the driver, support:
            # - Timezones
            # - Milliseconds, respectively Microseconds

            # No timezone
            if val_exp.utcoffset() is None:
                # No Milliseconds
                if not _driver_supports_milliseconds(driver):
                    if (
                        str(val_exp.year) in val
                        and str(val_exp.month) in val
                        and str(val_exp.day) in val
                        and str(val_exp.hour) in val
                        and str(val_exp.minute) in val
                        and str(val_exp.second) in val
                    ):
                        return True
                else:
                    # Microseconds
                    if (
                        str(val_exp.year) in val
                        and str(val_exp.month) in val
                        and str(val_exp.day) in val
                        and str(val_exp.hour) in val
                        and str(val_exp.minute) in val
                        and str(val_exp.second) in val
                        and str(val_exp.microsecond) in val
                    ):
                        return True
                    # Milliseconds
                    elif (
                        str(val_exp.year) in val
                        and str(val_exp.month) in val
                        and str(val_exp.day) in val
                        and str(val_exp.hour) in val
                        and str(val_exp.minute) in val
                        and str(val_exp.second) in val
                        and str(int(val_exp.microsecond / 1000)) in val
                    ):
                        return True
            # With timezone
            else:
                sign, hours, minutes = get_tz_offset(val_exp)
                if minutes > 0:
                    tz = f"{sign}{hours:02d}{minutes:02d}"
                else:
                    tz = f"{sign}{hours:02d}"
                print("tz", tz)
                # No Milliseconds
                if not _driver_supports_milliseconds(driver):
                    if (
                        str(val_exp.year) in val
                        and str(val_exp.month) in val
                        and str(val_exp.day) in val
                        and str(val_exp.hour) in val
                        and str(val_exp.minute) in val
                        and str(val_exp.second) in val
                        and tz in val
                    ):
                        return True
                else:
                    # Microseconds
                    if (
                        str(val_exp.year) in val
                        and str(val_exp.month) in val
                        and str(val_exp.day) in val
                        and str(val_exp.hour) in val
                        and str(val_exp.minute) in val
                        and str(val_exp.second) in val
                        and str(val_exp.microsecond) in val
                        and tz in val
                    ):
                        return True
                    # Milliseconds
                    elif (
                        str(val_exp.year) in val
                        and str(val_exp.month) in val
                        and str(val_exp.day) in val
                        and str(val_exp.hour) in val
                        and str(val_exp.minute) in val
                        and str(val_exp.second) in val
                        and str(int(val_exp.microsecond / 1000)) in val
                        and tz in val
                    ):
                        return True

        elif field_type == "time":

            # time fields can, depending on the driver, support:
            # - Timezones
            # - Milliseconds, respectively Microseconds

            if (
                not _driver_supports_timezones(driver, field_type)
                and val_exp.utcoffset() is not None
            ):
                val_exp = convert_time_to_utc(val_exp)

            # No timezone
            if val_exp.utcoffset() is None:
                # No Milliseconds
                if not _driver_supports_milliseconds(driver):
                    if (
                        str(val_exp.hour) in val
                        and str(val_exp.minute) in val
                        and str(val_exp.second) in val
                    ):
                        return True
                else:
                    # Microseconds
                    if (
                        str(val_exp.hour) in val
                        and str(val_exp.minute) in val
                        and str(val_exp.second) in val
                        and str(val_exp.microsecond) in val
                    ):
                        return True
                    # Milliseconds
                    elif (
                        str(val_exp.hour) in val
                        and str(val_exp.minute) in val
                        and str(val_exp.second) in val
                        and str(int(val_exp.microsecond / 1000)) in val
                    ):
                        return True
            # With timezone
            else:

                sign, hours, minutes = get_tz_offset(val_exp)
                if minutes > 0:
                    tz = f"{sign}{hours:02d}{minutes:02d}"
                else:
                    tz = f"{sign}{hours:02d}"
                # No Milliseconds
                if not _driver_supports_milliseconds(driver):
                    if (
                        str(val_exp.hour) in val
                        and str(val_exp.minute) in val
                        and str(val_exp.second) in val
                        and tz in val
                    ):
                        return True
                else:
                    # Microseconds
                    if (
                        str(val_exp.hour) in val
                        and str(val_exp.minute) in val
                        and str(val_exp.second) in val
                        and str(val_exp.microsecond) in val
                        and tz in val
                    ):
                        return True
                    # Milliseconds
                    elif (
                        str(val_exp.hour) in val
                        and str(val_exp.minute) in val
                        and str(val_exp.second) in val
                        and str(int(val_exp.microsecond / 1000)) in val
                        and tz in val
                    ):
                        return True
        return False

    schema = get_schema(driver, field_type)
    path = str(tmpdir.join(get_temp_filename(driver)))
    values_in, values_exp = zip(*generate_testdata(field_type, driver))
    records = get_records(driver, values_exp)

    with pytest.warns(UserWarning) as record:
        with fiona.open(path, "w", driver=driver, schema=schema) as c:
            c.writerecords(records)
        assert len(record) == 1
        assert "silently converts" in record[0].message.args[0]

    with fiona.open(path, "r") as c:
        assert get_schema_field(driver, c.schema) == "str"
        items = [get_field(driver, f) for f in c]
        assert len(items) == len(values_in)
        for val, val_exp in zip(items, values_exp):
            assert _validate(
                val, val_exp, field_type, driver
            ), f"{val} does not match {val_exp.isoformat()}"


@pytest.mark.filterwarnings("ignore:.*driver silently converts *:UserWarning")
@pytest.mark.parametrize(
    "driver,field_type", test_cases_datefield + test_cases_datefield_to_str
)
@pytest.mark.gdal
def test_datefield_null(tmpdir, driver, field_type):
    """
    Test handling of null values for date, time, datetime types for write capable drivers
    """

    def _validate(val, val_exp, field_type, driver):
        if (
            driver == "MapInfo File"
            and field_type == "time"
            and calc_gdal_version_num(2, 0, 0)
            <= get_gdal_version_num()
            < calc_gdal_version_num(3, 1, 1)
        ):
            return val == "00:00:00"
        if val is None or val == "":
            return True
        return False

    schema = get_schema(driver, field_type)
    path = str(tmpdir.join(get_temp_filename(driver)))
    values_in = [None]
    records = get_records(driver, values_in)

    with fiona.open(path, "w", driver=driver, schema=schema) as c:
        c.writerecords(records)

    with fiona.open(path, "r") as c:
        items = [get_field(driver, f) for f in c]
        assert len(items) == 1

        assert _validate(
            items[0], None, field_type, driver
        ), f"{items[0]} does not match {None}"


@pytest.mark.parametrize("driver, field_type", test_cases_datefield_not_supported)
@pytest.mark.gdal
def test_datetime_field_unsupported(tmpdir, driver, field_type):
    """Test if DriverSupportError is raised for unsupported field_types"""
    schema = get_schema(driver, field_type)
    path = str(tmpdir.join(get_temp_filename(driver)))
    values_in, values_out = zip(*generate_testdata(field_type, driver))
    records = get_records(driver, values_in)

    with pytest.raises(DriverSupportError):
        with fiona.open(path, "w", driver=driver, schema=schema) as c:
            c.writerecords(records)


@pytest.mark.parametrize("driver, field_type", test_cases_datefield_not_supported)
@pytest.mark.gdal
def test_datetime_field_type_marked_not_supported_is_not_supported(
    tmpdir, driver, field_type, monkeypatch
):
    """Test if a date/datetime/time field type marked as not not supported is really not supported

    Warning: Success of this test does not necessary mean that a field is not supported. E.g. errors can occur due to
    special schema requirements of drivers. This test only covers the standard case.

    """

    if driver == "BNA" and GDALVersion.runtime() < GDALVersion(2, 0):
        pytest.skip("BNA driver segfaults with gdal 1.11")

    monkeypatch.delitem(
        fiona.drvsupport._driver_field_type_unsupported[field_type], driver
    )

    schema = get_schema(driver, field_type)
    path = str(tmpdir.join(get_temp_filename(driver)))
    values_in, values_out = zip(*generate_testdata(field_type, driver))
    records = get_records(driver, values_in)

    is_good = True
    try:
        with fiona.open(path, "w", driver=driver, schema=schema) as c:
            c.writerecords(records)

        with fiona.open(path, "r") as c:
            if not get_schema_field(driver, c.schema) == field_type:
                is_good = False
            items = [get_field(driver, f) for f in c]
            for val_in, val_out in zip(items, values_out):
                if not val_in == val_out:
                    is_good = False
    except:
        is_good = False
    assert not is_good


def generate_tostr_testcases():
    """Flatten driver_converts_to_str to a list of (field_type, driver) tuples"""
    cases = []
    for field_type in _driver_converts_to_str:
        for driver in _driver_converts_to_str[field_type]:
            driver_supported = driver in supported_drivers
            driver_can_write = _driver_supports_mode(driver, "w")
            field_supported = _driver_supports_field(driver, field_type)
            converts_to_str = _driver_converts_field_type_silently_to_str(
                driver, field_type
            )
            if (
                driver_supported
                and driver_can_write
                and converts_to_str
                and field_supported
            ):
                cases.append((field_type, driver))
    return cases


@pytest.mark.filterwarnings("ignore:.*driver silently converts *:UserWarning")
@pytest.mark.parametrize("driver,field_type", test_cases_datefield_to_str)
@pytest.mark.gdal
def test_driver_marked_as_silently_converts_to_str_converts_silently_to_str(
    tmpdir, driver, field_type, monkeypatch
):
    """Test if a driver and field_type is marked in fiona.drvsupport.driver_converts_to_str to convert to str really
    silently converts to str

    If this test fails, it should be considered to replace the respective None value in
    fiona.drvsupport.driver_converts_to_str with a GDALVersion(major, minor) value.
    """

    monkeypatch.delitem(fiona.drvsupport._driver_converts_to_str[field_type], driver)

    schema = get_schema(driver, field_type)
    path = str(tmpdir.join(get_temp_filename(driver)))
    values_in, values_out = zip(*generate_testdata(field_type, driver))
    records = get_records(driver, values_in)

    with fiona.open(path, "w", driver=driver, schema=schema) as c:
        c.writerecords(records)

    with fiona.open(path, "r") as c:
        assert get_schema_field(driver, c.schema) == "str"


def test_read_timezone_geojson(path_test_tz_geojson):
    """Test if timezones are read correctly"""
    with fiona.open(path_test_tz_geojson) as c:
        items = list(c)
        assert items[0]["properties"]["test"] == "2015-04-22T00:00:00+07:00"


def test_property_setter_lookup(tmp_path):
    """Demonstrate fix for #1376."""

    class MyDate(datetime.date):
        pass

    feat = Feature.from_dict(
        {
            "properties": {"when": MyDate(2024, 4, 15)},
            "geometry": {"type": "Point", "coordinates": [0, 0]},
        }
    )
    with fiona.open(
        tmp_path / "test.gpkg",
        "w",
        driver="GPKG",
        crs="EPSG:4326",
        schema={"geometry": "Point", "properties": {"when": "date"}},
    ) as colxn:
        colxn.writerecords([feat])

    with fiona.open(tmp_path / "test.gpkg") as colxn:
        assert colxn.schema["properties"]["when"] == "date"
        feat = next(colxn)
        assert feat.properties["when"] == "2024-04-15"
