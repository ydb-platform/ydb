import json
import sys
from inspect import signature
from itertools import chain
from time import sleep
from typing import no_type_check

import pytest

from whenever import (
    _EXTENSION_LOADED,
    Date,
    DateDelta,
    DateTimeDelta,
    ImplicitlyIgnoringDST,
    Instant,
    InvalidOffsetError,
    MonthDay,
    OffsetDateTime,
    PlainDateTime,
    SystemDateTime,
    Time,
    TimeDelta,
    YearMonth,
    ZonedDateTime,
    hours,
    patch_current_time,
    seconds,
)

from .common import system_tz_ams


@pytest.mark.skipif(
    sys.version_info < (3, 13),
    reason="feature not supported until Python 3.13",
)
def test_multiple_interpreters():
    import _interpreters as interpreters

    for _ in range(10):
        interp_id = interpreters.create()
        interpreters.run_string(
            interp_id,
            "from whenever import Instant; Instant.now()",
        )
        interpreters.destroy(interp_id)


def test_exceptions():
    assert issubclass(ImplicitlyIgnoringDST, TypeError)
    assert issubclass(InvalidOffsetError, ValueError)


def test_version():
    from whenever import __version__

    assert isinstance(__version__, str)


def test_no_attr_on_module():
    with pytest.raises((AttributeError, ImportError), match="DoesntExist"):
        from whenever import DoesntExist  # type: ignore[attr-defined] # noqa


@pytest.mark.skipif(
    sys.implementation.name == "pypy",
    reason="time-machine doesn't support PyPy",
)
def test_time_machine():
    import time_machine

    with time_machine.travel("1980-03-02 02:00 UTC"):
        assert Instant.now() == Instant.from_utc(1980, 3, 2, hour=2)


@system_tz_ams()
def test_patch_time():

    i = Instant.from_utc(1980, 3, 2, hour=2)

    # simplest case: freeze time at fixed UTC
    with patch_current_time(i, keep_ticking=False) as p:
        assert Instant.now() == i
        assert Date.today_in_system_tz() == i.to_system_tz().date()
        p.shift(hours=3)
        p.shift(hours=1)
        assert Instant.now() == i.add(hours=4)

    # patch has ended
    assert Instant.now() > Instant.from_utc(2024, 1, 1)
    assert Date.today_in_system_tz() > Date(2024, 1, 1)

    # complex case: freeze time at zoned datetime and keep ticking
    with patch_current_time(
        i.to_tz("Europe/Amsterdam"), keep_ticking=True
    ) as p:
        assert (Instant.now() - i) < seconds(1)
        p.shift(hours=2)
        sleep(0.000001)
        assert hours(2) < (Instant.now() - i) < hours(2.1)
        p.shift(days=2, disambiguate="raise")
        sleep(0.000001)
        assert hours(50) < (Instant.now() - i) < hours(50.1)

    assert Instant.now() - i > hours(40_000)


@pytest.mark.skipif(
    not (
        _EXTENSION_LOADED
        # We rely on __text_signature__ being set automatically for 1-argument
        # methods in Python 3.13+.
        and sys.version_info > (3, 13)
    ),
    reason="text signatures only relevant for the Rust extension",
)
def test_text_signature():
    classes = [
        Instant,
        OffsetDateTime,
        ZonedDateTime,
        SystemDateTime,
        PlainDateTime,
        Date,
        Time,
        TimeDelta,
        DateDelta,
        DateTimeDelta,
    ]
    methods = (
        m
        for m in chain.from_iterable(cls.__dict__.values() for cls in classes)
        if callable(m)
    )

    for m in methods:
        if m.__name__.startswith("_"):
            continue
        sig = m.__text_signature__
        assert sig is not None
        signature(m)  # raises ValueError if invalid


@no_type_check
def test_pydantic():
    try:
        import pydantic
    except ImportError:
        pytest.skip("pydantic not installed")

    # NOTE: the type ignore is needed because we generally don't install pydantic
    # when type-checking.
    class Model(pydantic.BaseModel):  # type: ignore[misc]
        inst: Instant
        zdt: ZonedDateTime
        odt: OffsetDateTime
        sdt: SystemDateTime
        date: Date = Date(2024, 1, 4)  # default value for testing
        time: Time
        ddelta: DateDelta
        tdelta: TimeDelta
        dtdelta: DateTimeDelta
        monthday: MonthDay
        yearmonth: YearMonth

    # Older versions of pydantic use inspect.signature()
    # in schema generation. Let's make sure that works.
    signature(DateTimeDelta.__get_pydantic_core_schema__)

    inst = Instant.from_utc(2024, 1, 1, hour=12)
    zdt = ZonedDateTime(2024, 1, 1, hour=12, tz="Europe/Amsterdam")
    odt = OffsetDateTime(2024, 1, 1, hour=12, offset=1)
    sdt = SystemDateTime(2024, 1, 1, hour=12)
    time = Time(12, 0, 0)
    date = Date(2024, 1, 4)
    ddelta = DateDelta(days=3, months=9)
    tdelta = TimeDelta(hours=3, minutes=9)
    dtdelta = DateTimeDelta(days=3, months=9, hours=3, minutes=9)
    monthday = MonthDay(month=1, day=1)
    yearmonth = YearMonth(year=2024, month=1)

    m = Model(
        inst=inst,
        zdt=zdt,
        odt=odt,
        sdt=sdt,
        time=time,
        ddelta=ddelta,
        tdelta=tdelta,
        dtdelta=dtdelta,
        monthday=monthday,
        yearmonth=yearmonth,
    )

    assert m.inst is inst
    assert m.zdt is zdt
    assert m.odt is odt
    assert m.sdt is sdt
    assert m.date == date  # default value
    assert m.time is time
    assert m.ddelta is ddelta
    assert m.tdelta is tdelta
    assert m.dtdelta is dtdelta
    assert m.monthday is monthday
    assert m.yearmonth is yearmonth

    data = m.model_dump()
    m2 = Model.model_validate(data)
    assert m2.inst is inst
    assert m2.zdt is zdt
    assert m2.odt is odt
    assert m2.sdt is sdt
    assert m2.date == date  # default value
    assert m2.time is time
    assert m2.ddelta is ddelta
    assert m2.tdelta is tdelta
    assert m2.dtdelta is dtdelta
    assert m2.monthday is monthday
    assert m2.yearmonth is yearmonth

    json_str = m.model_dump_json()
    json_data = json.loads(json_str)
    assert json_data["inst"] == inst.format_common_iso()
    assert json_data["zdt"] == zdt.format_common_iso()
    assert json_data["odt"] == odt.format_common_iso()
    assert json_data["sdt"] == sdt.format_common_iso()
    assert json_data["date"] == date.format_common_iso()
    assert json_data["time"] == time.format_common_iso()
    assert json_data["ddelta"] == ddelta.format_common_iso()
    assert json_data["tdelta"] == tdelta.format_common_iso()
    assert json_data["dtdelta"] == dtdelta.format_common_iso()
    assert json_data["monthday"] == monthday.format_common_iso()
    assert json_data["yearmonth"] == yearmonth.format_common_iso()

    m3 = Model.model_validate_json(json_str)
    assert m3.inst == inst
    assert m3.zdt == zdt
    assert m3.odt == odt
    assert m3.sdt == sdt
    assert m3.date == date
    assert m3.time == time
    assert m3.ddelta == ddelta
    assert m3.tdelta == tdelta
    assert m3.dtdelta == dtdelta
    assert m3.monthday == monthday
    assert m3.yearmonth == yearmonth

    json_schema = Model.model_json_schema()
    assert json_schema == {
        "properties": {
            "date": {
                "default": "2024-01-04",
                "title": "Date",
                "type": "string",
            },
            "ddelta": {"title": "Ddelta", "type": "string"},
            "dtdelta": {"title": "Dtdelta", "type": "string"},
            "inst": {"title": "Inst", "type": "string"},
            "monthday": {"title": "Monthday", "type": "string"},
            "odt": {"title": "Odt", "type": "string"},
            "sdt": {"title": "Sdt", "type": "string"},
            "tdelta": {"title": "Tdelta", "type": "string"},
            "time": {"title": "Time", "type": "string"},
            "yearmonth": {"title": "Yearmonth", "type": "string"},
            "zdt": {"title": "Zdt", "type": "string"},
        },
        "required": [
            "inst",
            "zdt",
            "odt",
            "sdt",
            "time",
            "ddelta",
            "tdelta",
            "dtdelta",
            "monthday",
            "yearmonth",
        ],
        "title": "Model",
        "type": "object",
    }
    # This mode is apparently used by FastAPI, and could give unexpected results
    assert Model.model_json_schema(mode="serialization") == json_schema

    # The constructor should be able to handle strings
    assert (
        Model(
            inst=inst.format_common_iso(),
            zdt=zdt.format_common_iso(),
            odt=odt.format_common_iso(),
            sdt=sdt.format_common_iso(),
            date=date.format_common_iso(),
            time=time.format_common_iso(),
            ddelta=ddelta.format_common_iso(),
            tdelta=tdelta.format_common_iso(),
            dtdelta=dtdelta.format_common_iso(),
            monthday=monthday.format_common_iso(),
            yearmonth=yearmonth.format_common_iso(),
        )
        == m2
    )

    # Parsing errors
    try:
        Model(
            inst=123,  # not a string
            zdt=zdt.format_common_iso().encode(),  # bytes instead of str
            odt=odt.format_common_iso(),
            sdt=sdt.format_common_iso(),
            date=date.format_common_iso(),
            time=time.format_common_iso(),
            ddelta=ddelta.format_common_iso(),
            tdelta=tdelta.format_common_iso(),
            dtdelta=dtdelta.format_common_iso(),
            monthday=monthday.format_common_iso(),
            yearmonth=yearmonth.format_common_iso(),
        )
    except pydantic.ValidationError as e:
        assert e.error_count() == 2
    else:
        assert False, "Expected ValidationError not raised"

    # JSON parsing errors
    try:
        Model.model_validate_json(
            json.dumps(
                {
                    "inst": 123,  # not a string
                    "zdt": "INVALID",
                    "odt": "",
                    "sdt": None,
                    "date": date.format_common_iso(),
                    "time": time.format_common_iso(),
                    "ddelta": ddelta.format_common_iso(),
                    "tdelta": tdelta.format_common_iso(),
                    "dtdelta": dtdelta.format_common_iso(),
                    "monthday": monthday.format_common_iso(),
                    "yearmonth": yearmonth.format_common_iso(),
                }
            )
        )
    except pydantic.ValidationError as e:
        assert e.error_count() == 4
    else:
        assert False, "Expected ValidationError not raised"
