import numbers
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Literal, NamedTuple, TypeAlias

import ephem
from celery.app.base import Celery
from celery.utils.time import ffwd
from typing_extensions import override

class schedstate(NamedTuple):
    is_due: bool
    next: float

def cronfield(s: str | None) -> str: ...

class ParseException(Exception): ...

class BaseSchedule:
    nowfunc: Callable[[], datetime]
    def __init__(
        self,
        nowfun: Callable[[], datetime] | None = ...,
        app: Celery | None = ...,
    ) -> None: ...
    def now(self) -> datetime: ...
    def remaining_estimate(self, last_run_at: datetime) -> timedelta: ...
    def is_due(self, last_run_at: datetime) -> schedstate: ...
    def maybe_make_aware(self, dt: datetime) -> datetime: ...
    @property
    def app(self) -> Celery: ...
    @app.setter
    def app(self, app: Celery) -> None: ...
    @property
    def tz(self) -> str: ...
    @property
    def utc_enabled(self) -> bool: ...
    def to_local(self, dt: datetime) -> datetime: ...
    @override
    def __eq__(self, other: object) -> bool: ...

class schedule(BaseSchedule):
    def __init__(
        self,
        run_every: float | timedelta | None = ...,
        relative: bool = ...,
        nowfun: Callable[[], datetime] | None = ...,
        app: Celery | None = ...,
    ) -> None: ...
    @property
    def seconds(self) -> int: ...
    @property
    def human_seconds(self) -> str: ...

_ModuleLevelParseException = ParseException

class crontab_parser:
    ParseException: _ModuleLevelParseException
    def __init__(self, max_: int = ..., min_: int = ...) -> None: ...
    def parse(self, spec: str) -> set[int]: ...

class crontab(BaseSchedule):
    def __init__(
        self,
        minute: str | int | list[int] = ...,
        hour: str | int | list[int] = ...,
        day_of_week: str | int | list[int] = ...,
        day_of_month: str | int | list[int] = ...,
        month_of_year: str | int | list[int] = ...,
        nowfun: Callable[[], datetime] | None = ...,
        app: Celery | None = ...,
    ) -> None: ...
    def remaining_delta(
        self, last_run_at: datetime, tz: str | None = ..., ffwd: ffwd = ...
    ) -> tuple[datetime, timedelta, datetime]: ...

def maybe_schedule(s: numbers.Number | timedelta | BaseSchedule) -> schedule: ...

_SolarEvent: TypeAlias = Literal[
    "dawn_astronomical",
    "dawn_nautical",
    "dawn_civil",
    "sunrise",
    "solar_noon",
    "sunset",
    "dusk_civil",
    "dusk_nautical",
    "dusk_astronomical",
]

class solar(BaseSchedule):
    cal: ephem.Observer
    event: _SolarEvent
    lat: float
    lon: float
    def __init__(
        self,
        event: _SolarEvent,
        lat: float,
        lon: float,
        nowfun: Callable[[], datetime] | None = ...,
        app: Celery | None = ...,
    ) -> None: ...
