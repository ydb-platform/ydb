import asyncio
import dataclasses
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Union

from .typing import WEEKDAYS, OptionType, SecondsTimedelta, WeekdayOptionType, WorkerCoroutine
from .utils import import_string, to_seconds


@dataclass
class Options:
    month: OptionType
    day: OptionType
    weekday: WeekdayOptionType
    hour: OptionType
    minute: OptionType
    second: OptionType
    microsecond: int


def next_cron(
    previous_dt: datetime,
    *,
    month: OptionType = None,
    day: OptionType = None,
    weekday: WeekdayOptionType = None,
    hour: OptionType = None,
    minute: OptionType = None,
    second: OptionType = 0,
    microsecond: int = 123_456,
) -> datetime:
    """
    Find the next datetime matching the given parameters.
    """
    dt = previous_dt + timedelta(seconds=1)
    if isinstance(weekday, str):
        weekday = WEEKDAYS.index(weekday.lower())
    options = Options(
        month=month, day=day, weekday=weekday, hour=hour, minute=minute, second=second, microsecond=microsecond
    )

    while True:
        next_dt = _get_next_dt(dt, options)
        # print(dt, next_dt)
        if next_dt is None:
            return dt
        dt = next_dt


def _get_next_dt(dt_: datetime, options: Options) -> Optional[datetime]:  # noqa: C901
    for field, v in dataclasses.asdict(options).items():
        if v is None:
            continue
        if field == 'weekday':
            next_v = dt_.weekday()
        else:
            next_v = getattr(dt_, field)
        if isinstance(v, int):
            mismatch = next_v != v
        elif isinstance(v, (set, list, tuple)):
            mismatch = next_v not in v
        else:
            raise RuntimeError(v)
        # print(field, v, next_v, mismatch)
        if mismatch:
            micro = max(dt_.microsecond - options.microsecond, 0)
            if field == 'month':
                if dt_.month == 12:
                    return datetime(dt_.year + 1, 1, 1, tzinfo=dt_.tzinfo)
                else:
                    return datetime(dt_.year, dt_.month + 1, 1, tzinfo=dt_.tzinfo)
            elif field in ('day', 'weekday'):
                return (
                    dt_
                    + timedelta(days=1)
                    - timedelta(hours=dt_.hour, minutes=dt_.minute, seconds=dt_.second, microseconds=micro)
                )
            elif field == 'hour':
                return dt_ + timedelta(hours=1) - timedelta(minutes=dt_.minute, seconds=dt_.second, microseconds=micro)
            elif field == 'minute':
                return dt_ + timedelta(minutes=1) - timedelta(seconds=dt_.second, microseconds=micro)
            elif field == 'second':
                return dt_ + timedelta(seconds=1) - timedelta(microseconds=micro)
            else:
                if field != 'microsecond':
                    raise RuntimeError(field)
                return dt_ + timedelta(microseconds=options.microsecond - dt_.microsecond)
    return None


@dataclass
class CronJob:
    name: str
    coroutine: WorkerCoroutine
    month: OptionType
    day: OptionType
    weekday: WeekdayOptionType
    hour: OptionType
    minute: OptionType
    second: OptionType
    microsecond: int
    run_at_startup: bool
    unique: bool
    job_id: Optional[str]
    timeout_s: Optional[float]
    keep_result_s: Optional[float]
    keep_result_forever: Optional[bool]
    max_tries: Optional[int]
    next_run: Optional[datetime] = None

    def calculate_next(self, prev_run: datetime) -> None:
        self.next_run = next_cron(
            prev_run,
            month=self.month,
            day=self.day,
            weekday=self.weekday,
            hour=self.hour,
            minute=self.minute,
            second=self.second,
            microsecond=self.microsecond,
        )

    def __repr__(self) -> str:
        return '<CronJob {}>'.format(' '.join(f'{k}={v}' for k, v in self.__dict__.items()))


def cron(
    coroutine: Union[str, WorkerCoroutine],
    *,
    name: Optional[str] = None,
    month: OptionType = None,
    day: OptionType = None,
    weekday: WeekdayOptionType = None,
    hour: OptionType = None,
    minute: OptionType = None,
    second: OptionType = 0,
    microsecond: int = 123_456,
    run_at_startup: bool = False,
    unique: bool = True,
    job_id: Optional[str] = None,
    timeout: Optional[SecondsTimedelta] = None,
    keep_result: Optional[float] = 0,
    keep_result_forever: Optional[bool] = False,
    max_tries: Optional[int] = 1,
) -> CronJob:
    """
    Create a cron job, eg. it should be executed at specific times.

    Workers will enqueue this job at or just after the set times. If ``unique`` is true (the default) the
    job will only be run once even if multiple workers are running.

    :param coroutine: coroutine function to run
    :param name: name of the job, if None, the name of the coroutine is used
    :param month: month(s) to run the job on, 1 - 12
    :param day: day(s) to run the job on, 1 - 31
    :param weekday: week day(s) to run the job on, 0 - 6 or mon - sun
    :param hour: hour(s) to run the job on, 0 - 23
    :param minute: minute(s) to run the job on, 0 - 59
    :param second: second(s) to run the job on, 0 - 59
    :param microsecond: microsecond(s) to run the job on,
        defaults to 123456 as the world is busier at the top of a second, 0 - 1e6
    :param run_at_startup: whether to run as worker starts
    :param unique: whether the job should only be executed once at each time (useful if you have multiple workers)
    :param job_id: ID of the job, can be used to enforce job uniqueness, spanning multiple cron schedules
    :param timeout: job timeout
    :param keep_result: how long to keep the result for
    :param keep_result_forever: whether to keep results forever
    :param max_tries: maximum number of tries for the job
    """

    if isinstance(coroutine, str):
        name = name or 'cron:' + coroutine
        coroutine_: WorkerCoroutine = import_string(coroutine)
    else:
        coroutine_ = coroutine

    if not asyncio.iscoroutinefunction(coroutine_):
        raise RuntimeError(f'{coroutine_} is not a coroutine function')
    timeout = to_seconds(timeout)
    keep_result = to_seconds(keep_result)

    return CronJob(
        name or 'cron:' + coroutine_.__qualname__,
        coroutine_,
        month,
        day,
        weekday,
        hour,
        minute,
        second,
        microsecond,
        run_at_startup,
        unique,
        job_id,
        timeout,
        keep_result,
        keep_result_forever,
        max_tries,
    )
