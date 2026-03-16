from __future__ import annotations

import datetime
from typing import NamedTuple, TypeAlias, TypedDict

from typing_extensions import NotRequired

JSONValue: TypeAlias = (
    'dict[str, "JSONValue"] | list["JSONValue"] | str | int | float | bool | None'
)
JSONDict = dict[str, JSONValue]


class TimeDeltaParams(TypedDict):
    weeks: NotRequired[int]
    days: NotRequired[int]
    hours: NotRequired[int]
    minutes: NotRequired[int]
    seconds: NotRequired[int]
    milliseconds: NotRequired[int]
    microseconds: NotRequired[int]


class JobToDefer(NamedTuple):
    queue_name: str
    task_name: str
    priority: int
    lock: str | None
    queueing_lock: str | None
    args: JSONDict
    scheduled_at: datetime.datetime | None
