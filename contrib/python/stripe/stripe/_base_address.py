from typing import Optional
from typing_extensions import NotRequired, TypedDict, Literal


BaseAddress = Literal["api", "files", "connect", "meter_events"]


class BaseAddresses(TypedDict):
    api: NotRequired[Optional[str]]
    connect: NotRequired[Optional[str]]
    files: NotRequired[Optional[str]]
    meter_events: NotRequired[Optional[str]]
