from logging import Formatter, Logger, LogRecord, _SysExcInfoType
from typing import ClassVar

from celery.utils.term import colored
from typing_extensions import override

class LoggingProxy: ...

def get_logger(name: str) -> Logger: ...
def get_task_logger(name: str) -> Logger: ...

task_logger: Logger
worker_logger: Logger

class ColorFormatter(Formatter):
    use_color: bool

    COLORS: ClassVar[dict[str, colored]]
    colors: ClassVar[dict[str, colored]]
    def __init__(self, fmt: str | None = ..., use_color: bool = ...) -> None: ...
    @override
    def formatException(self, ei: _SysExcInfoType) -> str: ...
    @override
    def format(self, record: LogRecord) -> str: ...
