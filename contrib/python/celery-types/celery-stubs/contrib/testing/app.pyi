from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from celery import Celery

DEFAULT_TEST_CONFIG: dict[str, Any]

class Trap:
    def __getattr__(self, name: str) -> None: ...

class UnitLogging:
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...

def TestApp(
    name: str | None = None,
    config: dict[str, Any] | None = None,
    enable_logging: bool = False,
    set_as_current: bool = False,
    log: type[UnitLogging] | None = ...,
    backend: Any = None,
    broker: Any = None,
    **kwargs: Any,
) -> Celery: ...
@contextmanager
def set_trap(app: Celery) -> Generator[None, None, None]: ...
@contextmanager
def setup_default_app(
    app: Celery, use_trap: bool = False
) -> Generator[None, None, None]: ...
