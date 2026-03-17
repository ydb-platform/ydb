from collections.abc import Callable, Mapping, Sequence
from types import TracebackType
from typing import Any
from unittest.mock import Mock

from celery import Celery
from celery.canvas import Signature
from typing_extensions import Self

def TaskMessage(
    name: str,
    id: str | None = None,
    args: Sequence[Any] = (),
    kwargs: Mapping[Any, Any] | None = None,
    callbacks: Sequence[Signature[Any]] | None = None,
    errbacks: Sequence[Signature[Any]] | None = None,
    chain: Sequence[Signature[Any]] | None = None,
    shadow: str | None = None,
    utc: bool | None = None,
    **options: Any,
) -> Any: ...
def TaskMessage1(
    name: str,
    id: str | None = None,
    args: Sequence[Any] = (),
    kwargs: Mapping[Any, Any] | None = None,
    callbacks: Sequence[Signature[Any]] | None = None,
    errbacks: Sequence[Signature[Any]] | None = None,
    chain: Sequence[Signature[Any]] | None = None,
    **options: Any,
) -> Any: ...
def task_message_from_sig(
    app: Celery,
    sig: Signature[Any],
    utc: bool = True,
    TaskMessage: Callable[..., Any] = ...,
) -> Any: ...

class _ContextMock(Mock):
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None: ...

def ContextMock(*args: Any, **kwargs: Any) -> _ContextMock: ...
