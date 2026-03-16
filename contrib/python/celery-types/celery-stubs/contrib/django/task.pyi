from collections.abc import Mapping
from datetime import datetime
from typing import Any, TypeVar

import celery.result
import kombu
from celery.app.task import Task
from celery.canvas import Signature
from typing_extensions import ParamSpec

_P = ParamSpec("_P")
_R_co = TypeVar("_R_co", covariant=True)

class DjangoTask(Task[_P, _R_co]):
    def delay_on_commit(
        self, *args: _P.args, **kwargs: _P.kwargs
    ) -> celery.result.AsyncResult[_R_co]: ...
    def apply_async_on_commit(
        self,
        args: tuple[Any, ...] | None = ...,
        kwargs: dict[str, Any] | None = ...,
        task_id: str | None = ...,
        producer: kombu.Producer | None = ...,
        link: Signature[Any] | list[Signature[Any]] | None = ...,
        link_error: Signature[Any] | list[Signature[Any]] | None = ...,
        shadow: str | None = ...,
        *,
        # options
        countdown: float = ...,
        eta: datetime = ...,
        expires: float | datetime = ...,
        retry: bool = ...,
        retry_policy: Mapping[str, Any] = ...,
        queue: str | kombu.Queue = ...,
        exchange: str | kombu.Exchange = ...,
        routing_key: str = ...,
        priority: int = ...,
        serializer: str = ...,
        compression: str = ...,
        add_to_parent: bool = ...,
        publisher: kombu.Producer = ...,
        headers: dict[str, str] = ...,
        ignore_result: bool = ...,
        time_limit: int = ...,
        soft_time_limit: int = ...,
    ) -> celery.result.AsyncResult[_R_co]: ...
