from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Any

try:
    from opentelemetry.instrumentation.redis import RedisInstrumentor

    from logfire.integrations.redis import RequestHook, ResponseHook
except ImportError:
    raise RuntimeError(
        '`logfire.instrument_redis()` requires the `opentelemetry-instrumentation-redis` package.\n'
        'You can install this with:\n'
        "    pip install 'logfire[redis]'"
    )

from logfire._internal.constants import ATTRIBUTES_MESSAGE_KEY
from logfire._internal.utils import truncate_string

if TYPE_CHECKING:
    from opentelemetry.trace import Span
    from redis import Connection


def instrument_redis(
    *,
    capture_statement: bool,
    request_hook: RequestHook | None,
    response_hook: ResponseHook | None,
    **kwargs: Any,
) -> None:
    """Instrument the `redis` module so that spans are automatically created for each operation.

    See the `Logfire.instrument_redis` method for details.
    """
    if capture_statement:
        request_hook = _capture_statement_hook(request_hook)

    RedisInstrumentor().instrument(request_hook=request_hook, response_hook=response_hook, **kwargs)  # type: ignore[reportUnknownMemberType]


def _capture_statement_hook(request_hook: RequestHook | None = None) -> RequestHook:
    truncate_value = functools.partial(truncate_string, max_length=20, middle='...')

    def _capture_statement(
        span: Span, instance: Connection, command: tuple[object, ...], *args: Any, **kwargs: Any
    ) -> None:
        str_command = list(map(str, command))
        span.set_attribute('db.statement', ' '.join(str_command))
        span.set_attribute(ATTRIBUTES_MESSAGE_KEY, ' '.join(map(truncate_value, str_command)))
        if request_hook is not None:
            request_hook(span, instance, command, *args, **kwargs)

    return _capture_statement
