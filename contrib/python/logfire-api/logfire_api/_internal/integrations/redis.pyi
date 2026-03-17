from logfire._internal.constants import ATTRIBUTES_MESSAGE_KEY as ATTRIBUTES_MESSAGE_KEY
from logfire._internal.utils import truncate_string as truncate_string
from logfire.integrations.redis import RequestHook as RequestHook, ResponseHook as ResponseHook
from typing import Any

def instrument_redis(*, capture_statement: bool, request_hook: RequestHook | None, response_hook: ResponseHook | None, **kwargs: Any) -> None:
    """Instrument the `redis` module so that spans are automatically created for each operation.

    See the `Logfire.instrument_redis` method for details.
    """
