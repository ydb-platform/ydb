import inspect
import time
from collections.abc import Callable, Mapping, Sequence
from functools import wraps
from traceback import format_exception
from typing import Any, TypeVar

from django.utils.crypto import get_random_string
from typing_extensions import ParamSpec

T = TypeVar("T")
P = ParamSpec("P")


def is_module_level_function(func: Callable) -> bool:
    if not inspect.isfunction(func) or inspect.isbuiltin(func):
        return False

    if "<locals>" in func.__qualname__:
        return False

    return True


def normalize_json(obj: Any) -> Any:
    """Recursively normalize an object into JSON-compatible types."""
    match obj:
        case Mapping():
            return {normalize_json(k): normalize_json(v) for k, v in obj.items()}
        case bytes():
            try:
                return obj.decode("utf-8")
            except UnicodeDecodeError as e:
                raise ValueError(f"Unsupported value: {type(obj)}") from e
        case str() | int() | float() | bool() | None:
            return obj
        case Sequence():  # str and bytes were already handled.
            return [normalize_json(v) for v in obj]
        case _:  # Other types can't be serialized to JSON
            raise TypeError(f"Unsupported type: {type(obj)}")


def retry(*, retries: int = 3, backoff_delay: float = 0.1) -> Callable:
    """
    Retry the given code `retries` times, raising the final error.

    `backoff_delay` can be used to add a delay between attempts.
    """

    def wrapper(f: Callable[P, T]) -> Callable[P, T]:
        @wraps(f)
        def inner_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:  # type:ignore[return]
            for attempt in range(1, retries + 1):
                try:
                    return f(*args, **kwargs)
                except KeyboardInterrupt:
                    # Let the user ctrl-C out of the program without a retry
                    raise
                except BaseException:
                    if attempt == retries:
                        raise
                    time.sleep(backoff_delay)

        return inner_wrapper

    return wrapper


def get_module_path(val: Any) -> str:
    return f"{val.__module__}.{val.__qualname__}"


def get_exception_traceback(exc: BaseException) -> str:
    return "".join(format_exception(exc))


def get_random_id() -> str:
    """
    Return a random string for use as a Task or worker id.

    Whilst 64 characters is the max, just use 32 as a sensible middle-ground.
    """
    return get_random_string(32)
