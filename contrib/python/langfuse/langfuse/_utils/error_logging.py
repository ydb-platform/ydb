import functools
import logging
from typing import Any, Callable, List, Optional

logger = logging.getLogger("langfuse")


def catch_and_log_errors(func: Callable[..., Any]) -> Callable[..., Any]:
    """Catch all exceptions and log them. Do NOT re-raise the exception."""

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"An error occurred in {func.__name__}: {e}", exc_info=True)

    return wrapper


def auto_decorate_methods_with(
    decorator: Callable[[Callable[..., Any]], Callable[..., Any]],
    exclude: Optional[List[str]] = None,
) -> Callable[[type], type]:
    """Class decorator to automatically apply a given decorator to all
    methods of a class.
    """

    def class_decorator(cls: type) -> type:
        for attr_name, attr_value in cls.__dict__.items():
            if exclude and attr_name in exclude:
                continue
            if callable(attr_value):
                # Wrap callable attributes (methods) with the decorator
                setattr(cls, attr_name, decorator(attr_value))
            elif isinstance(attr_value, classmethod):
                # Special handling for classmethods
                original_method = attr_value.__func__
                decorated_method = decorator(original_method)
                setattr(cls, attr_name, classmethod(decorated_method))
            elif isinstance(attr_value, staticmethod):
                # Special handling for staticmethods
                original_method = attr_value.__func__
                decorated_method = decorator(original_method)
                setattr(cls, attr_name, staticmethod(decorated_method))
        return cls

    return class_decorator
