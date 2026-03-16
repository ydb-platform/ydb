from collections import UserDict
from contextvars import copy_context
from typing import Any

from starlette_context import _request_scope_context_storage
from starlette_context.errors import (
    ConfigurationError,
    ContextDoesNotExistError,
)


class _Context(UserDict):
    """
    A mapping with dict-like interface.

    It is using request context as a data store. Can be used only if context
    has been created in the middleware.
    """

    def __init__(self, *args: Any, **kwargs: Any):  # noqa
        # not calling super on purpose
        if args or kwargs:
            raise ConfigurationError("Can't instantiate with attributes")

    @property
    def data(self) -> dict:  # type: ignore
        """
        Dump this to json.

        Object itself it not serializable.
        """
        try:
            return _request_scope_context_storage.get()
        except LookupError:
            raise ContextDoesNotExistError

    def exists(self) -> bool:
        return _request_scope_context_storage in copy_context()

    def copy(self) -> dict:  # type: ignore
        """
        Read only context data.
        """
        import copy

        return copy.copy(self.data)

    def __repr__(self) -> str:
        # Opaque type to avoid default implementation
        # that could try to look into data while out of request cycle
        try:
            return f"<{__name__}.{self.__class__.__name__} {self.data}>"
        except ContextDoesNotExistError:
            return f"<{__name__}.{self.__class__.__name__} {{}}>"

    def __str__(self) -> str:
        try:
            return str(self.data)
        except ContextDoesNotExistError:
            return str({})


context = _Context()
