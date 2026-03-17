from typing import Callable, Optional

from .boundmethod import BoundMethod
from .client_protocol import ClientProtocol
from .methodspec import MethodSpec


class Method:
    def __init__(
        self,
        method_spec: MethodSpec,
        method_class: Optional[Callable[..., BoundMethod]] = None,
    ):
        self.name = method_spec.func.__name__
        self.method_spec = method_spec
        self.method_class = method_class
        self._on_error = None

    def __set_name__(self, owner, name):
        self.name = name
        if owner.method_class:
            self.method_class = owner.method_class
        else:
            raise ValueError(
                f"No type for bound method is specified. "
                f"Provide either `{owner.__name__}.method_class` attribute or "
                f"`method_class=` argument for decorator "
                f"on your `{name}` method",
            )

    def __get__(
        self,
        instance: Optional[ClientProtocol],
        objtype=None,
    ) -> BoundMethod:
        return self.method_class(
            name=self.name,
            method_spec=self.method_spec,
            client=instance,
            on_error=self._on_error,
        )

    def on_error(self, func) -> "Method":
        self._on_error = func
        return self
