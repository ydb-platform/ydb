from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Optional

from .element import Element

if TYPE_CHECKING:
    from .styles.base import BaseStyle


class Button(Element):
    def __init__(
        self,
        name: str,
        label: str,
        callback: Optional[Callable] = None,
        style: Optional[BaseStyle] = None,
        **metadata: Any,
    ):
        self.name = name
        self.label = label
        self.callback = callback

        super().__init__(style=style, metadata=metadata)

    def activate(self) -> Any:
        if self.callback:
            return self.callback()
        return True
