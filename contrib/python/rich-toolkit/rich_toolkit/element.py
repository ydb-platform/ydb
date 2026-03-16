from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, NamedTuple, Optional

if TYPE_CHECKING:
    from .styles import BaseStyle


class CursorOffset(NamedTuple):
    top: int
    left: int


class Element:
    metadata: Dict[Any, Any] = {}
    style: BaseStyle

    focusable: bool = True

    def __init__(
        self,
        style: Optional[BaseStyle] = None,
        metadata: Optional[Dict[Any, Any]] = None,
    ):
        from .styles import MinimalStyle

        self._cancelled = False
        self.metadata = metadata or {}
        self.style = style or MinimalStyle()

    @property
    def cursor_offset(self) -> CursorOffset:
        return CursorOffset(top=0, left=0)

    @property
    def should_show_cursor(self) -> bool:
        return False

    def handle_key(self, key: str) -> None:  # noqa: B027
        pass

    def on_cancel(self) -> None:  # noqa: B027
        self._cancelled = True
