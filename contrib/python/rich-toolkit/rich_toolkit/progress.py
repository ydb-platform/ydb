from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional

from rich.console import Console, RenderableType
from rich.live import Live
from rich.text import Text

from .element import Element

if TYPE_CHECKING:
    from .styles.base import BaseStyle


class ProgressLine(Element):
    def __init__(self, text: str | Text, parent: Progress):
        self.text = text
        self.parent = parent


class Progress(Live, Element):
    current_message: str | Text

    def __init__(
        self,
        title: str,
        style: Optional[BaseStyle] = None,
        console: Optional[Console] = None,
        transient: bool = False,
        transient_on_error: bool = False,
        inline_logs: bool = False,
        lines_to_show: int = -1,
        **metadata: Dict[Any, Any],
    ) -> None:
        self.title = title
        self.current_message = title
        self.is_error = False
        self._transient_on_error = transient_on_error
        self._inline_logs = inline_logs
        self.lines_to_show = lines_to_show

        self.logs: List[ProgressLine] = []

        self._cancelled = False

        Element.__init__(self, style=style, metadata=metadata)
        super().__init__(console=console, refresh_per_second=8, transient=transient)

    # TODO: remove this once rich uses "Self"
    def __enter__(self) -> "Progress":
        self.start(refresh=self._renderable is not None)

        return self

    def __exit__(self, exc_type: type | None, *args: object) -> None:
        if exc_type is KeyboardInterrupt:
            self._cancelled = True

        super().__exit__(exc_type, *args)

    def get_renderable(self) -> RenderableType:
        return self.style.render_element(self, done=not self._started)

    def log(self, text: str | Text) -> None:
        if self._inline_logs:
            self.logs.append(ProgressLine(text, self))
        else:
            self.current_message = text

    def set_error(self, text: str) -> None:
        self.current_message = text
        self.is_error = True
        self.transient = self._transient_on_error
