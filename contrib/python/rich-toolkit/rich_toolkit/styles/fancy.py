from typing import Any, Dict, List, Optional

from rich._loop import loop_first_last
from rich.console import Console, ConsoleOptions, RenderableType, RenderResult
from rich.segment import Segment
from rich.style import Style
from rich.text import Text
from typing_extensions import Literal

from rich_toolkit.container import Container
from rich_toolkit.element import CursorOffset, Element
from rich_toolkit.form import Form
from rich_toolkit.progress import Progress
from rich_toolkit.styles.base import BaseStyle


class FancyPanel:
    def __init__(
        self,
        renderable: RenderableType,
        style: BaseStyle,
        title: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        is_animated: Optional[bool] = None,
        animation_counter: Optional[int] = None,
        done: bool = False,
    ) -> None:
        self.renderable = renderable
        self._title = title
        self.metadata = metadata or {}
        self.width = None
        self.expand = True
        self.is_animated = is_animated
        self.counter = animation_counter or 0
        self.style = style
        self.done = done

    def _get_decoration(self, suffix: str = "") -> Segment:
        char = "┌" if self.metadata.get("title") else "◆"

        animated = not self.done and self.is_animated

        animation_status: Literal["started", "stopped", "error"] = (
            "started" if animated else "stopped"
        )

        color = self.style._get_animation_colors(
            steps=14, breathe=True, animation_status=animation_status
        )[self.counter % 14]

        return Segment(char + suffix, style=Style.from_color(color))

    def _strip_trailing_newlines(
        self, lines: List[List[Segment]]
    ) -> List[List[Segment]]:
        # remove all empty lines from the end of the list

        while lines and all(segment.text.strip() == "" for segment in lines[-1]):
            lines.pop()

        return lines

    def __rich_console__(
        self, console: "Console", options: "ConsoleOptions"
    ) -> "RenderResult":
        renderable = self.renderable

        lines = console.render_lines(
            renderable, options.update_width(options.max_width - 2)
        )
        lines = self._strip_trailing_newlines(lines)

        line_start = self._get_decoration()

        new_line = Segment.line()

        if self._title is not None:
            yield line_start
            yield Segment(" ")
            yield self._title

        for first, last, line in loop_first_last(lines):
            if first and not self._title:
                decoration = (
                    Segment("┌ ")
                    if self.metadata.get("title", False)
                    else self._get_decoration(suffix=" ")
                )
            elif last and self.metadata.get("started", True):
                decoration = Segment("└ ")
            else:
                decoration = Segment("│ ")

            yield decoration
            yield from line

            if not last:
                yield new_line


class FancyStyle(BaseStyle):
    _should_show_progress_title = False

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.cursor_offset = 2
        self.decoration_size = 2

    def _should_decorate(self, element: Any, parent: Optional[Element] = None) -> bool:
        return not isinstance(parent, (Progress, Container))

    def render_element(
        self,
        element: Any,
        is_active: bool = False,
        done: bool = False,
        parent: Optional[Element] = None,
        **metadata: Any,
    ) -> RenderableType:
        title: Optional[str] = None

        is_animated = False

        if isinstance(element, Progress):
            title = element.title
            is_animated = True

        rendered = super().render_element(
            element=element, is_active=is_active, done=done, parent=parent, **metadata
        )

        if self._should_decorate(element, parent):
            rendered = FancyPanel(
                rendered,
                title=title,
                metadata=metadata,
                is_animated=is_animated,
                done=done,
                animation_counter=self.animation_counter,
                style=self,
            )

        return rendered

    def empty_line(self) -> Text:
        """Return an empty line with decoration.

        Returns:
            A text object representing an empty line
        """
        return Text("│", style="fancy.normal")

    def get_cursor_offset_for_element(
        self, element: Element, parent: Optional[Element] = None
    ) -> CursorOffset:
        """Get the cursor offset for an element.

        Args:
            element: The element to get the cursor offset for

        Returns:
            The cursor offset
        """
        from rich_toolkit.input import Input

        if isinstance(element, Form):
            return element.cursor_offset

        offset = element.cursor_offset
        top = offset.top

        if isinstance(element, Input) and not element.inline and element.label:
            label_lines = self._count_label_lines(
                element.label, decoration_width=self.decoration_size
            )
            top = label_lines + 1

        return CursorOffset(
            top=top,
            left=self.decoration_size + offset.left,
        )
