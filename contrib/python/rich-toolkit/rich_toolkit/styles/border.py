from typing import Any, Optional, Tuple, Union

from rich import box
from rich.color import Color
from rich.console import Group, RenderableType
from rich.style import Style
from rich.text import Text

from rich_toolkit._rich_components import Panel
from rich_toolkit.container import Container
from rich_toolkit.element import CursorOffset, Element
from rich_toolkit.form import Form
from rich_toolkit.input import Input
from rich_toolkit.menu import Menu
from rich_toolkit.progress import Progress

from .base import BaseStyle


class BorderedStyle(BaseStyle):
    box = box.SQUARE

    def empty_line(self) -> RenderableType:
        return ""

    def _box(
        self,
        content: RenderableType,
        title: Union[str, Text, None],
        is_active: bool,
        border_color: Color,
        after: Tuple[RenderableType, ...] = (),
    ) -> RenderableType:
        return Group(
            Panel(
                content,
                title=title,
                title_align="left",
                highlight=is_active,
                width=50,
                box=self.box,
                border_style=Style(color=border_color),
            ),
            *after,
        )

    def render_container(
        self,
        element: Container,
        is_active: bool = False,
        done: bool = False,
        parent: Optional[Element] = None,
    ) -> RenderableType:
        content = super().render_container(element, is_active, done, parent)

        if isinstance(element, Form):
            return self._box(content, element.title, is_active, Color.parse("white"))

        return content

    def render_input(
        self,
        element: Input,
        is_active: bool = False,
        done: bool = False,
        parent: Optional[Element] = None,
        **metadata: Any,
    ) -> RenderableType:
        validation_message: Tuple[RenderableType, ...] = ()

        if isinstance(parent, Form):
            return super().render_input(element, is_active, done, parent, **metadata)

        if messages := self.render_validation_message(element):
            validation_message = tuple(messages)

        title = self.render_input_label(
            element,
            is_active=is_active,
            parent=parent,
        )

        # Determine border color based on validation state
        if element.valid is False:
            try:
                border_color = self.console.get_style("error").color or Color.parse(
                    "red"
                )
            except Exception:
                # Fallback if error style is not defined
                border_color = Color.parse("red")
        else:
            border_color = Color.parse("white")

        return self._box(
            self.render_input_value(element, is_active=is_active, parent=parent),
            title,
            is_active,
            border_color,
            after=validation_message,
        )

    def render_menu(
        self,
        element: Menu,
        is_active: bool = False,
        done: bool = False,
        parent: Optional[Element] = None,
        **metadata: Any,
    ) -> RenderableType:
        validation_message: Tuple[RenderableType, ...] = ()

        content: list[RenderableType] = []

        if done:
            content.append(
                Text(
                    element.result_display_name,
                    style=self.console.get_style("result"),
                )
            )

        else:
            separator = Text("\t" if element.inline else "\n")
            menu = self._build_menu_options(element, separator)
            filter_parts = self._build_filter_parts(element)

            content.extend(filter_parts)
            content.append(menu)

            if messages := self.render_validation_message(element):
                validation_message = tuple(messages)

        result = Group(*content)

        return self._box(
            result,
            self.render_input_label(element),
            is_active,
            Color.parse("white"),
            after=validation_message,
        )

    def render_progress(
        self,
        element: Progress,
        is_active: bool = False,
        done: bool = False,
        parent: Optional[Element] = None,
    ) -> RenderableType:
        if done and element._cancelled:
            content = Text.assemble(
                ("Cancelled.", self.console.get_style("cancelled")),
            )
            return self._box(
                content, element.title, is_active, border_color=Color.parse("white")
            )

        content: str | Group | Text = element.current_message
        title: Union[str, Text, None] = None

        title = element.title

        if element.logs and element._inline_logs:
            lines_to_show = (
                element.logs[-element.lines_to_show :]
                if element.lines_to_show > 0
                else element.logs
            )

            content = Group(
                *[
                    self.render_element(
                        line,
                        index=index,
                        max_lines=element.lines_to_show,
                        total_lines=len(element.logs),
                    )
                    for index, line in enumerate(lines_to_show)
                ]
            )

        border_color = Color.parse("white")

        if not done:
            colors = self._get_animation_colors(
                steps=10, animation_status="started", breathe=True
            )

            border_color = colors[self.animation_counter % 10]

        return self._box(content, title, is_active, border_color=border_color)

    def get_cursor_offset_for_element(
        self, element: Element, parent: Optional[Element] = None
    ) -> CursorOffset:
        top_offset = element.cursor_offset.top
        left_offset = element.cursor_offset.left + 2

        if isinstance(element, Input) and element.inline:
            # we don't support inline inputs yet in border style
            top_offset += 1
            inline_left_offset = (len(element.label) - 1) if element.label else 0
            left_offset = element.cursor_offset.left - inline_left_offset

        if isinstance(parent, Form):
            top_offset += 1

        return CursorOffset(top=top_offset, left=left_offset)
