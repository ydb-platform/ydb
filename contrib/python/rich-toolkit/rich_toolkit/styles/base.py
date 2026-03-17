from __future__ import annotations

from typing import Any, Dict, Optional, Type, TypeVar, Union

from rich.color import Color
from rich.console import Console, ConsoleRenderable, Group, RenderableType
from rich.text import Text
from rich.theme import Theme
from typing_extensions import Literal

from rich_toolkit.button import Button
from rich_toolkit.container import Container
from rich_toolkit.element import CursorOffset, Element
from rich_toolkit.input import Input
from rich_toolkit.menu import Menu
from rich_toolkit.progress import Progress, ProgressLine
from rich_toolkit.spacer import Spacer
from rich_toolkit.utils.colors import (
    fade_text,
    get_terminal_background_color,
    get_terminal_text_color,
    lighten,
)

ConsoleRenderableClass = TypeVar(
    "ConsoleRenderableClass", bound=Type[ConsoleRenderable]
)


class BaseStyle:
    brightness_multiplier = 0.1

    base_theme = {
        "tag.title": "bold",
        "tag": "bold",
        "text": "#ffffff",
        "selected": "green",
        "result": "white",
        "progress": "on #893AE3",
        "error": "red",
        "cancelled": "red italic",
        # is there a way to make nested styles?
        # like label.active uses active style if not set?
        "active": "green",
        "title.error": "white",
        "title.cancelled": "white",
        "placeholder": "grey62",
        "placeholder.cancelled": "indian_red strike",
    }

    _should_show_progress_title = True

    def __init__(
        self,
        theme: Optional[Dict[str, str]] = None,
        background_color: str = "#000000",
        text_color: str = "#FFFFFF",
    ):
        self.background_color = get_terminal_background_color(background_color)
        self.text_color = get_terminal_text_color(text_color)
        self.animation_counter = 0

        base_theme = Theme(self.base_theme)
        self.console = Console(theme=base_theme)

        if theme:
            self.console.push_theme(Theme(theme))

    def empty_line(self) -> RenderableType:
        return " "

    def _get_animation_colors(
        self,
        steps: int = 5,
        breathe: bool = False,
        animation_status: Literal["started", "stopped", "error"] = "started",
        **metadata: Any,
    ) -> list[Color]:
        animated = animation_status == "started"

        if animation_status == "error":
            base_color = self.console.get_style("error").color

            if base_color is None:
                base_color = Color.parse("red")

        else:
            base_color = self.console.get_style("progress").bgcolor

        if not base_color:
            base_color = Color.from_rgb(255, 255, 255)

        if breathe:
            steps = steps // 2

        if animated and base_color.triplet is not None:
            colors = [
                lighten(base_color, self.brightness_multiplier * i)
                for i in range(0, steps)
            ]

        else:
            colors = [base_color] * steps

        if breathe:
            colors = colors + colors[::-1]

        return colors

    def _count_label_lines(self, label: str, decoration_width: int = 0) -> int:
        available_width = self.console.width - decoration_width
        if available_width <= 0:
            return 1
        renderable = Text.from_markup(label) if isinstance(label, str) else label
        lines = self.console.render_lines(
            renderable,
            self.console.options.update_width(available_width),
            pad=False,
        )
        return len(lines)

    def get_cursor_offset_for_element(
        self, element: Element, parent: Optional[Element] = None
    ) -> CursorOffset:
        offset = element.cursor_offset
        if isinstance(element, Input) and not element.inline and element.label:
            label_lines = self._count_label_lines(element.label)
            return CursorOffset(top=label_lines + 1, left=offset.left)
        return offset

    def render_element(
        self,
        element: Any,
        is_active: bool = False,
        done: bool = False,
        parent: Optional[Element] = None,
        **kwargs: Any,
    ) -> RenderableType:
        if isinstance(element, str):
            return self.render_string(element, is_active, done, parent)
        elif isinstance(element, Button):
            return self.render_button(element, is_active, done, parent)
        elif isinstance(element, Container):
            return self.render_container(element, is_active, done, parent)
        elif isinstance(element, Input):
            return self.render_input(element, is_active, done, parent)
        elif isinstance(element, Menu):
            return self.render_menu(element, is_active, done, parent)
        elif isinstance(element, Progress):
            self.animation_counter += 1

            return self.render_progress(element, is_active, done, parent)
        elif isinstance(element, ProgressLine):
            return self.render_progress_log_line(
                element.text,
                parent=parent,
                index=kwargs.get("index", 0),
                max_lines=kwargs.get("max_lines", -1),
                total_lines=kwargs.get("total_lines", -1),
            )
        elif isinstance(element, Spacer):
            return self.render_spacer()
        elif isinstance(element, ConsoleRenderable):
            return element

        raise ValueError(f"Unknown element type: {type(element)}")

    def render_string(
        self,
        string: str,
        is_active: bool = False,
        done: bool = False,
        parent: Optional[Element] = None,
    ) -> RenderableType:
        return string

    def render_button(
        self,
        element: Button,
        is_active: bool = False,
        done: bool = False,
        parent: Optional[Element] = None,
    ) -> RenderableType:
        style = "black on blue" if is_active else "white on black"
        return Text(f" {element.label} ", style=style)

    def render_spacer(self) -> RenderableType:
        return ""

    def render_container(
        self,
        container: Container,
        is_active: bool = False,
        done: bool = False,
        parent: Optional[Element] = None,
    ) -> RenderableType:
        content = []

        for i, element in enumerate(container.elements):
            content.append(
                self.render_element(
                    element,
                    is_active=i == container.active_element_index,
                    done=done,
                    parent=container,
                )
            )

        return Group(*content, "\n" if not done else "")

    def render_input(
        self,
        element: Input,
        is_active: bool = False,
        done: bool = False,
        parent: Optional[Element] = None,
    ) -> RenderableType:
        label = self.render_input_label(element, is_active=is_active, parent=parent)
        text = self.render_input_value(
            element, is_active=is_active, parent=parent, done=done
        )

        contents = []

        if element.inline or done:
            if done and element.password:
                text = "*" * len(element.text)
            if label:
                text = f"{label} {text}"

            contents.append(text)
        else:
            if label:
                contents.append(label)

            contents.append(text)

        if validation_message := self.render_validation_message(element):
            contents.extend(validation_message)

        # TODO: do we need this?
        element._height = len(contents)

        return Group(*contents)

    def render_validation_message(
        self, element: Union[Input, Menu]
    ) -> Optional[list[RenderableType]]:
        if element._cancelled:
            return [Text(""), "[cancelled]Cancelled.[/]"]

        if element.valid is False:
            return [Text(""), f"[error]{element.validation_message}[/]"]

        return None

    # TODO: maybe don't reuse this for menus
    def render_input_value(
        self,
        input: Union[Menu, Input],
        is_active: bool = False,
        parent: Optional[Element] = None,
        done: bool = False,
    ) -> RenderableType:
        text = input.text

        if isinstance(input, Input) and input.password and text:
            text = "*" * len(text)

        if input._cancelled:
            if not text:
                return ""

            return f"[placeholder.cancelled]{text}[/]"

        if done:
            if (
                not text
                and isinstance(input, Input)
                and input.default_as_placeholder
                and input.default
            ):
                text = input.default

            return f"[result]{text}[/]"

        if not text:
            placeholder = input.placeholder if isinstance(input, Input) else ""

            # Use zero-width space when placeholder is empty to prevent
            # the line from being stripped as a trailing blank line
            placeholder = placeholder or "\u200b"
            return f"[placeholder]{placeholder}[/]"

        return f"[text]{text}[/]"

    def render_input_label(
        self,
        input: Union[Input, Menu],
        is_active: bool = False,
        parent: Optional[Element] = None,
    ) -> Union[str, Text, None]:
        from rich_toolkit.form import Form

        label: Union[str, Text, None] = None

        if input.label:
            label = input.label

            if isinstance(parent, Form):
                if is_active:
                    label = f"[active]{label}[/]"
                elif input.valid is False:
                    label = f"[error]{label}[/]"

        return label

    def _build_menu_options(self, element: Menu, separator: Text) -> Text:
        """Build the menu Text containing scroll indicators and option items."""
        menu = Text(justify="left")

        checked_prefix = Text(element.active_prefix + " ")
        unchecked_prefix = Text(element.inactive_prefix + " ")

        start, end = element.visible_options_range
        visible_options = element.options[start:end]
        needs_scrolling = element._needs_scrolling()

        # Reserve space for scroll indicators to prevent layout shift
        if needs_scrolling:
            if element.has_more_above:
                menu.append(Text(element.MORE_ABOVE_INDICATOR + "\n", style="dim"))
            else:
                menu.append(Text(" " * len(element.MORE_ABOVE_INDICATOR) + "\n"))

        for idx, option in enumerate(visible_options):
            actual_idx = start + idx
            is_at_cursor = actual_idx == element.selected

            # Prefix reflects checked state (multi-select) or cursor (single-select)
            if element.multiple:
                is_marked = element.is_option_checked_by_ref(option)
            else:
                is_marked = is_at_cursor
            prefix = checked_prefix if is_marked else unchecked_prefix

            # Style reflects cursor position regardless of checked state
            style = self.console.get_style("selected" if is_at_cursor else "text")

            is_last = idx == len(visible_options) - 1

            menu.append(
                Text.assemble(
                    prefix,
                    option["name"],
                    separator if not is_last else "",
                    style=style,
                )
            )

        if needs_scrolling:
            if element.has_more_below:
                menu.append(Text("\n" + element.MORE_BELOW_INDICATOR, style="dim"))
            else:
                menu.append(Text("\n" + " " * len(element.MORE_BELOW_INDICATOR)))

        if not element.options:
            menu = Text("No results found", style=self.console.get_style("text"))

        return menu

    def _build_filter_parts(self, element: Menu) -> list[RenderableType]:
        if not element.allow_filtering:
            return []

        filter_parts: list[RenderableType] = []

        filter_line = Text.assemble(
            (element.filter_prompt, self.console.get_style("text")),
            (element.text, self.console.get_style("text")),
        )

        if hint := element.selection_count_hint:
            filter_line.append(f" {hint}", style="dim")

        filter_line.append("\n")
        filter_parts.append(filter_line)

        return filter_parts

    def render_menu(
        self,
        element: Menu,
        is_active: bool = False,
        done: bool = False,
        parent: Optional[Element] = None,
    ) -> RenderableType:
        label = self.render_input_label(element, is_active=is_active, parent=parent)

        if done:
            result_content = Text()

            if label:
                result_content.append(label)
                result_content.append(" ")

            # For single-select menus, check if selection is valid
            # For multi-select menus, result_display_name only uses checked items
            should_show_cancelled = element._cancelled
            if not element.multiple:
                selection_is_valid = 0 <= element.selected < len(element.options)
                should_show_cancelled = should_show_cancelled or not selection_is_valid

            if should_show_cancelled:
                result_content.append(
                    "Cancelled.",
                    style=self.console.get_style("cancelled"),
                )
            else:
                result_content.append(
                    element.result_display_name,
                    style=self.console.get_style("result"),
                )

            return result_content

        separator = Text("  " if element.inline else "\n")
        menu = self._build_menu_options(element, separator)
        filter_parts = self._build_filter_parts(element)

        content: list[RenderableType] = []

        if label:
            content.append(label)

        content.extend(filter_parts)
        content.append(menu)

        if message := self.render_validation_message(element):
            content.extend(message)

        return Group(*content)

    def render_progress(
        self,
        element: Progress,
        is_active: bool = False,
        done: bool = False,
        parent: Optional[Element] = None,
    ) -> RenderableType:
        if done and element._cancelled:
            return Text.assemble(
                element.title,
                " ",
                ("Cancelled.", self.console.get_style("cancelled")),
            )

        content: str | Group | Text = element.current_message

        if element.logs and element._inline_logs:
            lines_to_show = (
                element.logs[-element.lines_to_show :]
                if element.lines_to_show > 0
                else element.logs
            )

            start_content = [element.title, ""]

            if not self._should_show_progress_title:
                start_content = []

            content = Group(
                *start_content,
                *[
                    self.render_element(
                        line,
                        index=index,
                        max_lines=element.lines_to_show,
                        total_lines=len(element.logs),
                        parent=element,
                    )
                    for index, line in enumerate(lines_to_show)
                ],
            )

        return content

    def render_progress_log_line(
        self,
        line: str | Text,
        index: int,
        max_lines: int = -1,
        total_lines: int = -1,
        parent: Optional[Element] = None,
    ) -> Text:
        line = Text.from_markup(line) if isinstance(line, str) else line
        if max_lines == -1:
            return line

        shown_lines = min(total_lines, max_lines)

        # this is the minimum brightness based on the max_lines
        min_brightness = 0.4
        # but we want to have a slightly higher brightness if there's less than max_lines
        # otherwise you could get the something like this:

        # line 1 -> very dark
        # line 2 -> slightly darker
        # line 3 -> normal

        # which is ok, but not great, so we we increase the brightness if there's less than max_lines
        # so that the last line is always the brightest
        current_min_brightness = min_brightness + abs(shown_lines - max_lines) * 0.1
        current_min_brightness = min(max(current_min_brightness, min_brightness), 1.0)

        brightness_multiplier = ((index + 1) / shown_lines) * (
            1.0 - current_min_brightness
        ) + current_min_brightness

        return fade_text(
            line,
            text_color=Color.parse(self.text_color),
            background_color=self.background_color,
            brightness_multiplier=brightness_multiplier,
        )
