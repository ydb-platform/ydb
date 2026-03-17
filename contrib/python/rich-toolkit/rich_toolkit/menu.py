from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Generic,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

import click
from rich.console import Console, RenderableType
from rich.text import Text
from typing_extensions import Any, Literal, TypedDict

from ._input_handler import TextInputHandler

from .element import CursorOffset, Element

if TYPE_CHECKING:
    from .styles.base import BaseStyle

ReturnValue = TypeVar("ReturnValue")


class Option(TypedDict, Generic[ReturnValue]):
    name: str
    value: ReturnValue


class Menu(Generic[ReturnValue], TextInputHandler, Element):
    DOWN_KEYS = [TextInputHandler.DOWN_KEY, "j"]
    UP_KEYS = [TextInputHandler.UP_KEY, "k"]
    LEFT_KEYS = [TextInputHandler.LEFT_KEY, "h"]
    RIGHT_KEYS = [TextInputHandler.RIGHT_KEY, "l"]

    current_selection_char = "●"
    selection_char = "○"
    checked_char = "■"
    unchecked_char = "□"
    filter_prompt = "Filter: "

    @property
    def selection_count_hint(self) -> Optional[str]:
        """Return a hint like '(3 selected)' when filtering hides checked items."""
        if not self.multiple or not self.allow_filtering or not self.checked:
            return None
        return f"({len(self.checked)} selected)"

    @property
    def active_prefix(self) -> str:
        """Prefix for the active/checked option."""
        return self.checked_char if self.multiple else self.current_selection_char

    @property
    def inactive_prefix(self) -> str:
        """Prefix for inactive/unchecked options."""
        return self.unchecked_char if self.multiple else self.selection_char

    # Scroll indicators
    MORE_ABOVE_INDICATOR = "  ↑ more"
    MORE_BELOW_INDICATOR = "  ↓ more"

    def __init__(
        self,
        label: str,
        options: List[Option[ReturnValue]],
        inline: bool = False,
        allow_filtering: bool = False,
        multiple: bool = False,
        *,
        style: Optional[BaseStyle] = None,
        cursor_offset: int = 0,
        max_visible: Optional[int] = None,
        **metadata: Any,
    ):
        if multiple and inline:
            raise ValueError("multiple and inline cannot both be True")

        self.label = Text.from_markup(label)
        self.inline = inline
        self.allow_filtering = allow_filtering
        self.multiple = multiple

        self.selected = 0
        self.checked: Set[int] = set()

        self._options = options
        self._option_index = {id(opt): idx for idx, opt in enumerate(options)}

        self._padding_bottom = 1
        self.valid = None

        # Scrolling state
        self._scroll_offset: int = 0
        self._max_visible: Optional[int] = max_visible

        cursor_offset = cursor_offset + len(self.filter_prompt)

        Element.__init__(self, style=style, metadata=metadata)
        super().__init__()

    def get_key(self) -> Optional[str]:
        char = click.getchar()

        if char == "\r":
            return "enter"

        if self.allow_filtering:
            left_keys, right_keys = [[self.LEFT_KEY], [self.RIGHT_KEY]]
            down_keys, up_keys = [[self.DOWN_KEY], [self.UP_KEY]]
        else:
            left_keys, right_keys = self.LEFT_KEYS, self.RIGHT_KEYS
            down_keys, up_keys = self.DOWN_KEYS, self.UP_KEYS

        next_keys, prev_keys = (
            (right_keys, left_keys) if self.inline else (down_keys, up_keys)
        )

        if char in next_keys:
            return "next"
        if char in prev_keys:
            return "prev"

        if self.allow_filtering:
            return char

        return None

    @property
    def options(self) -> List[Option[ReturnValue]]:
        if self.allow_filtering:
            return [
                option
                for option in self._options
                if self.text.lower() in option["name"].lower()
            ]

        return self._options

    def get_max_visible(self, console: Optional[Console] = None) -> Optional[int]:
        """Calculate the maximum number of visible options based on terminal height.

        Args:
            console: Console to get terminal height from. If None, uses default.

        Returns:
            Maximum number of visible options, or None if no limit needed.
        """
        if self._max_visible is not None:
            return self._max_visible

        if self.inline:
            # Inline menus don't need scrolling
            return None

        if console is None:
            console = Console()

        # Reserve space for: label (1), filter line if enabled (1),
        # scroll indicators (2), validation message (1), margins (2)
        reserved_lines = 6
        if self.allow_filtering:
            reserved_lines += 1

        available_height = console.height - reserved_lines
        # At least show 3 options
        return max(3, available_height)

    @property
    def visible_options_range(self) -> Tuple[int, int]:
        """Returns (start, end) indices for visible options."""
        max_visible = self.get_max_visible()
        total_options = len(self.options)

        if max_visible is None or total_options <= max_visible:
            return (0, total_options)

        start = self._scroll_offset
        end = min(start + max_visible, total_options)
        return (start, end)

    @property
    def has_more_above(self) -> bool:
        """Check if there are more options above the visible window."""
        return self._scroll_offset > 0

    @property
    def has_more_below(self) -> bool:
        """Check if there are more options below the visible window."""
        max_visible = self.get_max_visible()
        if max_visible is None:
            return False
        return self._scroll_offset + max_visible < len(self.options)

    def _ensure_selection_visible(self) -> None:
        """Adjust scroll offset to ensure the selected item is visible."""
        max_visible = self.get_max_visible()
        if max_visible is None:
            return

        # If selection is above visible window, scroll up
        if self.selected < self._scroll_offset:
            self._scroll_offset = self.selected

        # If selection is below visible window, scroll down
        elif self.selected >= self._scroll_offset + max_visible:
            self._scroll_offset = self.selected - max_visible + 1

    def _reset_scroll(self) -> None:
        """Reset scroll offset (used when filter changes)."""
        self._scroll_offset = 0

    def _get_option_index(self, option: Option[ReturnValue]) -> int:
        """Return the index of an option in _options using identity lookup."""
        return self._option_index[id(option)]

    def _toggle_current(self) -> None:
        """Toggle the checked state of the current cursor item."""
        if not self.options:
            return
        option_index = self._get_option_index(self.options[self.selected])
        self.checked ^= {option_index}

    def is_option_checked(self, filtered_index: int) -> bool:
        """Check if a filtered-list option is checked."""
        return self._get_option_index(self.options[filtered_index]) in self.checked

    def is_option_checked_by_ref(self, option: Option[ReturnValue]) -> bool:
        """Check if an option is checked using its object identity."""
        return self._get_option_index(option) in self.checked

    @property
    def result_display_name(self) -> str:
        """Return the display name for the result (used when the menu is done)."""
        if self.multiple:
            return ", ".join(self._options[i]["name"] for i in sorted(self.checked))
        return self.options[self.selected]["name"]

    def _update_selection(self, key: Literal["next", "prev"]) -> None:
        if key == "next":
            self.selected += 1
        elif key == "prev":
            self.selected -= 1

        if self.selected < 0:
            self.selected = len(self.options) - 1

        if self.selected >= len(self.options):
            self.selected = 0

        # Ensure the selected item is visible after navigation
        self._ensure_selection_visible()

    def render_result(self) -> RenderableType:
        result_text = Text()

        result_text.append(self.label)
        result_text.append(" ")

        result_text.append(
            self.result_display_name,
            style=self.console.get_style("result"),
        )

        return result_text

    def is_next_key(self, key: str) -> bool:
        keys = self.RIGHT_KEYS if self.inline else self.DOWN_KEYS

        if self.allow_filtering:
            keys = [keys[0]]

        return key in keys

    def is_prev_key(self, key: str) -> bool:
        keys = self.LEFT_KEYS if self.inline else self.UP_KEYS

        if self.allow_filtering:
            keys = [keys[0]]

        return key in keys

    def handle_key(self, key: str) -> None:
        current_selection: Optional[str] = None
        previous_filter_text = self.text

        if self.multiple and key == " ":
            self._toggle_current()
            return

        if self.is_next_key(key):
            self._update_selection("next")
        elif self.is_prev_key(key):
            self._update_selection("prev")
        else:
            if self.options:
                current_selection = self.options[self.selected]["name"]

            super().handle_key(key)

        if current_selection:
            matching_index = next(
                (
                    index
                    for index, option in enumerate(self.options)
                    if option["name"] == current_selection
                ),
                0,
            )

            self.selected = matching_index

        # Reset scroll when filter text changes
        if self.allow_filtering and self.text != previous_filter_text:
            self._reset_scroll()
            self._ensure_selection_visible()

    @property
    def validation_message(self) -> Optional[str]:
        if self.valid is False:
            if self.multiple:
                return "Please select at least one option"
            return "This field is required"

        return None

    def on_blur(self):
        self.on_validate()

    def on_validate(self):
        if self.multiple:
            self.valid = len(self.checked) > 0
        else:
            self.valid = len(self.options) > 0

    @property
    def should_show_cursor(self) -> bool:
        return self.allow_filtering

    def ask(self) -> Union[ReturnValue, List[ReturnValue]]:
        from .container import Container

        container = Container(style=self.style, metadata=self.metadata)

        container.elements = [self]

        container.run()

        if self.multiple:
            return [self._options[i]["value"] for i in sorted(self.checked)]

        return self.options[self.selected]["value"]

    @property
    def cursor_offset(self) -> CursorOffset:
        # For non-inline menus with filtering, cursor is on the filter line
        # top = 2 accounts for: label (1) + filter line position (1 from start)
        # The filter line comes BEFORE scroll indicators, so no adjustment needed
        top = 2

        left_offset = len(self.filter_prompt) + self.cursor_left

        return CursorOffset(top=top, left=left_offset)

    def _needs_scrolling(self) -> bool:
        """Check if scrolling is needed (more options than can be displayed)."""
        max_visible = self.get_max_visible()
        if max_visible is None:
            return False
        return len(self.options) > max_visible
