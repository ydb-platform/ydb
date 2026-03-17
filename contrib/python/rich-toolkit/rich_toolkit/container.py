from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from rich.control import Control, ControlType
from rich.live_render import LiveRender
from rich.segment import Segment

from ._getchar import getchar
from ._input_handler import TextInputHandler

from .element import Element

if TYPE_CHECKING:
    from .styles import BaseStyle


class Container(Element):
    def __init__(
        self,
        style: Optional[BaseStyle] = None,
        metadata: Optional[Dict[Any, Any]] = None,
    ):
        self.elements: List[Element] = []
        self.active_element_index = 0
        self.previous_element_index = 0
        self._live_render = LiveRender("")

        super().__init__(style=style, metadata=metadata)

        self.console = self.style.console

    def _refresh(self, done: bool = False):
        content = self.style.render_element(self, done=done)
        self._live_render.set_renderable(content)

        active_element = self.elements[self.active_element_index]

        should_show_cursor = (
            active_element.should_show_cursor
            if hasattr(active_element, "should_show_cursor")
            else False
        )

        # Always show cursor when done to restore terminal state
        if done:
            should_show_cursor = True

        self.console.print(
            Control.show_cursor(should_show_cursor),
            *self.move_cursor_at_beginning(),
            self._live_render,
        )

        if not done:
            self.console.print(
                *self.move_cursor_to_active_element(),
            )

    @property
    def _active_element(self) -> Element:
        return self.elements[self.active_element_index]

    def _get_size(self, element: Element) -> Tuple[int, int]:
        renderable = self.style.render_element(element, done=False, parent=self)

        lines = self.console.render_lines(renderable, self.console.options, pad=False)

        return Segment.get_shape(lines)

    def _get_element_position(self, element_index: int) -> int:
        position = 0

        for i in range(element_index + 1):
            current_element = self.elements[i]

            if i == element_index:
                position += self.style.get_cursor_offset_for_element(
                    current_element, parent=self
                ).top
            else:
                size = self._get_size(current_element)
                position += size[1]

        return position

    @property
    def _active_element_position(self) -> int:
        return self._get_element_position(self.active_element_index)

    def get_offset_for_element(self, element_index: int) -> int:
        if self._live_render._shape is None:
            return 0

        position = self._get_element_position(element_index)

        _, height = self._live_render._shape

        return height - position

    def get_offset_for_active_element(self) -> int:
        return self.get_offset_for_element(self.active_element_index)

    def move_cursor_to_active_element(self) -> Tuple[Control, ...]:
        move_up = self.get_offset_for_active_element()

        move_cursor = (
            (Control((ControlType.CURSOR_UP, move_up)),) if move_up > 0 else ()
        )

        cursor_left = self.style.get_cursor_offset_for_element(
            self._active_element, parent=self
        ).left

        return (Control.move_to_column(cursor_left), *move_cursor)

    def move_cursor_at_beginning(self) -> Tuple[Control, ...]:
        if self._live_render._shape is None:
            return (Control(),)

        original = (self._live_render.position_cursor(),)

        # Use the previous element type and index for cursor positioning
        move_down = self.get_offset_for_element(self.previous_element_index)

        if move_down == 0:
            return original

        return (
            Control(
                (ControlType.CURSOR_DOWN, move_down),
            ),
            *original,
        )

    def handle_enter_key(self) -> bool:
        from .input import Input
        from .menu import Menu

        active_element = self.elements[self.active_element_index]

        if isinstance(active_element, (Input, Menu)):
            active_element.on_validate()

            if active_element.valid is False:
                return False

        return True

    def _focus_next(self) -> None:
        self.active_element_index += 1

        if self.active_element_index >= len(self.elements):
            self.active_element_index = 0

        if self._active_element.focusable is False:
            self._focus_next()

    def _focus_previous(self) -> None:
        self.active_element_index -= 1

        if self.active_element_index < 0:
            self.active_element_index = len(self.elements) - 1

        if self._active_element.focusable is False:
            self._focus_previous()

    def run(self):
        self._refresh()

        while True:
            try:
                key = getchar()

                self.previous_element_index = self.active_element_index

                if key in (TextInputHandler.SHIFT_TAB_KEY, TextInputHandler.TAB_KEY):
                    if hasattr(self._active_element, "on_blur"):
                        self._active_element.on_blur()

                    if key == TextInputHandler.SHIFT_TAB_KEY:
                        self._focus_previous()
                    else:
                        self._focus_next()

                active_element = self.elements[self.active_element_index]
                active_element.handle_key(key)

                if key == TextInputHandler.ENTER_KEY:
                    if self.handle_enter_key():
                        break

                self._refresh()

            except KeyboardInterrupt:
                for element in self.elements:
                    element.on_cancel()

                self._refresh(done=True)
                exit()

        self._refresh(done=True)
