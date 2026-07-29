import glob
import os
from typing import Optional

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.events import Key
from textual.screen import ModalScreen
from textual.widgets import Input, Label, ListItem, ListView, Static


class PathSuggestionItem(ListItem):
    def __init__(self, path: str) -> None:
        self.path = path
        super().__init__(Label(path))


class PathPickerScreen(ModalScreen[Optional[str]]):
    CSS = """
    PathPickerScreen {
        color: $foreground;
        background: $background 60%;
        align-horizontal: center;
    }

    #path-picker {
        width: 100%;
        height: auto;
        max-height: 16;
        margin-top: 3;
        background: $surface;
    }

    #path-picker:dark {
        background: $panel-darken-1;
    }

    #path-picker-input-row {
        height: auto;
        layout: horizontal;
        border: hkey $border;
        padding-left: 1;
        padding-right: 1;
    }

    #path-picker-input {
        width: 1fr;
        height: auto;
        border: none;
    }

    #path-picker-mode {
        display: none;
    }

    #path-suggestions {
        height: auto;
        max-height: 10;
    }
    """

    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
        Binding("ctrl+d", "toggle_directories_only", "Dirs/all"),
    ]

    def __init__(self, initial_path: str, directories_only: bool = True) -> None:
        super().__init__()
        self._initial_path = initial_path
        self._directories_only = directories_only
        self._suggestions: list[str] = []

    def compose(self) -> ComposeResult:
        yield Vertical(
            Horizontal(
                Input(value=self._initial_path, id="path-picker-input"),
                Static(self._mode_label(), id="path-picker-mode"),
                id="path-picker-input-row",
            ),
            ListView(id="path-suggestions"),
            id="path-picker",
        )

    async def on_mount(self) -> None:
        await self._refresh_suggestions()
        path_input = self.query_one("#path-picker-input", Input)
        path_input.focus()
        path_input.cursor_position = len(path_input.value)

    def _mode_label(self) -> str:
        return "dirs" if self._directories_only else "all"

    def _current_path(self) -> str:
        return self.query_one("#path-picker-input", Input).value

    def _matching_paths(self, value: str) -> list[str]:
        expanded_pattern = os.path.expanduser(value) + "*"
        matches = glob.glob(expanded_pattern)
        if self._directories_only:
            matches = [path for path in matches if os.path.isdir(path)]

        def display_path(path: str) -> str:
            if value.startswith("~"):
                home = os.path.expanduser("~")
                if path == home:
                    return "~"
                if path.startswith(home + os.sep):
                    return "~" + path[len(home):]
            return path

        return sorted(display_path(path) for path in matches)[:10]

    async def _refresh_suggestions(self) -> None:
        self._suggestions = self._matching_paths(self._current_path())
        suggestions = self.query_one("#path-suggestions", ListView)
        await suggestions.clear()
        await suggestions.extend(PathSuggestionItem(path) for path in self._suggestions)
        suggestions.index = 0 if self._suggestions else None
        self.query_one("#path-picker-mode", Static).update(self._mode_label())

    async def _complete_with(self, path: str) -> None:
        path_input = self.query_one("#path-picker-input", Input)
        path_input.value = path
        path_input.cursor_position = len(path)
        path_input.focus()
        await self._refresh_suggestions()

    def _accept_current_input(self) -> None:
        if len(self._suggestions) == 1:
            self.dismiss(self._suggestions[0])
        else:
            self.dismiss(self._current_path())

    async def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id == "path-picker-input":
            await self._refresh_suggestions()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "path-picker-input":
            event.stop()
            self._accept_current_input()

    async def on_list_view_selected(self, event: ListView.Selected) -> None:
        if event.list_view.id == "path-suggestions" and isinstance(event.item, PathSuggestionItem):
            event.stop()
            await self._complete_with(event.item.path)

    def on_key(self, event: Key) -> None:
        if event.key == "down" and self.focused is self.query_one("#path-picker-input", Input):
            suggestions = self.query_one("#path-suggestions", ListView)
            if self._suggestions:
                suggestions.focus()
                event.stop()

    async def action_toggle_directories_only(self) -> None:
        self._directories_only = not self._directories_only
        await self._refresh_suggestions()

    def action_cancel(self) -> None:
        self.dismiss(None)
