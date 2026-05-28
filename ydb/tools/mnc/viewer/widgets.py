import glob
import os
from typing import Optional

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.events import Key
from textual.screen import ModalScreen
from textual.widgets import Input, Label, ListItem, ListView, Static


class TabListItem(ListItem):
    def __init__(self, title: str, tab_id: str, active: bool) -> None:
        self.tab_id = tab_id
        prefix = "* " if active else "  "
        super().__init__(Label(f"{prefix}{title}"))


class OpenTabListItem(ListItem):
    def __init__(self, title: str, action: str) -> None:
        self.action = action
        super().__init__(Label(title))


class ConfigFieldItem(ListItem):
    def __init__(self, field_name: str, title: str, value: str, path_picker: bool = False) -> None:
        self.field_name = field_name
        self.title = title
        self.path_picker = path_picker
        self._saved_value = value
        self.input = Input(value=value, id=f"config-field-{field_name}")
        self.input.disabled = True
        super().__init__(
            Horizontal(
                Label(title, classes="config-field-title"),
                self.input,
                classes="config-field-row",
            )
        )

    @property
    def value(self) -> str:
        return self.input.value

    def start_edit(self) -> None:
        self._saved_value = self.input.value
        self.input.disabled = False
        self.input.focus()
        self.input.cursor_position = len(self.input.value)

    def save_edit(self) -> None:
        self._saved_value = self.input.value
        self.input.disabled = True

    def cancel_edit(self) -> None:
        self.input.value = self._saved_value
        self.input.disabled = True


class MncConfigForm(Vertical):
    DEFAULT_CSS = """
    MncConfigForm {
        height: 1fr;
    }

    #mnc-config-fields {
        height: 1fr;
    }

    .config-field-row {
        height: 3;
        width: 100%;
    }

    .config-field-title {
        width: 18;
        height: 3;
        content-align: center middle;
    }

    .config-field-row Input {
        width: 1fr;
        height: 3;
    }
    """

    def __init__(self, config: dict[str, str], default_git_ydb_root: str) -> None:
        super().__init__()
        self._config = config
        self._default_git_ydb_root = default_git_ydb_root

    def compose(self) -> ComposeResult:
        yield ListView(
            ConfigFieldItem(
                "git_ydb_root",
                "git_ydb_root",
                self._config.get("git_ydb_root", self._default_git_ydb_root),
                path_picker=True,
            ),
            id="mnc-config-fields",
        )


class PathSuggestionItem(ListItem):
    def __init__(self, path: str) -> None:
        self.path = path
        super().__init__(Label(path))


class PathPickerScreen(ModalScreen[Optional[str]]):
    CSS = """
    PathPickerScreen {
        align: center middle;
    }

    #path-picker {
        width: 100;
        max-width: 95%;
        height: auto;
        max-height: 16;
        border: thick $background 80%;
        background: $surface;
        padding: 1 2;
    }

    #path-picker-input-row {
        height: 3;
        layout: horizontal;
    }

    #path-picker-input {
        width: 1fr;
        height: 3;
    }

    #path-picker-mode {
        width: 14;
        height: 3;
        content-align: center middle;
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


class TabPickerScreen(ModalScreen[Optional[str]]):
    CSS = """
    TabPickerScreen {
        align: center middle;
    }

    #tab-picker {
        width: 48;
        max-height: 20;
        border: thick $background 80%;
        background: $surface;
        padding: 1 2;
    }

    #tab-picker-title {
        height: 1;
        margin-bottom: 1;
        text-style: bold;
    }

    #tab-list {
        height: 1fr;
    }
    """

    BINDINGS = [
        ("escape", "cancel", "Cancel"),
    ]

    def __init__(self, tabs: list[tuple[str, str]], active_tab: Optional[str]) -> None:
        super().__init__()
        self._tabs = tabs
        self._active_tab = active_tab

    def compose(self) -> ComposeResult:
        initial_index = 0
        for index, (tab_id, _title) in enumerate(self._tabs):
            if tab_id == self._active_tab:
                initial_index = index
                break

        yield Vertical(
            Static("Tabs", id="tab-picker-title"),
            ListView(
                *[
                    TabListItem(title, tab_id, tab_id == self._active_tab)
                    for tab_id, title in self._tabs
                ],
                initial_index=initial_index,
                id="tab-list",
            ),
            id="tab-picker",
        )

    def on_mount(self) -> None:
        self.query_one("#tab-list", ListView).focus()

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        if isinstance(event.item, TabListItem):
            self.dismiss(event.item.tab_id)

    def action_cancel(self) -> None:
        self.dismiss(None)
