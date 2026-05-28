import glob
import os
from dataclasses import dataclass
from typing import Optional

import yaml
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.events import Key
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Label, ListItem, ListView, Static

import ydb.tools.mnc.scheme as mnc_scheme


CONFIG_FIELD_TITLES = {
    "git_ydb_root": "YDB source root",
}


class OverviewStatusCard(ListItem):
    def __init__(self, title: str, status: str, action: Optional[str] = None) -> None:
        self.action = action
        status_class = status.lower().replace(" ", "-")
        super().__init__(
            Vertical(
                Label(title, classes="overview-status-title"),
                Label(status, classes=f"overview-status-value status-{status_class}"),
                classes="overview-status-card-content",
            ),
            classes="overview-status-card",
        )


class OverviewPane(Vertical):
    DEFAULT_CSS = """
    OverviewPane {
        height: 1fr;
    }

    #overview-status-row {
        height: 7;
        width: 100%;
        padding: 1 2;
        layout: horizontal;
        background: transparent;
    }

    .overview-status-card {
        width: 32;
        height: 5;
        margin-right: 2;
    }

    .overview-status-card-content {
        height: 5;
        width: 100%;
        padding: 0 2;
        background: $surface;
    }

    .overview-status-card-content:dark {
        background: $panel-darken-1;
    }

    #overview-status-row > .overview-status-card.-highlight {
        background: transparent;
    }

    #overview-status-row > .overview-status-card.-highlight .overview-status-card-content {
        background: $block-cursor-blurred-background;
    }

    #overview-status-row > .overview-status-card.-highlight .overview-status-title {
        color: $block-cursor-blurred-foreground;
    }

    .overview-status-title {
        height: 2;
        content-align: left middle;
        text-style: bold;
    }

    .overview-status-value {
        height: 2;
        content-align: left middle;
    }

    .status-ok {
        color: $success;
    }

    .status-error {
        color: $error;
    }

    .status-not-selected {
        color: $text-muted;
    }
    """

    def __init__(self, mnc_config_ok: bool, cluster_config_status: str = "NOT SELECTED") -> None:
        super().__init__()
        self._mnc_config_status = "OK" if mnc_config_ok else "ERROR"
        self._cluster_config_status = cluster_config_status

    def compose(self) -> ComposeResult:
        yield ListView(
            OverviewStatusCard("MNC Config", self._mnc_config_status, action="open_mnc_config"),
            OverviewStatusCard("Cluster Config", self._cluster_config_status, action="open_cluster_config"),
            id="overview-status-row",
        )

    def on_key(self, event: Key) -> None:
        focused = self.screen.focused
        status_row = self.query_one("#overview-status-row", ListView)

        if event.key in ("left", "right") and focused is status_row:
            if status_row.index is None:
                status_row.index = 0
            elif event.key == "left" and status_row.index > 0:
                status_row.index -= 1
            elif event.key == "right" and status_row.index < len(status_row.children) - 1:
                status_row.index += 1
            event.stop()


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
                Vertical(self.input, classes="config-field-value"),
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
        background: $surface;
    }

    #mnc-config-fields:dark {
        background: $panel-darken-1;
    }

    #mnc-config-fields > ConfigFieldItem {
        height: 3;
    }

    #mnc-config-fields > ConfigFieldItem.-highlight {
        background: transparent;
        color: $block-cursor-blurred-foreground;
    }

    #mnc-config-fields > ConfigFieldItem.-highlight .config-field-title,
    #mnc-config-fields > ConfigFieldItem.-highlight .config-field-value {
        background: $primary-background;
    }

    #mnc-config-fields > ConfigFieldItem.-highlight .config-field-title,
    #mnc-config-fields > ConfigFieldItem.-highlight Input {
        color: $block-cursor-blurred-foreground;
        text-style: bold;
    }

    .config-field-row {
        height: 3;
        width: 100%;
    }

    .config-field-title {
        width: 22;
        height: 3;
        padding-left: 2;
        margin-right: 1;
        background: $background;
        content-align: left middle;
        text-style: bold;
    }

    .config-field-value {
        width: 1fr;
        height: 3;
        padding: 0 1;
        background: $background;
    }

    .config-field-row Input {
        width: 1fr;
        height: 1;
        margin-top: 1;
        border: none;
        background: transparent;
        padding: 0;
    }

    .config-field-row Input:focus {
        border: none;
        background-tint: 0%;
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
                CONFIG_FIELD_TITLES["git_ydb_root"],
                self._config.get("git_ydb_root", self._default_git_ydb_root),
                path_picker=True,
            ),
            id="mnc-config-fields",
        )


@dataclass
class ConfigCandidate:
    name: str
    path: str


@dataclass
class ConfigValidation:
    errors: list[str]

    @property
    def ok(self) -> bool:
        return not self.errors


def _validate_multinode_config(path: str) -> ConfigValidation:
    try:
        with open(path) as file:
            config = yaml.safe_load(file)
    except Exception as error:
        return ConfigValidation([f"Failed to parse config: {error}"])

    try:
        validated, errors = mnc_scheme.apply_scheme(config, mnc_scheme.multinode.scheme)
    except Exception as error:
        return ConfigValidation([f"Failed to validate config: {error}"])

    if validated is None:
        return ConfigValidation(errors or ["Config does not match multinode scheme"])
    return ConfigValidation([])


class ConfigCandidateItem(ListItem):
    def __init__(self, candidate: ConfigCandidate) -> None:
        self.candidate = candidate
        super().__init__(
            Vertical(
                Label(candidate.name, classes="config-candidate-name"),
                Label(candidate.path, classes="config-candidate-path"),
                classes="config-candidate-content",
            ),
            classes="config-candidate-item",
        )


class ClusterConfigDetails(VerticalScroll, inherit_bindings=False):
    def compose(self) -> ComposeResult:
        yield Horizontal(
            Label("", id="cluster-config-details-name"),
            Label("SELECTED", id="cluster-config-selected-badge", classes="cluster-config-badge"),
            Label("OK", id="cluster-config-ok-badge", classes="cluster-config-badge"),
            Label("FAIL", id="cluster-config-fail-badge", classes="cluster-config-badge"),
            id="cluster-config-details-header",
        )
        yield Label("", id="cluster-config-details-path")
        yield Static("", id="cluster-config-validation-errors", markup=False)
        yield Static("", id="cluster-config-details-content", markup=False)

    def update_empty(self, message: str) -> None:
        self.query_one("#cluster-config-details-name", Label).update(message)
        self.query_one("#cluster-config-details-path", Label).update("")
        self.query_one("#cluster-config-validation-errors", Static).display = False
        self.query_one("#cluster-config-details-content", Static).update("")
        for badge_id in ("#cluster-config-selected-badge", "#cluster-config-ok-badge", "#cluster-config-fail-badge"):
            self.query_one(badge_id, Label).display = False
        self.scroll_to(y=0, animate=False, force=True)

    def update_candidate(
        self,
        candidate: ConfigCandidate,
        selected: bool,
        validation: ConfigValidation,
        content: str,
    ) -> None:
        self.query_one("#cluster-config-details-name", Label).update(candidate.name)
        self.query_one("#cluster-config-details-path", Label).update(candidate.path)

        self.query_one("#cluster-config-selected-badge", Label).display = selected
        self.query_one("#cluster-config-ok-badge", Label).display = validation.ok
        self.query_one("#cluster-config-fail-badge", Label).display = not validation.ok

        errors = self.query_one("#cluster-config-validation-errors", Static)
        if validation.errors:
            errors.display = True
            errors.update(
                "Config is not compatible with multinode scheme:\n"
                + "\n".join(f"- {error}" for error in validation.errors)
                + "\n"
            )
        else:
            errors.display = False
            errors.update("")

        self.query_one("#cluster-config-details-content", Static).update(content)
        self.scroll_to(y=0, animate=False, force=True)

    def key_up(self, event: Key) -> None:
        self.scroll_up(animate=False, force=True)
        event.stop()

    def key_down(self, event: Key) -> None:
        self.scroll_down(animate=False, force=True)
        event.stop()

    def key_left(self, event: Key) -> None:
        self.screen.query_one("#cluster-configs", ListView).focus()
        event.stop()


class ClusterConfigPane(Horizontal):
    DEFAULT_CSS = """
    ClusterConfigPane {
        height: 1fr;
        padding: 0 1 1 1;
    }

    #cluster-config-left {
        width: 2fr;
        height: 1fr;
        background: $surface;
    }

    #cluster-config-left:dark {
        background: $panel-darken-1;
    }

    #cluster-configs {
        height: 1fr;
        background: transparent;
    }

    #cluster-configs > .config-candidate-item {
        height: 2;
    }

    .config-candidate-content {
        height: 2;
        padding-left: 1;
    }

    .config-candidate-name {
        height: 1;
        text-style: bold;
        content-align: left middle;
    }

    .config-candidate-path {
        height: 1;
        color: $text-muted;
        content-align: left middle;
    }

    #cluster-configs > .config-candidate-item.-highlight {
        background: $block-cursor-blurred-background;
    }

    #cluster-configs > .config-candidate-item.-highlight .config-candidate-name {
        color: $block-cursor-blurred-foreground;
        text-style: bold;
    }

    #cluster-configs > .config-candidate-item.-highlight .config-candidate-path {
        color: $text;
    }

    #cluster-config-details {
        width: 3fr;
        height: 1fr;
        padding: 1 2;
        background: $surface;
        margin-left: 1;
    }

    #cluster-config-details:dark {
        background: $panel-darken-1;
    }

    #cluster-config-details:focus {
        background: $primary-background;
    }

    #cluster-config-details-header {
        height: 1;
        width: 1fr;
        margin-bottom: 1;
    }

    #cluster-config-details-name {
        width: auto;
        margin-right: 1;
        text-style: bold;
    }

    #cluster-config-details-path {
        height: 1;
        color: $text-muted;
        margin-bottom: 1;
    }

    .cluster-config-badge {
        height: 1;
        width: auto;
        padding: 0 1;
        margin-right: 1;
        text-style: bold;
    }

    #cluster-config-selected-badge {
        background: $secondary-muted;
        color: $text-secondary;
    }

    #cluster-config-ok-badge {
        background: $success-muted;
        color: $text-success;
    }

    #cluster-config-fail-badge {
        background: $error-muted;
        color: $text-error;
    }

    #cluster-config-validation-errors {
        height: auto;
        color: $text-error;
        margin-bottom: 1;
    }

    #cluster-config-details-content {
        height: auto;
        width: 1fr;
    }
    """

    def __init__(self, candidates: list[ConfigCandidate]) -> None:
        super().__init__()
        self._candidates = candidates
        self._selected_candidate: Optional[ConfigCandidate] = None

    def compose(self) -> ComposeResult:
        with Vertical(id="cluster-config-left"):
            yield ListView(id="cluster-configs")
        yield ClusterConfigDetails(id="cluster-config-details")

    def on_mount(self) -> None:
        self._refresh()
        self.query_one("#cluster-configs", ListView).focus()

    def on_list_view_highlighted(self, event: ListView.Highlighted) -> None:
        if event.list_view.id == "cluster-configs" and isinstance(event.item, ConfigCandidateItem):
            event.stop()
            self._show_details(event.item.candidate)

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        if event.list_view.id == "cluster-configs" and isinstance(event.item, ConfigCandidateItem):
            event.stop()
            self._selected_candidate = event.item.candidate
            self._show_details(event.item.candidate)

    def on_key(self, event: Key) -> None:
        if event.key == "right" and self.screen.focused is self.query_one("#cluster-configs", ListView):
            self.query_one("#cluster-config-details", ClusterConfigDetails).focus()
            event.stop()

    def _refresh(self) -> None:
        list_view = self.query_one("#cluster-configs", ListView)
        list_view.clear()
        list_view.extend([ConfigCandidateItem(candidate) for candidate in self._candidates])
        list_view.index = 0 if self._candidates else None
        if self._candidates:
            self._show_details(self._candidates[0])
        else:
            self.query_one("#cluster-config-details", ClusterConfigDetails).update_empty("No configs found")

    def _show_details(self, candidate: ConfigCandidate) -> None:
        try:
            with open(candidate.path) as file:
                content = file.read()
        except Exception as error:
            content = f"Failed to read config: {error}"
        self.query_one("#cluster-config-details", ClusterConfigDetails).update_candidate(
            candidate,
            candidate == self._selected_candidate,
            _validate_multinode_config(candidate.path),
            content,
        )


class PathSuggestionItem(ListItem):
    def __init__(self, path: str) -> None:
        self.path = path
        super().__init__(Label(path))


class InvalidPathModal(ModalScreen[None]):
    CSS = """
    InvalidPathModal {
        color: $foreground;
        background: $background 60%;
        align: center middle;
    }

    #invalid-path-dialog {
        width: 72;
        height: auto;
        padding: 1 2;
        border: hkey $error;
        background: $surface;
    }

    #invalid-path-dialog:dark {
        background: $panel-darken-1;
    }

    #invalid-path-title {
        height: auto;
        text-style: bold;
        color: $error;
        margin-bottom: 1;
    }

    #invalid-path-message {
        height: auto;
        margin-bottom: 1;
    }

    #invalid-path-actions {
        height: auto;
        align-horizontal: right;
    }
    """

    BINDINGS = [
        Binding("escape", "close", "Close"),
    ]

    def __init__(self, path: str) -> None:
        super().__init__()
        self._path = path

    def compose(self) -> ComposeResult:
        yield Vertical(
            Label("Invalid path", id="invalid-path-title"),
            Static(
                f"Path is not suitable for YDB source root:\n{self._path}\n\nSelect an existing directory.",
                id="invalid-path-message",
            ),
            Horizontal(
                Button("OK", id="invalid-path-ok", variant="primary"),
                id="invalid-path-actions",
            ),
            id="invalid-path-dialog",
        )

    def on_mount(self) -> None:
        self.query_one("#invalid-path-ok", Button).focus()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "invalid-path-ok":
            event.stop()
            self.dismiss(None)

    def action_close(self) -> None:
        self.dismiss(None)


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
