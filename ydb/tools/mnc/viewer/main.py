import os
from typing import Optional

import yaml

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.events import Key
from textual.screen import ModalScreen
from textual.widget import Widget
from textual.widgets import Footer, Header, Input, Label, ListItem, ListView, Static, TabbedContent, TabPane, Tabs


MNC_CONFIG_PATH = os.path.join(os.environ.get("HOME", "/"), ".mnc", "mnc.yaml")
DEFAULT_GIT_YDB_ROOT = os.path.join(os.environ.get("HOME", "/"), "ydbwork", "ydb")


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
    def __init__(self, field_name: str, title: str, value: str) -> None:
        self.field_name = field_name
        self.title = title
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

    def __init__(self, config: dict[str, str]) -> None:
        super().__init__()
        self._config = config

    def compose(self) -> ComposeResult:
        yield ListView(
            ConfigFieldItem(
                "git_ydb_root",
                "git_ydb_root",
                self._config.get("git_ydb_root", DEFAULT_GIT_YDB_ROOT),
            ),
            id="mnc-config-fields",
        )


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


class Viewer(App):
    BINDINGS = [
        Binding("left_square_bracket,[", "previous_tab", "Previous tab", priority=True),
        Binding("right_square_bracket,]", "next_tab", "Next tab", priority=True),
        Binding("t", "open_tab_picker", "Tabs", priority=True),
        Binding("ctrl+w", "close_tab", "Close tab", priority=True),
    ]
    
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._created_tabs: set[str] = set()
        self._tab_order = ["general"]
        self._tab_titles = {
            "general": "General",
        }
        self._editing_config_field: Optional[ConfigFieldItem] = None
        self._mnc_config = self._load_mnc_config()

    def compose(self) -> ComposeResult:
        yield Header()

        with TabbedContent(initial="general", id="tabs"):
            with TabPane("General", id="general"):
                yield ListView(
                    OpenTabListItem("MNC Config", "open_mnc_config"),
                    id="general-tabs",
                )

        yield Footer()

    def on_mount(self) -> None:
        self._disable_tab_header_focus()
        self._focus_active_tab_content()

    def action_show_tab(self, tab_id: str) -> None:
        self.query_one("#tabs", TabbedContent).active = tab_id

    def _disable_tab_header_focus(self) -> None:
        for tabs_header in self.query(Tabs):
            tabs_header.can_focus = False

    def _focus_active_tab_content(self) -> None:
        tabs = self.query_one("#tabs", TabbedContent)
        if tabs.active is None:
            return

        active_pane = self.query_one(f"#{tabs.active}", TabPane)
        for widget in active_pane.children:
            if (
                getattr(widget, "can_focus", False)
                and not getattr(widget, "disabled", False)
                and getattr(widget, "visible", True)
            ):
                widget.focus()
                return

        for widget in active_pane.query("*"):
            if (
                getattr(widget, "can_focus", False)
                and not getattr(widget, "disabled", False)
                and getattr(widget, "visible", True)
            ):
                widget.focus()
                return

    def _load_mnc_config(self) -> dict[str, str]:
        try:
            with open(MNC_CONFIG_PATH) as file:
                config = yaml.safe_load(file) or {}
        except FileNotFoundError:
            config = {}

        return {
            "git_ydb_root": str(config.get("git_ydb_root", DEFAULT_GIT_YDB_ROOT)),
        }

    def _save_mnc_config(self) -> None:
        os.makedirs(os.path.dirname(MNC_CONFIG_PATH), exist_ok=True)
        with open(MNC_CONFIG_PATH, "w") as file:
            yaml.safe_dump(self._mnc_config, file)

    def _tab_choices(self) -> list[tuple[str, str]]:
        return [(tab_id, self._tab_titles[tab_id]) for tab_id in self._tab_order]

    def _move_tab(self, step: int) -> None:
        tabs = self.query_one("#tabs", TabbedContent)
        if not self._tab_order:
            return

        active = tabs.active
        current_index = self._tab_order.index(active) if active in self._tab_order else 0
        tabs.active = self._tab_order[(current_index + step) % len(self._tab_order)]
        
    def check_action(self, action: str, parameters: tuple[object, ...]) -> bool:
        if self._editing_config_field is not None:
            return False

        if action == "close_tab":
            tabs = self.query_one("#tabs", TabbedContent)
            return tabs.active in self._created_tabs
        return True
    
    def on_tabbed_content_tab_activated(
        self,
        event: TabbedContent.TabActivated,
    ) -> None:
        self.refresh_bindings()
        self.call_after_refresh(self._focus_active_tab_content)

    def action_open_tab_picker(self) -> None:
        def show_selected_tab(tab_id: Optional[str]) -> None:
            if tab_id is not None:
                self.action_show_tab(tab_id)

        tabs = self.query_one("#tabs", TabbedContent)
        self.push_screen(TabPickerScreen(self._tab_choices(), tabs.active), show_selected_tab)

    def action_previous_tab(self) -> None:
        self._move_tab(-1)

    def action_next_tab(self) -> None:
        self._move_tab(1)

    async def _open_tab(self, tab_id: str, title: str, content: Widget) -> None:
        tabs = self.query_one("#tabs", TabbedContent)
        if tab_id in self._tab_order:
            tabs.active = tab_id
            return

        self._created_tabs.add(tab_id)
        self._tab_order.append(tab_id)
        self._tab_titles[tab_id] = title

        await tabs.add_pane(TabPane(title, content, id=tab_id))
        self._disable_tab_header_focus()
        tabs.active = tab_id
        self.refresh_bindings()

    async def action_open_mnc_config(self) -> None:
        await self._open_tab("mnc-config", "MNC Config", MncConfigForm(self._mnc_config))

    async def on_list_view_selected(self, event: ListView.Selected) -> None:
        if isinstance(event.item, OpenTabListItem):
            await self.run_action(event.item.action)
        elif isinstance(event.item, ConfigFieldItem):
            self._start_config_field_edit(event.item)

    def _start_config_field_edit(self, field: ConfigFieldItem) -> None:
        if self._editing_config_field is not None:
            return

        self._editing_config_field = field
        field.start_edit()
        self.refresh_bindings()

    def _save_config_field_edit(self) -> None:
        if self._editing_config_field is None:
            return

        field = self._editing_config_field
        field.save_edit()
        self._mnc_config[field.field_name] = field.value
        self._save_mnc_config()
        self._editing_config_field = None
        self.refresh_bindings()
        self.call_after_refresh(self._focus_mnc_config_fields)

    def _cancel_config_field_edit(self) -> None:
        if self._editing_config_field is None:
            return

        self._editing_config_field.cancel_edit()
        self._editing_config_field = None
        self.refresh_bindings()
        self.call_after_refresh(self._focus_mnc_config_fields)

    def _focus_mnc_config_fields(self) -> None:
        self.query_one("#mnc-config-fields", ListView).focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if self._editing_config_field is not None and event.input is self._editing_config_field.input:
            event.stop()
            self._save_config_field_edit()

    def on_key(self, event: Key) -> None:
        if self._editing_config_field is not None and event.key == "escape":
            event.stop()
            self._cancel_config_field_edit()
        
    async def action_close_tab(self) -> None:
        tabs = self.query_one("#tabs", TabbedContent)
        if tabs.active in self._created_tabs:
            tab_id = tabs.active
            self._created_tabs.remove(tab_id)
            self._tab_order.remove(tab_id)
            self._tab_titles.pop(tab_id)
            await tabs.remove_pane(tab_id)
            self.refresh_bindings()


def main() -> None:
    Viewer().run()
