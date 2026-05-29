import asyncio
import os
import shlex
import shutil
import subprocess
from typing import Optional

import yaml

from textual.app import App, ComposeResult, ScreenStackError, SuspendNotSupported
from textual.binding import Binding
from textual.command import CommandPalette
from textual.css.query import NoMatches
from textual.events import Key
from textual.widget import Widget
from textual.widgets import Footer, Header, Input, ListView, TabbedContent, TabPane, Tabs

from ydb.tools.mnc.lib import agent_client
from ydb.tools.mnc.viewer.commands import TabCommands, ViewerCommands
from ydb.tools.mnc.viewer.widgets import (
    AgentHostStatus,
    AgentsState,
    ClusterConfigPane,
    ConfigCandidate,
    ConfigFieldItem,
    InvalidPathModal,
    MncConfigForm,
    OverviewPane,
    OverviewStatusCard,
    PathPickerScreen,
    ViewerState,
)


MNC_CONFIG_PATH = os.path.join(os.environ.get("HOME", "/"), ".mnc", "mnc.yaml")
DEFAULT_GIT_YDB_ROOT = os.path.join(os.environ.get("HOME", "/"), "ydbwork", "ydb")
DEFAULT_EDITOR = "vi"

class Viewer(App):
    COMMANDS = App.COMMANDS | {ViewerCommands}

    BINDINGS = [
        Binding("left_square_bracket,[", "previous_tab", "Previous tab", priority=True),
        Binding("right_square_bracket,]", "next_tab", "Next tab", priority=True),
        Binding("t", "open_tab_picker", "Tabs", priority=True),
        Binding("ctrl+w", "close_tab", "Close tab", priority=True),
    ]
    
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._created_tabs: set[str] = set()
        self._available_tab_order = ["general", "mnc-config", "cluster-config"]
        self._opened_tab_order = ["general"]
        self._tab_titles = {
            "general": "Overview",
            "mnc-config": "MNC Config",
            "cluster-config": "Cluster Config",
        }
        self._tab_descriptions = {
            "general": "Overview viewer and cluster state",
            "mnc-config": "Read and edit MNC Config",
            "cluster-config": "Select and inspect cluster config",
        }
        self._editing_config_field: Optional[ConfigFieldItem] = None
        self._mnc_config = self._load_mnc_config()
        self._state = ViewerState(mnc_config_ok=self._mnc_config_ok())
        self._navigation_generation = 0
        self._agent_check_generation = 0

    def compose(self) -> ComposeResult:
        yield Header()

        with TabbedContent(initial="general", id="tabs"):
            with TabPane("General", id="general"):
                yield OverviewPane(self._state)

        yield Footer()

    def on_mount(self) -> None:
        self._disable_tab_header_focus()
        self._focus_active_tab_content()

    def _bump_navigation_generation(self) -> int:
        self._navigation_generation += 1
        return self._navigation_generation

    def action_show_tab(self, tab_id: str) -> None:
        self._bump_navigation_generation()
        self.query_one("#tabs", TabbedContent).active = tab_id
        self._focus_active_tab_content(tab_id)

    def _restore_active_tab(self, tab_id: str, generation: int) -> None:
        if self._navigation_generation != generation or tab_id not in self._opened_tab_order:
            return

        try:
            tabs = self.query_one("#tabs", TabbedContent)
        except NoMatches:
            return

        tabs.active = tab_id
        self._focus_active_tab_content(tab_id)

    def _disable_tab_header_focus(self) -> None:
        for tabs_header in self.query(Tabs):
            tabs_header.can_focus = False

    def _focus_active_tab_content(self, expected_tab_id: Optional[str] = None) -> None:
        try:
            tabs = self.query_one("#tabs", TabbedContent)
        except NoMatches:
            return

        if tabs.active is None:
            return
        if expected_tab_id is not None and tabs.active != expected_tab_id:
            return

        try:
            active_pane = self.query_one(f"#{tabs.active}", TabPane)
        except NoMatches:
            return

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

    def _is_valid_git_ydb_root(self, path: str) -> bool:
        return bool(path) and os.path.isdir(path)

    def _mnc_config_ok(self) -> bool:
        return (
            os.path.isfile(MNC_CONFIG_PATH)
            and self._is_valid_git_ydb_root(self._mnc_config.get("git_ydb_root", ""))
        )

    def _refresh_overview(self) -> None:
        try:
            self.query_one(OverviewPane).refresh_state()
        except NoMatches:
            return

    def _refresh_mnc_config_status(self) -> None:
        self._state.mnc_config_ok = self._mnc_config_ok()
        self._refresh_overview()

    def _on_viewer_state_changed(self) -> None:
        self._refresh_overview()
        self._start_agents_check()

    def _selected_cluster_hosts(self) -> list[str]:
        selected = self._state.selected_cluster_config
        if selected is None or selected.validation.config is None:
            return []

        hosts = selected.validation.config.get("hosts") or []
        return list(dict.fromkeys(str(host) for host in hosts))

    def _start_agents_check(self) -> None:
        self._agent_check_generation += 1
        generation = self._agent_check_generation
        hosts = self._selected_cluster_hosts()

        if self._state.selected_cluster_config is None:
            self._state.agents = AgentsState()
            self._refresh_overview()
            return

        self._state.agents = AgentsState(
            status="CHECKING",
            hosts=[AgentHostStatus(host, "CHECKING") for host in hosts],
        )
        self._refresh_overview()
        self.run_worker(
            self._check_agents(hosts, generation),
            name="check-agents",
            group="check-agents",
            exclusive=True,
            exit_on_error=False,
        )

    async def _check_agents(self, hosts: list[str], generation: int) -> None:
        host_statuses = await asyncio.gather(*(self._check_agent_host(host) for host in hosts))
        if generation != self._agent_check_generation:
            return

        status = "OK" if host_statuses and all(host.status == "OK" for host in host_statuses) else "FAIL"
        self._state.agents = AgentsState(status=status, hosts=host_statuses)
        self._refresh_overview()

    async def _check_agent_host(self, host: str) -> AgentHostStatus:
        try:
            result = await agent_client.CheckAgentHealthOnHost(host).action()
        except Exception as error:
            return AgentHostStatus(host, "FAIL", str(error))

        if result is True:
            return AgentHostStatus(host, "OK")
        return AgentHostStatus(
            host,
            "FAIL",
            getattr(result, "message", "") or "Agent is not available",
        )

    def _show_invalid_path(self, path: str) -> None:
        self.push_screen(
            InvalidPathModal(path),
            lambda _: self.call_after_refresh(self._focus_mnc_config_fields),
        )

    def _validate_config_field(self, field_name: str, value: str) -> bool:
        if field_name == "git_ydb_root":
            return self._is_valid_git_ydb_root(value)
        return True

    def _discover_cluster_config_candidates(self) -> list[ConfigCandidate]:
        roots = [os.path.dirname(MNC_CONFIG_PATH)]
        git_ydb_root = self._mnc_config.get("git_ydb_root", "")
        if self._is_valid_git_ydb_root(git_ydb_root):
            roots.append(
                os.path.join(
                    git_ydb_root,
                    "junk",
                    os.environ.get("USER", "ydb"),
                    ".mnc",
                )
            )

        candidates = []
        seen = set()
        for root in roots:
            if not root or not os.path.isdir(root):
                continue
            for name in sorted(os.listdir(root)):
                path = os.path.join(root, name)
                if not os.path.isfile(path):
                    continue
                base, ext = os.path.splitext(name)
                if ext not in (".yaml", ".yml", ""):
                    continue
                candidate_name = base if ext else name
                key = (candidate_name, path)
                if key in seen:
                    continue
                seen.add(key)
                candidates.append(ConfigCandidate(candidate_name, path))
        return candidates

    def _tab_choices(self, opened_only: bool = False) -> list[tuple[str, str, str]]:
        tab_order = self._opened_tab_order if opened_only else self._available_tab_order
        return [
            (tab_id, self._tab_titles[tab_id], self._tab_descriptions[tab_id])
            for tab_id in tab_order
        ]

    def tab_choices(self, opened_only: bool = False) -> list[tuple[str, str, str]]:
        return self._tab_choices(opened_only=opened_only)

    def _move_tab(self, step: int) -> None:
        tabs = self.query_one("#tabs", TabbedContent)
        if not self._opened_tab_order:
            return

        active = tabs.active
        current_index = self._opened_tab_order.index(active) if active in self._opened_tab_order else 0
        next_tab = self._opened_tab_order[(current_index + step) % len(self._opened_tab_order)]
        self._bump_navigation_generation()
        tabs.active = next_tab
        self._focus_active_tab_content(next_tab)
        
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
        try:
            self.refresh_bindings()
        except ScreenStackError:
            return
        self.call_after_refresh(self._focus_active_tab_content, event.pane.id)

    def action_open_tab_picker(self) -> None:
        if not CommandPalette.is_open(self):
            self.push_screen(
                CommandPalette(
                    providers={TabCommands},
                    placeholder="Open tab...",
                    id="--tab-palette",
                )
            )

    def action_previous_tab(self) -> None:
        self._move_tab(-1)

    def action_next_tab(self) -> None:
        self._move_tab(1)

    async def _open_tab(self, tab_id: str, title: str, content: Widget) -> None:
        tabs = self.query_one("#tabs", TabbedContent)
        if tab_id in self._opened_tab_order:
            self._bump_navigation_generation()
            tabs.active = tab_id
            self._focus_active_tab_content(tab_id)
            return

        self._bump_navigation_generation()
        self._created_tabs.add(tab_id)
        self._opened_tab_order.append(tab_id)

        await tabs.add_pane(TabPane(title, content, id=tab_id))
        self._disable_tab_header_focus()
        tabs.active = tab_id
        self.refresh_bindings()

    async def action_open_mnc_config(self) -> None:
        await self._open_tab(
            "mnc-config",
            self._tab_titles["mnc-config"],
            MncConfigForm(self._mnc_config, DEFAULT_GIT_YDB_ROOT),
        )

    async def action_open_cluster_config(self) -> None:
        await self._open_tab(
            "cluster-config",
            self._tab_titles["cluster-config"],
            ClusterConfigPane(
                self._discover_cluster_config_candidates(),
                self._state,
                self._on_viewer_state_changed,
                self._discover_cluster_config_candidates,
                self._edit_file,
            ),
        )

    async def on_list_view_selected(self, event: ListView.Selected) -> None:
        if isinstance(event.item, OverviewStatusCard):
            if event.item.action is not None:
                await self.run_action(event.item.action)
        elif isinstance(event.item, ConfigFieldItem):
            await self._activate_config_field(event.item)

    async def _activate_config_field(self, field: ConfigFieldItem) -> None:
        if field.path_picker:
            self._open_path_picker(field)
        else:
            self._start_config_field_edit(field)

    def _normalize_config_path(self, path: str) -> str:
        return os.path.abspath(os.path.expanduser(path))

    def _open_path_picker(self, field: ConfigFieldItem) -> None:
        def update_path(path: Optional[str]) -> None:
            if path is None:
                self.call_after_refresh(self._focus_mnc_config_fields)
                return

            normalized_path = self._normalize_config_path(path)
            if not self._validate_config_field(field.field_name, normalized_path):
                self._show_invalid_path(normalized_path)
                return

            field.input.value = normalized_path
            field.save_edit()
            self._mnc_config[field.field_name] = normalized_path
            self._save_mnc_config()
            self._refresh_mnc_config_status()
            self.refresh_bindings()
            self.call_after_refresh(self._focus_mnc_config_fields)

        self.push_screen(PathPickerScreen(field.value, directories_only=True), update_path)

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
        if not self._validate_config_field(field.field_name, field.value):
            self._show_invalid_path(field.value)
            return

        field.save_edit()
        self._mnc_config[field.field_name] = field.value
        self._save_mnc_config()
        self._refresh_mnc_config_status()
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

    def _editor_command(self) -> list[str]:
        editor = os.environ.get("VISUAL") or os.environ.get("EDITOR")
        if editor:
            return shlex.split(editor)
        return [shutil.which("vim") or shutil.which("vi") or DEFAULT_EDITOR]

    def _edit_file(self, path: str) -> Optional[str]:
        command = self._editor_command() + [path]
        try:
            with self.suspend():
                result = subprocess.run(command, check=False)
        except FileNotFoundError:
            return f"Editor not found: {command[0]}"
        except SuspendNotSupported as error:
            return str(error)
        if result.returncode != 0:
            return f"Editor exited with status {result.returncode}"
        return None
        
    async def action_close_tab(self) -> None:
        tabs = self.query_one("#tabs", TabbedContent)
        if tabs.active in self._created_tabs:
            tab_id = tabs.active
            tab_index = self._opened_tab_order.index(tab_id)
            self._created_tabs.remove(tab_id)
            self._opened_tab_order.remove(tab_id)
            next_index = min(tab_index, len(self._opened_tab_order) - 1)
            if next_index >= 0:
                generation = self._bump_navigation_generation()
                next_tab = self._opened_tab_order[next_index]
                tabs.active = next_tab
                self._focus_active_tab_content(next_tab)
            await tabs.remove_pane(tab_id)
            if next_index >= 0:
                self._restore_active_tab(next_tab, generation)
                self.call_later(self._restore_active_tab, next_tab, generation)
                self.set_timer(0.01, lambda: self._restore_active_tab(next_tab, generation))
            self.refresh_bindings()


def main() -> None:
    Viewer().run()
