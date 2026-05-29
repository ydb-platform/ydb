import glob
import os
import shutil
from datetime import datetime
from dataclasses import dataclass, field
from typing import Callable, Optional

from rich.console import Group
from rich.panel import Panel
from rich.text import Text
import yaml
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.events import Click, Key
from textual.screen import ModalScreen
from textual.widgets import Button, Checkbox, DataTable, Input, Label, ListItem, ListView, Static

import ydb.tools.mnc.scheme as mnc_scheme


CONFIG_FIELD_TITLES = {
    "git_ydb_root": "YDB source root",
}
MNC_DEPLOY_FLAG_CHECKBOX_PREFIX = "mnc-deploy-flag-"


def mnc_deploy_flag_checkbox_id(flag: str) -> str:
    return MNC_DEPLOY_FLAG_CHECKBOX_PREFIX + flag


def mnc_deploy_flag_from_checkbox_id(checkbox_id: Optional[str]) -> Optional[str]:
    if checkbox_id is None or not checkbox_id.startswith(MNC_DEPLOY_FLAG_CHECKBOX_PREFIX):
        return None
    return checkbox_id[len(MNC_DEPLOY_FLAG_CHECKBOX_PREFIX):]


def _status_class(status: str) -> str:
    return "status-" + status.lower().replace(" ", "-")


class OverviewStatusCard(ListItem):
    def __init__(
        self,
        title: str,
        status: str,
        action: Optional[str] = None,
        id: Optional[str] = None,
        status_kind: Optional[str] = None,
    ) -> None:
        self.action = action
        self._status = status
        self._status_kind = status_kind or status
        self._status_label = Label(status, classes=f"overview-status-value {_status_class(self._status_kind)}")
        super().__init__(
            Vertical(
                Label(title, classes="overview-status-title"),
                self._status_label,
                classes="overview-status-card-content",
            ),
            id=id,
            classes="overview-status-card",
        )

    @property
    def status(self) -> str:
        return self._status

    def update_status(self, status: str, status_kind: Optional[str] = None) -> None:
        self._status = status
        self._status_kind = status_kind or status
        self._status_label.update(status)
        self._status_label.set_classes(f"overview-status-value {_status_class(self._status_kind)}")

    def _on_click(self, event: Click) -> None:
        if self.action is None:
            return
        event.stop()
        self.call_later(self.app.run_action, self.action)


class OverviewAgentsCard(ListItem):
    action = "open_agents"

    def __init__(self, state: "ViewerState") -> None:
        super().__init__(
            Vertical(
                Label("Hosts", id="overview-agents-title"),
                Label(
                    state.agents_status(),
                    id="overview-agents-status",
                    classes=f"overview-status-value {_status_class(state.agents_status_kind())}",
                ),
                Static(state.agents_details(), id="overview-agents-hosts", markup=False),
                classes="overview-agents-card-content",
            ),
            id="overview-agents-card",
        )

    def _on_click(self, event: Click) -> None:
        event.stop()
        self.call_later(self.app.run_action, self.action)


class OverviewListView(ListView, can_focus=True, can_focus_children=False, inherit_bindings=False):
    BINDINGS = [
        Binding("enter", "select_cursor", "Select", show=False),
    ]


class OverviewStatusList(OverviewListView):
    def key_left(self, event: Key) -> None:
        if self.index is None:
            self.index = 0
        elif self.index > 0:
            self.index -= 1
        event.stop()

    def key_right(self, event: Key) -> None:
        if self.index is None:
            self.index = 0
        elif self.index < len(self.children) - 1:
            self.index += 1
        event.stop()

    def key_down(self, event: Key) -> None:
        self.parent.focus_agents_card()
        event.stop()


class OverviewAgentsList(OverviewListView):
    def key_up(self, event: Key) -> None:
        self.parent.focus_status_card()
        event.stop()


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

    .status-fail {
        color: $error;
    }

    .status-not-selected {
        color: $text-muted;
    }

    .status-checking {
        color: $warning;
    }

    .status-running {
        color: $success;
    }

    .status-stopped,
    .status-not-installed {
        color: $error;
    }

    #overview-agents-list {
        width: 1fr;
        height: 1fr;
        min-height: 5;
        margin: 0 2;
        background: transparent;
    }

    #overview-agents-card {
        height: auto;
        min-height: 5;
    }

    .overview-agents-card-content {
        height: auto;
        min-height: 5;
        padding: 0 2;
        background: $surface;
    }

    .overview-agents-card-content:dark {
        background: $panel-darken-1;
    }

    #overview-agents-list > #overview-agents-card.-highlight {
        background: transparent;
    }

    #overview-agents-list > #overview-agents-card.-highlight .overview-agents-card-content {
        background: $block-cursor-blurred-background;
    }

    #overview-agents-list > #overview-agents-card.-highlight #overview-agents-title {
        color: $block-cursor-blurred-foreground;
    }

    #overview-agents-title {
        height: 2;
        content-align: left middle;
        text-style: bold;
    }

    #overview-agents-status {
        height: 2;
        content-align: left middle;
    }

    #overview-agents-hosts {
        height: auto;
        color: $text-muted;
    }
    """

    def __init__(self, state: "ViewerState") -> None:
        super().__init__()
        self._state = state

    def compose(self) -> ComposeResult:
        yield OverviewStatusList(
            OverviewStatusCard(
                "Settings",
                self._state.mnc_config_status(),
                action="open_mnc_config",
                id="overview-mnc-config-card",
            ),
            OverviewStatusCard(
                "Cluster",
                self._state.cluster_config_status(),
                action="open_cluster_config",
                id="overview-cluster-config-card",
                status_kind=self._state.cluster_config_status_kind(),
            ),
            id="overview-status-row",
        )
        yield OverviewAgentsList(
            OverviewAgentsCard(self._state),
            initial_index=None,
            id="overview-agents-list",
        )

    def refresh_state(self) -> None:
        self.query_one("#overview-mnc-config-card", OverviewStatusCard).update_status(
            self._state.mnc_config_status()
        )
        self.query_one("#overview-cluster-config-card", OverviewStatusCard).update_status(
            self._state.cluster_config_status(),
            self._state.cluster_config_status_kind(),
        )
        agents_status = self.query_one("#overview-agents-status", Label)
        agents_status.update(self._state.agents_status())
        agents_status.set_classes(f"overview-status-value {_status_class(self._state.agents_status_kind())}")
        self.query_one("#overview-agents-hosts", Static).update(self._state.agents_details())

    async def on_list_view_selected(self, event: ListView.Selected) -> None:
        if isinstance(event.item, (OverviewStatusCard, OverviewAgentsCard)):
            event.stop()
            if event.item.action is not None:
                await self.app.run_action(event.item.action)

    def focus_status_card(self) -> None:
        agents_list = self.query_one("#overview-agents-list", ListView)
        status_row = self.query_one("#overview-status-row", ListView)
        agents_list.index = None
        status_row.index = 0
        self.screen.set_focus(status_row)

    def focus_agents_card(self) -> None:
        status_row = self.query_one("#overview-status-row", ListView)
        agents_list = self.query_one("#overview-agents-list", ListView)
        status_row.index = None
        agents_list.index = 0
        self.screen.set_focus(agents_list)

    def on_list_view_highlighted(self, event: ListView.Highlighted) -> None:
        if event.item is None:
            return

        status_row = self.query_one("#overview-status-row", ListView)
        agents_list = self.query_one("#overview-agents-list", ListView)
        if event.list_view is status_row:
            agents_list.index = None
        elif event.list_view is agents_list:
            status_row.index = None

    def on_key(self, event: Key) -> None:
        focused = self.screen.focused
        status_row = self.query_one("#overview-status-row", ListView)
        agents_list = self.query_one("#overview-agents-list", ListView)

        if event.key in ("left", "right") and focused is status_row:
            if status_row.index is None:
                status_row.index = 0
            elif event.key == "left" and status_row.index > 0:
                status_row.index -= 1
            elif event.key == "right" and status_row.index < len(status_row.children) - 1:
                status_row.index += 1
            event.stop()
        elif event.key == "down" and focused is status_row:
            self.focus_agents_card()
            event.stop()
        elif event.key == "up" and focused is agents_list:
            self.focus_status_card()
            event.stop()


class AgentsPane(VerticalScroll, inherit_bindings=False):
    DEFAULT_CSS = """
    AgentsPane {
        height: 1fr;
        padding: 0 1 1 1;
    }

    #agents-summary {
        height: auto;
        min-height: 4;
        padding: 0 2;
        background: $surface;
    }

    #agents-summary:dark {
        background: $panel-darken-1;
    }

    #agents-title {
        height: 2;
        content-align: left middle;
        text-style: bold;
    }

    #agents-status {
        height: 2;
        content-align: left middle;
    }

    #agents-hosts {
        height: auto;
        min-height: 3;
        margin-top: 1;
        background: transparent;
    }

    .hosts-empty-message {
        height: auto;
        min-height: 3;
        padding: 1 2;
        background: $surface;
        color: $text-muted;
    }

    .hosts-empty-message:dark {
        background: $panel-darken-1;
    }

    AgentsPane:focus #agents-summary {
        background: $primary-background;
    }

    HostCard {
        height: auto;
        margin-bottom: 1;
        padding: 1 2;
        background: $surface;
        border: solid $primary;
    }

    HostCard:dark {
        background: $panel-darken-1;
    }

    .host-card-header {
        height: 2;
        width: 100%;
    }

    .host-title {
        width: 1fr;
        height: 2;
        content-align: left middle;
        text-style: bold;
    }

    .host-status {
        width: auto;
        height: 2;
        content-align: right middle;
        text-style: bold;
    }

    .host-section {
        height: auto;
        margin-top: 1;
    }

    .host-section-title {
        height: 1;
        text-style: bold;
        color: $text;
    }

    .host-field-row {
        height: auto;
        min-height: 1;
        width: 100%;
    }

    .host-field-name {
        width: 18;
        color: $text-muted;
    }

    .host-field-value {
        width: 1fr;
    }

    .host-message {
        color: $text-muted;
    }

    HostTasksTable {
        height: 7;
        width: 100%;
        margin-top: 1;
    }

    .host-empty-line {
        height: 1;
        margin-top: 1;
        color: $text-muted;
    }

    .disk-row {
        height: auto;
        min-height: 1;
        width: 100%;
        margin-top: 1;
    }

    .disk-name {
        width: 24;
        text-style: bold;
    }

    .disk-detail {
        width: 1fr;
        color: $text-muted;
    }
    """

    def __init__(self, state: "ViewerState") -> None:
        super().__init__()
        self._state = state

    def compose(self) -> ComposeResult:
        yield Vertical(
            Label("Hosts", id="agents-title"),
            Label(
                self._state.agents_status(),
                id="agents-status",
                classes=f"overview-status-value {_status_class(self._state.agents_status_kind())}",
            ),
            id="agents-summary",
        )
        yield HostsContainer(self._state, id="agents-hosts")

    def refresh_state(self) -> None:
        agents_status = self.query_one("#agents-status", Label)
        agents_status.update(self._state.agents_status())
        agents_status.set_classes(f"overview-status-value {_status_class(self._state.agents_status_kind())}")
        self.query_one("#agents-hosts", HostsContainer).refresh(recompose=True)

    def key_up(self, event: Key) -> None:
        self.scroll_up(animate=False, force=True)
        event.stop()

    def key_down(self, event: Key) -> None:
        self.scroll_down(animate=False, force=True)
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
        height: auto;
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

    #mnc-deploy-flags {
        height: auto;
        margin-top: 1;
        padding: 1 2;
        background: $surface;
    }

    #mnc-deploy-flags:dark {
        background: $panel-darken-1;
    }

    .config-group-title {
        height: 2;
        content-align: left middle;
        text-style: bold;
    }

    .config-checkbox {
        height: auto;
        margin-bottom: 1;
        background: transparent;
    }
    """

    def __init__(self, config: dict[str, object], default_git_ydb_root: str) -> None:
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
        deploy_flags = set(self._config.get("deploy_flags") or [])
        yield Vertical(
            Label("Deploy flags", classes="config-group-title"),
            *(
                Checkbox(
                    flag,
                    value=flag in deploy_flags,
                    id=mnc_deploy_flag_checkbox_id(flag),
                    classes="config-checkbox",
                )
                for flag in mnc_scheme.common.deploy_flags
            ),
            id="mnc-deploy-flags",
        )


@dataclass
class ConfigCandidate:
    name: str
    path: str


def _config_candidate_name_from_path(path: str) -> str:
    name = os.path.basename(path)
    base, ext = os.path.splitext(name)
    return base if ext else name


def _config_path_for_name(candidate: ConfigCandidate, name: str) -> str:
    name = name.strip()
    base_name = os.path.basename(name)
    base, ext = os.path.splitext(base_name)
    if not ext:
        ext = os.path.splitext(candidate.path)[1] or ".yaml"
        base_name = base_name + ext
    return os.path.join(os.path.dirname(candidate.path), base_name)


@dataclass
class ConfigValidation:
    errors: list[str]
    config: Optional[dict] = None

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
    return ConfigValidation([], validated)


@dataclass
class SelectedClusterConfig:
    candidate: ConfigCandidate
    validation: ConfigValidation


@dataclass
class OperationRequest:
    operation_id: str
    waiting: int = 15
    bin_path: Optional[str] = None
    do_not_init: bool = False
    ignore_failed_stop: bool = False


@dataclass
class OperationBacktraceFrame:
    path: str
    line: int
    function: str
    source: str = ""


@dataclass
class OperationState:
    status: str = "IDLE"
    operation_id: str = ""
    message: str = "No operation has been started."
    result_renderable: object = None
    error_type: str = ""
    error_message: str = ""
    backtrace: list[OperationBacktraceFrame] = field(default_factory=list)
    progress_backend: object = None


def _display_value(value: object) -> str:
    return str(value) if value not in (None, "") else "-"


def _format_task_time(value: object) -> str:
    if not isinstance(value, (int, float)):
        return "-"
    return datetime.fromtimestamp(value).strftime("%Y-%m-%d %H:%M")


def _field_row(name: str, value: str, value_classes: str = "") -> Horizontal:
    return Horizontal(
        Label(name, classes="host-field-name"),
        Label(value, classes=("host-field-value " + value_classes).strip()),
        classes="host-field-row",
    )


class HostTasksTable(DataTable, can_focus=False):
    def __init__(self, tasks: list[dict]) -> None:
        super().__init__(
            show_header=True,
            show_row_labels=False,
            zebra_stripes=True,
            show_cursor=False,
            cursor_type="none",
            cell_padding=1,
        )
        self.tasks = tasks[:5]

    def on_mount(self) -> None:
        self.add_columns("ID", "Type", "Status", "Created", "Error")
        for task in self.tasks:
            self.add_row(
                _display_value(task.get("id")),
                _display_value(task.get("type")),
                _display_value(task.get("status")),
                _format_task_time(task.get("created_at")),
                _display_value(task.get("error")),
            )


def _disk_row(disk: dict) -> Horizontal:
    label = disk.get("partlabel") or "<unknown>"
    error = disk.get("error")
    if error:
        return Horizontal(
            Label(label, classes="disk-name"),
            Label(f"ERROR {error}", classes="disk-detail status-fail"),
            classes="disk-row",
        )

    device = disk.get("path") or disk.get("device") or "-"
    size = disk.get("size") or "-"
    parts = disk.get("parts") or []
    return Horizontal(
        Label(label, classes="disk-name"),
        Label(f"{device}, size {size}, parts {len(parts)}", classes="disk-detail"),
        classes="disk-row",
    )


class HostCard(Vertical):
    def __init__(self, host: "AgentHostStatus") -> None:
        super().__init__()
        self.host = host

    def compose(self) -> ComposeResult:
        yield Horizontal(
            Label(self.host.host, classes="host-title"),
            Label(self.host.status, classes=f"host-status {_status_class(self.host.status)}"),
            classes="host-card-header",
        )
        yield self._agent_section()
        yield self._tasks_section()
        yield self._disks_section()

    def _agent_section(self) -> Vertical:
        children = [
            Label("Agent", classes="host-section-title"),
            _field_row("Status", self.host.status, _status_class(self.host.status)),
        ]
        if self.host.message:
            children.append(_field_row("Message", self.host.message, "host-message"))
        if self.host.agent_is_running():
            features = ", ".join(self.host.enabled_features) if self.host.enabled_features else "none"
            children.append(_field_row("Enabled features", features))
        return Vertical(*children, classes="host-section")

    def _tasks_section(self) -> Vertical:
        children = [Label("Last tasks", classes="host-section-title")]
        if self.host.tasks_error:
            children.append(Label(self.host.tasks_error, classes="host-empty-line"))
        elif not self.host.last_tasks:
            children.append(Label("No tasks yet", classes="host-empty-line"))
        else:
            children.append(HostTasksTable(self.host.last_tasks))
        return Vertical(*children, classes="host-section")

    def _disks_section(self) -> Vertical:
        children = [Label("Disks", classes="host-section-title")]
        if self.host.disks_error:
            children.append(Label(self.host.disks_error, classes="host-empty-line"))
        elif not self.host.disks:
            children.append(Label("No managed disks", classes="host-empty-line"))
        else:
            children.extend(_disk_row(disk) for disk in self.host.disks)
        return Vertical(*children, classes="host-section")


class HostsContainer(Vertical):
    def __init__(self, state: "ViewerState", *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._state = state

    def compose(self) -> ComposeResult:
        if self._state.agents.status == "NOT SELECTED":
            yield Static("Select cluster config to inspect hosts", classes="hosts-empty-message", markup=False)
            return
        if self._state.agents.status == "CHECKING":
            hosts = ", ".join(host.host for host in self._state.agents.hosts)
            yield Static(f"Checking agents on {hosts}", classes="hosts-empty-message", markup=False)
            return
        if not self._state.agents.hosts:
            yield Static("No hosts in selected cluster config", classes="hosts-empty-message", markup=False)
            return

        for host in self._state.agents.hosts:
            yield HostCard(host)


@dataclass
class AgentHostStatus:
    host: str
    status: str
    message: str = ""
    enabled_features: list[str] = field(default_factory=list)
    last_tasks: list[dict] = field(default_factory=list)
    tasks_error: str = ""
    disks: list[dict] = field(default_factory=list)
    disks_error: str = ""

    def agent_is_running(self) -> bool:
        return self.status in ("Running", "OK")


@dataclass
class AgentsState:
    status: str = "NOT SELECTED"
    hosts: list[AgentHostStatus] = field(default_factory=list)

    def ok_count(self) -> int:
        return sum(1 for host in self.hosts if host.agent_is_running())

    def total_count(self) -> int:
        return len(self.hosts)


@dataclass
class ViewerState:
    mnc_config_ok: bool
    selected_cluster_config: Optional[SelectedClusterConfig] = None
    agents: AgentsState = field(default_factory=AgentsState)
    operation: OperationState = field(default_factory=OperationState)

    def mnc_config_status(self) -> str:
        return "OK" if self.mnc_config_ok else "ERROR"

    def cluster_config_status(self) -> str:
        if self.selected_cluster_config is None:
            return "NOT SELECTED"
        return self.selected_cluster_config.candidate.name

    def cluster_config_status_kind(self) -> str:
        if self.selected_cluster_config is None:
            return "NOT SELECTED"
        return "OK" if self.selected_cluster_config.validation.ok else "FAIL"

    def agents_status(self) -> str:
        if self.agents.status in ("NOT SELECTED", "CHECKING"):
            return self.agents.status
        if self.agents.total_count() == 0:
            return self.agents.status
        return self.agents.status

    def agents_status_kind(self) -> str:
        return self.agents.status

    def agents_details(self) -> str:
        if self.agents.status == "NOT SELECTED":
            return "Select cluster config to inspect hosts"
        if self.agents.status == "CHECKING":
            return "Checking agents on " + ", ".join(host.host for host in self.agents.hosts)
        if not self.agents.hosts:
            return "No hosts in selected cluster config"
        details = [f"Agents: {self.agents.ok_count()}/{self.agents.total_count()}"]
        details.extend(
            f"{host.host}: {host.status}" + (f" ({host.message})" if host.message else "")
            for host in self.agents.hosts
            if not host.agent_is_running() or host.message
        )
        return "\n".join(details)

    def is_selected_cluster_config(self, candidate: ConfigCandidate) -> bool:
        return (
            self.selected_cluster_config is not None
            and self.selected_cluster_config.candidate.path == candidate.path
        )


class OperationsPane(VerticalScroll, inherit_bindings=False):
    DEFAULT_CSS = """
    OperationsPane {
        height: 1fr;
        padding: 0 1 1 1;
    }

    #operations-form,
    #operations-result {
        height: auto;
        padding: 1 2;
        background: $surface;
    }

    #operations-form:dark,
    #operations-result:dark {
        background: $panel-darken-1;
    }

    #operations-result {
        margin-top: 1;
    }

    .operations-title {
        height: 2;
        content-align: left middle;
        text-style: bold;
    }

    .operations-row {
        height: auto;
        min-height: 1;
        width: 100%;
        margin-bottom: 1;
    }

    .operations-label {
        width: 20;
        color: $text-muted;
    }

    .operations-value {
        width: 1fr;
    }

    #operations-install-arguments {
        height: auto;
    }

    .operations-input {
        height: auto;
        width: 1fr;
        margin-bottom: 1;
    }

    .operations-checkbox {
        height: auto;
        margin-bottom: 1;
        background: transparent;
    }

    #operations-error {
        height: auto;
        color: $text-error;
        margin-bottom: 1;
    }

    #operations-actions {
        height: auto;
        align-horizontal: right;
    }

    #operations-output {
        height: auto;
        min-height: 3;
        color: $text-muted;
    }

    #operations-live {
        height: auto;
        margin-top: 1;
    }

    #operations-steps-pane {
        width: 2fr;
        height: auto;
        padding-right: 1;
    }

    #operations-details-pane {
        width: 3fr;
        height: auto;
        padding-left: 1;
    }

    .operations-live-title {
        height: 2;
        content-align: left middle;
        color: $text-muted;
        text-style: bold;
    }

    #operations-steps,
    #operations-details {
        height: auto;
        min-height: 3;
    }
    """

    def __init__(
        self,
        state: ViewerState,
        on_run: Callable[[OperationRequest], None],
        operation_id: str,
        operation_title: str,
    ) -> None:
        super().__init__()
        self._state = state
        self._on_run = on_run
        self._operation_id = operation_id
        self._operation_title = operation_title

    def compose(self) -> ComposeResult:
        yield Vertical(
            Label(self._operation_title, classes="operations-title"),
            self._summary_row("Cluster", "", id="operations-cluster"),
            self._summary_row("Config path", "", id="operations-config-path"),
            self._summary_row("Hosts", "", id="operations-hosts"),
            Label("Arguments", classes="operations-title"),
            Vertical(
                Label("waiting", classes="operations-label"),
                Input(value="15", placeholder="15", id="operations-waiting", classes="operations-input"),
                Label("bin_path", classes="operations-label"),
                Input(value="", placeholder="optional path to ydb binary", id="operations-bin-path", classes="operations-input"),
                Checkbox("do_not_init", value=False, id="operations-do-not-init", classes="operations-checkbox"),
                id="operations-install-arguments",
            ),
            Checkbox(
                "ignore_failed_stop",
                value=False,
                id="operations-ignore-failed-stop",
                classes="operations-checkbox",
            ),
            Static("", id="operations-error", markup=False),
            Horizontal(
                Button("Run", id="operations-run", variant="error"),
                id="operations-actions",
            ),
            id="operations-form",
        )
        yield Vertical(
            Label("Execution", classes="operations-title"),
            Static("", id="operations-output", markup=False),
            Horizontal(
                Vertical(
                    Label("Steps", classes="operations-live-title"),
                    Static("", id="operations-steps"),
                    id="operations-steps-pane",
                ),
                Vertical(
                    Label("Details", classes="operations-live-title"),
                    Static("", id="operations-details"),
                    id="operations-details-pane",
                ),
                id="operations-live",
            ),
            id="operations-result",
        )

    def on_mount(self) -> None:
        self.refresh_state()
        if self._operation_id == "install":
            self.query_one("#operations-waiting", Input).focus()
        else:
            self.query_one("#operations-run", Button).focus()

    def refresh_state(self) -> None:
        selected = self._selected_cluster()
        self.query_one("#operations-cluster .operations-value", Label).update(
            selected.candidate.name if selected is not None else "Select a valid cluster first"
        )
        self.query_one("#operations-config-path .operations-value", Label).update(
            selected.candidate.path if selected is not None else "-"
        )
        self.query_one("#operations-hosts .operations-value", Label).update(self._hosts_text())

        started = self._operation_started()
        self.query_one("#operations-form", Vertical).display = not started
        self.query_one("#operations-result", Vertical).display = started
        self.query_one("#operations-install-arguments", Vertical).display = self._operation_id == "install"
        self.query_one("#operations-error", Static).update(self._validation_error())
        self.query_one("#operations-run", Button).disabled = bool(self._validation_error())
        self._refresh_operation_output()

    def _summary_row(self, label: str, value: str, id: str) -> Horizontal:
        return Horizontal(
            Label(label, classes="operations-label"),
            Label(value, classes="operations-value"),
            id=id,
            classes="operations-row",
        )

    def _selected_cluster(self) -> Optional[SelectedClusterConfig]:
        selected = self._state.selected_cluster_config
        if selected is None or selected.validation.config is None or not selected.validation.ok:
            return None
        return selected

    def _hosts(self) -> list[str]:
        selected = self._selected_cluster()
        if selected is None:
            return []
        hosts = selected.validation.config.get("hosts") or []
        return list(dict.fromkeys(str(host) for host in hosts))

    def _hosts_text(self) -> str:
        hosts = self._hosts()
        return f"{len(hosts)}: {', '.join(hosts)}" if hosts else "0"

    def _validation_error(self) -> str:
        if self._operation_started():
            return ""
        if self._state.operation.status == "RUNNING":
            return "Operation is running."
        if self._selected_cluster() is None:
            return "Select a valid cluster first."
        if not self._hosts():
            return "No hosts in selected cluster."
        if self._operation_id == "install":
            waiting = self.query_one("#operations-waiting", Input).value.strip()
            try:
                value = int(waiting)
            except ValueError:
                return "waiting must be a positive integer."
            if value <= 0:
                return "waiting must be a positive integer."
        return ""

    def _operation_started(self) -> bool:
        operation = self._state.operation
        return operation.operation_id == self._operation_id and operation.status != "IDLE"

    def _request(self) -> Optional[OperationRequest]:
        error = self._validation_error()
        if error:
            self.query_one("#operations-error", Static).update(error)
            return None

        ignore_failed_stop = bool(self.query_one("#operations-ignore-failed-stop", Checkbox).value)
        if self._operation_id == "uninstall":
            return OperationRequest(
                operation_id="uninstall",
                ignore_failed_stop=ignore_failed_stop,
            )

        bin_path = self.query_one("#operations-bin-path", Input).value.strip() or None
        return OperationRequest(
            operation_id="install",
            waiting=int(self.query_one("#operations-waiting", Input).value.strip()),
            bin_path=bin_path,
            do_not_init=bool(self.query_one("#operations-do-not-init", Checkbox).value),
            ignore_failed_stop=ignore_failed_stop,
        )

    def _operation_output(self):
        operation = self._state.operation
        title = self._operation_title if operation.operation_id == self._operation_id else "Operation"
        status_style = {
            "OK": "bold green",
            "FAIL": "bold red",
            "RUNNING": "bold yellow",
            "IDLE": "dim",
        }.get(operation.status, "bold")
        header = Text.assemble((operation.status, status_style), ": ", (title, "bold"))
        if operation.error_type:
            return Panel(
                Group(
                    Text.assemble(
                        (operation.error_type, "bold red"),
                        (": ", "red"),
                        (operation.error_message, "red"),
                    ),
                    "",
                    self._backtrace_text(operation.backtrace),
                ),
                title=f"[bold red]{operation.status}:[/] {title}",
                border_style="red",
                expand=False,
                title_align="left",
            )
        if operation.result_renderable is not None:
            return self._result_renderable(operation.result_renderable)
        if operation.message:
            return Group(header, "", Text(operation.message))
        return header

    def _refresh_operation_output(self) -> None:
        operation = self._state.operation
        backend = operation.progress_backend if operation.operation_id == self._operation_id else None
        show_live = backend is not None and operation.status == "RUNNING" and not operation.error_type

        self.query_one("#operations-live", Horizontal).display = show_live
        if show_live:
            self.query_one("#operations-steps", Static).update(backend.render_tree_body())
            self.query_one("#operations-details", Static).update(backend.render_details_body())

        self.query_one("#operations-output", Static).update(self._operation_output())

    def _result_renderable(self, renderable):
        if isinstance(renderable, str):
            try:
                return Text.from_markup(renderable)
            except Exception:
                return Text(renderable)
        return renderable

    def _backtrace_text(self, frames: list[OperationBacktraceFrame]) -> Text:
        text = Text("Backtrace", style="bold")
        if not frames:
            text.append("\n  -")
            return text
        for index, frame in enumerate(frames, 1):
            text.append(f"\n  {index}. ", style="dim")
            text.append(f"{frame.path}:{frame.line}", style="cyan")
            text.append(" in ", style="dim")
            text.append(frame.function, style="magenta")
            if frame.source:
                text.append("\n     ")
                text.append(frame.source)
        return text

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "operations-run":
            event.stop()
            request = self._request()
            if request is not None:
                self._on_run(request)

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id == "operations-waiting":
            self.refresh_state()

    def _live_backend(self):
        operation = self._state.operation
        if (
            operation.operation_id != self._operation_id
            or operation.status != "RUNNING"
            or operation.error_type
        ):
            return None
        return operation.progress_backend

    def _move_live_selection(self, delta: int) -> bool:
        backend = self._live_backend()
        if backend is None:
            return False
        backend.move_selection(delta)
        self._refresh_operation_output()
        return True

    def _toggle_live_selection(self) -> bool:
        backend = self._live_backend()
        if backend is None:
            return False
        backend.toggle_selected()
        self._refresh_operation_output()
        return True

    def key_up(self, event: Key) -> None:
        if self._move_live_selection(-1):
            event.stop()
            return
        self.scroll_up(animate=False, force=True)
        event.stop()

    def key_down(self, event: Key) -> None:
        if self._move_live_selection(1):
            event.stop()
            return
        self.scroll_down(animate=False, force=True)
        event.stop()

    def key_enter(self, event: Key) -> None:
        if self._toggle_live_selection():
            event.stop()

    def key_space(self, event: Key) -> None:
        if self._toggle_live_selection():
            event.stop()


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
    BINDINGS = [
        Binding("r", "rename_config", "Rename"),
        Binding("e", "edit_config", "Edit"),
        Binding("c", "copy_config", "Copy"),
    ]

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

    def __init__(
        self,
        candidates: list[ConfigCandidate],
        state: ViewerState,
        on_state_changed: Callable[[], None],
        load_candidates: Callable[[], list[ConfigCandidate]],
        edit_file: Callable[[str], Optional[str]],
    ) -> None:
        super().__init__()
        self._candidates = candidates
        self._state = state
        self._on_state_changed = on_state_changed
        self._load_candidates = load_candidates
        self._edit_file = edit_file

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
            self._select_candidate(event.item.candidate)

    def on_key(self, event: Key) -> None:
        if event.key == "right" and self.screen.focused is self.query_one("#cluster-configs", ListView):
            self.query_one("#cluster-config-details", ClusterConfigDetails).focus()
            event.stop()

    def action_rename_config(self) -> None:
        candidate = self._highlighted_candidate()
        if candidate is None:
            return

        self.app.push_screen(
            ConfigNameModal("Rename config", candidate.name, "Rename"),
            lambda name: self._rename_candidate(candidate, name),
        )

    def action_copy_config(self) -> None:
        candidate = self._highlighted_candidate()
        if candidate is None:
            return

        self.app.push_screen(
            ConfigNameModal("Copy config", f"{candidate.name}_copy", "Copy"),
            lambda name: self._copy_candidate(candidate, name),
        )

    def action_edit_config(self) -> None:
        candidate = self._highlighted_candidate()
        if candidate is None:
            return

        error = self._edit_file(candidate.path)
        if error is not None:
            self._show_message("Editor failed", error)
            return

        self._reload_candidates(candidate.path)
        self._select_candidate_by_path(candidate.path)

    def _refresh(self, preferred_path: Optional[str] = None) -> None:
        list_view = self.query_one("#cluster-configs", ListView)
        list_view.clear()
        list_view.extend([ConfigCandidateItem(candidate) for candidate in self._candidates])
        selected_index = next(
            (
                index
                for index, candidate in enumerate(self._candidates)
                if candidate.path == preferred_path or self._state.is_selected_cluster_config(candidate)
            ),
            None,
        )
        list_view.index = selected_index if selected_index is not None else (0 if self._candidates else None)
        if self._candidates:
            self._show_details(self._candidates[list_view.index or 0])
        else:
            self.query_one("#cluster-config-details", ClusterConfigDetails).update_empty("No configs found")

    def _highlighted_candidate(self) -> Optional[ConfigCandidate]:
        item = self.query_one("#cluster-configs", ListView).highlighted_child
        if isinstance(item, ConfigCandidateItem):
            return item.candidate
        return None

    def _reload_candidates(self, preferred_path: Optional[str] = None) -> None:
        self._candidates = self._load_candidates()
        self._refresh(preferred_path)

    def _select_candidate_by_path(self, path: str) -> None:
        candidate = next((candidate for candidate in self._candidates if candidate.path == path), None)
        if candidate is None:
            if (
                self._state.selected_cluster_config is not None
                and self._state.selected_cluster_config.candidate.path == path
            ):
                self._state.selected_cluster_config = None
                self._on_state_changed()
            return
        self._select_candidate(candidate)

    def _select_candidate(self, candidate: ConfigCandidate) -> bool:
        validation = _validate_multinode_config(candidate.path)
        if not validation.ok:
            if self._state.is_selected_cluster_config(candidate):
                self._state.selected_cluster_config = None
                self._on_state_changed()
            self._show_details(candidate, validation)
            return False
        self._state.selected_cluster_config = SelectedClusterConfig(candidate, validation)
        self._on_state_changed()
        self._show_details(candidate, validation)
        return True

    def _rename_candidate(self, candidate: Optional[ConfigCandidate], name: Optional[str]) -> None:
        if candidate is None or name is None:
            return

        target_path = self._target_path(candidate, name)
        if target_path is None:
            return
        if target_path == candidate.path:
            return
        if os.path.exists(target_path):
            self._show_message("Rename failed", f"Config already exists:\n{target_path}")
            return

        os.rename(candidate.path, target_path)
        renamed = ConfigCandidate(_config_candidate_name_from_path(target_path), target_path)
        if self._state.is_selected_cluster_config(candidate):
            validation = _validate_multinode_config(target_path)
            self._state.selected_cluster_config = SelectedClusterConfig(renamed, validation)
            self._on_state_changed()
        self._reload_candidates(target_path)

    def _copy_candidate(self, candidate: Optional[ConfigCandidate], name: Optional[str]) -> None:
        if candidate is None or name is None:
            return

        target_path = self._target_path(candidate, name)
        if target_path is None:
            return
        if os.path.exists(target_path):
            self._show_message("Copy failed", f"Config already exists:\n{target_path}")
            return

        shutil.copy2(candidate.path, target_path)
        self._reload_candidates(target_path)

    def _target_path(self, candidate: ConfigCandidate, name: str) -> Optional[str]:
        if not name.strip():
            self._show_message("Invalid config name", "Config name must not be empty.")
            return None
        if os.path.basename(name) != name:
            self._show_message("Invalid config name", "Config name must not include directories.")
            return None
        return _config_path_for_name(candidate, name)

    def _show_message(self, title: str, message: str) -> None:
        self.app.push_screen(MessageModal(title, message))

    def _show_details(self, candidate: ConfigCandidate, validation: Optional[ConfigValidation] = None) -> None:
        try:
            with open(candidate.path) as file:
                content = file.read()
        except Exception as error:
            content = f"Failed to read config: {error}"
        validation = validation or _validate_multinode_config(candidate.path)
        self.query_one("#cluster-config-details", ClusterConfigDetails).update_candidate(
            candidate,
            self._state.is_selected_cluster_config(candidate),
            validation,
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


class MessageModal(ModalScreen[None]):
    CSS = """
    MessageModal {
        color: $foreground;
        background: $background 60%;
        align: center middle;
    }

    #message-dialog {
        width: 72;
        height: auto;
        padding: 1 2;
        border: hkey $error;
        background: $surface;
    }

    #message-dialog:dark {
        background: $panel-darken-1;
    }

    #message-title {
        height: auto;
        text-style: bold;
        color: $error;
        margin-bottom: 1;
    }

    #message-body {
        height: auto;
        margin-bottom: 1;
    }

    #message-actions {
        height: auto;
        align-horizontal: right;
    }
    """

    BINDINGS = [
        Binding("escape", "close", "Close"),
    ]

    def __init__(self, title: str, message: str) -> None:
        super().__init__()
        self._title = title
        self._message = message

    def compose(self) -> ComposeResult:
        yield Vertical(
            Label(self._title, id="message-title"),
            Static(self._message, id="message-body"),
            Horizontal(
                Button("OK", id="message-ok", variant="primary"),
                id="message-actions",
            ),
            id="message-dialog",
        )

    def on_mount(self) -> None:
        self.query_one("#message-ok", Button).focus()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "message-ok":
            event.stop()
            self.dismiss(None)

    def action_close(self) -> None:
        self.dismiss(None)


class ConfigNameModal(ModalScreen[Optional[str]]):
    CSS = """
    ConfigNameModal {
        color: $foreground;
        background: $background 60%;
        align: center middle;
    }

    #config-name-dialog {
        width: 72;
        height: auto;
        padding: 1 2;
        border: hkey $border;
        background: $surface;
    }

    #config-name-dialog:dark {
        background: $panel-darken-1;
    }

    #config-name-title {
        height: auto;
        text-style: bold;
        margin-bottom: 1;
    }

    #config-name-input {
        height: auto;
        margin-bottom: 1;
    }

    #config-name-actions {
        height: auto;
        align-horizontal: right;
    }
    """

    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
    ]

    def __init__(self, title: str, initial_name: str, action_label: str) -> None:
        super().__init__()
        self._title = title
        self._initial_name = initial_name
        self._action_label = action_label

    def compose(self) -> ComposeResult:
        yield Vertical(
            Label(self._title, id="config-name-title"),
            Input(value=self._initial_name, id="config-name-input"),
            Horizontal(
                Button(self._action_label, id="config-name-ok", variant="primary"),
                Button("Cancel", id="config-name-cancel"),
                id="config-name-actions",
            ),
            id="config-name-dialog",
        )

    def on_mount(self) -> None:
        name_input = self.query_one("#config-name-input", Input)
        name_input.focus()
        name_input.cursor_position = len(name_input.value)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "config-name-input":
            event.stop()
            self._accept()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "config-name-ok":
            event.stop()
            self._accept()
        elif event.button.id == "config-name-cancel":
            event.stop()
            self.dismiss(None)

    def _accept(self) -> None:
        self.dismiss(self.query_one("#config-name-input", Input).value.strip())

    def action_cancel(self) -> None:
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
