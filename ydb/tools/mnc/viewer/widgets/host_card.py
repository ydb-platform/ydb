from datetime import datetime

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import DataTable, Label, Static

from ydb.tools.mnc.viewer.styles.common import status_class
from ydb.tools.mnc.viewer.widgets.state import AgentHostStatus, ViewerState


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
            Label(self.host.status, classes=f"host-status {status_class(self.host.status)}"),
            classes="host-card-header",
        )
        yield self._agent_section()
        yield self._tasks_section()
        yield self._disks_section()

    def _agent_section(self) -> Vertical:
        children = [
            Label("Agent", classes="host-section-title"),
            _field_row("Status", self.host.status, status_class(self.host.status)),
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
