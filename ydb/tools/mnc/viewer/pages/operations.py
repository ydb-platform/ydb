from typing import Callable, Optional

from rich.console import Group
from rich.panel import Panel
from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.css.query import NoMatches
from textual.events import Key
from textual.widgets import Button, Checkbox, Input, Label, Static

from ydb.tools.mnc.viewer.styles.operations import OPERATIONS_CSS
from ydb.tools.mnc.viewer.widgets.config_models import SelectedClusterConfig
from ydb.tools.mnc.viewer.widgets.operation_form import (
    OperationFormButton,
    OperationFormCheckbox,
    OperationFormInput,
)
from ydb.tools.mnc.viewer.widgets.operation_models import OperationBacktraceFrame, OperationRequest
from ydb.tools.mnc.viewer.widgets.state import ViewerState


class OperationsPane(VerticalScroll, inherit_bindings=False):
    DEFAULT_CSS = OPERATIONS_CSS

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
                OperationFormInput(value="15", placeholder="15", id="operations-waiting", classes="operations-input"),
                Label("bin_path", classes="operations-label"),
                OperationFormInput(value="", placeholder="optional path to ydb binary", id="operations-bin-path", classes="operations-input"),
                OperationFormCheckbox("do_not_init", value=False, id="operations-do-not-init", classes="operations-checkbox"),
                id="operations-install-arguments",
            ),
            OperationFormCheckbox(
                "ignore_failed_stop",
                value=False,
                id="operations-ignore-failed-stop",
                classes="operations-checkbox",
            ),
            Static("", id="operations-error", markup=False),
            Horizontal(
                OperationFormButton("Run", id="operations-run", variant="error"),
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
        self.call_after_refresh(self._focus_operation_form_item, 0)

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

    def _operation_form_focus_ids(self) -> list[str]:
        if self._operation_id == "install":
            return [
                "operations-waiting",
                "operations-bin-path",
                "operations-do-not-init",
                "operations-ignore-failed-stop",
                "operations-run",
            ]
        return [
            "operations-ignore-failed-stop",
            "operations-run",
        ]

    def _focus_operation_form_item(self, index: int) -> bool:
        focus_ids = self._operation_form_focus_ids()
        if not focus_ids:
            return False
        index = max(0, min(index, len(focus_ids) - 1))
        step = 1 if index < len(focus_ids) - 1 else -1
        while 0 <= index < len(focus_ids):
            try:
                widget = self.query_one("#" + focus_ids[index])
            except NoMatches:
                index += step
                continue
            if (
                getattr(widget, "visible", True)
                and getattr(widget, "display", True)
                and not getattr(widget, "disabled", False)
            ):
                self.screen.set_focus(widget)
                return True
            index += step
        return False

    def move_operation_form_focus_from(self, widget_id: Optional[str], step: int) -> bool:
        if self._operation_started() or step == 0:
            return False
        focus_ids = self._operation_form_focus_ids()
        if widget_id not in focus_ids:
            return False
        current_index = focus_ids.index(widget_id)
        target_index = current_index + step
        if target_index < 0 or target_index >= len(focus_ids):
            return True
        return self._focus_operation_form_item(target_index)

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
