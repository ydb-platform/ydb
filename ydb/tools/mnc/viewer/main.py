import asyncio
import io
import os
import shlex
import shutil
import subprocess
import tempfile
import textwrap
import traceback
from dataclasses import dataclass
from typing import Callable, Optional

import rich.console
import yaml

from textual.app import App, ComposeResult, ScreenStackError, SuspendNotSupported
from textual.binding import Binding
from textual.command import CommandPalette
from textual.css.query import NoMatches
from textual.events import Key
from textual.widget import Widget
from textual.widgets import Checkbox, Footer, Header, Input, ListView, TabbedContent, TabPane, Tabs

from ydb.tools.mnc.cli.commands import install as install_command
from ydb.tools.mnc.cli.commands import uninstall as uninstall_command
from ydb.tools.mnc.lib import agent_client, deploy_ctx, output as mnc_output
from ydb.tools.mnc.lib.progress_live import LiveBackend
from ydb.tools.mnc.scheme import common as scheme_common
from ydb.tools.mnc.viewer.commands import OperationsCommands, TabCommands, ViewerCommands
from ydb.tools.mnc.viewer.widgets import (
    AgentHostStatus,
    AgentsPane,
    AgentsState,
    ClusterConfigPane,
    ConfigCandidate,
    ConfigFieldItem,
    InvalidPathModal,
    MncConfigForm,
    OperationBacktraceFrame,
    OperationRequest,
    OperationState,
    OperationsPane,
    OverviewPane,
    OverviewAgentsCard,
    OverviewStatusCard,
    PathPickerScreen,
    ViewerState,
    MNC_DEPLOY_FLAG_OPTIONS,
    mnc_deploy_flag_checkbox_id,
    mnc_deploy_flag_option_from_checkbox_id,
)


MNC_CONFIG_PATH = os.path.join(os.environ.get("HOME", "/"), ".mnc", "mnc.yaml")
DEFAULT_GIT_YDB_ROOT = os.path.join(os.environ.get("HOME", "/"), "ydbwork", "ydb")
DEFAULT_EDITOR = "vi"
OPERATION_OUTPUT_LIMIT = 12000
DEPLOY_CONTEXT_DEFAULTS = {
    "deploy_path": "/Berkanavt",
    "binary_project": "ydb/apps/ydbd",
    "relative_binary_path": "ydb/apps/ydbd/ydbd",
    "transit_bin_through_first_node": False,
    "do_rebuild": True,
    "do_strip": True,
    "do_redeploy_bin": True,
    "affinity": None,
    "git_ydb_root": None,
    "source_root": None,
    "source_ya_path": None,
    "bin_filename": "ydbd",
    "path_to_bin": None,
    "is_manual_path_to_bin": False,
    "secure": False,
    "certs_local_dir": None,
}


@dataclass
class _OperationExecution:
    result: object
    output: str
    progress_backend: Optional[object] = None


class _ViewerOperationBackend(LiveBackend):
    def __init__(self, console: rich.console.Console, on_update: Callable[["_ViewerOperationBackend"], None]) -> None:
        super().__init__(console=console)
        self._on_update = on_update

    def add_task(self, description: str, total: Optional[float] = None, **kwargs):
        if kwargs.get("parent") is not None:
            description = str(description).lstrip()
        task_id = super().add_task(description, total=total, **kwargs)
        self._emit()
        return task_id

    def update(
        self,
        task_id,
        *,
        advance: Optional[float] = None,
        completed: Optional[float] = None,
        total: Optional[float] = None,
        visible: Optional[bool] = None,
        **kwargs,
    ) -> None:
        super().update(
            task_id,
            advance=advance,
            completed=completed,
            total=total,
            visible=visible,
            **kwargs,
        )
        self._emit()

    def append_log(self, line: str, step_id: Optional[str] = None) -> None:
        super().append_log(line, step_id=step_id)
        self._emit()

    def _emit(self) -> None:
        self._on_update(self)


class Viewer(App):
    COMMANDS = App.COMMANDS | {ViewerCommands}

    BINDINGS = [
        Binding("left_square_bracket,[", "previous_tab", "Previous tab", priority=True),
        Binding("right_square_bracket,]", "next_tab", "Next tab", priority=True),
        Binding("t", "open_tab_picker", "Tabs", priority=True),
        Binding("o", "open_operation_picker", "Operations", priority=True),
        Binding("ctrl+w", "close_tab", "Close tab", priority=True),
    ]

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._created_tabs: set[str] = set()
        self._available_tab_order = ["general", "mnc-config", "cluster-config", "agents"]
        self._opened_tab_order = ["general"]
        self._tab_titles = {
            "general": "Overview",
            "mnc-config": "Settings",
            "cluster-config": "Cluster",
            "agents": "Hosts",
        }
        self._tab_descriptions = {
            "general": "Overview viewer and cluster state",
            "mnc-config": "Read and edit settings",
            "cluster-config": "Select and inspect cluster",
            "agents": "Inspect selected cluster hosts",
        }
        self._editing_config_field: Optional[ConfigFieldItem] = None
        self._mnc_config = self._load_mnc_config()
        self._state = ViewerState(mnc_config_ok=self._mnc_config_ok())
        self._navigation_generation = 0
        self._agent_check_generation = 0
        self._operation_running = False

    def compose(self) -> ComposeResult:
        yield Header()

        with TabbedContent(initial="general", id="tabs"):
            with TabPane("Overview", id="general"):
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

    def _load_mnc_config(self) -> dict[str, object]:
        try:
            with open(MNC_CONFIG_PATH) as file:
                config = yaml.safe_load(file) or {}
        except (OSError, yaml.YAMLError):
            config = {}
        if not isinstance(config, dict):
            config = {}

        deploy_flags = config.get("deploy_flags") or []
        if not isinstance(deploy_flags, list):
            deploy_flags = []
        deploy_flags = [flag for flag in deploy_flags if flag in scheme_common.deploy_flags]
        return {
            "git_ydb_root": str(config.get("git_ydb_root", DEFAULT_GIT_YDB_ROOT)),
            "deploy_flags": deploy_flags,
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
            pass

        try:
            self.query_one(AgentsPane).refresh_state()
        except NoMatches:
            pass

        try:
            self.query_one(OperationsPane).refresh_state()
        except NoMatches:
            pass

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
        if not hosts:
            if generation != self._agent_check_generation:
                return
            self._state.agents = AgentsState(status="OK", hosts=[])
            self._refresh_overview()
            return

        host_statuses = await asyncio.gather(*(self._check_agent_host(host) for host in hosts))
        if generation != self._agent_check_generation:
            return

        status = "OK" if all(host.agent_is_running() for host in host_statuses) else "FAIL"
        self._state.agents = AgentsState(status=status, hosts=host_statuses)
        self._refresh_overview()

    async def _check_agent_host(self, host: str) -> AgentHostStatus:
        try:
            health = await agent_client.get_json(host, "/health")
        except Exception as error:
            return AgentHostStatus(
                host,
                "Not Installed",
                str(error),
                tasks_error="Agent is not available",
                disks_error="Agent is not available",
            )

        if health is None:
            return AgentHostStatus(
                host,
                "Not Installed",
                "Agent is not available",
                tasks_error="Agent is not available",
                disks_error="Agent is not available",
            )

        if health.get("status") != "healthy":
            return AgentHostStatus(
                host,
                "Stopped",
                str(health.get("status") or "Agent is not healthy"),
                tasks_error="Agent is not running",
                disks_error="Agent is not running",
            )

        tasks_response, disks_response = await asyncio.gather(
            agent_client.get_json(host, "/tasks"),
            agent_client.post_json(host, "/disks/info", {}),
        )
        last_tasks, tasks_error = self._last_tasks(tasks_response)
        disks, disks_error = self._agent_disks(disks_response)
        return AgentHostStatus(
            host,
            "Running",
            enabled_features=list(health.get("enabled_features", []) or []),
            last_tasks=last_tasks,
            tasks_error=tasks_error,
            disks=disks,
            disks_error=disks_error,
        )

    def _last_tasks(self, response: Optional[dict]) -> tuple[list[dict], str]:
        if response is None:
            return [], "Failed to load tasks"

        tasks = response.get("tasks")
        if not isinstance(tasks, list):
            return [], "Failed to load tasks"

        def sort_key(task: dict):
            return task.get("created_at") or 0

        return sorted(tasks, key=sort_key, reverse=True)[:5], ""

    def _agent_disks(self, response: Optional[dict]) -> tuple[list[dict], str]:
        if response is None:
            return [], "Failed to load disks"

        disks = response.get("disks")
        if not isinstance(disks, list):
            return [], "Failed to load disks"
        return disks, ""

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

    def operation_choices(self) -> list[tuple[str, str, str]]:
        return [
            ("install", "Install", "Install multinode cluster using selected config"),
            ("uninstall", "Uninstall", "Uninstall multinode cluster from selected hosts"),
        ]

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

    def action_open_operation_picker(self) -> None:
        if not CommandPalette.is_open(self):
            self.push_screen(
                CommandPalette(
                    providers={OperationsCommands},
                    placeholder="Select operation...",
                    id="--operation-palette",
                )
            )

    def action_open_operations(self) -> None:
        self.action_open_operation_picker()

    def _handle_operation_request(self, request: Optional[OperationRequest]) -> None:
        if request is None:
            return
        if self._operation_running:
            self._set_operation_state(
                OperationState(
                    status="RUNNING",
                    operation_id=self._state.operation.operation_id or request.operation_id,
                    message="Wait for the current operation to finish.",
                )
            )
            return
        self._operation_running = True
        title = self._operation_title(request.operation_id)
        self._set_operation_state(
            OperationState(
                status="RUNNING",
                operation_id=request.operation_id,
                message=f"{title} is running.",
            )
        )
        self.notify(f"{title} started", title="Operation")
        self.run_worker(
            self._run_operation(request),
            name=f"operation-{request.operation_id}",
            group="operations",
            exclusive=True,
            exit_on_error=False,
        )

    def _operation_title(self, operation_id: str) -> str:
        operation = next((choice for choice in self.operation_choices() if choice[0] == operation_id), None)
        return operation[1] if operation is not None else operation_id

    def _set_operation_state(self, state: OperationState) -> None:
        self._state.operation = state
        try:
            self.query_one(OperationsPane).refresh_state()
        except NoMatches:
            pass

    async def _run_operation(self, request: OperationRequest) -> None:
        title = self._operation_title(request.operation_id)
        try:
            execution = await self._execute_operation(request)
        except Exception as error:
            progress_backend = None
            if self._state.operation.operation_id == request.operation_id:
                progress_backend = self._state.operation.progress_backend
            self._set_operation_state(self._operation_exception_state(request.operation_id, error, progress_backend))
            self.notify(f"{title} failed", title="Operation", severity="error")
            return
        finally:
            self._operation_running = False

        result, output, progress_backend = self._operation_result(execution)
        ok = bool(result)
        result_renderable = self._operation_result_renderable(result)
        self._set_operation_state(
            OperationState(
                status="OK" if ok else "FAIL",
                operation_id=request.operation_id,
                message=self._operation_result_message(result, output, ok, result_renderable is not None),
                result_renderable=result_renderable,
                progress_backend=progress_backend,
            )
        )
        self.notify(
            f"{title} completed" if ok else f"{title} failed",
            title="Operation",
            severity="information" if ok else "error",
        )
        if not ok:
            return
        self._start_agents_check()

    async def _execute_operation(self, request: OperationRequest):
        selected = self._state.selected_cluster_config
        if selected is None or selected.validation.config is None:
            raise RuntimeError("Select a valid cluster first.")

        hosts = self._selected_cluster_hosts()
        config = selected.validation.config
        deploy_context_snapshot = self._deploy_context_snapshot()
        previous_backend = mnc_output.get_progress_backend_override()
        try:
            with tempfile.TemporaryDirectory() as work_directory:
                self._prepare_deploy_context(config, work_directory)
                output = io.StringIO()
                console = rich.console.Console(file=output, force_terminal=False, width=120)
                backend = _ViewerOperationBackend(
                    console,
                    lambda progress_backend: self._set_operation_state(
                        OperationState(
                            status="RUNNING",
                            operation_id=request.operation_id,
                            message=f"{self._operation_title(request.operation_id)} is running.",
                            progress_backend=progress_backend,
                        )
                    ),
                )
                mnc_output.set_progress_backend_override(backend)
                try:
                    if request.operation_id == "install":
                        result = await install_command.act(
                            hosts,
                            config,
                            waiting=request.waiting,
                            bin_path=request.bin_path,
                            do_not_init=request.do_not_init,
                            ignore_failed_stop=request.ignore_failed_stop,
                            console=console,
                        )
                    elif request.operation_id == "uninstall":
                        result = await uninstall_command.act(
                            hosts,
                            config,
                            ignore_failed_stop=request.ignore_failed_stop,
                            console=console,
                        )
                    else:
                        raise RuntimeError(f"Unknown operation: {request.operation_id}")
                finally:
                    mnc_output.set_progress_backend_override(previous_backend)
                return _OperationExecution(result=result, output=output.getvalue().strip(), progress_backend=backend)
        finally:
            self._restore_deploy_context(deploy_context_snapshot)
            mnc_output.set_progress_backend_override(previous_backend)

    def _prepare_deploy_context(self, config: dict, work_directory: str) -> None:
        for name, value in DEPLOY_CONTEXT_DEFAULTS.items():
            setattr(deploy_ctx, name, value)
        deploy_ctx.work_directory = work_directory
        deploy_ctx.apply_cfg_mnc(self._mnc_config)
        deploy_ctx.apply_cfg(self._operation_config(config), install_command.expected_config)
        if deploy_ctx.path_to_bin is None:
            raise RuntimeError("Failed to prepare binary path from MNC config.")

    def _operation_config(self, config: dict) -> dict:
        operation_config = dict(config)
        operation_config["deploy_flags"] = scheme_common.merge_deploy_flags(
            config.get("deploy_flags") or [],
            self._mnc_config.get("deploy_flags") or [],
        )
        return operation_config

    @staticmethod
    def _deploy_context_snapshot() -> dict[str, object]:
        names = set(DEPLOY_CONTEXT_DEFAULTS) | {"work_directory"}
        return {name: getattr(deploy_ctx, name) for name in names}

    @staticmethod
    def _restore_deploy_context(snapshot: dict[str, object]) -> None:
        for name, value in snapshot.items():
            setattr(deploy_ctx, name, value)

    def _operation_result(self, execution) -> tuple[object, str, Optional[object]]:
        if isinstance(execution, _OperationExecution):
            return execution.result, execution.output, execution.progress_backend
        return execution, "", None

    def _operation_result_renderable(self, result: object) -> Optional[object]:
        to_rich_panel = getattr(result, "to_rich_panel", None)
        if not callable(to_rich_panel):
            return None
        return to_rich_panel(show_all=True)

    def _operation_result_message(self, result: object, output: str, ok: bool, has_result_renderable: bool) -> str:
        if has_result_renderable:
            return ""
        parts = []
        result_text = "" if isinstance(result, bool) else str(result).strip()
        if result_text:
            parts.append(result_text)
        if output:
            parts.append(self._trim_operation_output(output))
        if parts:
            return "\n\n".join(parts)
        return "Operation completed." if ok else "Operation failed."

    def _operation_exception_state(
        self,
        operation_id: str,
        error: Exception,
        progress_backend: Optional[object] = None,
    ) -> OperationState:
        frames = []
        for frame in traceback.extract_tb(error.__traceback__):
            path = self._traceback_path(frame.filename)
            source = ""
            if frame.line:
                source = textwrap.shorten(frame.line.strip(), width=120, placeholder=" ...")
            frames.append(OperationBacktraceFrame(path, frame.lineno, frame.name, source))
        message = self._plain_operation_exception(error, frames)
        return OperationState(
            status="FAIL",
            operation_id=operation_id,
            message=message,
            error_type=error.__class__.__name__,
            error_message=str(error),
            backtrace=frames,
            progress_backend=progress_backend,
        )

    def _plain_operation_exception(self, error: Exception, frames: list[OperationBacktraceFrame]) -> str:
        lines = [
            f"{error.__class__.__name__}: {error}",
            "",
            "Backtrace:",
        ]
        for frame in frames:
            lines.append(f"  {frame.path}:{frame.line} in {frame.function}")
            if frame.source:
                lines.append(f"    {frame.source}")
        return self._trim_operation_output("\n".join(lines))

    @staticmethod
    def _traceback_path(filename: str) -> str:
        try:
            path = os.path.relpath(filename, os.getcwd())
        except ValueError:
            return filename
        if path.startswith(".." + os.sep):
            return filename
        return path

    @staticmethod
    def _trim_operation_output(output: str) -> str:
        if len(output) <= OPERATION_OUTPUT_LIMIT:
            return output
        return "... output truncated ...\n" + output[-OPERATION_OUTPUT_LIMIT:]

    def action_previous_tab(self) -> None:
        self._move_tab(-1)

    def action_next_tab(self) -> None:
        self._move_tab(1)

    async def _open_tab(self, tab_id: str, title: str, content: Widget) -> None:
        tabs = self.query_one("#tabs", TabbedContent)
        if tab_id in self._opened_tab_order:
            generation = self._bump_navigation_generation()
            tabs.active = tab_id
            self._focus_active_tab_content(tab_id)
            self._keep_active_tab(tab_id, generation)
            return

        generation = self._bump_navigation_generation()
        self._created_tabs.add(tab_id)
        self._opened_tab_order.append(tab_id)

        await tabs.add_pane(TabPane(title, content, id=tab_id))
        self._disable_tab_header_focus()
        tabs.active = tab_id
        self._focus_active_tab_content(tab_id)
        self._keep_active_tab(tab_id, generation)
        self.refresh_bindings()

    def _keep_active_tab(self, tab_id: str, generation: int) -> None:
        self._restore_active_tab(tab_id, generation)
        self.call_later(self._restore_active_tab, tab_id, generation)
        self.set_timer(0.01, lambda: self._restore_active_tab(tab_id, generation))

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

    async def action_open_agents(self) -> None:
        await self._open_tab(
            "agents",
            self._tab_titles["agents"],
            AgentsPane(self._state),
        )

    async def action_open_operation(self, operation_id: str) -> None:
        choices = {choice_id: (title, description) for choice_id, title, description in self.operation_choices()}
        if operation_id not in choices:
            self.notify(f"Unknown operation: {operation_id}", title="Operation", severity="error")
            return

        if self._operation_running and self._state.operation.operation_id:
            operation_id = self._state.operation.operation_id
        else:
            self._state.operation = OperationState(
                status="IDLE",
                operation_id=operation_id,
                message="No operation has been started.",
            )

        title, description = choices[operation_id]
        tab_id = "operation"
        self._tab_titles[tab_id] = title
        self._tab_descriptions[tab_id] = description

        if tab_id in self._opened_tab_order:
            tabs = self.query_one("#tabs", TabbedContent)
            self._created_tabs.discard(tab_id)
            self._opened_tab_order.remove(tab_id)
            await tabs.remove_pane(tab_id)

        await self._open_tab(
            tab_id,
            title,
            OperationsPane(self._state, self._handle_operation_request, operation_id, title),
        )

    async def on_list_view_selected(self, event: ListView.Selected) -> None:
        if isinstance(event.item, (OverviewStatusCard, OverviewAgentsCard)):
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
        try:
            self.query_one(MncConfigForm).focus_config_fields()
        except NoMatches:
            fields = self.query_one("#mnc-config-fields", ListView)
            fields.index = 0
            fields.focus()

    def on_checkbox_changed(self, event: Checkbox.Changed) -> None:
        deploy_flag_option = mnc_deploy_flag_option_from_checkbox_id(event.checkbox.id)
        if deploy_flag_option is None:
            return
        self._save_mnc_deploy_flags()

    def _save_mnc_deploy_flags(self) -> None:
        deploy_flags = []
        for option in MNC_DEPLOY_FLAG_OPTIONS:
            try:
                checkbox = self.query_one("#" + mnc_deploy_flag_checkbox_id(option.option_id), Checkbox)
            except NoMatches:
                continue
            if checkbox.value:
                deploy_flags.append(option.enabled_flag)
            elif option.disabled_flag is not None:
                deploy_flags.append(option.disabled_flag)
        self._mnc_config["deploy_flags"] = deploy_flags
        self._save_mnc_config()

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
