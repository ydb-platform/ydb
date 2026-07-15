import asyncio

from textual.app import App
from textual.containers import Horizontal, Vertical
from textual.css.query import NoMatches
from textual.widgets import Static

from ydb.tools.mnc.lib import output, progress
from ydb.tools.mnc.lib.exceptions import CliError, ConfigError
from ydb.tools.mnc.lib.progress_live import LiveBackend


class RuntimeProgressApp(App):
    CSS = """
    Screen { layout: vertical; background: transparent; }
    #title { height: 3; padding: 1 2; text-style: bold; color: $accent; }
    #body { layout: horizontal; height: 1fr; padding: 0 1 1 1; }
    #left { width: 2fr; height: 1fr; border: solid $primary; }
    #right { width: 3fr; height: 1fr; padding: 1 2; border: solid $primary; margin-left: 1; }
    #steps { height: 1fr; }
    #details { height: auto; margin-bottom: 1; }
    #logs { height: 1fr; }
    """
    BINDINGS = [
        ("up,k", "cursor_up", "Up"),
        ("down,j", "cursor_down", "Down"),
        ("enter,space", "toggle_selected", "Collapse"),
    ]

    def __init__(self, backend, action, progress_context, result_from_exception, refresh_per_second: int):
        super().__init__()
        self.backend = backend
        self.action = action
        self.progress_context = progress_context
        self.result_from_exception = result_from_exception
        self.refresh_per_second = refresh_per_second
        self.error = None

    def compose(self):
        yield Static("Run command", id="title")
        with Horizontal(id="body"):
            with Vertical(id="left"):
                yield Static(id="steps")
            with Vertical(id="right"):
                yield Static(id="details")
                yield Static(id="logs")

    def on_mount(self):
        self._refresh()
        self.set_interval(1 / max(1, self.refresh_per_second), self._refresh)
        self._action_task = asyncio.create_task(self._run_action())

    def action_cursor_up(self):
        self.backend.move_selection(-1)
        self._refresh()

    def action_cursor_down(self):
        self.backend.move_selection(1)
        self._refresh()

    def action_toggle_selected(self):
        self.backend.toggle_selected()
        self._refresh()

    async def _run_action(self):
        try:
            result = await self.action(self.progress_context)
            self.backend.result = result
        except asyncio.CancelledError:
            return
        except Exception as exc:
            self.error = exc
            self.backend.result = self.result_from_exception(exc)
        self._refresh()
        self.exit(self.backend.result)

    def _refresh(self):
        try:
            self.query_one("#steps", Static).update(self.backend.render_tree_body())
            self.query_one("#details", Static).update(self.backend.render_details_body())
            self.query_one("#logs", Static).update(self.backend.render_logs_body())
        except NoMatches:
            return


class TuiApp:
    def __init__(self, console=None, refresh_per_second: int = 4):
        self.console = console or output.get_console()
        self.backend = LiveBackend(console=self.console)
        self.refresh_per_second = refresh_per_second

    def _result_from_exception(self, error: Exception) -> progress.TaskResult:
        if isinstance(error, CliError) and isinstance(error.result, progress.TaskResult):
            return error.result
        if isinstance(error, (CliError, ConfigError)):
            return progress.TaskResult(level=progress.TaskResultLevel.ERROR, step_title="Command", message=str(error))
        return progress.TaskResult(level=progress.TaskResultLevel.ERROR, step_title="Command", exception=error)

    async def run_async(self, action):
        output.set_progress_backend_override(self.backend)
        runtime_app = None
        try:
            progress_context = progress.MyProgress(backend=self.backend)
            progress_context.__enter__()
            try:
                runtime_app = RuntimeProgressApp(
                    self.backend,
                    action,
                    progress_context,
                    self._result_from_exception,
                    self.refresh_per_second,
                )
                await runtime_app.run_async()
            finally:
                progress_context.__exit__(None, None, None)
        finally:
            output.set_progress_backend_override(None)
        if isinstance(self.backend.result, progress.TaskResult):
            self.console.print(self.backend.result.to_rich_panel())
        if runtime_app is not None and runtime_app.error is not None:
            setattr(runtime_app.error, '_mnc_tui_reported', True)
            raise runtime_app.error
        return self.backend.result
