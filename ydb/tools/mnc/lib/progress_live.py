import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import rich.box
import rich.console
import rich.errors
import rich.layout
import rich.markup
import rich.panel
import rich.table
import rich.text
from rich.progress import TaskID

from ydb.tools.mnc.lib.progress import ProgressBackend


@dataclass
class LiveTaskState:
    task_id: TaskID
    title: str
    total: Optional[float] = None
    completed: float = 0.0
    parent: Optional[TaskID] = None
    children: List[TaskID] = field(default_factory=list)
    visible: bool = True
    started_at: float = field(default_factory=time.monotonic)
    finished: bool = False

    def progress_text(self) -> str:
        if self.total in (None, 0):
            return ""
        percent = min(100.0, 100.0 * float(self.completed or 0) / float(self.total))
        return f" {percent:5.1f}%"

    def status_icon(self) -> str:
        if self.total not in (None, 0) and self.completed >= self.total:
            return "✔"
        return "⠋"


class LiveBackend(ProgressBackend):
    """Rich Live compatible progress backend.

    The backend keeps a renderable state model. It can be used in tests without
    entering a real Live context and by `TuiApp` for runtime monitoring.
    """

    def __init__(self, console: rich.console.Console = None, max_log_lines: int = 500):
        self._console = console or rich.console.Console()
        self._tasks: Dict[TaskID, LiveTaskState] = {}
        self._root_tasks: List[TaskID] = []
        self._next_id = 0
        self._selected_index = 0
        self._collapsed = set()
        self._logs: List[tuple[Optional[str], str]] = []
        self._max_log_lines = max_log_lines
        self.result = None

    def add_task(self, description: str, total: Optional[float] = None, **kwargs) -> TaskID:
        task_id = self._next_id
        self._next_id += 1
        parent = kwargs.get("parent")
        visible = kwargs.get("visible", True) is not False
        state = LiveTaskState(task_id=task_id, title=description, total=total, parent=parent, visible=visible)
        self._tasks[task_id] = state
        if parent is not None and parent in self._tasks:
            self._tasks[parent].children.append(task_id)
        else:
            self._root_tasks.append(task_id)
        return task_id

    def update(self, task_id: TaskID, *, advance: Optional[float] = None,
               completed: Optional[float] = None, total: Optional[float] = None,
               visible: Optional[bool] = None, **kwargs) -> None:
        task = self._tasks.get(task_id)
        if task is None:
            return
        if total is not None:
            task.total = total
        if advance is not None:
            task.completed += advance
        if completed is not None:
            task.completed = completed
        if visible is not None:
            task.visible = visible
        if task.total not in (None, 0) and task.completed >= task.total:
            task.finished = True

    def get_task_total(self, task_id: TaskID) -> Optional[float]:
        task = self._tasks.get(task_id)
        return None if task is None else task.total

    def get_task_completed(self, task_id: TaskID) -> float:
        task = self._tasks.get(task_id)
        return 0.0 if task is None else float(task.completed or 0)

    def console(self) -> rich.console.Console:
        return self._console

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb) -> None:
        pass

    def append_log(self, line: str, step_id: Optional[str] = None) -> None:
        if not line:
            return
        for item in str(line).splitlines():
            self._logs.append((step_id, item))
        if len(self._logs) > self._max_log_lines:
            self._logs = self._logs[-self._max_log_lines:]

    def visible_rows(self) -> List[tuple[int, LiveTaskState]]:
        rows: List[tuple[int, LiveTaskState]] = []

        def visit(task_id: TaskID, level: int) -> None:
            task = self._tasks.get(task_id)
            if task is None or not task.visible:
                return
            rows.append((level, task))
            if task_id in self._collapsed:
                return
            for child_id in task.children:
                visit(child_id, level + 1)

        for root_id in self._root_tasks:
            visit(root_id, 0)
        if self._selected_index >= len(rows):
            self._selected_index = max(0, len(rows) - 1)
        return rows

    def selected_task(self) -> Optional[LiveTaskState]:
        rows = self.visible_rows()
        if not rows:
            return None
        return rows[self._selected_index][1]

    def move_selection(self, delta: int) -> None:
        rows = self.visible_rows()
        if not rows:
            self._selected_index = 0
            return
        self._selected_index = max(0, min(len(rows) - 1, self._selected_index + delta))

    def toggle_selected(self) -> None:
        task = self.selected_task()
        if task is None or not task.children:
            return
        if task.task_id in self._collapsed:
            self._collapsed.remove(task.task_id)
        else:
            self._collapsed.add(task.task_id)

    def _task_title_text(self, title: str) -> rich.text.Text:
        try:
            return rich.text.Text.from_markup(title)
        except rich.errors.MarkupError:
            return rich.text.Text(str(title))

    def render_tree_body(self):
        table = rich.table.Table.grid(expand=True)
        table.add_column(ratio=1)
        rows = self.visible_rows()
        if not rows:
            table.add_row(rich.text.Text("No tasks yet", style="dim"))
            return table
        for idx, (level, task) in enumerate(rows):
            style = "reverse" if idx == self._selected_index else ""
            marker = "▸" if task.task_id in self._collapsed else "▾" if task.children else " "
            row = rich.text.Text(f"{'  ' * level}{marker} {task.status_icon()} ", style=style)
            row.append_text(self._task_title_text(task.title))
            row.append(task.progress_text(), style=style)
            table.add_row(row)
        return table

    def render_tree(self):
        return rich.panel.Panel(self.render_tree_body(), title="Steps", border_style="cyan")

    def render_details_body(self):
        task = self.selected_task()
        if task is None:
            return rich.text.Text("No selected task", style="dim")
        elapsed = time.monotonic() - task.started_at
        text = rich.text.Text()
        text.append("Task id: ", style="bold")
        text.append(f"{task.task_id}\n")
        text.append("Title: ", style="bold")
        text.append_text(self._task_title_text(task.title))
        text.append("\nCompleted: ", style="bold")
        text.append(f"{task.completed}\n")
        text.append("Total: ", style="bold")
        text.append(f"{task.total}\n")
        text.append("Elapsed: ", style="bold")
        text.append(f"{elapsed:.1f}s")
        return text

    def render_details(self):
        return rich.panel.Panel(self.render_details_body(), title="Details", border_style="magenta")

    def log_window_width(self) -> int:
        # The logs pane is the right column. Keep this in sync with render() ratios:
        # tree:right = 1:2, so logs get roughly two thirds of terminal width.
        width = getattr(self._console.size, "width", 80) or 80
        return max(20, int(width * 2 / 3) - 4)

    def log_prefix_width(self, step_id: Optional[str] = None) -> int:
        return len(step_id[-8:] + " ") if step_id else 0

    def log_payload_width(self, step_id: Optional[str] = None) -> int:
        return max(1, self.log_window_width() - self.log_prefix_width(step_id))

    def render_logs_body(self):
        if not self._logs:
            return rich.text.Text("No logs yet", style="dim")
        text = rich.text.Text()
        for idx, (step_id, line) in enumerate(self._logs[-80:]):
            if idx:
                text.append("\n")
            if step_id:
                text.append(step_id[-8:] + " ", style="dim")
            text.append_text(rich.text.Text.from_ansi(str(line)))
        return text

    def render_logs(self):
        return rich.panel.Panel(self.render_logs_body(), title="Logs", border_style="green")

    def render(self):
        layout = rich.layout.Layout(name="root")
        layout.split_row(
            rich.layout.Layout(name="tree", ratio=1),
            rich.layout.Layout(name="right", ratio=2),
        )
        layout["right"].split_column(
            rich.layout.Layout(name="details", ratio=1),
            rich.layout.Layout(name="logs", ratio=2),
        )
        layout["tree"].update(self.render_tree())
        layout["details"].update(self.render_details())
        layout["logs"].update(self.render_logs())
        return layout
