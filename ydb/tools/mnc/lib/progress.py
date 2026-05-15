from typing import Dict, List, Optional, Callable, Awaitable, Any, Coroutine
from rich.progress import Progress, TaskID
from functools import wraps
import asyncio
import enum
import rich
import rich.traceback
import rich.rule
import traceback
from abc import ABC, abstractmethod
from pathlib import Path


class TaskNode:
    def __init__(self, progress: 'MyProgress', task_id: TaskID, parent: Optional['TaskNode'] = None, add_level: int = 0, is_cutted: bool = False):
        self._progress = progress
        self.task_id: TaskID = task_id
        self.parent: Optional['TaskNode'] = parent
        self.children: List['TaskNode'] = []
        self.add_level: int = add_level
        self._is_cutted: bool = is_cutted

    def add_child(self, child: 'TaskNode') -> None:
        self.children.append(child)

    async def add_subtask(self, title: str, *args, **kwargs) -> 'TaskNode':
        return await self._progress.add_task(title, *args, **kwargs, parent=self)

    async def update(self, advance: Optional[float] = None, *, completed: Optional[float] = None, total: Optional[float] = None, visible: bool = None) -> None:
        async with self._progress._mutex:
            if advance is not None or visible is not None:
                self._progress._progress.update(self.task_id, advance=advance, visible=visible)
            else:
                kwargs = {}
                if completed is not None:
                    kwargs['completed'] = completed
                if total is not None:
                    kwargs['total'] = total
                if kwargs:
                    self._progress._progress.update(self.task_id, **kwargs)
            await self._progress._recalculate_upwards(self)

    def level(self) -> int:
        if self.parent:
            return self.parent.level() + 1 + self.add_level
        return self.add_level

    def is_cutted(self) -> bool:
        return self._is_cutted


class HiddenTaskNode:
    def __init__(self, progress: 'MyProgress', add_level: int = 0):
        self._progress = progress
        self._add_level = add_level

    def add_child(self, child: 'TaskNode') -> None:
        pass

    async def add_subtask(self, title: str, *args, **kwargs) -> 'TaskNode':
        return await self._progress.add_task(title, *args, add_level=self.level(), **kwargs)

    def level(self) -> int:
        return self._add_level

    async def update(self, *args, **kwargs):
        pass

    def is_cutted(self) -> bool:
        return True


class DummyTaskNode:
    def __init__(self, progress: 'MyProgress'):
        self._progress = progress

    def add_child(self, child: 'TaskNode') -> None:
        pass

    async def add_subtask(self, title: str, *args, **kwargs) -> 'TaskNode':
        return DummyTaskNode(self._progress)

    def level(self) -> int:
        return -1

    async def update(self, *args, **kwargs):
        pass

    def is_cutted(self) -> bool:
        return True


class MyProgress:
    def __init__(self, *args, **kwargs):
        if not args:
            args = (
                rich.progress.TextColumn("[progress.description]{task.description}"),
                rich.progress.SpinnerColumn(),
                rich.progress.BarColumn(),
                rich.progress.TaskProgressColumn(),
                rich.progress.TimeElapsedColumn(),
                rich.progress.TimeRemainingColumn(compact=True),
            )
        self._progress = Progress(*args, **kwargs)
        self._nodes: Dict[TaskID, TaskNode] = {}
        self._mutex = asyncio.Lock()

    def __enter__(self):
        # Enter underlying Rich progress context but return this wrapper
        self._progress.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return self._progress.__exit__(exc_type, exc_value, traceback)

    async def add_task(self, title: str, *args, is_hidden: bool = False, add_level: int = 0, is_cutted: bool = False, **kwargs) -> TaskNode:
        if is_hidden:
            return HiddenTaskNode(self, add_level)
        async with self._mutex:
            parent: Optional[TaskNode] = kwargs.pop('parent', None)
            prefix = ''
            parent_id = None
            if parent is not None:
                prefix = '  ' * (parent.level() + 1 + add_level)
                parent_id = parent.task_id
            else:
                prefix = '  ' * add_level
            task_id = self._progress.add_task(prefix + title, *args, **kwargs, parent=parent_id)
            node = TaskNode(self, task_id, parent, add_level=add_level, is_cutted=is_cutted)
            self._nodes[task_id] = node
            if parent is not None:
                parent.add_child(node)
                # Ensure parent totals reflect children from the start
                await self._recalculate_upwards(node)
            return node

    def console(self) -> rich.console.Console:
        return self._progress.console

    # Internal helpers
    async def _recalculate_upwards(self, node: TaskNode) -> None:
        current = node.parent
        while current is not None and not current.is_cutted():
            total_sum = 0.0
            completed_sum = 0.0
            for child in current.children:
                task = self._progress.tasks[child.task_id]
                # Rich uses None for indeterminate totals; treat None as 0 here
                child_total = float(task.total or 0)
                child_completed = float(task.completed or 0)
                total_sum += child_total
                completed_sum += child_completed
            # If parent had no explicit total and there are children, set it to sum
            self._progress.update(current.task_id, total=total_sum or None, completed=completed_sum)
            current = current.parent

    def get_hidden_task(self, add_level: int = 0) -> HiddenTaskNode:
        return HiddenTaskNode(self, add_level)

    def get_dummy_task(self) -> DummyTaskNode:
        return DummyTaskNode(self)


def with_parent_task(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        task = kwargs.pop('parent_task', None)
        if task is None:
            progress = kwargs.pop('progress', None)
            if progress is None:
                with MyProgress() as progress:
                    task = progress.get_hidden_task()
                    return await func(*args, **kwargs, parent_task=task)
            else:
                task = progress.get_hidden_task()
                return await func(*args, **kwargs, parent_task=task)
        else:
            return await func(*args, **kwargs, parent_task=task)
    return wrapper


class TaskResultLevel(enum.Enum):
    OK = 0
    SKIPPED = 1
    WARNING = 2
    ERROR = 3


class MyTraceback:
    def __init__(
        self,
        exception: Exception,
        *,
        search_roots: Optional[List[Path]] = None,
        context_lines: int = 3,
        show_locals: bool = False,
        width: int = 150,
        word_wrap: bool = False,
        extra_lines: int = 2,
        use_panel: bool = True
    ):
        self.exception = exception
        self.context_lines = max(0, context_lines)
        self.show_locals = show_locals
        self.width = width
        self.word_wrap = word_wrap
        self.extra_lines = extra_lines
        self.use_panel = use_panel
        # Common textual formats if needed elsewhere
        self.traceback = traceback.format_exception(
            type(exception), exception, exception.__traceback__
        )
        self.traceback_str = "".join(self.traceback)

        # Determine sensible search roots to resolve relative filenames
        default_roots: List[Path] = []
        try:
            default_roots.append(Path.cwd())
        except Exception:
            pass
        try:
            # Repository root (e.g. /home/user/ydb)
            repo_root = Path(__file__).resolve().parents[3]
            default_roots.append(repo_root)
        except Exception:
            pass
        self.search_roots: List[Path] = []
        if search_roots:
            self.search_roots.extend([Path(p) for p in search_roots])
        self.search_roots.extend([p for p in default_roots if p.exists()])

        # Precompute renderable for convenience
        self._renderable = self._build_renderable()

    def _resolve_filename(self, filename: str) -> Optional[Path]:
        path = Path(filename)
        if path.is_absolute() and path.exists():
            return path
        # Try as-is relative to each search root
        for root in self.search_roots:
            candidate = (root / filename).resolve()
            if candidate.exists():
                return candidate
        # Fallback: try matching by tail (file name) within roots (first match wins)
        try:
            name = path.name
            for root in self.search_roots:
                for cand in root.rglob(name):
                    # Prefer paths that end with the same relative suffix if possible
                    try:
                        if str(cand).endswith(str(path)):
                            return cand
                    except Exception:
                        pass
                    return cand
        except Exception:
            pass
        return None

    def _frame_renderable(self, filename: str, lineno: int, name: str, id: int) -> rich.console.RenderableType:
        resolved = self._resolve_filename(filename)
        header = rich.text.Text.assemble(
            (str(resolved or filename), "pygments.string"),
            (":", "pygments.text"),
            (str(lineno), "pygments.number"),
            " in ",
            (name, "pygments.function"),
            style="pygments.text",
        )
        header = f"  [bold white]{id}.[/] [yellow]{filename}:[medium_purple1]{lineno} [white]in [dark_cyan bold]{name}"
        return header

    def _build_renderable(self) -> rich.console.RenderableType:
        # Walk the traceback to preserve locals if needed and to get filenames/lines
        frames: List[tuple[str, int, str]] = []
        tb = self.exception.__traceback__
        while tb is not None:
            frame = tb.tb_frame
            code = frame.f_code
            frames.append((code.co_filename, tb.tb_lineno, code.co_name))
            tb = tb.tb_next

        items: List[rich.console.RenderableType] = []
        # Header similar to standard traceback
        for id, (filename, lineno, name) in enumerate(frames):
            items.append(self._frame_renderable(filename, lineno, name, id + 1))

        exc_line = rich.text.Text.assemble(
            (self.exception.__class__.__name__, "bold red"),
            (": ", "bold red"),
            (str(self.exception), "red"),
        )
        items.append(exc_line)

        if self.use_panel:
            return rich.panel.Panel(
                rich.console.Group(*items),
                title="[bold red]Traceback[/] (most recent call last)",
                border_style="red",
                expand=False,
                title_align="left",
            )
        else:
            return rich.console.Group(
                "",
                "[bold red]Traceback[/] [red](most recent call last):[/]",
                *items
            )

    @property
    def renderable(self) -> rich.console.RenderableType:
        return self._renderable

    # Allow printing the instance directly via rich Console
    def __rich_console__(self, console: "rich.console.Console", options: "rich.console.ConsoleOptions"):
        yield self._renderable


class TaskResult:
    def __init__(self, level: TaskResultLevel = None, step_title: str = None, message: str = None, exception: Exception = None, subresults: List['TaskResult'] = None):
        self.level = level
        self.step_title = step_title
        self.message = message
        self.exception = exception
        self.subresults = subresults
        if self.exception is not None and self.level is None:
            self.level = TaskResultLevel.ERROR
        self_ok = bool(self)
        if self_ok and self.level is None:
            self.level = TaskResultLevel.OK
        elif not self_ok and self.level is None:
            self.level = TaskResultLevel.ERROR
        elif self.level is None:
            self.level = TaskResultLevel.OK

    def __bool__(self):
        if self.level is not None:
            return self.level != TaskResultLevel.ERROR
        if self.exception is not None:
            return False
        return all(map(bool, self.subresults))

    def __str__(self):
        return self.to_string()

    def to_string(self) -> str:
        strs = []
        if self.level is not None:
            strs.append(f'level: {self.level.name}')
        if self.step_title is not None:
            strs.append(f'step_title: {self.step_title}')
        if self.message is not None:
            strs.append(f'message: {self.message}')
        if self.exception is not None:
            tb_str = "".join(traceback.format_exception(
                type(self.exception), self.exception, self.exception.__traceback__
            ))
            strs.append(f'exception: {self.exception.__class__.__name__}: {self.exception}\n{tb_str}')
        if self.subresults is not None:
            for subresult in self.subresults:
                if subresult is not None:
                    strs.append(subresult.to_string())
                else:
                    strs.append('MISSING SUBRESULT')
        return '\n'.join(strs)

    def to_rich_panel(self, show_all=False, title_prefix=None) -> rich.panel.Panel:
        color = 'dark_green'
        status = 'OK'
        if self.level == TaskResultLevel.OK:
            color = 'dark_green'
            status = 'DONE'
        elif self.level == TaskResultLevel.ERROR:
            color = 'dark_red'
            status = 'ERROR'
        elif self.level == TaskResultLevel.WARNING:
            color = 'gold3'
            status = 'WARN'
        elif self.level == TaskResultLevel.SKIPPED:
            color = 'grey62'
            status = 'SKIP'

        items = []
        if self.message is not None and isinstance(self.message, str):
            items.append(rich.text.Text(self.message, style=color))
        elif self.message is not None and isinstance(self.message, rich.console.RenderableType):
            items.append(self.message)
        if self.exception is not None:
            tb_rich = MyTraceback(self.exception, use_panel=False).renderable
            items.append(tb_rich)

        subresults_count = 0
        subresult_for_print = None
        if self.subresults is not None and len(self.subresults) > 0:
            for subresult in self.subresults:
                if subresult is not None:
                    if not show_all and subresult.level in [TaskResultLevel.OK, TaskResultLevel.SKIPPED]:
                        continue
                    subresult_for_print = subresult
                    subresults_count += 1
                    items.append(subresult.to_rich_panel(show_all=show_all))

        if subresults_count == 1:
            if title_prefix is not None:
                title_prefix = f"{title_prefix} [yellow bold]/[/] {self.step_title}"
            else:
                title_prefix = self.step_title
            return subresult_for_print.to_rich_panel(show_all=show_all, title_prefix=title_prefix)

        if items:
            group = rich.console.Group(*items)
            if title_prefix is not None:
                title = f"{title_prefix} [yellow bold]/[/] {self.step_title}"
            else:
                title = self.step_title
            return rich.panel.Panel(group, title=f"[{color}]{status}:[/] {title}", border_style=color, expand=False, title_align="left")
        else:
            if title_prefix is not None:
                title = f"{title_prefix} [yellow bold]/[/] {self.step_title}"
            else:
                title = self.step_title
            return f"[{color}]{status}:[/] {title}"


class KVStorage:
    def __init__(self):
        self.kv_storage = {}
        self.lock = asyncio.Lock()

    async def __aenter__(self):
        await self.lock.acquire()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.lock.release()

    def get_unsafe(self, key: str, default: Any = None) -> Any:
        return self.kv_storage.get(key, default)

    def set_unsafe(self, key: str, value: Any) -> None:
        self.kv_storage[key] = value

    async def get(self, key: str, default: Any = None) -> Any:
        async with self.lock:
            return self.get_unsafe(key, default)

    async def set(self, key: str, value: Any) -> None:
        async with self.lock:
            self.set_unsafe(key, value)


async def run_predicate(predicate: Callable[[], Awaitable[bool]] or Callable[[], bool]) -> bool:
    if isinstance(predicate, Coroutine):
        return await predicate()
    else:
        return predicate()


class StepBase(ABC):
    @abstractmethod
    async def run(self, parent_task: TaskNode, subtasks: List[TaskNode] = None, kv_storage: KVStorage = None) -> TaskResult:
        pass

    @abstractmethod
    def task(self) -> TaskNode:
        pass


class Step:
    def __init__(
        self,
        title: str,
        command: Callable[[TaskNode, Dict[str, Any]], Awaitable[bool]],
        predicate: Callable[[], Awaitable[bool]] = None,
        otherwise: "Step" = None,
        task_args: Dict[str, Any] = {},
    ):
        self.title = title
        self.command = command
        self.predicate = predicate
        self.otherwise = otherwise
        self._task = None
        self.task_args = task_args
        self.kv_storage = None

    async def run(self, parent_task: TaskNode, subtasks: List[TaskNode] = None, kv_storage: KVStorage = None) -> TaskResult:
        if kv_storage is None:
            kv_storage = KVStorage()
        self.kv_storage = kv_storage
        if self.predicate is None or await run_predicate(self.predicate):
            self._task = await parent_task.add_subtask(self.title, **self.task_args)
            if subtasks is not None:
                subtasks.append(self._task)
            try:
                result = await self.command(self._task, kv_storage)
            except Exception as e:
                return TaskResult(level=TaskResultLevel.ERROR, step_title=self.title, exception=e)
            if result is None:
                return TaskResult(level=TaskResultLevel.ERROR, step_title=self.title, message="INTERNAL ERROR: command returned None")
            if isinstance(result, TaskResult):
                result.step_title = self.title
                return result
            if isinstance(result, bool):
                return TaskResult(level=TaskResultLevel.OK if result else TaskResultLevel.ERROR, step_title=self.title)
            if isinstance(result, str):
                if result:
                    return TaskResult(level=TaskResultLevel.ERROR, step_title=self.title, message=result)
            if isinstance(result, tuple):
                if len(result) == 2:
                    level, message = result
                    if not isinstance(level, TaskResultLevel) or not isinstance(message, str):
                        return TaskResult(level=TaskResultLevel.ERROR, step_title=self.title, message="INTERNAL ERROR: incorrect result type")
                    return TaskResult(level=level, message=message)
                else:
                    return TaskResult(level=TaskResultLevel.ERROR, step_title=self.title, message="INTERNAL ERROR: incorrect result type")
            return TaskResult(level=TaskResultLevel.OK, step_title=self.title)
        elif self.otherwise is not None:
            return await self.otherwise.run(parent_task, subtasks, kv_storage)
        else:
            return TaskResult(level=TaskResultLevel.SKIPPED, step_title=self.title)

    def task(self) -> TaskNode:
        return self._task


class SimpleStep(Step):
    def __init__(self, title: str, action: Callable[[], Awaitable[bool]] = None, predicate: Callable[[], Awaitable[bool]] = None, otherwise: "Step" = None):
        if action is None and hasattr(self, 'action'):
            action = self.action

        async def command_from_action(parent_task: TaskNode, kv_storage: Dict[str, Any]):
            result = await action()
            if result:
                await parent_task.update(advance=1)
            return result

        if action is not None:
            super().__init__(title, command_from_action, predicate, otherwise, {"total": 1})
        elif hasattr(self, 'command'):
            super().__init__(title, self.command, predicate, otherwise, {"total": 1})
        else:
            raise ValueError("SimpleStep must have either action or command")


class KVStorageSimpleStep(Step):
    def __init__(self, title: str, kv_storage_command: Callable[[KVStorage], Awaitable[bool]], predicate: Callable[[], Awaitable[bool]] = None, otherwise: "Step" = None):
        async def command(parent_task: TaskNode, kv_storage: Dict[str, Any]):
            result = await kv_storage_command(kv_storage)
            if result:
                await parent_task.update(advance=1)
            return result

        super().__init__(title, command, predicate, otherwise, {"total": 1})


class StepGroup:
    def __init__(
        self,
        title: str,
        steps: List[Step],
        predicate: Callable[[], Awaitable[bool]] = None,
        inflight: int = 0,
        otherwise: "StepGroup" = None,
        is_sequential: bool = False,
        task_args: Dict[str, Any] = {},
        show_tasks: bool = False,
    ):
        self.title = title
        self.steps = steps
        self.predicate = predicate
        self.otherwise = otherwise
        self._task = None
        self.is_sequential = is_sequential
        self.inflight = inflight
        self.task_args = task_args
        self.kv_storage = None
        self.show_tasks = show_tasks

    def _make_wrap_step(self, semaphore: Optional[asyncio.Semaphore], inner_subtasks: List[TaskNode]):
        async def wrap_step(step: Step):
            if semaphore is not None:
                async with semaphore:
                    result = await step.run(self._task, inner_subtasks, self.kv_storage)
            else:
                result = await step.run(self._task, inner_subtasks, self.kv_storage)
            await self._task.update(advance=1)
            return result
        return wrap_step

    async def run(self, parent_task: TaskNode, subtasks: List[TaskNode] = None, kv_storage: KVStorage = None) -> TaskResult:
        if kv_storage is None:
            kv_storage = KVStorage()
        self.kv_storage = kv_storage
        if self.predicate is None or await run_predicate(self.predicate):
            self._task = await parent_task.add_subtask(self.title, total=len(self.steps), is_cutted=True, **self.task_args)
            if subtasks is not None:
                subtasks.append(self._task)
            inner_subtasks = []

            results = []

            if self.is_sequential:
                result = True
                for step in self.steps:
                    result = await step.run(self._task, inner_subtasks, self.kv_storage)
                    results.append(result)
                    await self._task.update(advance=1)
                    if not result:
                        return TaskResult(level=TaskResultLevel.ERROR, step_title=self.title, subresults=results)
            else:
                if self.inflight > 0:
                    semaphore = asyncio.Semaphore(self.inflight)
                else:
                    semaphore = None
                wrap_step = self._make_wrap_step(semaphore, inner_subtasks)
                results = await asyncio.gather(*[wrap_step(step) for step in self.steps], return_exceptions=True)
                if not all(map(bool, results)):
                    return TaskResult(level=TaskResultLevel.ERROR, step_title=self.title, subresults=results)

            if not self.show_tasks:
                for subtask in inner_subtasks:
                    await subtask.update(visible=False)
            return TaskResult(level=TaskResultLevel.OK, step_title=self.title, subresults=results)
        elif self.otherwise is not None:
            return await self.otherwise.run(parent_task, subtasks, kv_storage=self.kv_storage)
        else:
            return TaskResult(level=TaskResultLevel.SKIPPED, step_title=self.title)

    def task(self) -> TaskNode:
        return self._task


class SequentialStepGroup(StepGroup):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, is_sequential=True, **kwargs)


class ParallelStepGroup(StepGroup):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, is_sequential=False, **kwargs)


async def run_steps(steps: List[StepBase], progress: MyProgress = None, show_tasks: bool = False, title: str = None, **kwargs):
    title = title or "[bold]Result[/]"
    if progress is None:
        with MyProgress(**kwargs) as progress:
            task = progress.get_hidden_task()
            group = SequentialStepGroup(title=title, steps=steps, task_args={"is_hidden": True}, show_tasks=show_tasks)
            return await group.run(task)
    else:
        task = progress.get_hidden_task()
        group = SequentialStepGroup(title=title, steps=steps, task_args={"is_hidden": True}, show_tasks=show_tasks)
        return await group.run(task)
