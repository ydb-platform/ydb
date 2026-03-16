from __future__ import annotations

import asyncio
import inspect
import linecache
import sys
import traceback
from asyncio.coroutines import _format_coroutine  # type: ignore
from datetime import timedelta
from pathlib import Path
from types import FrameType
from typing import Any, List, Set

from .types import TerminatedTaskInfo


def _format_task(task: "asyncio.Task[Any]") -> str:
    """
    A simpler version of task's repr()
    """
    coro = _format_coroutine(task.get_coro()).partition(" ")[0]
    return f"<Task name={task.get_name()} coro={coro}>"


def _format_terminated_task(tinfo: TerminatedTaskInfo) -> str:
    """
    A simpler version of task's repr()
    """
    status = []
    if tinfo.cancelled:
        status.append("cancelled")
    if tinfo.exc_repr and not tinfo.cancelled:
        status.append(f"exc={tinfo.exc_repr}")
    return f"<TerminatedTask name={tinfo.name} coro={tinfo.coro} {' '.join(status)}>"


def _format_filename(filename: str) -> str:
    """
    Simplifies the site-pkg directory path of the given source filename.
    """
    stdlib = (
        f"{sys.prefix}/lib/python{sys.version_info.major}.{sys.version_info.minor}/"
    )
    site_pkg = f"{sys.prefix}/lib/python{sys.version_info.major}.{sys.version_info.minor}/site-packages/"
    home = f"{Path.home()}/"
    cwd = f"{Path.cwd()}/"
    if filename.startswith(site_pkg):
        return "<sitepkg>/" + filename[len(site_pkg) :]
    if filename.startswith(stdlib):
        return "<stdlib>/" + filename[len(stdlib) :]
    if filename.startswith(cwd):
        return "<cwd>/" + filename[len(cwd) :]
    if filename.startswith(home):
        return "<home>/" + filename[len(home) :]
    return filename


def _format_timedelta(td: timedelta) -> str:
    seconds = int(td.total_seconds())
    periods = [
        ("y", 60 * 60 * 24 * 365),
        ("m", 60 * 60 * 24 * 30),
        ("d", 60 * 60 * 24),
        ("h", 60 * 60),
        (":", 60),
        ("", 1),
    ]
    parts = []
    for period_name, period_seconds in periods:
        period_value, seconds = divmod(seconds, period_seconds)
        if period_name in (":", ""):
            parts.append(f"{period_value:02d}{period_name}")
        else:
            if period_value == 0:
                continue
            parts.append(f"{period_value}{period_name}")
    parts.append(f"{td.microseconds / 1e6:.03f}"[1:])
    return "".join(parts)


def _filter_stack(
    stack: List[traceback.FrameSummary],
) -> List[traceback.FrameSummary]:
    """
    Filters out commonly repeated frames of the asyncio internals from the given stack.
    """
    # strip the task factory frame in the vanilla event loop
    if (
        stack[-1].filename.endswith("asyncio/base_events.py")
        and stack[-1].name == "create_task"
    ):
        stack = stack[:-1]
    # strip the loop.create_task frame
    if (
        stack[-1].filename.endswith("asyncio/tasks.py")
        and stack[-1].name == "create_task"
    ):
        stack = stack[:-1]
    _cut_idx = 0
    for _cut_idx, f in reversed(list(enumerate(stack))):
        # uvloop
        if f.filename.endswith("asyncio/runners.py") and f.name == "run":
            break
        # vanilla
        if f.filename.endswith("asyncio/events.py") and f.name == "_run":
            break
    return stack[_cut_idx + 1 :]


def _extract_stack_from_task(
    task: "asyncio.Task[Any]",
) -> List[traceback.FrameSummary]:
    """
    Extracts the stack as a list of FrameSummary objects from an asyncio task.
    """
    frames: List[Any] = []
    coro = task._coro  # type: ignore
    while coro:
        f = getattr(coro, "cr_frame", getattr(coro, "gi_frame", None))
        if f is not None:
            frames.append(f)
        coro = getattr(coro, "cr_await", getattr(coro, "gi_yieldfrom", None))
    extracted_list = []
    checked: Set[str] = set()
    for f in frames:
        lineno = f.f_lineno
        co = f.f_code
        filename = co.co_filename
        name = co.co_name
        if filename not in checked:
            checked.add(filename)
            linecache.checkcache(filename)
        line = linecache.getline(filename, lineno, f.f_globals)
        extracted_list.append(traceback.FrameSummary(filename, lineno, name, line=line))
    return extracted_list


def _extract_stack_from_frame(frame: FrameType) -> List[traceback.FrameSummary]:
    stack = traceback.StackSummary.extract(traceback.walk_stack(frame))
    stack.reverse()
    return stack


def _extract_stack_from_exception(e: BaseException) -> List[traceback.FrameSummary]:
    stack = traceback.StackSummary.extract(traceback.walk_tb(e.__traceback__))
    stack.reverse()
    return stack


def get_default_args(func):
    signature = inspect.signature(func)
    return {
        k: v.default
        for k, v in signature.parameters.items()
        if v.default is not inspect.Parameter.empty
    }
