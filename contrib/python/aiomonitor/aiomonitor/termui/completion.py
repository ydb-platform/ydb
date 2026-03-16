from __future__ import annotations

import asyncio
import signal
from typing import TYPE_CHECKING, Iterable, List

import click
from click.parser import split_arg_string
from click.shell_completion import CompletionItem, _resolve_context, _resolve_incomplete
from prompt_toolkit.completion import CompleteEvent, Completer, Completion
from prompt_toolkit.document import Document

from ..context import current_monitor

if TYPE_CHECKING:
    from ..monitor import Monitor


class ClickCompleter(Completer):
    def __init__(self, root_command: click.BaseCommand) -> None:
        self._root_command = root_command

    def get_completions(
        self,
        document: Document,
        complete_event: CompleteEvent,
    ) -> Iterable[Completion]:
        args = split_arg_string(document.current_line)
        incomplete = document.get_word_under_cursor()
        if incomplete and args and args[-1] == incomplete:
            args.pop()
        click_ctx = _resolve_context(self._root_command, {}, "", args)
        cmd_or_param, incomplete = _resolve_incomplete(click_ctx, args, incomplete)
        completions: List[CompletionItem] = cmd_or_param.shell_complete(
            click_ctx, incomplete
        )
        start_position = document.find_backwards(incomplete) or 0
        for completion in completions:
            yield Completion(
                completion.value,
                start_position=start_position,
            )


def complete_task_id(
    ctx: click.Context,
    param: click.Parameter,
    incomplete: str,
) -> Iterable[str]:
    # ctx here is created in the completer and does not have ctx.obj set as monitor.
    # We take the monitor instance from the global context variable instead.
    try:
        self: Monitor = current_monitor.get()
    except LookupError:
        return []
    return [
        task_id
        for task_id in map(
            str, sorted(map(id, asyncio.all_tasks(loop=self._monitored_loop)))
        )
        if task_id.startswith(incomplete)
    ][:10]


def complete_trace_id(
    ctx: click.Context,
    param: click.Parameter,
    incomplete: str,
) -> Iterable[str]:
    try:
        self: Monitor = current_monitor.get()
    except LookupError:
        return []
    return [
        trace_id
        for trace_id in map(str, sorted(self._terminated_tasks.keys()))
        if trace_id.startswith(incomplete)
    ][:10]


def complete_signal_names(
    ctx: click.Context,
    param: click.Parameter,
    incomplete: str,
) -> Iterable[str]:
    return [sig.name for sig in signal.Signals if sig.name.startswith(incomplete)]
