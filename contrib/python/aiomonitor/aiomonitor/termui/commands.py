from __future__ import annotations

import asyncio
import functools
import logging
import os
import shlex
import signal
import sys
import textwrap
import traceback
from contextvars import copy_context
from typing import TYPE_CHECKING, List, TextIO, Tuple

import click
from prompt_toolkit import PromptSession
from prompt_toolkit.application.current import get_app_session
from prompt_toolkit.contrib.telnet.server import TelnetConnection
from prompt_toolkit.formatted_text import FormattedText
from prompt_toolkit.shortcuts import print_formatted_text
from terminaltables import AsciiTable

from .. import console
from ..context import command_done, current_monitor, current_stdout
from ..exceptions import MissingTask
from .completion import (
    ClickCompleter,
    complete_signal_names,
    complete_task_id,
    complete_trace_id,
)

if TYPE_CHECKING:
    from ..monitor import Monitor

log = logging.getLogger(__name__)

__all__ = (
    "interact",
    "monitor_cli",
    "auto_command_done",
    "auto_async_command_done",
    "custom_help_option",
)


def _get_current_stdout() -> TextIO:
    stdout = current_stdout.get(None)
    if stdout is None:
        return sys.stdout
    else:
        return stdout


def _get_current_stderr() -> TextIO:
    stdout = current_stdout.get(None)
    if stdout is None:
        return sys.stderr
    else:
        return stdout


click.utils._default_text_stdout = _get_current_stdout
click.utils._default_text_stderr = _get_current_stderr


def print_ok(msg: str) -> None:
    print_formatted_text(
        FormattedText([
            ("ansibrightgreen", "✓ "),
            ("", msg),
        ])
    )


def print_fail(msg: str) -> None:
    print_formatted_text(
        FormattedText([
            ("ansibrightred", "✗ "),
            ("", msg),
        ])
    )


async def interact(self: Monitor, connection: TelnetConnection) -> None:
    """
    The interactive loop for each telnet client connection.
    """
    await asyncio.sleep(0.3)  # wait until telnet negotiation is done
    tasknum = len(asyncio.all_tasks(loop=self._monitored_loop))  # TODO: refactor
    s = "" if tasknum == 1 else "s"
    intro = (
        f"\nAsyncio Monitor: {tasknum} task{s} running\n"
        f"Type help for available commands\n"
    )
    print(intro, file=connection.stdout)

    # Override the Click's stdout/stderr reference cache functions
    # to let them use the correct stdout handler.
    current_monitor_token = current_monitor.set(self)
    current_stdout_token = current_stdout.set(connection.stdout)
    # NOTE: prompt_toolkit's all internal console output automatically uses
    #       an internal contextvar to keep the stdout consistent with the
    #       current telnet connection.
    prompt_session: PromptSession[str] = PromptSession(
        completer=ClickCompleter(monitor_cli),
        complete_while_typing=False,
    )
    lastcmd = "noop"
    style_prompt = "#5fd7ff bold"
    try:
        while True:
            try:
                user_input = (
                    await prompt_session.prompt_async(
                        FormattedText([
                            (style_prompt, self.prompt),
                        ])
                    )
                ).strip()
            except KeyboardInterrupt:
                print_fail("To terminate, press Ctrl+D or type 'exit'.")
            except (EOFError, asyncio.CancelledError):
                return
            except Exception:
                print_fail(traceback.format_exc())
            else:
                command_done_event = asyncio.Event()
                command_done_token = command_done.set(command_done_event)
                try:
                    if not user_input and lastcmd is not None:
                        user_input = lastcmd
                    args = shlex.split(user_input)
                    term_size = prompt_session.output.get_size()
                    ctx = copy_context()
                    ctx.run(
                        monitor_cli.main,
                        args,
                        prog_name="",
                        obj=self,
                        standalone_mode=False,  # type: ignore
                        max_content_width=term_size.columns,
                    )
                    await command_done_event.wait()
                    if args[0] == "console":
                        lastcmd = "noop"
                    else:
                        lastcmd = user_input
                except (click.BadParameter, click.UsageError) as e:
                    print_fail(str(e))
                except asyncio.CancelledError:
                    return
                except Exception:
                    print_fail(traceback.format_exc())
                finally:
                    command_done.reset(command_done_token)
    finally:
        current_stdout.reset(current_stdout_token)
        current_monitor.reset(current_monitor_token)


class AliasGroupMixin(click.Group):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._commands = {}
        self._aliases = {}

    def command(self, *args, **kwargs):
        aliases = kwargs.pop("aliases", [])
        decorator = super().command(*args, **kwargs)

        def _decorator(f):
            cmd = decorator(click.pass_context(f))
            if aliases:
                self._commands[cmd.name] = aliases
                for alias in aliases:
                    self._aliases[alias] = cmd.name
            return cmd

        return _decorator

    def group(self, *args, **kwargs):
        aliases = kwargs.pop("aliases", [])
        # keep the same class type
        kwargs["cls"] = type(self)
        decorator = super().group(*args, **kwargs)
        if not aliases:
            return decorator

        def _decorator(f):
            cmd = decorator(f)
            if aliases:
                self._commands[cmd.name] = aliases
                for alias in aliases:
                    self._aliases[alias] = cmd.name
            return cmd

        return _decorator

    def get_command(self, ctx, cmd_name):
        if cmd_name in self._aliases:
            cmd_name = self._aliases[cmd_name]
        command = super().get_command(ctx, cmd_name)
        if command:
            return command

    def format_commands(self, ctx, formatter):
        commands = []
        for subcommand in self.list_commands(ctx):
            cmd = self.get_command(ctx, subcommand)
            # What is this, the tool lied about a command. Ignore it
            if cmd is None:
                continue
            if cmd.hidden:
                continue
            if subcommand in self._commands:
                aliases = ",".join(sorted(self._commands[subcommand]))
                subcommand = "{0} ({1})".format(subcommand, aliases)
            commands.append((subcommand, cmd))

        # allow for 3 times the default spacing
        if len(commands):
            limit = formatter.width - 6 - max(len(cmd[0]) for cmd in commands)
            rows = []
            for subcommand, cmd in commands:
                help = cmd.get_short_help_str(limit)
                rows.append((subcommand, help))
            if rows:
                with formatter.section("Commands"):
                    formatter.write_dl(rows)


@click.group(cls=AliasGroupMixin, add_help_option=False)
def monitor_cli():
    """
    To see the usage of each command, run them with "--help" option.
    """
    pass


def auto_command_done(cmdfunc):
    @functools.wraps(cmdfunc)
    def _inner(ctx: click.Context, *args, **kwargs):
        command_done_event = command_done.get()
        try:
            return cmdfunc(ctx, *args, **kwargs)
        finally:
            command_done_event.set()

    return _inner


def auto_async_command_done(cmdfunc):
    @functools.wraps(cmdfunc)
    async def _inner(ctx: click.Context, *args, **kwargs):
        command_done_event = command_done.get()
        command_done_event.clear()
        try:
            return await cmdfunc(ctx, *args, **kwargs)
        finally:
            command_done_event.set()

    return _inner


def custom_help_option(cmdfunc):
    """
    A custom help option to ensure setting `command_done_event`.
    """

    @auto_command_done
    def show_help(ctx: click.Context, param: click.Parameter, value: bool) -> None:
        if not value:
            return
        click.echo(ctx.get_help(), color=ctx.color)
        ctx.exit()

    return click.option(
        "--help",
        is_flag=True,
        expose_value=False,
        is_eager=True,
        callback=show_help,
        help="Show the help message",
    )(cmdfunc)


@monitor_cli.command(name="noop", hidden=True)
@auto_command_done
def do_noop(ctx: click.Context) -> None:
    pass


@monitor_cli.command(name="help", aliases=["?", "h"])
@custom_help_option
@auto_command_done
def do_help(ctx: click.Context) -> None:
    """Show the list of commands"""
    click.echo(monitor_cli.get_help(ctx))


@monitor_cli.command(name="signal")
@click.argument("signame", type=str, shell_complete=complete_signal_names)
@custom_help_option
@auto_command_done
def do_signal(ctx: click.Context, signame: str) -> None:
    """Send a Unix signal"""
    if hasattr(signal, signame):
        os.kill(os.getpid(), getattr(signal, signame))
        print_ok(f"Sent signal to {signame} PID {os.getpid()}")
    else:
        print_fail(f"Unknown signal {signame}")


@monitor_cli.command(name="stacktrace", aliases=["st", "stack"])
@custom_help_option
@auto_command_done
def do_stacktrace(ctx: click.Context) -> None:
    """Print a stack trace from the event loop thread"""
    self: Monitor = ctx.obj
    stdout = _get_current_stdout()
    tid = self._event_loop_thread_id
    assert tid is not None
    frame = sys._current_frames()[tid]
    traceback.print_stack(frame, file=stdout)


@monitor_cli.command(name="cancel", aliases=["ca"])
@click.argument("taskid", shell_complete=complete_task_id)
@custom_help_option
def do_cancel(ctx: click.Context, taskid: str) -> None:
    """Cancel an indicated task"""
    self: Monitor = ctx.obj

    @auto_async_command_done
    async def _do_cancel(ctx: click.Context) -> None:
        try:
            await self.cancel_monitored_task(taskid)
            print_ok(f"Cancelled task {taskid}")
        except ValueError as e:
            print_fail(repr(e))

    task = self._ui_loop.create_task(_do_cancel(ctx))
    self._termui_tasks.add(task)


@monitor_cli.command(name="exit", aliases=["q", "quit"])
@custom_help_option
@auto_command_done
def do_exit(ctx: click.Context) -> None:
    """Leave the monitor client session"""
    raise asyncio.CancelledError("exit by user")


@monitor_cli.command(name="console")
@custom_help_option
def do_console(ctx: click.Context) -> None:
    """Switch to async Python REPL"""
    self: Monitor = ctx.obj
    if not self._console_enabled:
        print_fail("Python console is disabled for this session!")
        return

    @auto_async_command_done
    async def _console(ctx: click.Context) -> None:
        log.info("Starting aioconsole at %s:%d", self._host, self._console_port)
        app_session = get_app_session()
        server = await console.start(
            self._host,
            self._console_port,
            self.console_locals,
            self._monitored_loop,
        )
        try:
            await console.proxy(
                app_session.input,
                app_session.output,
                self._host,
                self._console_port,
            )
        except asyncio.CancelledError:
            raise
        finally:
            await console.close(server, self._monitored_loop)
            log.info("Terminated aioconsole at %s:%d", self._host, self._console_port)
            print_ok("The console session is closed.")

    # Since we are already inside the UI's event loop,
    # spawn the async command function as a new task and let it
    # set `command_done_event` internally.
    task = self._ui_loop.create_task(_console(ctx))
    self._termui_tasks.add(task)


@monitor_cli.command(name="ps", aliases=["p"])
@click.option("-f", "--filter", "filter_", help="filter by coroutine or task name")
@click.option("-p", "--persistent", is_flag=True, help="show only persistent tasks")
@custom_help_option
@auto_command_done
def do_ps(
    ctx: click.Context,
    filter_: str,
    persistent: bool,
) -> None:
    """Show task table"""
    headers = (
        "Task ID",
        "State",
        "Name",
        "Coroutine",
        "Created Location",
        "Since",
    )
    self: Monitor = ctx.obj
    stdout = _get_current_stdout()
    table_data: List[Tuple[str, str, str, str, str, str]] = [headers]
    tasks = self.format_running_task_list(filter_, persistent)
    for task in tasks:
        table_data.append((
            task.task_id,
            task.state,
            task.name,
            task.coro,
            task.created_location,
            task.since,
        ))
    table = AsciiTable(table_data)
    table.inner_row_border = False
    table.inner_column_border = False
    if filter_ or persistent:
        stdout.write(
            f"{len(tasks)} tasks running (showing {len(table_data) - 1} tasks)\n"
        )
    else:
        stdout.write(f"{len(tasks)} tasks running\n")
    stdout.write(table.table)
    stdout.write("\n")
    stdout.flush()


@monitor_cli.command(name="ps-terminated", aliases=["pt", "pst"])
@click.option("-f", "--filter", "filter_", help="filter by coroutine or task name")
@click.option("-p", "--persistent", is_flag=True, help="show only persistent tasks")
@custom_help_option
@auto_command_done
def do_ps_terminated(
    ctx: click.Context,
    filter_: str,
    persistent: bool,
) -> None:
    """List recently terminated/cancelled tasks"""
    headers = (
        "Trace ID",
        "Name",
        "Coro",
        "Since Started",
        "Since Terminated",
    )
    self: Monitor = ctx.obj
    stdout = _get_current_stdout()
    table_data: List[Tuple[str, str, str, str, str]] = [headers]
    tasks = self.format_terminated_task_list(filter_, persistent)
    for task in tasks:
        table_data.append((
            task.task_id,
            task.name,
            task.coro,
            task.started_since,
            task.terminated_since,
        ))
    table = AsciiTable(table_data)
    table.inner_row_border = False
    table.inner_column_border = False
    if filter_ or persistent:
        stdout.write(
            f"{len(tasks)} tasks terminated (showing {len(table_data) - 1} tasks)\n"
        )
    else:
        stdout.write(f"{len(tasks)} tasks terminated (old ones may be stripped)\n")
    stdout.write(table.table)
    stdout.write("\n")
    stdout.flush()


@monitor_cli.command(name="where", aliases=["w"])
@click.argument("taskid", shell_complete=complete_task_id)
@custom_help_option
@auto_command_done
def do_where(ctx: click.Context, taskid: str) -> None:
    """Show stack frames and the task creation chain of a task"""
    self: Monitor = ctx.obj
    stdout = _get_current_stdout()
    try:
        formatted_stack_list = self.format_running_task_stack(taskid)
    except MissingTask as e:
        print_fail(f"No task {e.task_id}")
        return
    for item_type, item_text in formatted_stack_list:
        if item_type == "header":
            stdout.write("\n")
            print_formatted_text(
                FormattedText([
                    ("ansiwhite", item_text),
                ])
            )
        else:
            stdout.write(textwrap.indent(item_text.strip("\n"), "  "))
            stdout.write("\n")


@monitor_cli.command(name="where-terminated", aliases=["wt"])
@click.argument("trace_id", shell_complete=complete_trace_id)
@custom_help_option
@auto_command_done
def do_where_terminated(ctx: click.Context, trace_id: str) -> None:
    """Show stack frames and the termination/cancellation chain of a task"""
    self: Monitor = ctx.obj
    stdout = _get_current_stdout()
    try:
        formatted_stack_list = self.format_terminated_task_stack(trace_id)
    except MissingTask as e:
        print_fail(f"No task {e.task_id}")
        return
    for item_type, item_text in formatted_stack_list:
        if item_type == "header":
            stdout.write("\n")
            print_formatted_text(
                FormattedText([
                    ("ansiwhite", item_text),
                ])
            )
        else:
            stdout.write(textwrap.indent(item_text.strip("\n"), "  "))
            stdout.write("\n")
