import asyncio.subprocess
import subprocess
import logging
from typing import Optional

from ydb.tools.mnc.lib import logs, progress


logger = logging.getLogger(__name__)
debug_shell = False


class Result:
    def __init__(self, returncode: int, stdout: str, stderr: str, log_path: Optional[str] = None):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.log_path = log_path

    def __bool__(self):
        return not self.returncode

    @staticmethod
    async def from_async_process(proc: asyncio.subprocess.Process):
        stdout, stderr = await proc.communicate()
        decoded_stdout, decoded_stderr = '', ''
        if stdout is not None:
            decoded_stdout = stdout.decode('utf-8', errors='ignore')
        if stderr is not None:
            decoded_stderr = stderr.decode('utf-8', errors='ignore')
        return Result(proc.returncode, decoded_stdout, decoded_stderr)


def join_commands(cmds, inner_level=0):
    new_cmds = []
    for cmd in cmds:
        if isinstance(cmd, str):
            if debug_shell:
                new_cmds.append(f'echo run {cmd}')
            new_cmds.append(cmd)
        else:
            new_cmds.append(cmd.to_cmd(inner_level + 1))
    return ' && '.join(new_cmds)


def _cmd_to_shell(cmd):
    if isinstance(cmd, GroupOfShellCommands):
        return cmd.to_cmd(0)
    return cmd


def _cmd_title(cmd):
    if isinstance(cmd, GroupOfShellCommands):
        return cmd._name
    return str(cmd)


def _result_to_task_result(result):
    if result:
        return True
    message = f"[red]Return code:[/] {result.returncode}"
    if result.stdout:
        message += f"\n\n[bold]stdout[/]\n{result.stdout}"
    if result.stderr:
        message += f"\n\n[bold]stderr[/]\n{result.stderr}"
    if result.log_path:
        message += f"\n\n[bold]Full log:[/] {result.log_path}"
    return progress.TaskResult(message=message, level=progress.TaskResultLevel.ERROR)


class GroupOfShellCommands:
    TAB = '@'

    def __init__(self, name):
        self._name = name
        self._commands = []

    def to_cmd(self, inner_level):
        if self._commands:
            cmds = join_commands(self._commands, inner_level=inner_level)
            return f'echo {GroupOfShellCommands.TAB * inner_level} {self._name} && {cmds}'
        else:
            return f'echo {GroupOfShellCommands.TAB * inner_level} {self._name}'


class ParallelledGroupOfShellCommands(GroupOfShellCommands):
    class Shard(GroupOfShellCommands):
        def __init__(self, name, idx, cmds):
            GroupOfShellCommands.__init__(self, f'shard {idx} of "{name}"')
            self._commands = cmds

    def __init__(self, name):
        GroupOfShellCommands.__init__(self, name)

    def subgroups(self, maximum_cmd_in_one_task):
        cmds_by_task = [[]]
        for cmd in self._commands:
            if len(cmds_by_task[-1]) < maximum_cmd_in_one_task:
                cmds_by_task[-1].append(cmd)
            else:
                cmds_by_task.append([cmd])
        return [ParallelledGroupOfShellCommands.Shard(self._name, idx, cmds) for idx, cmds in enumerate(cmds_by_task)]


def _write_log_output(step_title: str, step_id: Optional[str], stdout: str, stderr: str) -> Optional[str]:
    log_path, log_file = logs.open_log_file(step_title, step_id)
    if log_file is None:
        return None
    with log_file:
        if stdout:
            log_file.write(stdout)
            if not stdout.endswith("\n"):
                log_file.write("\n")
        if stderr:
            log_file.write(stderr)
            if not stderr.endswith("\n"):
                log_file.write("\n")
    return log_path


async def shell(cmd_line, stdout=None, stderr=None, step_title: str = None, step_id: Optional[str] = None) -> Result:
    cmd_line = _cmd_to_shell(cmd_line)
    stdout_pipe = stdout or asyncio.subprocess.PIPE
    stderr_pipe = stderr or asyncio.subprocess.PIPE
    proc = await asyncio.create_subprocess_shell(cmd_line, stdout=stdout_pipe, stderr=stderr_pipe)

    stdout, stderr = await proc.communicate()
    stdout, stderr = stdout.decode('utf-8'), stderr.decode('utf-8')
    log_path = None
    if step_title:
        log_path = _write_log_output(step_title, step_id, stdout, stderr)
    return Result(proc.returncode, stdout, stderr, log_path=log_path)


async def async_shell(cmd_line, stdout=None, stderr=None) -> asyncio.subprocess.Process:
    cmd_line = _cmd_to_shell(cmd_line)
    stdout_pipe = stdout or asyncio.subprocess.PIPE
    stderr_pipe = stderr or asyncio.subprocess.PIPE
    return await asyncio.create_subprocess_shell(cmd_line, stdout=stdout_pipe, stderr=stderr_pipe)


async def run(args: list[str], stdout=None, stderr=None, step_title: str = None, step_id: Optional[str] = None) -> Result:
    stdout_pipe = stdout or asyncio.subprocess.PIPE
    stderr_pipe = stderr or asyncio.subprocess.PIPE
    proc = await asyncio.create_subprocess_exec(*args, stdout=stdout_pipe, stderr=stderr_pipe)
    stdout, stderr = await proc.communicate()
    stdout, stderr = stdout.decode('utf-8'), stderr.decode('utf-8')
    log_path = None
    if step_title:
        log_path = _write_log_output(step_title, step_id, stdout, stderr)
    return Result(proc.returncode, stdout, stderr, log_path=log_path)


async def async_run(args: list[str], stdout=None, stderr=None) -> asyncio.subprocess.Process:
    stdout_pipe = stdout or asyncio.subprocess.PIPE
    stderr_pipe = stderr or asyncio.subprocess.PIPE
    return await asyncio.create_subprocess_exec(*args, stdout=stdout_pipe, stderr=stderr_pipe)


async def ssh_run(host: str, cmd: str, stdout=None, stderr=None, step_title: str = None, step_id: Optional[str] = None) -> Result:
    cmd = _cmd_to_shell(cmd)
    stdout_pipe = stdout or asyncio.subprocess.PIPE
    stderr_pipe = stderr or asyncio.subprocess.PIPE
    proc = await asyncio.create_subprocess_exec('ssh', '-A', host, cmd, stdout=stdout_pipe, stderr=stderr_pipe)
    stdout, stderr = await proc.communicate()
    stdout, stderr = stdout.decode('utf-8'), stderr.decode('utf-8')
    log_path = None
    if step_title:
        log_path = _write_log_output(step_title, step_id, stdout, stderr)
    return Result(proc.returncode, stdout, stderr, log_path=log_path)


async def async_ssh_run(host: str, cmd: str, stdout=None, stderr=None) -> asyncio.subprocess.Process:
    cmd = _cmd_to_shell(cmd)
    stdout_pipe = stdout or asyncio.subprocess.PIPE
    stderr_pipe = stderr or asyncio.subprocess.PIPE
    return await asyncio.create_subprocess_exec('ssh', '-A', host, cmd, stdout=stdout_pipe, stderr=stderr_pipe)


def sync_shell(cmd_line, stdout=None, stderr=None) -> Result:
    cmd_line = _cmd_to_shell(cmd_line)
    stdout_pipe = stdout or subprocess.PIPE
    stderr_pipe = stderr or subprocess.PIPE
    proc = subprocess.run(cmd_line, shell=True, stdout=stdout_pipe, stderr=stderr_pipe)
    stdout, stderr = '', ''
    if proc.stdout is not None:
        stdout = proc.stdout.decode('utf-8')
    if proc.stderr is not None:
        stderr = proc.stderr.decode('utf-8')
    return Result(proc.returncode, stdout, stderr)


async def chain_ssh_cmd(host, *cmds, **kwargs) -> Result:
    return await ssh_run(host, join_commands(cmds), **kwargs)


async def chain_shell(*cmds, **kwargs) -> Result:
    return await shell(join_commands(cmds), **kwargs)


async def parallel_shell(*cmds, task: progress.TaskNode = None):
    async def wrapper(cmd):
        result = await shell(cmd)
        if task is not None:
            await task.update(advance=1)
        return result

    return all(await asyncio.gather(*(wrapper(cmd) for cmd in cmds)))


def make_shell_step(cmd, title: str = None):
    async def command(task: progress.TaskNode, kv_storage):
        result = await shell(cmd, step_title=title or f"[bold cyan]shell[/] {_cmd_title(cmd)}", step_id=getattr(task, 'step_id', None))
        await task.update(advance=1)
        return _result_to_task_result(result)

    return progress.Step(
        title=title or f"[bold cyan]shell[/] {_cmd_title(cmd)}",
        command=command,
        task_args={"total": 1},
    )


def make_chain_shell_step(*cmds, title: str = None):
    async def command(task: progress.TaskNode, kv_storage):
        result = await chain_shell(*cmds, step_title=title or "[bold cyan]shell chain[/]", step_id=getattr(task, 'step_id', None))
        await task.update(advance=1)
        return _result_to_task_result(result)

    return progress.Step(
        title=title or "[bold cyan]shell chain[/]",
        command=command,
        task_args={"total": 1},
    )


def make_parallel_shell_step(*cmds, title: str = None, inflight: int = 0):
    return progress.ParallelStepGroup(
        title=title or "[bold cyan]parallel shell[/]",
        steps=[make_shell_step(cmd) for cmd in cmds],
        inflight=inflight,
    )


def make_ssh_step(host: str, cmd, title: str = None):
    async def command(task: progress.TaskNode, kv_storage):
        result = await ssh_run(host, cmd, step_title=title or f"[yellow]{host}[/] [bold cyan]ssh[/] {_cmd_title(cmd)}", step_id=getattr(task, 'step_id', None))
        await task.update(advance=1)
        return _result_to_task_result(result)

    return progress.Step(
        title=title or f"[yellow]{host}[/] [bold cyan]ssh[/] {_cmd_title(cmd)}",
        command=command,
        task_args={"total": 1},
    )


def make_chain_ssh_step(host: str, *cmds, title: str = None):
    async def command(task: progress.TaskNode, kv_storage):
        result = await chain_ssh_cmd(host, *cmds, step_title=title or f"[yellow]{host}[/] [bold cyan]ssh chain[/]", step_id=getattr(task, 'step_id', None))
        await task.update(advance=1)
        return _result_to_task_result(result)

    return progress.Step(
        title=title or f"[yellow]{host}[/] [bold cyan]ssh chain[/]",
        command=command,
        task_args={"total": 1},
    )
