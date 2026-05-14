import asyncio.subprocess
import subprocess
import logging

from ydb.tools.mnc.lib import progress


logger = logging.getLogger(__name__)
debug_shell = False


class Result:
    def __init__(self, returncode: int, stdout: str, stderr: str):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr

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


async def shell(cmd_line, stdout=None, stderr=None) -> Result:
    if isinstance(cmd_line, GroupOfShellCommands):
        cmd_line = cmd_line.to_cmd(0)
    stdout_pipe = stdout or asyncio.subprocess.PIPE
    stderr_pipe = stderr or asyncio.subprocess.PIPE
    proc = await asyncio.create_subprocess_shell(cmd_line, stdout=stdout_pipe, stderr=stderr_pipe)

    stdout, stderr = await proc.communicate()
    stdout, stderr = stdout.decode('utf-8'), stderr.decode('utf-8')
    return Result(proc.returncode, stdout, stderr)


async def async_shell(cmd_line, stdout=None, stderr=None) -> asyncio.subprocess.Process:
    if isinstance(cmd_line, GroupOfShellCommands):
        cmd_line = cmd_line.to_cmd(0)
    stdout_pipe = stdout or asyncio.subprocess.PIPE
    stderr_pipe = stderr or asyncio.subprocess.PIPE
    return await asyncio.create_subprocess_shell(cmd_line, stdout=stdout_pipe, stderr=stderr_pipe)


async def run(args: list[str], stdout=None, stderr=None) -> Result:
    stdout_pipe = stdout or asyncio.subprocess.PIPE
    stderr_pipe = stderr or asyncio.subprocess.PIPE
    proc = await asyncio.create_subprocess_exec(*args, stdout=stdout_pipe, stderr=stderr_pipe)
    stdout, stderr = await proc.communicate()
    stdout, stderr = stdout.decode('utf-8'), stderr.decode('utf-8')
    return Result(proc.returncode, stdout, stderr)


async def async_run(args: list[str], stdout=None, stderr=None) -> asyncio.subprocess.Process:
    stdout_pipe = stdout or asyncio.subprocess.PIPE
    stderr_pipe = stderr or asyncio.subprocess.PIPE
    return await asyncio.create_subprocess_exec(*args, stdout=stdout_pipe, stderr=stderr_pipe)


async def ssh_run(host: str, cmd: str, stdout=None, stderr=None) -> Result:
    if isinstance(cmd, GroupOfShellCommands):
        cmd = cmd.to_cmd(0)
    stdout_pipe = stdout or asyncio.subprocess.PIPE
    stderr_pipe = stderr or asyncio.subprocess.PIPE
    proc = await asyncio.create_subprocess_exec('ssh', '-A', host, cmd, stdout=stdout_pipe, stderr=stderr_pipe)
    stdout, stderr = await proc.communicate()
    stdout, stderr = stdout.decode('utf-8'), stderr.decode('utf-8')
    return Result(proc.returncode, stdout, stderr)


async def async_ssh_run(host: str, cmd: str, stdout=None, stderr=None) -> asyncio.subprocess.Process:
    if isinstance(cmd, GroupOfShellCommands):
        cmd = cmd.to_cmd(0)
    stdout_pipe = stdout or asyncio.subprocess.PIPE
    stderr_pipe = stderr or asyncio.subprocess.PIPE
    return await asyncio.create_subprocess_exec('ssh', '-A', host, cmd, stdout=stdout_pipe, stderr=stderr_pipe)


def sync_shell(cmd_line, stdout=None, stderr=None) -> Result:
    if isinstance(cmd_line, GroupOfShellCommands):
        cmd_line = cmd_line.to_cmd(0)
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
