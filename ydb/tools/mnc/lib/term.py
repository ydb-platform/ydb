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


async def shell(cmd_line, silent_error=False, stdout=False, stderr=False):
    if isinstance(cmd_line, GroupOfShellCommands):
        cmd_line = cmd_line.to_cmd(0)
    logger.debug(f'start shell run; cmd: "{cmd_line}"')
    stdout_pipe = asyncio.subprocess.PIPE if not stdout else asyncio.subprocess.STDOUT
    stderr_pipe = asyncio.subprocess.PIPE if not stderr else asyncio.subprocess.STDOUT
    proc = await asyncio.create_subprocess_shell(cmd_line, stdout=stdout_pipe, stderr=stderr_pipe)

    stdout, stderr = await proc.communicate()
    stdout, stderr = stdout.decode('utf-8'), stderr.decode('utf-8')
    if not silent_error and proc.returncode:
        logger.error(
            f'cmd shell failed; cmd: "{cmd_line}" returncode: {proc.returncode}\noutput:\n"""\n{stdout}\n"""\nerror:\n"""\n{stderr}\n"""'
        )
    else:
        logger.debug(f'cmd run successfully ended: cmd: "{cmd_line}" output:\n"""{stdout}"""')
    return Result(proc.returncode, stdout, stderr)


async def run(args: list[str], silent_error=False, stdout=False, stderr=False):
    logger.debug(f'start cmd run; args: {args}')

    stdout_pipe = asyncio.subprocess.PIPE if not stdout else asyncio.subprocess.STDOUT
    stderr_pipe = asyncio.subprocess.PIPE if not stderr else asyncio.subprocess.STDOUT
    proc = await asyncio.create_subprocess_exec(*args, stdout=stdout_pipe, stderr=stderr_pipe)
    stdout, stderr = await proc.communicate()
    stdout, stderr = stdout.decode('utf-8'), stderr.decode('utf-8')
    if not silent_error and proc.returncode:
        logger.error(
            f'cmd run failed; args: {args} returncode: {proc.returncode}\noutput:\n"""\n{stdout}\n"""\nerror:\n"""\n{stderr}\n"""'
        )
    else:
        logger.debug(f'cmd run successfully ended: args: {args} output:\n"""{stdout}"""')
    return Result(proc.returncode, stdout, stderr)


async def ssh_run(host: str, cmd: str, silent_error=False, stdout=False, stderr=False):
    if isinstance(cmd, GroupOfShellCommands):
        cmd = cmd.to_cmd(0)
    logger.debug(f'start ssh run; host: {host} cmd: "{cmd}"')

    stdout_pipe = asyncio.subprocess.PIPE if not stdout else asyncio.subprocess.STDOUT
    stderr_pipe = asyncio.subprocess.PIPE if not stderr else asyncio.subprocess.STDOUT
    proc = await asyncio.create_subprocess_exec('ssh', '-A', host, cmd, stdout=stdout_pipe, stderr=stderr_pipe)
    stdout, stderr = await proc.communicate()
    stdout, stderr = stdout.decode('utf-8'), stderr.decode('utf-8')
    if not silent_error and proc.returncode:
        logger.error(
            f'ssh run failed; host: {host} cmd: "{cmd}" returncode: {proc.returncode}\noutput:\n"""{stdout}"""\nerror:\n"""{stderr}"""'
        )
    else:
        logger.debug(
            f'ssh run successfully ended: host {host} cmd: "{cmd}" output:\n"""{stdout}"""\nerror:\n"""{stderr}"""'
        )
    return Result(proc.returncode, stdout, stderr)


def sync_shell(cmd_line, silent_error=False, stdout=False, stderr=False):
    if isinstance(cmd_line, GroupOfShellCommands):
        cmd_line = cmd_line.to_cmd(0)
    logger.debug(f'start sync_shell run; cmd: "{cmd_line}"')
    stdout_pipe = subprocess.PIPE if not stdout else None
    stderr_pipe = subprocess.PIPE if not stderr else None
    proc = subprocess.run(cmd_line, shell=True, stdout=stdout_pipe, stderr=stderr_pipe)
    stdout, stderr = '', ''
    if proc.stdout is not None:
        stdout = proc.stdout.decode('utf-8')
    if proc.stderr is not None:
        stderr = proc.stderr.decode('utf-8')
    if not silent_error and proc.returncode:
        logger.error(
            f'cmd shell failed; cmd: "{cmd_line}" returncode: {proc.returncode}\noutput:\n"""\n{stdout}\n"""\nerror:\n"""\n{stderr}\n"""'
        )
    else:
        logger.debug(f'cmd run successfully ended: cmd: "{cmd_line}" output:\n"""{stdout}"""')
    return Result(proc.returncode, stdout, stderr)


async def chain_ssh_cmd(host, *cmds):
    return bool(await ssh_run(host, join_commands(cmds)))


async def chain_shell(*cmds):
    return bool(await shell(join_commands(cmds)))


async def parallel_shell(*cmds, task: progress.TaskNode = None):
    async def wrapper(cmd):
        result = await shell(cmd)
        if task is not None:
            await task.update(advance=1)
        return result

    return all(await asyncio.gather(*(wrapper(cmd) for cmd in cmds)))
