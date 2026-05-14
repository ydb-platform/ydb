import asyncio
import asyncio.subprocess
import os.path
import logging
import os
import pty
import re
import rich.text
import rich.console


from ydb.tools.mnc.lib import deploy_ctx, progress
from . import term


logger = logging.getLogger(__name__)


async def async_ya_make(root: str, project: str, build_args: list[str], stream=None):
    project_path = os.path.join(root, project)
    str_args = ' '.join(build_args)
    cmd = f"'{root}'/ya make {str_args} '{project_path}'"
    if stream is None:
        return await term.async_shell(cmd)
    else:
        return await term.async_shell(cmd, stdout=stream, stderr=asyncio.subprocess.STDOUT)


async def async_build_projects(project: str, build_args: list[str], use_arcadia=False, stream=None):
    root = deploy_ctx.source_root
    if use_arcadia:
        root = deploy_ctx.arcadia_root
    return await async_ya_make(root, project, build_args, stream)


ya_make_skip_words = [
    'Configuring dependencies for platform',
    'Configuring dependencies for platform tools',
    'modules rendered',
    'modules configured',
    'ymakes processing',
]


async def runtime_action(action, task: progress.TaskNode = None):
    master_fd, slave_fd = pty.openpty()

    proc = None
    fl = 0.0
    try:
        proc = await action(stream=slave_fd)
        os.close(slave_fd)
        saved_output = []
        while True:
            data = await asyncio.to_thread(os.read, master_fd, 4096)
            if not data:
                break
            data = data.decode(errors="ignore")
            match = re.search(r'[0-9]+(?:\.[0-9]+)?%', data)
            if match:
                fl = float(match.group(0).replace('%', ''))
                await task.update(completed=min(fl, 99.0))
            else:
                skip = False
                for word in ya_make_skip_words:
                    if word in data:
                        skip = True
                        break
                if not skip:
                    saved_output.append(rich.text.Text.from_ansi(data))
    except OSError:
        pass
    finally:
        os.close(master_fd)
        if proc is not None and proc.returncode is not None:
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
    result = await term.Result.from_async_process(proc)
    if result:
        await task.update(completed=100)
        return True
    else:
        return progress.TaskResult(message=rich.console.Group(*saved_output, f"[red]Return code: {result.returncode}[/]"), level=progress.TaskResultLevel.ERROR)


def make_runtime_build_action(project: str, build_args: list[str], use_arcadia: bool, task: progress.TaskNode = None):
    return runtime_action(
        action=lambda stream: async_build_projects(project, build_args, stream=stream, use_arcadia=use_arcadia),
        task=task,
    )


def make_build_kikimr_step(build_args):
    return progress.Step(
        title=f"[bold cyan]build[/] [yellow]{deploy_ctx.binary_project}[/]",
        command=lambda task, kv_storage: make_runtime_build_action(deploy_ctx.binary_project, build_args, use_arcadia=False, task=task),
        task_args={"total": 100},
    )


def make_build_mnc_agent_step(build_args):
    project = 'ydb/tools/mnc/agent'
    return progress.Step(
        title=f"[bold cyan]build[/] [yellow]{project}[/]",
        command=lambda task, kv_storage: make_runtime_build_action(project, build_args, use_arcadia=True, task=task),
        task_args={"total": 100},
    )


async def rm_previous_stripped_bin(bin_path, task: progress.TaskNode = None):
    if not os.path.exists(bin_path):
        return True
    proc = await term.async_shell(f'rm -f {bin_path}')
    res = await term.Result.from_async_process(proc)
    if not res:
        return progress.TaskResult(message=f"[red]Failed to remove[/] [yellow]{bin_path}[/]\n\n{res.stderr}", level=progress.TaskResultLevel.ERROR)
    await task.update(completed=1)
    return True


def make_rm_previous_stripped_bin_step(bin_path):
    return progress.Step(
        title=f"[bold cyan]rm[/] [yellow]{bin_path}[/]",
        command=lambda task, kv_storage: rm_previous_stripped_bin(bin_path, task),
        task_args={"total": 1},
    )


async def cp_to_strip_action(original_bin_path, stripped_bin_path, task: progress.TaskNode = None):
    proc = await term.async_shell(
        f'cp --dereference {original_bin_path} {stripped_bin_path}'
    )
    res = await term.Result.from_async_process(proc)
    if not res:
        return progress.TaskResult(message=f"[red]Failed to copy[/] [yellow]{original_bin_path}[/] [yellow]{stripped_bin_path}[/]\n\n{res.stderr}", level=progress.TaskResultLevel.ERROR)
    await task.update(completed=1)
    return True


def make_cp_to_strip_step(original_bin_path, stripped_bin_path):
    return progress.Step(
        title=f"[bold cyan]cp[/] [yellow]{original_bin_path}[/] [yellow]{stripped_bin_path}[/]",
        command=lambda task, kv_storage: cp_to_strip_action(original_bin_path, stripped_bin_path, task),
        task_args={"total": 1},
    )


async def strip_action(stripped_bin_path, task: progress.TaskNode = None):
    proc = await term.async_shell(f'strip {stripped_bin_path}')
    res = await term.Result.from_async_process(proc)
    if not res:
        return progress.TaskResult(message=f"[red]Failed to strip[/] [yellow]{stripped_bin_path}[/]\n\n{res.stderr}", level=progress.TaskResultLevel.ERROR)
    await task.update(completed=1)
    return True


def make_strip_step(stripped_bin_path):
    return progress.Step(
        title=f"[bold cyan]strip[/] [yellow]{stripped_bin_path}[/]",
        command=lambda task, kv_storage: strip_action(stripped_bin_path, task),
        task_args={"total": 0},
    )


def make_strip_kikimr_step():
    root = deploy_ctx.source_root
    original_bin_path = f"{root}/{deploy_ctx.relative_binary_path}"
    stripped_bin_path = f"{original_bin_path}_stripped"

    if (deploy_ctx.is_manual_path_to_bin):
        original_bin_path = deploy_ctx.path_to_bin
        stripped_bin_path = f"{original_bin_path}_stripped"

    return progress.SequentialStepGroup(
        title="[bold green]Stripping[/] [yellow]{deploy_ctx.binary_project}[/]",
        steps=[
            make_rm_previous_stripped_bin_step(stripped_bin_path),
            make_cp_to_strip_step(original_bin_path, stripped_bin_path),
            make_strip_step(stripped_bin_path),
        ],
    )


async def async_rsync(source_local_path, destination_host, destination_path, stream=None):
    return await term.async_shell(f'rsync -L --progress {source_local_path} {destination_host}:{destination_path}', stdout=stream, stderr=asyncio.subprocess.STDOUT)


def make_runtime_rsync_action(source_local_path, destination_host, destination_path, task: progress.TaskNode = None):
    return runtime_action(
        action=lambda stream: async_rsync(source_local_path, destination_host, destination_path, stream=stream),
        task=task,
    )


def make_rsync_step(source_local_path, destination_host, destination_path):
    name = os.path.basename(source_local_path)
    return progress.Step(
        title=f"[bold cyan]rsync[/] [green]{name}[/] to [yellow]{destination_host}[/]",
        command=lambda task, kv_storage: make_runtime_rsync_action(source_local_path, destination_host, destination_path, task),
        task_args={"total": 100},
    )


async def async_remote_rsync(source_host, source_local_path, destination_host, destination_path, stream=None):
    return await term.async_ssh_run(source_host, f"rsync -L --progress '{source_local_path}' '{destination_host}:{destination_path}'", stdout=stream, stderr=asyncio.subprocess.STDOUT)


def make_runtime_remote_rsync_action(source_host, source_local_path, destination_host, destination_path, task: progress.TaskNode = None):
    return runtime_action(
        action=lambda stream: async_remote_rsync(source_host, source_local_path, destination_host, destination_path, stream=stream),
        task=task,
    )


def make_remote_rsync_step(source_host, source_local_path, destination_hosts, destination_path):
    name = os.path.basename(source_local_path)
    return progress.ParallelStepGroup(
        title=f'[bold cyan]remote rsync[/] [green]{name}[/] from [yellow]{source_host}[/]',
        steps=[
            progress.Step(
                title=f"[bold cyan]rsync[/] [green]{name}[/] to [yellow]{destination_host}[/]",
                command=lambda task, kv_storage: make_runtime_remote_rsync_action(source_host, source_local_path, destination_host, destination_path, task),
                task_args={"total": 100},
            )
            for destination_host in destination_hosts
        ],
    )
