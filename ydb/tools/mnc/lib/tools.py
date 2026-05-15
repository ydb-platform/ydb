import asyncio
import asyncio.subprocess
import os.path
import logging
import os
import pty
import re
import rich.text
import rich.console


from ydb.tools.mnc.lib import deploy_ctx, progress, term


logger = logging.getLogger(__name__)


async def chain_async(*tasks):
    running_tasks = []
    try:
        for task in tasks:
            if asyncio.iscoroutine(task):
                running_task = asyncio.create_task(task)
                running_tasks.append(running_task)
                result = await running_task
            else:
                result = await task

            if not result:
                for remaining_task in running_tasks:
                    if not remaining_task.done():
                        remaining_task.cancel()
                return False
        return True
    except Exception:
        for task in running_tasks:
            if not task.done():
                task.cancel()
        raise


async def parallel_async(*tasks):
    if tasks:
        return all(map(bool, await asyncio.gather(*(task for task in tasks))))
    return True


def make_chain_step(*steps: progress.StepBase, title: str = "[bold cyan]chain[/]"):
    return progress.SequentialStepGroup(title=title, steps=list(steps))


def make_parallel_step(*steps: progress.StepBase, title: str = "[bold cyan]parallel[/]", inflight: int = 0):
    return progress.ParallelStepGroup(title=title, steps=list(steps), inflight=inflight)


def sed_command(src: str, dest: str, replace: dict):
    args = ' '.join((f"-e 's;{key};{value};g'") for key, value in replace.items())
    return f'sed {args} {src} > {dest}'


async def async_ya_make(root: str, project: str, build_args: list[str], stream=None):
    project_path = os.path.join(root, project)
    str_args = ' '.join(build_args)
    cmd = f"'{root}'/ya make {str_args} '{project_path}'"
    if stream is None:
        return await term.async_shell(cmd)
    else:
        return await term.async_shell(cmd, stdout=stream, stderr=asyncio.subprocess.STDOUT)


async def async_build_projects(project: str, build_args: list[str], stream=None):
    return await async_ya_make(deploy_ctx.source_root, project, build_args, stream)


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


def make_runtime_build_action(project: str, build_args: list[str], task: progress.TaskNode = None):
    return runtime_action(
        action=lambda stream: async_build_projects(project, build_args, stream=stream),
        task=task,
    )


def make_build_ydb_step(build_args):
    return progress.Step(
        title=f"[bold cyan]build[/] [yellow]{deploy_ctx.binary_project}[/]",
        command=lambda task, kv_storage: make_runtime_build_action(deploy_ctx.binary_project, build_args, task=task),
        task_args={"total": 100},
    )


def make_build_mnc_agent_step(build_args):
    project = 'ydb/tools/mnc/agent'
    return progress.Step(
        title=f"[bold cyan]build[/] [yellow]{project}[/]",
        command=lambda task, kv_storage: make_runtime_build_action(project, build_args, task=task),
        task_args={"total": 100},
    )


async def ask_cms_about_restart(
    node_id: int,
    build_args: list[str],
    availability_mode: str,
    grpc_endpoint: str = None,
):
    if deploy_ctx.do_rebuild:
        result = await async_build_projects(deploy_ctx.binary_project, build_args)
        result = await term.Result.from_async_process(result)
        if not result:
            return result

    path_to_ydb = os.path.join(deploy_ctx.source_root, deploy_ctx.relative_binary_path)
    additional_args = []
    if grpc_endpoint:
        additional_args = ['--server', grpc_endpoint]

    args = [
        path_to_ydb,
        *additional_args,
        'cms',
        'request',
        'restart',
        'host',
        str(node_id),
        '--user',
        'multinode_configure',
        '--duration',
        '60',
        '--dry',
        '--reason',
        'rolling-restart',
        '--availability-mode',
        availability_mode,
    ]
    return await term.run(args)


def make_ask_cms_about_restart_step(
    node_id: int,
    build_args: list[str],
    availability_mode: str,
    grpc_endpoint: str = None,
):
    async def command(task: progress.TaskNode, kv_storage):
        result = await ask_cms_about_restart(node_id, build_args, availability_mode, grpc_endpoint)
        await task.update(advance=1)
        if result:
            return True
        return progress.TaskResult(
            message=f"[red]Failed to request restart[/]\n\n{result.stderr}",
            level=progress.TaskResultLevel.ERROR,
        )

    return progress.Step(
        title=f"[bold cyan]cms restart request[/] [yellow]{node_id}[/]",
        command=command,
        task_args={"total": 1},
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


def make_strip_ydb_step():
    root = deploy_ctx.source_root
    original_bin_path = f"{root}/{deploy_ctx.relative_binary_path}"

    if (deploy_ctx.is_manual_path_to_bin):
        original_bin_path = deploy_ctx.path_to_bin

    stripped_bin_path = deploy_ctx.get_stripped_bin_path(original_bin_path)

    if deploy_ctx.is_stripped_bin_path(original_bin_path):
        return progress.SequentialStepGroup(
            title="[bold green]Stripping[/] [yellow]{deploy_ctx.binary_project}[/]",
            steps=[],
        )

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
