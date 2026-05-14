import asyncio
import os.path
import functools
import itertools
import logging
import os


from . import term, deploy_ctx


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
                print("chain_async False on task", task.__name__)
                return False
        return True
    except Exception:
        # Cancel all running tasks on any exception
        for task in running_tasks:
            if not task.done():
                task.cancel()
        raise


async def parallel_async(*tasks):
    if tasks:
        return all(map(bool, await asyncio.gather(*(task for task in tasks))))
    return True


def oncerun(f):
    already_runned = False
    save_result = None

    @functools.wraps(f)
    def _f(*args, **kwargs):
        nonlocal already_runned, save_result
        if already_runned:
            return save_result
        else:
            save_result = f(*args, **kwargs)
            already_runned = True
            return save_result

    return _f


def ya_make(root: str, project: str, build_args: list[str]):
    project_path = os.path.join(root, project)
    str_args = ' '.join(build_args)
    cmd = f"'{root}'/ya make {str_args} '{project_path}'"
    proc = term.sync_shell(cmd, stdout=True, stderr=True)
    return not proc.returncode


def build_projects(project: str, build_args: list[str], use_arcadia=False):
    root = deploy_ctx.source_root
    if use_arcadia:
        root = deploy_ctx.arcadia_root
    return ya_make(root, project, build_args)


@oncerun
def build_kikimr(build_args):
    if deploy_ctx.is_manual_path_to_bin:
        return True
    return build_projects(deploy_ctx.binary_project, build_args)


@oncerun
def strip_kikimr():
    root = deploy_ctx.source_root
    original_bin_path = f"{root}/{deploy_ctx.relative_binary_path}"
    stripped_bin_path = f"{root}/{deploy_ctx.binary_project}/stripped_{deploy_ctx.bin_kind}"

    if (deploy_ctx.is_manual_path_to_bin):
        original_bin_path = deploy_ctx.path_to_bin
        stripped_bin_path = f"{original_bin_path}_stripped"

    res = term.sync_shell(
        f'cp --dereference {original_bin_path} {stripped_bin_path}'
    )
    if not res:
        return res
    return term.sync_shell(f'strip {stripped_bin_path}')


build_mnc_server = oncerun(functools.partial(build_projects, 'ydb/tools/mnc/server', ['-r'], use_arcadia=True))
build_mnc_agent = oncerun(functools.partial(build_projects, 'ydb/tools/mnc/agent', ['-r'], use_arcadia=True))


def run(path_to_exe, *args):
    cli_args = itertools.chain([path_to_exe], args)
    cmd = ' '.join((str(x) for x in cli_args))
    proc = term.sync_shell(cmd, stdout=False, stderr=False)
    return not proc.returncode


async def async_run(path_to_exe, *args):
    cli_args = itertools.chain([path_to_exe], args)
    cmd = ' '.join((str(x) for x in cli_args))
    proc = await term.shell(cmd, stdout=False, stderr=False)
    return not proc.returncode


def run_with_result(path_to_exe, *args, silent_error=False):
    cli_args = itertools.chain([path_to_exe], args)
    cmd = ' '.join((str(x) for x in cli_args))
    return term.sync_shell(cmd, stdout=False, stderr=False, silent_error=silent_error)


def chain_await(*args):
    for arg in args:
        ok = arg
        if not ok:
            return False
    return True


def ask_cms_about_restart(
    node_id: int,
    build_args: list[str],
    availability_mode: str,
    grpc_endpoint: str = None,
    silent_error=False,
):
    ok = True
    if deploy_ctx.do_rebuild:
        ok = build_kikimr(build_args)
    if not ok:
        return False
    path_to_kikimr = os.path.join(deploy_ctx.source_root, deploy_ctx.relative_binary_path)
    if grpc_endpoint:
        additional_args = ['--server', grpc_endpoint]
    return run_with_result(
        path_to_kikimr,
        *additional_args,
        'cms',
        'request',
        'restart',
        'host',
        str(node_id),
        '--user',
        'multinode_configure',
        '--duration 60',
        '--dry',
        '--reason',
        'rolling-restart',
        '--availability-mode',
        availability_mode,
        silent_error=silent_error,
    )


def sync_rsync_file(source_local_path: str, destination_host: str, destination_path: str):
    return run('rsync', '-Lq', '--progress', source_local_path, f'{destination_host}:{destination_path}')


async def rsync_file(source_local_path: str, destination_host: str, destination_path: str):
    return await async_run('rsync', '-Lq', '--progress', source_local_path, f'{destination_host}:{destination_path}')


async def remote_parallel_rsync(
    source_host: str, source_local_path: str, destination_hosts: list[str], destination_path: str
):
    return await parallel_async(
        *(
            term.ssh_run(source_host, f"rsync -Lq --progress '{source_local_path}' '{host}:{destination_path}'")
            for host in destination_hosts
        )
    )


def sed_command(src: str, dest: str, replace: dict):
    args = ' '.join((f"-e 's;{key};{value};g'") for key, value in replace.items())
    return f'sed {args} {src} > {dest}'
