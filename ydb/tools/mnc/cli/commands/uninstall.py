import logging
from typing import List

from ydb.tools.mnc.lib import agent_client, common, progress, service
from ydb.tools.mnc.scheme import multinode
import rich

from . import disks


logger = logging.getLogger(__name__)

expected_config = multinode.scheme


def batch_list(lst, batch_size):
    if not lst:
        return []
    n = len(lst)
    return [lst[i:min(n, i + batch_size)] for i in range(0, n, batch_size)]


async def stop_host(host: str, batch_size: int = 10, ignore_failed_stop: bool = False, parent_task: progress.TaskNode = None):
    processes = await service.get_processes(host)
    batched_processes = batch_list(processes, batch_size)
    await parent_task.update(total=len(processes))
    failed = []
    for batch in batched_processes:
        ok = await service.cmd_agent_ydb_operation(host, 'stop', batch)
        if not ok:
            failed.extend(batch)
            if not ignore_failed_stop:
                return progress.TaskResult(
                    level=progress.TaskResultLevel.ERROR,
                    message=f'Failed to stop nodes on {host}: {batch}',
                )
        await parent_task.update(advance=len(batch))
    if failed:
        return progress.TaskResult(
            level=progress.TaskResultLevel.WARNING,
            message=f'Ignored failed stop on {host}: {failed}',
        )
    return True


def make_stop_host_step(host: str, batch_size: int = 10, ignore_failed_stop: bool = False):
    return progress.Step(
        title=f"[yellow]{host}[/] [bold cyan]stop[/]",
        command=lambda parent_task, kv_storage: stop_host(host, batch_size, ignore_failed_stop=ignore_failed_stop, parent_task=parent_task),
    )


def make_group_stop_host_step(hosts: List[str], batch_size: int = 10, ignore_failed_stop: bool = False):
    return progress.ParallelStepGroup(
        title="[bold blue]Stop hosts[/]",
        steps=[make_stop_host_step(host, batch_size, ignore_failed_stop=ignore_failed_stop) for host in hosts],
    )


async def uninstall_host(host: str, batch_size: int = 10, parent_task: progress.TaskNode = None):
    processes = await service.get_processes(host)
    batched_processes = batch_list(processes, batch_size)
    await parent_task.update(total=len(processes))
    for batch in batched_processes:
        ok = await service.cmd_agent_ydb_operation(host, 'uninstall', batch)
        if not ok:
            return progress.TaskResult(
                level=progress.TaskResultLevel.ERROR,
                message=f'Failed to uninstall nodes on {host}: {batch}',
            )
        await parent_task.update(advance=len(batch))
    return True


def make_uninstall_host_step(host, batch_size: int = 10):
    return progress.Step(
        title=f"[yellow]{host}[/] [bold cyan]uninstall[/]",
        command=lambda parent_task, kv_storage: uninstall_host(host, batch_size, parent_task=parent_task),
    )


def make_group_uninstall_host_step(hosts, batch_size: int = 10):
    return progress.ParallelStepGroup(
        title="[bold blue]Uninstall multinode[/]",
        steps=[make_uninstall_host_step(host, batch_size) for host in hosts],
    )


def make_group_return_disks_step(hosts, config):
    return progress.SimpleStep(
        title="[bold blue]Return disks[/]",
        action=lambda: disks.act_unite(hosts, config),
        predicate=lambda: config['sector_map']['use'] != 'always',
    )


def make_uninstall_steps(hosts, config, ignore_failed_stop: bool = False):
    return progress.SequentialStepGroup(
        title="[bold blue]Demote[/]",
        steps=[
            agent_client.CheckAgentHealthOnHosts(hosts),
            make_group_stop_host_step(hosts, ignore_failed_stop=ignore_failed_stop),
            make_group_uninstall_host_step(hosts),
            make_group_return_disks_step(hosts, config),
        ]
    )


async def act(hosts, config, ignore_failed_stop=False, console=None):
    uninstall_steps = make_uninstall_steps(hosts, config, ignore_failed_stop=ignore_failed_stop)
    with progress.MyProgress(console=console) as pgbar:
        result = await progress.run_steps([uninstall_steps], progress=pgbar, title="[bold]Uninstall[/]")
    console.print(result.to_rich_panel())
    return bool(result)


def add_arguments(parser):
    common.add_common_options(parser)
    parser.add_argument('--ignore-failed-stop', action='store_const', const=True, default=False, help='ignore failed stop')


async def do(args):
    console = rich.console.Console()
    hosts = await common.get_machines(args.config)
    return await act(
        hosts,
        args.config,
        ignore_failed_stop=args.ignore_failed_stop,
        console=console,
    )
