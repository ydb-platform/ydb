import logging
import asyncio
import os.path

import rich


from ydb.tools.mnc.lib import agent_client, common, deploy_ctx, progress
from ydb.tools.mnc.lib.legacy_commands import configs, deploy, disks, service, init
from ydb.tools.mnc.scheme import multinode
from ydb.tools.mnc.lib.draft import tools, term

from . import uninstall


logger = logging.getLogger(__name__)


expected_config = multinode.scheme


def title(text):
    print('\033[1;35m', text, '\033[0m\n', sep='')


def make_build_dependencies_step(config: dict):
    steps = []
    if config['ydb_config_type'] == 'v1':
        steps.append(tools.make_build_cfg_step(config['build_args']))
    if not deploy_ctx.is_manual_path_to_bin:
        steps.append(tools.make_build_kikimr_step(config['build_args']))
    if deploy_ctx.do_strip:
        steps.append(tools.make_strip_kikimr_step())
    if steps:
        return progress.SequentialStepGroup(title='[bold blue]Build dependencies[/]', steps=steps)
    return None


def make_deploy_file_steps(hosts: list[str], bin_path: str, deploy_path: str):
    steps = []
    name = os.path.basename(bin_path)
    if deploy_ctx.transit_bin_through_first_node and len(hosts) > 1:
        steps.append(tools.make_rsync_step(bin_path, hosts[0], deploy_path))
        steps.append(tools.make_remote_rsync_step(hosts[0], deploy_path, hosts[1:], deploy_path))
        return progress.SequentialStepGroup(title=f'[bold blue]Deploy[/] [green]{name}[/]', steps=steps)
    else:
        for host in hosts:
            steps.append(tools.make_rsync_step(bin_path, host, deploy_path))
        return progress.ParallelStepGroup(title=f'[bold blue]Deploy[/] [green]{name}[/]', steps=steps, inflight=4)


def make_move_bin_steps(hosts: str, temp_path: str, final_path: str):
    def make_action(host: str):
        async def action():
            result = await term.ssh_run(
                host,
                f'mkdir -p {deploy_ctx.deploy_path} && mkdir -p {deploy_ctx.deploy_path}/kikimr && mkdir -p {deploy_ctx.deploy_path}/kikimr/bin && sudo mv {temp_path} {final_path}',
            )
            if result:
                return True
            else:
                return progress.TaskResult(level=progress.TaskResultLevel.ERROR, message='Failed to move bin\n' + result.stderr + '\n' + f"[red]Return code: {result.returncode}[/]")
        return action

    return progress.ParallelStepGroup(
        title='[bold blue]Move bin[/]',
        steps=[progress.SimpleStep(title='[bold blue]Move bin[/]', action=make_action(host)) for host in hosts],
    )


def make_deploy_bin_steps(
    hosts: list[str],
    config,
):
    source_bin_path = deploy_ctx.path_to_bin

    if deploy_ctx.do_strip:
        if deploy_ctx.is_manual_path_to_bin:
            source_bin_path += "_stripped"

    temp_path = 'kikimr'
    final_path = f'{deploy_ctx.deploy_path}/kikimr/bin/kikimr'

    steps = [
        make_deploy_file_steps(hosts, source_bin_path, temp_path),
        make_move_bin_steps(hosts, temp_path, final_path),
    ]

    return progress.SequentialStepGroup(title='[bold blue]Deploy bin[/]', steps=steps)


async def service_host(host: str, operation: str, node_type: str, batch_size: int = 10, parent_task: progress.TaskNode = None):
    processes = await service.get_processes(host)
    processes = [p for p in processes if node_type in p]
    batched_processes = uninstall.batch_list(processes, batch_size)
    await parent_task.update(total=len(processes))
    for batch in batched_processes:
        ok = await service.cmd_agent_kikimr_operation(host, operation, batch)
        if not ok:
            return progress.TaskResult(
                level=progress.TaskResultLevel.ERROR,
                message=f'Failed to {operation} {node_type} nodes on {host}: {batch}',
            )
        await parent_task.update(advance=len(batch))
    return True


def make_service_host_step(host: str, operation: str, node_type: str, batch_size: int = 10):
    return progress.Step(
        title=f"[yellow]{host}[/] [bold cyan]{operation}[/]",
        command=lambda parent_task, kv_storage: service_host(host, operation, node_type, batch_size, parent_task=parent_task),
    )


def make_group_service_host_step(hosts, operation: str, node_type: str, batch_size: int = 10):
    return progress.ParallelStepGroup(
        title=f"[bold blue]{operation} multinode[/]",
        steps=[make_service_host_step(host, operation, node_type, batch_size) for host in hosts],
    )


def make_split_disks_step(hosts: list[str], config: dict):
    return progress.SimpleStep(
        title='[bold blue]Split disks',
        action=lambda: disks.act_split(hosts, config, part_size=common.Memory('{0}GB'.format(config['disk_size']))),
        predicate=lambda: config['sector_map']['use'] != 'always',
    )


def make_format_disks_step(hosts: list[str], config: dict):
    return progress.SimpleStep(
        title='[bold blue]Format disks',
        action=lambda: disks.act_obliterate(hosts, config),
        predicate=lambda: config['sector_map']['use'] != 'always',
    )


async def generate_cfg(hosts, config: dict, parent_task: progress.TaskNode = None):
    commands = [
        f'mkdir -p {deploy_ctx.work_directory}/static',
        f'mkdir -p {deploy_ctx.work_directory}/dynamic',
        f'mkdir -p {deploy_ctx.work_directory}/nbs',
        f'mkdir -p {deploy_ctx.work_directory}/nbs/cfg',
    ]
    result = await term.shell(' && '.join(commands))
    if not result:
        return progress.TaskResult(level=progress.TaskResultLevel.ERROR, message='Failed to create work directory')
    commands = [
        f'rm -rf {deploy_ctx.work_directory}/static/*',
        f'rm -rf {deploy_ctx.work_directory}/dynamic/*',
        f'rm -rf {deploy_ctx.work_directory}/nbs/cfg/*',
    ]
    result = await term.shell(' && '.join(commands))
    if not result:
        return progress.TaskResult(level=progress.TaskResultLevel.ERROR, message='Failed to remove work directory')
    result = await configs.act_generate(hosts, config, parent_task=parent_task)
    if not result:
        return progress.TaskResult(level=progress.TaskResultLevel.ERROR, message='Failed to generate configs')
    return True


def make_waiting_step(waiting: int):
    async def sleep(task: progress.TaskNode):
        for i in range(waiting):
            await asyncio.sleep(1)
            await task.update(advance=1)
        return True

    return progress.Step(
        title=f'[bold blue]Waiting[/] [yellow]{waiting}s[/]',
        command=lambda parent_task, kv_storage: sleep(parent_task),
        task_args={'total': waiting},
    )


def make_generate_configs_step(hosts: list[str], config: dict):
    return progress.Step(
        title='[bold blue]Generate configs',
        command=lambda parent_task, kv_storage: generate_cfg(hosts, config, parent_task=parent_task),
    )


def make_install_multinode_step(hosts: list[str], config: dict):
    return progress.Step(
        title='[bold blue]Install multinode',
        command=lambda parent_task, kv_storage: deploy.act_install(hosts, config, parent_task=parent_task),
    )


async def init_static_v1_action(config: dict, parent_task: progress.TaskNode = None):
    init_storage = await parent_task.add_subtask("[bold green]Init storage", total=1)
    init_cms = await parent_task.add_subtask("[bold green]Init CMS", total=1)
    init_compute = await parent_task.add_subtask("[bold green]Init compute", total=1)

    ok = await tools.chain_async(
        term.shell(f'chmod +x {deploy_ctx.work_directory}/dynamic/init_storage.bash; {deploy_ctx.work_directory}/dynamic/init_storage.bash'),
        term.shell(f'rm {deploy_ctx.work_directory}/dynamic/Configure-{config["domain"]["name"]}.txt', silent_error=True),
        term.shell(
            f'cp {deploy_ctx.work_directory}/special_dynamic/Configure-domain.txt {deploy_ctx.work_directory}/dynamic/Configure-{config["domain"]["name"]}.txt'
        ),
    )
    if not ok:
        return progress.TaskResult(level=progress.TaskResultLevel.ERROR, message='Failed to init storage')
    await init_storage.update(advance=1)

    ok = await term.shell(f'chmod +x {deploy_ctx.work_directory}/dynamic/init_cms.bash && {deploy_ctx.work_directory}/dynamic/init_cms.bash')
    if not ok:
        return progress.TaskResult(level=progress.TaskResultLevel.ERROR, message='Failed to init CMS')
    await init_cms.update(advance=1)
    ok = await term.shell(f'chmod +x {deploy_ctx.work_directory}/dynamic/init_compute.bash && {deploy_ctx.work_directory}/dynamic/init_compute.bash')
    if not ok:
        return progress.TaskResult(level=progress.TaskResultLevel.ERROR, message='Failed to init compute')
    await init_compute.update(advance=1)

    await init_storage.update(visible=False)
    await init_cms.update(visible=False)
    await init_compute.update(visible=False)
    return True


def make_init_static_v1_step(config: dict):
    return progress.Step(
        title='[bold blue]Init static',
        command=lambda parent_task, kv_storage: init_static_v1_action(config, parent_task=parent_task),
        task_args={'total': 0},
    )


def make_init_static_v2_step(config: dict):
    return progress.Step(
        title='[bold blue]Init static',
        command=lambda parent_task, kv_storage: init.act_static(config, parent_task=parent_task),
    )


async def init_dynamic_v1_action(config: dict, parent_task: progress.TaskNode = None):
    init_db_task = await parent_task.add_subtask("[bold green]Init databases", total=1)
    ok = await term.shell(f'chmod +x {deploy_ctx.work_directory}/dynamic/init_databases.bash && {deploy_ctx.work_directory}/dynamic/init_databases.bash')
    if not ok:
        logger.error('Failed to init databases')
        return False
    await init_db_task.update(advance=1)
    await init_db_task.update(visible=False)
    return True


def make_init_dynamic_v1_step(config: dict):
    return progress.Step(
        title='[bold blue]Init dynamic',
        command=lambda parent_task, kv_storage: init_dynamic_v1_action(config, parent_task=parent_task),
        task_args={'total': 1},
    )


async def init_dynamic_v2_action(config: dict, parent_task: progress.TaskNode = None):
    ok = await init.act_dynamic(config, parent_task=parent_task)
    if not ok:
        return progress.TaskResult(level=progress.TaskResultLevel.ERROR, message='Failed to init dynamic')
    return True


def make_init_dynamic_v2_step(config: dict):
    return progress.Step(
        title='[bold blue]Init dynamic',
        command=lambda parent_task, kv_storage: init_dynamic_v2_action(config, parent_task=parent_task),
    )


def make_install_steps(hosts, config, waiting: int, do_not_init: bool, ignore_failed_stop: bool):
    steps = [
        agent_client.CheckAgentHealthOnHosts(hosts),
    ]

    if deploy_ctx.do_rebuild:
        build_dependencies_step = make_build_dependencies_step(config)
        if build_dependencies_step:
            steps.append(build_dependencies_step)

    steps.append(uninstall.make_uninstall_steps(hosts, config, ignore_failed_stop=ignore_failed_stop))

    if deploy_ctx.do_redeploy_bin:
        steps.append(make_deploy_bin_steps(hosts, config))

    if config['sector_map']['use'] != 'always':
        steps.append(make_split_disks_step(hosts, config))
        steps.append(make_format_disks_step(hosts, config))

    steps.append(make_generate_configs_step(hosts, config))

    steps.append(make_install_multinode_step(hosts, config))
    steps.append(make_group_service_host_step(hosts, 'start', 'static', 10))

    if do_not_init:
        return progress.SequentialStepGroup(title='[bold blue]Install[/]', steps=steps)

    steps.append(make_waiting_step(waiting))

    if config['ydb_config_type'] == 'v1':
        steps.append(make_init_static_v1_step(config))
    else:
        steps.append(make_init_static_v2_step(config))

    if config['domain'] is not None:
        steps.append(make_waiting_step(5))

        if config['ydb_config_type'] == 'v1':
            steps.append(make_init_dynamic_v1_step(config))
        else:
            steps.append(make_init_dynamic_v2_step(config))

        steps.append(make_group_service_host_step(hosts, 'start', 'dynamic', 10))

    return progress.SequentialStepGroup(title='[bold blue]Install[/]', steps=steps)


async def act(
    hosts,
    config,
    waiting=60,
    bin_path=None,
    do_not_init=False,
    ignore_failed_stop=False,
    console=None,
):
    if bin_path is not None:
        deploy_ctx.update_path_to_bin(bin_path)

    install_steps = make_install_steps(hosts, config, waiting, do_not_init, ignore_failed_stop)

    with progress.MyProgress(console=console) as pbar:
        result = await progress.run_steps([install_steps], progress=pbar, title="[bold]Install[/]")
    console.print(result.to_rich_panel())
    return bool(result)


def add_arguments(parser):
    common.add_common_options(parser)
    parser.add_argument('--waiting', dest='waiting', type=int, default=15)
    parser.add_argument('--bin-path', '--bin_path', default=None, type=str, help='path to binary file')
    parser.add_argument('--do-not-init', '--do_not_init', action='store_const', const=True, default=False, help='do not init bsc and etc')
    parser.add_argument('--ignore-failed-stop', action='store_const', const=True, default=False, help='ignore failed stop')


async def do(args):
    console = rich.console.Console()
    hosts = await common.get_machines(args.config)
    return await act(
        hosts,
        args.config,
        waiting=args.waiting,
        bin_path=args.bin_path,
        do_not_init=args.do_not_init,
        ignore_failed_stop=args.ignore_failed_stop,
        console=console,
    )
