import asyncio
import logging
import aiohttp

from ydb.tools.mnc.lib import common, deploy_ctx, progress, term, tools


logger = logging.getLogger(__name__)


allowed_commands = ['start', 'restart', 'stop']
allowed_node_types = ['static', 'dynamic']


nodes_semaphore = None


async def cmd(command: str, host: str, processes: list[str], force=False, has_agent: bool = False):
    if not check_correct_cmd(command):
        logger.error(f'Command was not allowed; command: {command} allowed commands: {allowed_commands}')
        return False
    if has_agent:
        return await cmd_agent_ydb_operation(host, command, processes)
    return await cmd_custom(command, host, processes, force=force)


def check_correct_cmd(command: str):
    return command in allowed_commands


static_prefix = 'ydb_node_static_'
dynamic_prefix = 'ydb_node_dynamic_'


async def cmd_custom_start(host: str, process: str, run_command: str, prepare_command=''):
    run_path = f'{deploy_ctx.deploy_path}/run'
    return bool(
        await term.ssh_run(
            host,
            f'{prepare_command}{";" if prepare_command else ""}'
            f'sudo touch {deploy_ctx.deploy_path}/{process}/stdout;'
            f'sudo chmod 777 {deploy_ctx.deploy_path}/{process}/stdout;'
            f'{run_command} &> '
            f'{deploy_ctx.deploy_path}/{process}/stdout < /dev/null &(echo $! | sudo tee {run_path}/{process}.pid)',
        )
    )


async def cmd_custom_stop(host: str, process: str, bin_name: str, force=False):
    run_path = f'{deploy_ctx.deploy_path}/run'
    ok = bool(await term.ssh_run(host, f'sudo kill -9 $(pgrep -P `cat {run_path}/{process}.pid`) && sudo kill -9 `cat {run_path}/{process}.pid` && rm {run_path}/{process}.pid'))
    if not ok and force:
        await term.ssh_run(
            host,
            f'processes="`pgrep {bin_name}`"; if [[ "$processes" != "" ]] ; then sudo kill -9 $processes ; fi',
        )
        return True
    return ok


async def cmd_custom_ydb_start(host: str, process: str):
    if process.startswith(static_prefix):
        node_idx = process[len(static_prefix) :]
        cfg_name = f'ydb-{node_idx}.cfg'
    else:
        node_idx = process[len(dynamic_prefix) :]
        cfg_name = f'dynamic_server_{node_idx}.cfg'
    if deploy_ctx.affinity:
        affinity_cmd = f'taskset -c {deploy_ctx.affinity}'
    else:
        affinity_cmd = ''
    return await cmd_custom_start(
        host,
        process,
        run_command=f'{affinity_cmd} sudo {deploy_ctx.deploy_path}/ydb/bin/ydb ${{ydb_arg}}',
        prepare_command=f'. {deploy_ctx.deploy_path}/{process}/cfg/{cfg_name}'
    )


async def cmd_custom_ydb_stop(host: str, process: str, force=False):
    return await cmd_custom_stop(host, process, 'ydb', force=force)


async def cmd_custom_ydb_restart(host: str, process: str):
    return await tools.chain_async(cmd_custom_ydb_stop(host, process), cmd_custom_ydb_start(host, process))


custom_cmd_func_dict = {
    'start': cmd_custom_ydb_start,
    'stop': cmd_custom_ydb_stop,
    'restart': cmd_custom_ydb_restart,
}


async def cmd_agent_ydb_operation(host: str, operation: str, processes: list[str]):
    async with aiohttp.ClientSession() as session:
        args = '&'.join([f'node={process}' for process in processes])
        async with session.get(f'http://{host}:8999/nodes/{operation}?{args}') as response:
            if response.status != 200:
                logger.error(f'Failed to {operation} nodes {processes} on {host}: response status {response.status}')
                return False
            result = await response.json()
            for operation in result['operations']:
                if not operation['success']:
                    logger.error(f'Failed to {operation["operation"]} node {operation["node"]}: {operation["message"]}')
            return all(operation['success'] for operation in result['operations'])


async def cmd_custom(command: str, host: str, processes: list[str], force=False):
    cmd_func = custom_cmd_func_dict[command]
    if command == 'stop':
        return await tools.parallel_async(*(cmd_func(host, process, force=force) for process in processes))
    return await tools.parallel_async(*(cmd_func(host, process) for process in processes))


async def check_agent(host: str):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f'http://{host}:8999/health') as response:
                return response.status == 200 and 'nodes' in (await response.json())['enabled_features']
        except Exception as e:
            logger.info(f'Failed to check agent on {host}: {e}')
            return False


async def get_processes(host: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(f'http://{host}:8999/nodes') as response:
            return [node['node'] for node in (await response.json())['nodes']]


@progress.with_parent_task
async def one_host(command: str, host: str, ydb_node_ids: list[int] = None, node_type: str = None, force=False, parent_task: progress.TaskNode = None, subtasks: list[progress.TaskNode] = []):
    has_agent = await check_agent(host)
    processes = []
    current = []
    prefix = 'ydb_node'
    if node_type == "dynamic":
        prefix = 'ydb_node_dynamic'
    if node_type == "static":
        prefix = 'ydb_node_static'
    if ydb_node_ids is None and has_agent:
        processes = await get_processes(host)
        processes = [proc for proc in processes if proc.startswith(prefix)]
    elif ydb_node_ids is None:
        processes = (
            await term.ssh_run(host, f'cd {deploy_ctx.deploy_path}; ls | grep {prefix}')
        ).stdout.split()
        processes = [proc for proc in processes if proc]
    else:
        processes = ['{1}_{0}'.format(id, prefix) for id in ydb_node_ids]

    batch_size = 10
    n = len(processes)
    if n > 0:
        subtask = await parent_task.add_subtask(f'[yellow]{host} [bold cyan]{command}', total=n)
    else:
        subtask = await parent_task.add_subtask(f'[yellow]{host} [bold cyan]{command}', total=1)
        await subtask.update(advance=1)
    subtasks.append(subtask)
    for idx in range(len(processes)):
        current.append(processes[idx])
        if idx % batch_size == batch_size - 1:
            ok = await cmd(command, host, current, force=force, has_agent=has_agent)
            if not ok:
                print(
                    'ERROR failed operation on ', host, ': ', command, ' ', len(processes), '/', len(processes), sep=''
                )
                return False
            await parent_task.update(advance=batch_size)
            if idx + 1 < n and command != 'stop' and not has_agent:
                await asyncio.sleep(10)
            current = []
    if current:
        ok = await cmd(command, host, current, force=force, has_agent=has_agent)
        if not ok:
            print('ERROR failed operation on ', host, ': ', command, ' ', len(processes), '/', len(processes), sep='')
            return False
        await subtask.update(advance=len(current))
    return True


@progress.with_parent_task
async def act_hosts(command: str, hosts: list, node_type: str = None, parent_task: progress.TaskNode = None):
    subtasks = []
    result = await asyncio.gather(
        *(one_host(command, host, node_type=node_type, force=True, parent_task=parent_task, subtasks=subtasks) for host in hosts)
    )
    for subtask in subtasks:
        await subtask.update(visible=False)
    return all(result)


@progress.with_parent_task
async def rolling_restart_static(
    locations_by_host: dict[str, list[int]], build_args: list[str], availability_mode: str, parent_task: progress.TaskNode = None
):
    n = sum(len(ydb_node_ids) for ydb_node_ids in locations_by_host.values())
    subtask = await parent_task.add_subtask('static rolling-restart', total=n)

    for host, ydb_node_ids in locations_by_host.items():
        for id in ydb_node_ids:
            async with nodes_semaphore:
                allow = ''
                while not allow.startswith('ALLOW'):
                    if allow:
                        await term.shell('sleep 5s')
                    grpc_port = 2134 + id if id < 10 else 21000 + id
                    grpc_endpoint = '{0}:{1}'.format(host, grpc_port)
                    result = await tools.ask_cms_about_restart(
                        id,
                        build_args,
                        availability_mode,
                        grpc_endpoint=grpc_endpoint,
                    )
                    allow = result.stdout
                ok = await one_host('restart', host, [id], node_type='static')
                if not ok:
                    logger.error('Error during rolling-restart, can\'t restart ydb_node_{0} on {1}'.format(id, host))
                    return False
                await subtask.update(advance=1)
    return True


async def get_dynamic_nodes_from_host(host: str):
    processes = (
        await term.ssh_run(host, f'cd {deploy_ctx.deploy_path}; ls | grep ydb_node_dynamic_')
    ).stdout.split()
    return [x for x in processes if x.startswith('ydb_node_dynamic_')]


@progress.with_parent_task
async def rolling_restart_dynamic_node(host: str, time_to_wait: int, parent_task: progress.TaskNode = None):
    processes = await get_dynamic_nodes_from_host(host)
    subtask = await parent_task.add_subtask('[bold green] Rolling-restart dynamic nodes', total=len(processes))
    for proc in processes:
        async with nodes_semaphore:
            # ! make it through regexp
            id = int(proc.replace('ydb_node_dynamic_', ''))
            ok = await one_host('restart', host, [id], node_type='dynamic', parent_task=parent_task._progress.get_dummy_task())
            if not ok:
                logger.error(
                    'Error during rolling-restart, can\'t restart ydb_node_dynamic_{0} on {1}'.format(id, host)
                )
                return False
            await subtask.update(advance=1)
            await term.shell('sleep {0}s'.format(time_to_wait))
    return True


@progress.with_parent_task
async def rolling_restart_dynamic(hosts: list[str], time_to_wait: int, parent_task: progress.TaskNode = None):
    subtask = await parent_task.add_subtask('dynamic rolling-restart')
    return all(await asyncio.gather(*(rolling_restart_dynamic_node(host, time_to_wait, parent_task=subtask) for host in hosts)))


@progress.with_parent_task
async def act_nodes(
    command: str,
    hosts: list[str],
    config: dict,
    nodes: list[str] = None,
    exclude_nodes: list[str] = None,
    type: str = None,
    time_to_wait: int = 10,
    in_flight: int = 1,
    availability_mode: str = 'max',
    parent_task: progress.TaskNode = None,
):
    nodes_per_host = config['nodes_per_host']
    freehost = config['freehost']
    locations_by_host = common.get_node_locations_by_host(
        hosts, nodes_per_host, freehost, nodes, exclude_nodes
    )
    if command == 'rolling_restart':
        if (nodes or exclude_nodes) and type != 'static':
            print('For rolling-restart dynamic node specification was not implemented')
            return False
        global nodes_semaphore
        nodes_semaphore = asyncio.Semaphore(in_flight)
        ok = True
        if type in ('static', 'all'):
            ok = ok and await rolling_restart_static(
                locations_by_host, config['build_args'], availability_mode, parent_task=parent_task
            )
        if type in ('dynamic', 'all'):
            ok = ok and await rolling_restart_dynamic(hosts, time_to_wait, parent_task=parent_task)
        return ok
    else:
        if type != 'static':
            print('The operation "{0}" was not implemented for dynamic nodes'.format(command))
            return False
        return await tools.parallel_async(
            *(
                one_host(command, host, ydb_node_ids, node_type='static', parent_task=parent_task)
                for host, ydb_node_ids in locations_by_host.items()
            )
        )
