import asyncio
import logging
import aiohttp

from ydb.tools.mnc.lib import common, deploy_ctx, term, tools, progress
from ydb.tools.mnc.scheme import multinode


logger = logging.getLogger(__name__)


allowed_commands = ['start', 'restart', 'stop']
allowed_node_types = ['static', 'dynamic']


expected_config = multinode.scheme


nodes_semaphore = None


def check_correct_cmd(command: str):
    return command in allowed_commands


async def cmd_systemd(command: str, host: str, processes: list[str]):
    if not check_correct_cmd(command):
        logger.error(f'Command was not allowed; command: {command} allowed commands: {allowed_commands}')
        return False
    cmd_line = '& '.join((f'sudo systemctl {command} {proc}' for proc in processes))
    ok = await term.ssh_run(host, cmd_line + '& wait')
    return ok and bool(await term.shell('sleep 5s'))


async def cmd_init(command: str, host: str, processes: list[str]):
    if not check_correct_cmd(command):
        logger.error(f'Command was not allowed; command: {command} allowed commands: {allowed_commands}')
        return False
    cmd_line = '& '.join((f'sudo {command} {proc}' for proc in processes))
    ok = await term.ssh_run(host, cmd_line + '& wait')
    return ok and bool(await term.shell('sleep 5s'))


static_prefix = 'test_kikimr_static_'
dynamic_prefix = 'test_kikimr_dynamic_'


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
    ok = bool(await term.ssh_run(host, f'sudo kill -9 $(pgrep -P `cat {run_path}/{process}.pid`) && sudo kill -9 `cat {run_path}/{process}.pid` && rm {run_path}/{process}.pid', silent_error=True))
    if not ok and force:
        await term.ssh_run(
            host,
            f'processes="`pgrep {bin_name}`"; if [[ "$processes" != "" ]] ; then sudo kill -9 $processes ; fi',
            silent_error=True,
        )
        return True
    return ok


async def cmd_custom_kikimr_start(host: str, process: str):
    if process.startswith(static_prefix):
        node_idx = process[len(static_prefix) :]
        cfg_name = f'kikimr-{node_idx}.cfg'
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
        run_command=f'{affinity_cmd} sudo {deploy_ctx.deploy_path}/kikimr/bin/kikimr ${{kikimr_arg}}',
        prepare_command=f'. {deploy_ctx.deploy_path}/{process}/cfg/{cfg_name}'
    )


async def cmd_custom_kikimr_stop(host: str, process: str, force=False):
    return await cmd_custom_stop(host, process, 'kikimr', force=force)


async def cmd_custom_kikimr_restart(host: str, process: str):
    return await tools.chain_async(cmd_custom_kikimr_stop(host, process), cmd_custom_kikimr_start(host, process))


custom_cmd_func_dict = {
    'start': cmd_custom_kikimr_start,
    'stop': cmd_custom_kikimr_stop,
    'restart': cmd_custom_kikimr_restart,
}


async def cmd_agent_kikimr_operation(host: str, operation: str, processes: list[str]):
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


async def check_systemd(host: str):
    path = await term.ssh_run(host, 'readlink /sbin/init', silent_error=True)
    if path:
        return path.stdout.strip().endswith('systemd')
    else:
        return False


async def check_agent(host: str):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f'http://{host}:8999/health') as response:
                return response.status == 200 and 'nodes' in (await response.json())['enabled_features']
        except Exception as e:
            logger.info(f'Failed to check agent on {host}: {e}')
            return False


async def cmd(command: str, host: str, processes: list[str], with_systemd: bool = None, force=False, has_agent: bool = False):
    if not deploy_ctx.use_services:
        if has_agent:
            return await cmd_agent_kikimr_operation(host, command, processes)
        else:
            return await cmd_custom(command, host, processes, force=force)
    if with_systemd is None:
        with_systemd = await check_systemd(host)
    if with_systemd is None:
        return False
    if with_systemd:
        return await cmd_systemd(command, host, processes)
    else:
        return await cmd_init(command, host, processes)


async def one_host_original(command: str, host: str):
    with_systemd = await check_systemd(host)
    if with_systemd is None:
        return False
    processes = (
        await term.ssh_run(host, f'cd {deploy_ctx.deploy_path}; ls | grep kikimr_', silent_error=True)
    ).stdout.split()
    processes = [x for x in processes if x.startswith('kikimr_')]
    systemctl = "systemctl" if with_systemd else ""
    serv_cmd = "sudo {0} {1}".format(systemctl, command)

    def run_cmd(args):
        return term.ssh_run(host, ' '.join([serv_cmd, *args]), silent_error=True)

    await run_cmd(['kikimr'])
    for pr in processes:
        [_, num] = pr.split('_')
        await run_cmd(['kikimr-multi', 'slot={0}'.format(num)])


async def act_hosts_original(command: str, hosts: list):
    return all(await asyncio.gather(*(one_host_original(command, host) for host in hosts)))


async def get_processes(host: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(f'http://{host}:8999/nodes') as response:
            return [node['node'] for node in (await response.json())['nodes']]


@progress.with_parent_task
async def one_host(command: str, host: str, test_kikimr_ids: list[int] = None, node_type: str = None, force=False, parent_task: progress.TaskNode = None, subtasks: list[progress.TaskNode] = []):
    has_agent = await check_agent(host)
    if deploy_ctx.use_services:
        with_systemd = await check_systemd(host)
        if with_systemd is None:
            return False
    else:
        with_systemd = False
    processes = []
    current = []
    prefix = 'test_kikimr'
    if node_type == "dynamic":
        prefix = 'test_kikimr_dynamic'
    if node_type == "static":
        prefix = 'test_kikimr_static'
    if test_kikimr_ids is None and has_agent:
        processes = await get_processes(host)
        processes = [proc for proc in processes if proc.startswith(prefix)]
    elif test_kikimr_ids is None:
        processes = (
            await term.ssh_run(host, f'cd {deploy_ctx.deploy_path}; ls | grep {prefix}', silent_error=True)
        ).stdout.split()
        processes = [proc for proc in processes if proc]
    else:
        processes = ['{1}_{0}'.format(id, prefix) for id in test_kikimr_ids]

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
            ok = await cmd(command, host, current, with_systemd, force=force, has_agent=has_agent)
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
        ok = await cmd(command, host, current, with_systemd, force=force, has_agent=has_agent)
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
    n = sum(len(test_kikimr_ids) for test_kikimr_ids in locations_by_host.values())
    subtask = await parent_task.add_subtask('static rolling-restart', total=n)

    for host, test_kikimr_ids in locations_by_host.items():
        for id in test_kikimr_ids:
            async with nodes_semaphore:
                allow = ''
                while not allow.startswith('ALLOW'):
                    if allow:
                        await term.shell('sleep 5s')
                    grpc_port = 2134 + id if id < 10 else 21000 + id
                    grpc_endpoint = '{0}:{1}'.format(host, grpc_port)
                    result = tools.ask_cms_about_restart(
                        id,
                        build_args,
                        availability_mode,
                        grpc_endpoint=grpc_endpoint,
                        silent_error=True,
                    )
                    allow = result.stdout
                ok = await one_host('restart', host, [id], node_type='static')
                if not ok:
                    logger.error('Error during rolling-restart, can\'t restart test_kikimr_{0} on {1}'.format(id, host))
                    return False
                await subtask.update(advance=1)
    return True


async def get_dynamic_nodes_from_host(host: str):
    processes = (
        await term.ssh_run(host, f'cd {deploy_ctx.deploy_path}; ls | grep test_kikimr_dynamic_', silent_error=True)
    ).stdout.split()
    return [x for x in processes if x.startswith('test_kikimr_dynamic_')]


@progress.with_parent_task
async def rolling_restart_dynamic_node(host: str, time_to_wait: int, parent_task: progress.TaskNode = None):
    processes = await get_dynamic_nodes_from_host(host)
    subtask = await parent_task.add_subtask('[bold green] Rolling-restart dynamic nodes', total=len(processes))
    for proc in processes:
        async with nodes_semaphore:
            # ! make it through regexp
            id = int(proc.replace('test_kikimr_dynamic_', ''))
            ok = await one_host('restart', host, [id], node_type='dynamic', parent_task=parent_task._progress.get_dummy_task())
            if not ok:
                logger.error(
                    'Error during rolling-restart, can\'t restart test_kikimr_dynamic_{0} on {1}'.format(id, host)
                )
                return False
            await subtask.update(advance=1)
            await term.shell('sleep {0}s'.format(time_to_wait))
    return True


@progress.with_parent_task
async def rolling_restart_dynamic(hosts: list[str], time_to_wait: int, parent_task: progress.TaskNode = None):
    subtask = parent_task.add_subtask('dynamic rolling-restart')
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
                one_host(command, host, test_kikimr_ids, node_type='static', parent_task=parent_task)
                for host, test_kikimr_ids in locations_by_host.items()
            )
        )


def add_arguments(parser):
    subparsers = parser.add_subparsers(help='Commands', dest='cmd', required=True)

    orig_parser = subparsers.add_parser('orig')
    common.add_common_options(orig_parser)
    orig_parser.add_argument('operation', choices=tuple(allowed_commands))

    hosts_parser = subparsers.add_parser('hosts')
    common.add_common_options(hosts_parser)
    hosts_parser.add_argument('operation', choices=tuple(allowed_commands))
    hosts_parser.add_argument('--node-type', '--node_type', dest='node_type', choices=tuple(allowed_node_types), default=None)

    nodes_parser = subparsers.add_parser('nodes')
    common.add_common_options(nodes_parser)
    nodes_parser.add_argument('operation', choices=('stop', 'start', 'restart', 'rolling_restart'))
    nodes_parser.add_argument('--nodes', '-N', dest='nodes', nargs='*', default=None, help='default: All')
    nodes_parser.add_argument('--exclude-nodes', '--exclude_nodes', dest='exclude_nodes', nargs='*', default=None)
    nodes_parser.add_argument(
        '--type', '-t', dest='type', choices=('static', 'dynamic', 'all'), default='static', help='default: static'
    )
    nodes_parser.add_argument('--time-to-wait', type=int, default=10, help='in seconds for dynamic rolling_restart')
    nodes_parser.add_argument('--in-flight', type=int, default=1)
    nodes_parser.add_argument('--availability-mode', choices=('max', 'keep', 'force'), default='max')


async def do_hosts_orig(args):
    hosts = await common.get_machines(args.config)
    ok = await act_hosts_original(args.operation, hosts)
    if ok:
        print('success')
    else:
        print('operation failed')


async def do_hosts(args):
    hosts = await common.get_machines(args.config)
    ok = await act_hosts(args.operation, hosts, args.node_type)
    if ok:
        print('success')
    else:
        print('operation failed')


async def do_nodes(args):
    hosts = await common.get_machines(args.config)
    ok = await act_nodes(
        args.operation,
        list(hosts),
        args.config,
        args.nodes,
        args.exclude_nodes,
        args.type,
        args.time_to_wait,
        args.in_flight,
        args.availability_mode,
    )
    if ok:
        print('success')
    else:
        print('operation failed')


async def do(args):
    if args.cmd == 'orig':
        await do_hosts_orig(args)
    if args.cmd == 'hosts':
        await do_hosts(args)
    if args.cmd == 'nodes':
        await do_nodes(args)
