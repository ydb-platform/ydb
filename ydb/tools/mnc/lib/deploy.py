import asyncio
import logging

from ydb.tools.mnc.lib import common, configs, service, term, tools, deploy_ctx, progress


logger = logging.getLogger(__name__)


tmp_cfg_path = '/var/tmp/cfg00'


async def run(func, *args, **kwargs):
    return not (await func(*args, **kwargs)).returncode


async def for_each_async(hosts, act):
    return await asyncio.gather(*(act(host) for host in hosts))


async def check_installed(host):
    if await term.ssh_run(host, f'ls "{deploy_ctx.deploy_path}/ydb_node*"'):
        logger.error(f'already installed on host {host}')
        return True
    return False


class PrepareTmpDirCommands(term.GroupOfShellCommands):
    def __init__(self):
        term.GroupOfShellCommands.__init__(self, 'prepare tmp dir')
        self._commands = [
            f'(sudo rm -rf {tmp_cfg_path} || true)',
            f'sudo mkdir {tmp_cfg_path}',
            f'sudo chmod 777 {tmp_cfg_path}',
        ]


class PrepareHostCommands(term.GroupOfShellCommands):
    def __init__(self):
        term.GroupOfShellCommands.__init__(self, 'prepare host')
        self._commands = [
            PrepareTmpDirCommands(),
            f'sudo mkdir -p {deploy_ctx.deploy_path} && sudo chmod 777 {deploy_ctx.deploy_path}',
            f'sudo mkdir -p {deploy_ctx.deploy_path}/ydb && sudo chmod 777 {deploy_ctx.deploy_path}/ydb',
            f'sudo mkdir -p {deploy_ctx.deploy_path}/ydb/bin && sudo chmod 777 {deploy_ctx.deploy_path}/ydb/bin ',
            f'sudo mkdir -p {deploy_ctx.deploy_path}/ydb/cfg && sudo chmod 777 {deploy_ctx.deploy_path}/ydb/cfg ',
            f'echo \"Not a secret at all\" >{tmp_cfg_path}/fake-secret.txt ',
            f'sudo mv {tmp_cfg_path}/fake-secret.txt {deploy_ctx.deploy_path}/ydb/cfg/fake-secret.txt ',
            f'sudo mkdir -p {deploy_ctx.deploy_path}/run && sudo chmod 777 {deploy_ctx.deploy_path}/run',
            f'sudo mkdir -p {deploy_ctx.deploy_path}/mnc_server && sudo chmod 777 {deploy_ctx.deploy_path}/mnc_server',
            f'sudo mkdir -p {deploy_ctx.deploy_path}/mnc_server/bin && sudo chmod 777 {deploy_ctx.deploy_path}/mnc_server/bin',
            f'sudo mkdir -p {deploy_ctx.deploy_path}/mnc_server/cfg && sudo chmod 777 {deploy_ctx.deploy_path}/mnc_server/cfg',
            f'sudo mkdir -p {deploy_ctx.deploy_path}/mnc_server/output && sudo chmod 777 {deploy_ctx.deploy_path}/mnc_server/output',
        ]


class PostInstalledCommands(term.GroupOfShellCommands):
    def __init__(self):
        term.GroupOfShellCommands.__init__(self, 'post installed actions')
        self._commands = [
            f'(sudo rm -rf {tmp_cfg_path} || true)',
        ]


class PrepareYdbStaticDirCommands(term.GroupOfShellCommands):
    def __init__(self, node_idx):
        term.GroupOfShellCommands.__init__(self, f'prepare ydb_node_static_{node_idx}')
        self._commands = [
            f'sudo mkdir -p {deploy_ctx.deploy_path}/ydb_node_static_{node_idx}',
            f'sudo mkdir -p {deploy_ctx.deploy_path}/ydb_node_static_{node_idx}/logs',
            f'sudo mkdir -p {deploy_ctx.deploy_path}/ydb_node_static_{node_idx}/cfg',
            f'(sudo rm {deploy_ctx.deploy_path}/ydb_node_static_{node_idx}/cfg/* || true)',
            f'sudo touch {deploy_ctx.deploy_path}/ydb_node_static_{node_idx}/logs/ydb.log',
            f'sudo chmod 777 {deploy_ctx.deploy_path}/ydb_node_static_{node_idx}/logs/ydb.log',
            f'sudo touch {deploy_ctx.deploy_path}/ydb_node_static_{node_idx}/logs/ydb.start',
            f'sudo chmod 777 {deploy_ctx.deploy_path}/ydb_node_static_{node_idx}/logs/ydb.start',
        ]


class InstallStaticNodeCommands(term.GroupOfShellCommands):
    def __init__(self, node_idx):
        term.GroupOfShellCommands.__init__(self, f'install ydb_node_static_{node_idx}')
        self._commands = [
            PrepareYdbStaticDirCommands(node_idx),
            f'sudo cp {tmp_cfg_path}/config.yaml {deploy_ctx.deploy_path}/ydb_node_static_{node_idx}/cfg/',
            f'sudo cp {tmp_cfg_path}/ydb-{node_idx}.cfg {deploy_ctx.deploy_path}/ydb_node_static_{node_idx}/cfg/',
        ]
        if deploy_ctx.secure:
            self._commands += [
                f'HOST_FQDN=$(hostname -f); '
                f'if [ -d "{tmp_cfg_path}/$HOST_FQDN" ]; then '
                f'  sudo cp {tmp_cfg_path}/$HOST_FQDN/node.crt {deploy_ctx.deploy_path}/ydb_node_static_{node_idx}/cfg/node.crt || true; '
                f'  sudo cp {tmp_cfg_path}/$HOST_FQDN/node.key {deploy_ctx.deploy_path}/ydb_node_static_{node_idx}/cfg/node.key || true; '
                f'  sudo cp {tmp_cfg_path}/$HOST_FQDN/web.pem {deploy_ctx.deploy_path}/ydb_node_static_{node_idx}/cfg/web.pem || true; '
                f'fi; '
                f'[ -f {tmp_cfg_path}/ca.crt ] && sudo cp {tmp_cfg_path}/ca.crt {deploy_ctx.deploy_path}/ydb_node_static_{node_idx}/cfg/ca.crt || true',
            ]


class PrepareYdbDynamicDirCommands(term.GroupOfShellCommands):
    def __init__(self, node_idx):
        term.GroupOfShellCommands.__init__(self, f'prepare ydb_node_dynamic_{node_idx}')
        self._commands = [
            f'sudo mkdir -p {deploy_ctx.deploy_path}/ydb_node_dynamic_{node_idx}',
            f'sudo mkdir -p {deploy_ctx.deploy_path}/ydb_node_dynamic_{node_idx}/cfg',
            f'sudo mkdir -p {deploy_ctx.deploy_path}/ydb_node_dynamic_{node_idx}/logs',
            f'sudo mkdir -p {deploy_ctx.deploy_path}/ydb_node_dynamic_{node_idx}/tenants',
            f'(sudo rm {deploy_ctx.deploy_path}/ydb_node_dynamic_{node_idx}/tenants/* || true)',
            f'sudo touch {deploy_ctx.deploy_path}/ydb_node_dynamic_{node_idx}/logs/ydb.log',
            f'sudo chmod 777 {deploy_ctx.deploy_path}/ydb_node_dynamic_{node_idx}/logs/ydb.log',
            f'sudo touch {deploy_ctx.deploy_path}/ydb_node_dynamic_{node_idx}/logs/ydb.start',
            f'sudo chmod 777 {deploy_ctx.deploy_path}/ydb_node_dynamic_{node_idx}/logs/ydb.start',
        ]


class InstallDynamicNodeCommands(term.GroupOfShellCommands):
    def __init__(self, node_idx):
        term.GroupOfShellCommands.__init__(self, f'install ydb_node_dynamic_{node_idx}')
        self._commands = [
            PrepareYdbDynamicDirCommands(node_idx),
            f'sudo echo DC1 > {tmp_cfg_path}/location_{node_idx}',
            f'sudo cp {tmp_cfg_path}/location_{node_idx} {deploy_ctx.deploy_path}/ydb_node_dynamic_{node_idx}/tenants/location',
            f'sudo mkdir -p {deploy_ctx.deploy_path}/ydb_node_dynamic_{node_idx}/cfg',
            f'(sudo rm {deploy_ctx.deploy_path}/ydb_node_dynamic_{node_idx}/cfg/* || true)',
            f'sudo cp {tmp_cfg_path}/config.yaml {deploy_ctx.deploy_path}/ydb_node_dynamic_{node_idx}/cfg/',
            f'sudo cp {tmp_cfg_path}/dynamic_server_{node_idx}.cfg {deploy_ctx.deploy_path}/ydb_node_dynamic_{node_idx}/cfg/',
            f'sudo cp {tmp_cfg_path}/log-dynamic-{node_idx} {deploy_ctx.deploy_path}/ydb_node_dynamic_{node_idx}/cfg/log.txt',
        ]
        if deploy_ctx.secure:
            self._commands += [
                f'HOST_FQDN=$(hostname -f); '
                f'if [ -d "{tmp_cfg_path}/$HOST_FQDN" ]; then '
                f'  sudo cp {tmp_cfg_path}/$HOST_FQDN/node.crt {deploy_ctx.deploy_path}/ydb_node_dynamic_{node_idx}/cfg/node.crt || true; '
                f'  sudo cp {tmp_cfg_path}/$HOST_FQDN/node.key {deploy_ctx.deploy_path}/ydb_node_dynamic_{node_idx}/cfg/node.key || true; '
                f'  sudo cp {tmp_cfg_path}/$HOST_FQDN/web.pem {deploy_ctx.deploy_path}/ydb_node_dynamic_{node_idx}/cfg/web.pem || true; '
                f'fi; '
                f'[ -f {tmp_cfg_path}/ca.crt ] && sudo cp {tmp_cfg_path}/ca.crt {deploy_ctx.deploy_path}/ydb_node_dynamic_{node_idx}/cfg/ca.crt || true',
            ]


class PrepareNbsDirCommands(term.GroupOfShellCommands):
    def __init__(self, node_idx):
        term.GroupOfShellCommands.__init__(self, f'prepare test_nbs_{node_idx}')
        self._commands = [
            f'sudo mkdir -p {deploy_ctx.deploy_path}/test_nbs_{node_idx}',
            f'sudo mkdir -p {deploy_ctx.deploy_path}/test_nbs_{node_idx}/bin',
            f'sudo mkdir -p {deploy_ctx.deploy_path}/test_nbs_{node_idx}/cfg',
        ]


class InstallNbsNodeCommands(term.GroupOfShellCommands):
    def __init__(self, node_idx):
        term.GroupOfShellCommands.__init__(self, f'install test_nbs_{node_idx}')
        self._commands = [
            PrepareNbsDirCommands(node_idx),
        ]


class MakeArchiveWithConfigsCommands(term.GroupOfShellCommands):
    def __init__(self):
        term.GroupOfShellCommands.__init__(self, 'make archive with configs')
        cp_cmds = [
            f'cp {deploy_ctx.work_directory}/static/config.yaml {deploy_ctx.work_directory}/tmp/',
            f'cp {deploy_ctx.work_directory}/replaced/*.cfg {deploy_ctx.work_directory}/tmp/',
            f'cp {deploy_ctx.work_directory}/replaced/log-* {deploy_ctx.work_directory}/tmp/',
        ]
        # include certs when secure mode is enabled
        if deploy_ctx.secure:
            cp_cmds += (
                f'[ -d "{deploy_ctx.get_local_certs_dir()}" ] && cp -a "{deploy_ctx.get_local_certs_dir()}"/* "{deploy_ctx.work_directory}/tmp/" || true',
            )
        self._commands = [
            f'mkdir -p {deploy_ctx.work_directory}/tmp',
            f'touch {deploy_ctx.work_directory}/tmp/one',
            f'rm -rf {deploy_ctx.work_directory}/tmp/*',
            *cp_cmds,
            f'cd {deploy_ctx.work_directory}/tmp',
            'tar cf archive.tar *',
        ]


class UninstallNodesCommands(term.GroupOfShellCommands):
    def __init__(self):
        term.GroupOfShellCommands.__init__(self, 'uninstall nodes')
        self._commands = [
            f'(sudo rm -rf {deploy_ctx.deploy_path}/ydb_node* || true)',
        ]


class InstallCertsCommands(term.GroupOfShellCommands):
    def __init__(self):
        term.GroupOfShellCommands.__init__(self, 'install tls certs')
        if deploy_ctx.secure:
            self._commands = [
                'sudo mkdir -p /opt/ydb/certs',
                'HOST_FQDN=$(hostname -f); '
                f'[ -f {tmp_cfg_path}/ca.crt ] && sudo cp {tmp_cfg_path}/ca.crt {deploy_ctx.ca_remote_path} || true; '
                f'if [ -d {tmp_cfg_path}/$HOST_FQDN ]; then '
                f'  [ -f {tmp_cfg_path}/$HOST_FQDN/node.crt ] && sudo cp {tmp_cfg_path}/$HOST_FQDN/node.crt {deploy_ctx.cert_remote_path} || true; '
                f'  [ -f {tmp_cfg_path}/$HOST_FQDN/node.key ] && sudo cp {tmp_cfg_path}/$HOST_FQDN/node.key {deploy_ctx.key_remote_path} || true; '
                f'  [ -f {tmp_cfg_path}/$HOST_FQDN/web.pem ] && sudo cp {tmp_cfg_path}/$HOST_FQDN/web.pem {deploy_ctx.mon_cert_remote_path} || true; '
                f'fi',
                'sudo chown -R ydb:ydb /opt/ydb/certs',
                'sudo chmod 755 /opt/ydb/certs',
                'sudo chmod 644 /opt/ydb/certs/ca.crt || true',
                'sudo chmod 644 /opt/ydb/certs/node.crt || true',
                'sudo chmod 644 /opt/ydb/certs/node.key || true',
                'sudo chmod 644 /opt/ydb/certs/web.pem || true',
            ]
        else:
            self._commands = [':']


async def make_archive_with_configs():
    return await term.shell(MakeArchiveWithConfigsCommands())


async def prepare_tmp_dir(host):
    return await term.chain_ssh_cmd(host, PrepareTmpDirCommands())


async def prepare_host(host, parent_task: progress.TaskNode = None, subtasks: list[progress.TaskNode] = []):
    prepare_host_task = await parent_task.add_subtask(f"[yellow]{host} [bold cyan]prepare", total=1)
    subtasks.append(prepare_host_task)
    result = await term.chain_ssh_cmd(host, PrepareHostCommands())
    await prepare_host_task.update(advance=1)
    return result


@progress.with_parent_task
async def install_host(host, static_nodes, dynamic_nodes: list[int] = [], nbs_nodes: list[int] = [], parent_task: progress.TaskNode = None, subtasks: list[progress.TaskNode] = []):
    static_node_list = range(1, static_nodes + 1) if isinstance(static_nodes, int) else static_nodes

    extra_commands = [
        *(InstallStaticNodeCommands(node_id) for node_id in static_node_list),
        *(InstallDynamicNodeCommands(node_id) for node_id in dynamic_nodes),
        *(InstallNbsNodeCommands(node_id) for node_id in nbs_nodes),
        PostInstalledCommands(),
    ]
    groups = [extra_commands[i:min(i + 50, len(extra_commands))] for i in range(0, len(extra_commands), 50)]

    async def run_task(coro, task):
        result = await coro
        await task.update(advance=1)
        return result

    async def tasked(*args):
        task = await parent_task.add_subtask(f"[yellow]{host} [bold cyan]install", total=len(args))
        subtasks.append(task)
        return await tools.chain_async(*(run_task(cmd, task) for cmd in args))

    result = await tasked(
        term.shell(f'cd {deploy_ctx.work_directory}/tmp && scp archive.tar {host}:{tmp_cfg_path}'),
        term.chain_ssh_cmd(
            host,
            f'cd {tmp_cfg_path}',
            'sudo tar -xf archive.tar',
        ),
        term.chain_ssh_cmd(host, InstallCertsCommands()),
        *[term.chain_ssh_cmd(host, *group) for group in groups],
    )

    return result


@progress.with_parent_task
async def uninstall(host, disks, parent_task: progress.TaskNode = None, subtasks: list[progress.TaskNode] = []):
    uninstall_task = await parent_task.add_subtask(f"[yellow]{host} [bold cyan]uninstall", total=1)
    subtasks.append(uninstall_task)
    if await service.check_agent(host):
        nodes = await service.get_processes(host)
        result = await service.cmd_agent_ydb_operation(host, 'uninstall', nodes)
    else:
        result = await term.ssh_run(host, UninstallNodesCommands())

    await uninstall_task.update(advance=1)
    return result


@progress.with_parent_task
async def act_install(hosts, config, reinstall=False, parent_task: progress.TaskNode = None):
    freehost = config['freehost']

    is_installed = any(await for_each_async(hosts, check_installed))
    subtasks = []

    async def clear_subtasks():
        for subtask in subtasks:
            await subtask.update(visible=False)
        subtasks.clear()
        return True

    if reinstall and is_installed:
        logger.error('already installed on some hosts, use --reinstall for force installing')
        await act_uninstall(hosts, parent_task=parent_task, subtasks=subtasks)
    elif is_installed:
        return False

    node_counts = configs.NodeCountByKind(config, hosts)
    dynamic_node_range_per_host = node_counts.dynamic_node_count_by_host()
    nbs_node_range_per_host = node_counts.nbs_node_count_by_host()

    common_hosts = (host for host in hosts if host != freehost)
    prepare_commands = (prepare_host(host, parent_task=parent_task, subtasks=subtasks) for host in hosts)
    install_commands = [
        install_host(
            host,
            node_counts.nodes_per_host,
            dynamic_node_range_per_host[host],
            nbs_node_range_per_host[host],
            parent_task=parent_task,
            subtasks=subtasks,
        )
        for host in common_hosts
    ]
    if freehost:
        install_commands.append(install_host(freehost, 1, parent_task=parent_task, subtasks=subtasks))

    result = await tools.chain_async(
        make_archive_with_configs(),
        tools.parallel_async(*prepare_commands),
        tools.parallel_async(*install_commands),
        term.shell(f'rm -rf {deploy_ctx.work_directory}/tmp'),
        clear_subtasks(),
    )
    return result


@progress.with_parent_task
async def act_uninstall(hosts, parent_task: progress.TaskNode = None, subtasks: list[progress.TaskNode] = None):
    subtasks_2 = subtasks or []

    async def wrapper(*args, **kwargs):
        return await uninstall(*args, **kwargs, parent_task=parent_task, subtasks=subtasks_2)

    result = await common.for_each_host(hosts, [], wrapper)
    if subtasks is None:
        for subtask in subtasks_2:
            await subtask.update(visible=False)

    return result


@progress.with_parent_task
async def act_update_cfg(
    hosts: list[str],
    config,
    nodes: list[str] = None,
    exclude_nodes: list[str] = None,
    parent_task: progress.TaskNode = None,
):
    if not await make_archive_with_configs():
        return False
    nodes_per_host = config['nodes_per_host']
    freehost = config['freehost']
    locations_by_host = common.get_node_locations_by_host(
        hosts, nodes_per_host, freehost, nodes, exclude_nodes
    )
    for host, ids in locations_by_host.items():
        prepare_tmp_dir_task = await parent_task.add_subtask(f"[bold green]Prepare tmp dir on {host}", total=1)
        ok = await prepare_tmp_dir(host)
        await prepare_tmp_dir_task.update(advance=1)
        ok = ok and await install_host(host, ids, parent_task=parent_task)
        if not ok:
            logger.error(f'Operation was stopped by an error on install_host; host# {host} ids# {ids}')
            return False
    return True


@progress.with_parent_task
async def deploy_file(
    hosts: list[str],
    bin_path: str,
    result_path: str,
    parent_task: progress.TaskNode = None,
):
    prepare_hosts_task = await parent_task.add_subtask("[bold green]Prepare hosts", total=len(hosts))
    subtasks = []

    async def prepare_host_task(host):
        result = await prepare_host(host, parent_task=prepare_hosts_task, subtasks=subtasks)
        return result

    async def clear_subtasks():
        for subtask in subtasks:
            await subtask.update(visible=False)
        subtasks.clear()
        return True

    async def clear_prepare_hosts_task():
        await prepare_hosts_task.update(visible=False)
        return True

    update_bin_tasks = [
        tools.parallel_async(*(prepare_host_task(host) for host in hosts)),
        clear_subtasks(),
        clear_prepare_hosts_task(),
    ]
    if deploy_ctx.transit_bin_through_first_node:

        frist_host_task = await parent_task.add_subtask("[bold green]Deploy binary to first node", total=1)

        async def rsync_file_task(host):
            result = await tools.make_runtime_rsync_action(bin_path, host, result_path, frist_host_task)
            if not result:
                frist_host_task._progress.console().print(result.to_rich_panel())
            await frist_host_task.update(advance=1)
            return result

        other_hosts_task = await parent_task.add_subtask("[bold green]Deploy binary to other nodes", total=len(hosts) - 1)

        async def remote_rsync_task(first_node, other_host):
            result = await tools.make_runtime_remote_rsync_action(first_node, result_path, other_host, result_path, other_hosts_task)
            if not result:
                other_hosts_task._progress.console().print(result.to_rich_panel())
            await other_hosts_task.update(advance=1)
            return result

        subtasks += [frist_host_task, other_hosts_task]

        other_hosts = hosts[1:] if len(hosts) > 1 else []
        update_bin_tasks += (
            rsync_file_task(hosts[0]),
            *(remote_rsync_task(hosts[0], host) for host in other_hosts),
            clear_subtasks(),
        )
    else:
        deploy_bin_task = await parent_task.add_subtask("[bold green]Deploy binary", total=len(hosts))

        async def clear_deploy_bin_task():
            await deploy_bin_task.update(visible=False)
            return True

        async def rsync_file_task(host):
            task = await deploy_bin_task.add_subtask(f"[bold cyan]deploy to[/] [yellow]{host}[/]", total=100)
            subtasks.append(task)
            result = await tools.make_runtime_rsync_action(bin_path, host, result_path, task)
            if not result:
                task._progress.console().print(result.to_rich_panel())
            return result

        update_bin_tasks.append(tools.parallel_async(*(rsync_file_task(host) for host in hosts)))
        update_bin_tasks += [clear_subtasks(), clear_deploy_bin_task()]
    return await tools.chain_async(*update_bin_tasks)


@progress.with_parent_task
async def act_update_bin(
    hosts: list[str],
    config,
    parent_task: progress.TaskNode = None,
):
    source_bin_path = deploy_ctx.path_to_bin

    if deploy_ctx.do_rebuild and not deploy_ctx.is_manual_path_to_bin:
        build_ydb_step = tools.make_build_ydb_step(config['build_args'])
        result = await build_ydb_step.run(parent_task=parent_task)
        if not result:
            parent_task._progress.console().print(result)
            return False
        await build_ydb_step._task.update(visible=False)

    if deploy_ctx.do_strip:
        strip_ydb_step = tools.make_strip_ydb_step()
        result = await strip_ydb_step.run(parent_task=parent_task)
        if not result:
            parent_task._progress.console().print(result)
            return False
        await strip_ydb_step._task.update(visible=False)
        source_bin_path = deploy_ctx.get_stripped_bin_path(source_bin_path)

    temp_path = 'ydb'
    final_path = f'{deploy_ctx.deploy_path}/ydb/bin/ydb'

    if deploy_ctx.deploy_path != '/Berkanavt':
        temp_path = final_path

    update_bin_tasks = [deploy_file(hosts, source_bin_path, temp_path, parent_task=parent_task)]

    if deploy_ctx.deploy_path == '/Berkanavt':
        update_bin_tasks.append(
            tools.parallel_async(
                *(
                    term.ssh_run(
                        host,
                        f'mkdir -p {deploy_ctx.deploy_path}; mkdir -p {deploy_ctx.deploy_path}/ydb; mkdir -p {deploy_ctx.deploy_path}/ydb/bin; sudo cp {temp_path} {final_path}',
                    )
                    for host in hosts
                )
            ),
        )

    return await tools.chain_async(*update_bin_tasks)
