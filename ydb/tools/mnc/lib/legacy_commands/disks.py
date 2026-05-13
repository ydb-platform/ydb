import logging

from ydb.tools.mnc.lib import common, deploy_ctx, parted, term, tools, progress
from ydb.tools.mnc.scheme import multinode


logger = logging.getLogger(__name__)

expected_config = multinode.scheme


async def get_remote_partlabels(host: str, pattern: str):
    if pattern:
        cmd = "sudo ls /dev/disk/by-partlabel | grep '{0}'".format(pattern)
    else:
        cmd = "sudo ls /dev/disk/by-partlabel"

    result = await term.ssh_run(host, cmd)
    if result.returncode:
        return None
    return [x for x in result.stdout.strip().split('\n') if x]


async def check_link(host: str, partlabel: str, path: str):
    remote_path = await parted.get_link(host, partlabel)
    ok = remote_path is not None and remote_path.startswith(path)
    if not ok:
        logger.error(
            'link isn\'t correct host: {0} partlabel: {1} path: {2} real_path: {3}'.format(
                host, partlabel, path, remote_path
            )
        )
    return ok


async def check_disk_for_use(host: str, disk: dict):
    cmd_remote_partlabels = await get_remote_partlabels(host, disk['partlabel'])
    if cmd_remote_partlabels is not None:
        return any((disk['partlabel'] == partlabel for partlabel in cmd_remote_partlabels))
    return False


async def check_disk_for_split(host: str, disk: dict):
    cmd_remote_partlabels = await get_remote_partlabels(host, disk['partlabel'])
    if cmd_remote_partlabels is None:
        return False
    good_remote_partlabels = (x for x in cmd_remote_partlabels if x.startswith(disk['partlabel']))
    return await tools.chain_async(
        *(
            check_link(host, remote_partlabel, disk['device'])
            for remote_partlabel in good_remote_partlabels
        )
    )


async def check_devices(host: str, disks: list):
    disks_for_split = disks.get('disks_for_split', [])
    disks_for_use = disks.get('disks_for_use', [])
    return await tools.chain_async(
        tools.parallel_async(
            *(check_disk_for_split(host, disk) for disk in disks_for_split)
        ),
        tools.parallel_async(
            *(check_disk_for_use(host, disk) for disk in disks_for_use)
        )
    )


async def act_check(hosts: list[str], config: dict):
    return all(await common.for_each_host(hosts, config['disks'], check_devices))


async def info(host: str, disks: dict):
    res = [host]
    for disk in disks['disks_for_split']:
        device = common.Device(disk['partlabel'], disk['device'])
        info = await parted.parted_info(host, device)
        if info is None:
            logger.error(f"skip device partlabel: {disk['partlabel']}")
            continue
        res.append(' '.join((' ', info.path, str(info.size))))
        for part in info.parts:
            res.append(' '.join((' ' * 3, part.id, str(part.from_mem), str(part.to_mem), str(part.size), part.label)))

    for disk in disks['disks_for_use']:
        device = common.Device(disk['partlabel'], None)
        info = await parted.parted_info(host, device)
        if info is None:
            logger.error(f"skip device partlabel: {disk['partlabel']}")
            continue
        res.append(' '.join((' ', info.path, str(info.size))))
        for part in info.parts:
            res.append(' '.join((' ' * 3, part.id, str(part.from_mem), str(part.to_mem), str(part.size), part.label)))
    return res


async def act_info(hosts: list[str], config):
    return await common.for_each_host(hosts, config['disks'], info)


async def split(host: str, devices: list[common.Device], part_count: int = None, part_size: common.Memory = None, parent_task: progress.TaskNode = None, subtasks: list[progress.TaskNode] = []):
    if len(devices) == 0:
        return True
    action = "split" if part_count is not None else "unite"
    task = await parent_task.add_subtask(f"[yellow]{host} [bold cyan]{action} disks", total=len(devices))
    subtasks.append(task)

    for device in devices:
        await parted.parted_disk(host, device, part_size=part_size, part_count=part_count)
        await task.update(advance=1)
    return True


@progress.with_parent_task
async def act_split(hosts: list[str], config, part_count: int = None, part_size: common.Memory = None, parent_task: progress.TaskNode = None):
    disks_for_split = common.get_host_disks_for_split(config)
    check_result = await act_check(hosts, config)
    if not check_result:
        logger.error("disks checking wasn't passed")
        return False
    subtasks = []
    result = await tools.chain_async(
        *(
            split(host, disks_for_split.get(host, []), part_count, part_size, parent_task=parent_task, subtasks=subtasks)
            for host in hosts
        )
    )
    for subtask in subtasks:
        await subtask.update(visible=False)
    return result


@progress.with_parent_task
async def act_unite(hosts: list[str], config, parent_task: progress.TaskNode = None):
    return await act_split(hosts, config, part_count=1, parent_task=parent_task)


def obliterate(host, disk_label):
    return term.ssh_run(
        host,
        f"sudo {deploy_ctx.deploy_path}/kikimr/bin/kikimr admin blobstorage disk obliterate /dev/disk/by-partlabel/{disk_label}",
    )


async def format_disk(host: str, device: common.Device):
    info = await parted.parted_info(host, device)
    with_error = False
    for part in info.parts:
        if part.id == '1' or not part.label:
            continue
        path = device.path
        ok = await obliterate(host, part.label)
        if not ok:
            partlabel = device.partlabel
            logger.error(
                'formating was failed; host: {host} device: {device} partlabel: {label} part: {part}'.format(
                    host=host, device=path, label=partlabel, part=part.id
                )
            )
            with_error = True
    if with_error:
        return False
    return True


async def format(host: str, devices: list[common.Device], parent_task: progress.TaskNode = None, subtasks: list[progress.TaskNode] = []):
    if len(devices) == 0:
        return True
    task = await parent_task.add_subtask(f"[yellow]{host} [bold cyan]format disks", total=len(devices))
    subtasks.append(task)

    async def wrapper(disk):
        result = await format_disk(host, disk)
        await task.update(advance=1)
        return result

    fs = [wrapper(disk) for disk in devices]
    return await tools.chain_async(*fs)


@progress.with_parent_task
async def act_obliterate(hosts, config, parent_task: progress.TaskNode = None):
    disks = common.get_host_disks(config)
    subtasks = []
    result = await tools.parallel_async(
        *(
            format(host, disks.get(host, []), parent_task=parent_task, subtasks=subtasks)
            for host in hosts
        )
    )
    for subtask in subtasks:
        await subtask.update(visible=False)
    return result


def add_arguments(parser):
    subparsers = parser.add_subparsers(help='Commands', dest='cmd', required=True)

    check_parser = subparsers.add_parser('check')
    common.add_common_options(check_parser)

    info_parser = subparsers.add_parser('info')
    common.add_common_options(info_parser)

    split_parser = subparsers.add_parser('split')
    common.add_common_options(split_parser)
    split_parser.add_argument(
        '--part_count', type=int, default=None, help='count of parts which were made from each disk'
    )
    split_parser.add_argument('--part-size', '--part_size', type=common.Memory, default=None, help='size of each part')

    unite_parser = subparsers.add_parser('unite')
    common.add_common_options(unite_parser)

    obliterate_parser = subparsers.add_parser('obliterate')
    common.add_common_options(obliterate_parser)


async def do_check(args):
    hosts = await common.get_machines(args.config)
    ok = await act_check(hosts, args.config)
    if ok:
        print('all disks are ok')
    else:
        print('some disks aren\'t ok')


async def do_info(args):
    hosts = await common.get_machines(args.config)
    res = await act_info(hosts, args.config)
    for report in res:
        for line in report:
            print(line)


async def do_split(args):
    hosts = await common.get_machines(args.config)
    await act_split(hosts, args.config, part_count=args.part_count, part_size=args.part_size)


async def do_unite(args):
    hosts = await common.get_machines(args.config)
    await act_unite(hosts, args.config)


async def do_obliterate(args):
    hosts = await common.get_machines(args.config)
    await act_obliterate(hosts, args.config)


async def do(args):
    if args.cmd == 'check':
        await do_check(args)
    if args.cmd == 'info':
        await do_info(args)
    if args.cmd == 'split':
        await do_split(args)
    if args.cmd == 'unite':
        await do_unite(args)
    if args.cmd == 'obliterate':
        await do_obliterate(args)
