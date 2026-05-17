import asyncio
import logging

from ydb.tools.mnc.lib import agent_client, common, progress, tools
from ydb.tools.mnc.scheme import multinode


logger = logging.getLogger(__name__)

expected_config = multinode.scheme
INFO_AGENT_FAILURE_MESSAGE = " failed to receive disk info from agent"


def _device_to_json(device: common.Device):
    return {
        "partlabel": device.partlabel,
        "device": device.path,
    }


def _raw_disk_to_json(disk: dict):
    return {
        "partlabel": disk["partlabel"],
        "device": disk.get("device"),
    }


async def check_devices(host: str, disks: dict):
    payload = {
        "disks_for_split": [_raw_disk_to_json(disk) for disk in disks.get("disks_for_split", [])],
        "disks_for_use": [_raw_disk_to_json(disk) for disk in disks.get("disks_for_use", [])],
    }
    result = await agent_client.post_json(host, "/disks/check", payload)
    if result is None:
        return False
    for check in result.get("checks", []):
        if not check.get("success"):
            logger.error(f"disk check failed on {host}: {check.get('partlabel')}: {check.get('message')}")
    return result.get("success", False)


async def act_check(hosts: list[str], config: dict):
    return all(await common.for_each_host(hosts, config["disks"], check_devices))


def _format_disk_info(disk: dict):
    if disk.get("error"):
        return [" ".join((" ", disk.get("path") or disk.get("device") or disk["partlabel"], disk["error"]))]
    res = [" ".join((" ", disk.get("path") or "", disk.get("size") or ""))]
    for part in disk.get("parts", []):
        res.append(" ".join((" " * 3, part["id"], part["from_mem"], part["to_mem"], part["size"], part["label"])))
    return res


async def info(host: str):
    result = await agent_client.post_json(host, "/disks/info", {})
    if result is None:
        return [host, INFO_AGENT_FAILURE_MESSAGE]

    res = [host]
    for disk in result.get("disks", []):
        if disk.get("error"):
            logger.error(f"skip device partlabel: {disk.get('partlabel')}: {disk.get('error')}")
        res.extend(_format_disk_info(disk))
    return res


async def act_info(hosts: list[str], config):
    return await asyncio.gather(*(info(host) for host in hosts))


async def split(
    host: str,
    devices: list[common.Device],
    part_count: int = None,
    part_size: common.Memory = None,
    parent_task: progress.TaskNode = None,
):
    if len(devices) == 0:
        return True
    task = await parent_task.add_subtask(f"[yellow]{host} [bold cyan]split disks", total=len(devices))
    payload = {
        "devices": [_device_to_json(device) for device in devices],
        "part_count": part_count,
        "part_size": str(part_size) if part_size is not None else None,
    }
    result = await agent_client.post_json(host, "/disks/split", payload, parent_task=task)
    if result is None:
        await task.update(advance=len(devices), visible=False)
        return False
    for operation in result.get("operations", []):
        await task.update(advance=1)
        if not operation.get("success"):
            logger.error(f"disk split failed on {host}: {operation.get('partlabel')}: {operation.get('message')}")
    await task.update(visible=False)
    return result.get("success", False)


@progress.with_parent_task
async def act_split(hosts: list[str], config, part_count: int = None, part_size: common.Memory = None, parent_task: progress.TaskNode = None):
    disks_for_split = common.get_host_disks_for_split(config)
    check_result = await act_check(hosts, config)
    if not check_result:
        logger.error("disks checking wasn't passed")
        return False
    return await tools.chain_async(
        *(
            split(host, disks_for_split.get(host, []), part_count, part_size, parent_task=parent_task)
            for host in hosts
        )
    )


async def unite(host: str, devices: list[common.Device], parent_task: progress.TaskNode = None):
    if len(devices) == 0:
        return True
    task = await parent_task.add_subtask(f"[yellow]{host} [bold cyan]unite disks", total=len(devices))
    result = await agent_client.post_json(host, "/disks/unite", {"devices": [_device_to_json(device) for device in devices]}, parent_task=task)
    if result is None:
        await task.update(advance=len(devices), visible=False)
        return False
    for operation in result.get("operations", []):
        await task.update(advance=1)
        if not operation.get("success"):
            logger.error(f"disk unite failed on {host}: {operation.get('partlabel')}: {operation.get('message')}")
    await task.update(visible=False)
    return result.get("success", False)


@progress.with_parent_task
async def act_unite(hosts: list[str], config, parent_task: progress.TaskNode = None):
    disks_for_split = common.get_host_disks_for_split(config)
    return await tools.chain_async(
        *(
            unite(host, disks_for_split.get(host, []), parent_task=parent_task)
            for host in hosts
        )
    )


async def obliterate(host: str, devices: list[common.Device], parent_task: progress.TaskNode = None):
    if len(devices) == 0:
        return True
    task = await parent_task.add_subtask(f"[yellow]{host} [bold cyan]obliterate disks", total=len(devices))
    result = await agent_client.post_json(host, "/disks/obliterate", {"devices": [_device_to_json(device) for device in devices]}, parent_task=task)
    if result is None:
        await task.update(advance=len(devices), visible=False)
        return False
    for operation in result.get("operations", []):
        await task.update(advance=1)
        if not operation.get("success"):
            logger.error(f"disk obliterate failed on {host}: {operation.get('partlabel')}: {operation.get('message')}")
    await task.update(visible=False)
    return result.get("success", False)


@progress.with_parent_task
async def act_obliterate(hosts, config, parent_task: progress.TaskNode = None):
    disks = common.get_host_disks(config)
    return await tools.parallel_async(
        *(
            obliterate(host, disks.get(host, []), parent_task=parent_task)
            for host in hosts
        )
    )


def add_arguments(parser):
    subparsers = parser.add_subparsers(help="Commands", dest="cmd", required=True)

    check_parser = subparsers.add_parser("check")
    common.add_common_options(check_parser)

    info_parser = subparsers.add_parser("info")
    common.add_common_options(info_parser)

    split_parser = subparsers.add_parser("split")
    common.add_common_options(split_parser)
    split_size = split_parser.add_mutually_exclusive_group(required=True)
    split_size.add_argument("--part_count", type=int, default=None, help="count of parts which were made from each disk")
    split_size.add_argument("--part-size", "--part_size", type=common.Memory, default=None, help="size of each part")

    unite_parser = subparsers.add_parser("unite")
    common.add_common_options(unite_parser)

    obliterate_parser = subparsers.add_parser("obliterate")
    common.add_common_options(obliterate_parser)


async def do_check(args):
    hosts = await common.get_machines(args.config)
    ok = await act_check(hosts, args.config)
    if ok:
        print("all disks are ok")
    else:
        print("some disks aren't ok")
    return ok


async def do_info(args):
    hosts = await common.get_machines(args.config)
    res = await act_info(hosts, args.config)
    ok = True
    for report in res:
        if INFO_AGENT_FAILURE_MESSAGE in report:
            ok = False
        for line in report:
            print(line)
    return ok


async def do_split(args):
    hosts = await common.get_machines(args.config)
    return await act_split(hosts, args.config, part_count=args.part_count, part_size=args.part_size)


async def do_unite(args):
    hosts = await common.get_machines(args.config)
    return await act_unite(hosts, args.config)


async def do_obliterate(args):
    hosts = await common.get_machines(args.config)
    return await act_obliterate(hosts, args.config)


async def do(args):
    actions = {
        "check": do_check,
        "info": do_info,
        "split": do_split,
        "unite": do_unite,
        "obliterate": do_obliterate,
    }
    return await actions[args.cmd](args)
