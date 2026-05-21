import asyncio
import logging
import types

import ydb.apps.dstool.lib.common as dstool_common
import ydb.apps.dstool.lib.dstool_cmd_nbs_partition_create as dstool_nbs_partition_create
import ydb.apps.dstool.lib.dstool_cmd_nbs_partition_get_load_actor_adapter_actor_id as dstool_nbs_partition_get_actor
import ydb.apps.dstool.lib.dstool_cmd_nbs_partition_io as dstool_nbs_partition_io

from ydb.tools.mnc.lib import common, configs, output, progress
from ydb.tools.mnc.lib.exceptions import CliError
from ydb.tools.mnc.scheme import multinode


logger = logging.getLogger(__name__)


expected_config = multinode.scheme


def get_default_endpoint(hosts: list[str], config: dict):
    configs.init_ports(config)
    return f'grpc://{hosts[0]}:{configs.first_static_grpc_port}'


def storage_media_from_type(disk_type: str):
    if disk_type == 'mem':
        return dstool_nbs_partition_create.nbs.StorageMediaKind.STORAGE_MEDIA_MEMORY
    return dstool_nbs_partition_create.nbs.StorageMediaKind.STORAGE_MEDIA_DEFAULT


def build_create_partition_request(
    disk_id: str,
    blocks_count: int,
    block_size: int,
    pool: str,
    disk_type: str,
    sync_requests_batch_size: int,
):
    return dstool_nbs_partition_create.nbs.CreatePartitionRequest(
        DiskId=disk_id,
        BlockSize=block_size,
        BlocksCount=blocks_count,
        StoragePoolName=pool,
        StorageMedia=storage_media_from_type(disk_type),
        SyncRequestsBatchSize=sync_requests_batch_size,
    )


def build_get_load_actor_adapter_actor_id_request(disk_id: str):
    return dstool_nbs_partition_get_actor.nbs.GetLoadActorAdapterActorIdRequest(DiskId=disk_id)


def build_write_blocks_request(disk_id: str, start_index: int, data: str):
    return dstool_nbs_partition_io.nbs.WriteBlocksRequest(
        DiskId=disk_id,
        StartIndex=start_index,
        Blocks=dstool_nbs_partition_io.nbs.IOVector(Buffers=[data.encode()]),
    )


def build_read_blocks_request(disk_id: str, start_index: int, blocks_count: int):
    return dstool_nbs_partition_io.nbs.ReadBlocksRequest(
        DiskId=disk_id,
        StartIndex=start_index,
        BlocksCount=blocks_count,
    )


def make_dstool_args(endpoint: str, verbose: bool = False, quiet: bool = False):
    return types.SimpleNamespace(
        endpoint=[endpoint],
        grpc_port=2135,
        mon_port=8765,
        token_file=None,
        iam_token_file=None,
        verbose=verbose,
        debug=False,
        quiet=quiet,
        http_timeout=5,
        cafile=None,
        insecure=False,
    )


def invoke_create_partition(endpoint: str, request, verbose: bool = False, quiet: bool = False):
    return invoke_nbs_request(endpoint, 'CreatePartition', request, verbose=verbose, quiet=quiet)


def invoke_nbs_request(endpoint: str, request_type: str, request, verbose: bool = False, quiet: bool = False):
    dstool_common.apply_args(make_dstool_args(endpoint, verbose=verbose, quiet=quiet))
    return dstool_common.invoke_nbs_request(request_type, request)


def _format_create_partition_result(response):
    result = dstool_nbs_partition_create.nbs.CreatePartitionResult()
    response.operation.result.Unpack(result)
    return str(result)


def _format_get_actor_result(response):
    result = dstool_nbs_partition_get_actor.nbs.GetLoadActorAdapterActorIdResult()
    response.operation.result.Unpack(result)
    return result.ActorId or ''


def _format_read_blocks_result(response):
    result = dstool_nbs_partition_io.nbs.ReadBlocksResult()
    response.operation.result.Unpack(result)
    data_buffers = [buf.decode('utf-8', errors='replace') for buf in result.Blocks.Buffers]
    return ''.join(data_buffers) if data_buffers else ''


def _task_result_from_response(step_title: str, response, success_message: str, result_formatter=None):
    if dstool_common.get_status(response):
        message = success_message
        if result_formatter is not None:
            result_text = result_formatter(response)
        else:
            result_text = ''
        if isinstance(result_text, str) and result_text.strip():
            message += f'\n{result_text}'
        return progress.TaskResult(
            level=progress.TaskResultLevel.OK,
            step_title=step_title,
            message=message,
        )

    message = f'{success_message} failed\n{response}'
    return progress.TaskResult(
        level=progress.TaskResultLevel.ERROR,
        step_title=step_title,
        message=message,
    )


async def _invoke_request_as_task_result(args, endpoint: str, request_type: str, request, step_title: str, success_message: str, result_formatter=None):
    try:
        response = await asyncio.to_thread(
            invoke_nbs_request,
            endpoint,
            request_type,
            request,
            getattr(args, 'verbose', False),
            getattr(args, 'quiet', False),
        )
    except (dstool_common.ConnectionError, dstool_common.QueryError) as error:
        logger.error('%s through %s failed: %s', request_type, endpoint, error)
        return progress.TaskResult(
            level=progress.TaskResultLevel.ERROR,
            step_title=step_title,
            message=f'{success_message} failed through {endpoint}: {error}',
        )

    return _task_result_from_response(step_title, response, success_message, result_formatter=result_formatter)


async def _get_nbs_endpoint(args):
    if not configs.is_nbs_enabled(args.config):
        command = getattr(args, 'cmd', 'command')
        raise CliError(f'mnc nbs {command} requires nbs.enabled: true')
    configs.validate_nbs_config(args.config)

    hosts = await common.get_machines(args.config)
    return args.endpoint or get_default_endpoint(hosts, args.config)


async def do_create_disk(args):
    endpoint = await _get_nbs_endpoint(args)
    request = build_create_partition_request(
        disk_id=args.disk_id,
        blocks_count=args.blocks_count,
        block_size=args.block_size,
        pool=args.pool,
        disk_type=args.disk_type,
        sync_requests_batch_size=args.sync_requests_batch_size,
    )

    step_title = f'[bold blue]Create NBS disk[/] [green]{args.disk_id}[/]'
    return await _invoke_request_as_task_result(
        args,
        endpoint,
        'CreatePartition',
        request,
        step_title,
        'NBS disk created',
        result_formatter=_format_create_partition_result,
    )


async def do_get_load_actor_adapter_id(args):
    endpoint = await _get_nbs_endpoint(args)
    request = build_get_load_actor_adapter_actor_id_request(args.disk_id)
    step_title = f'[bold blue]Get NBS load actor adapter id[/] [green]{args.disk_id}[/]'
    return await _invoke_request_as_task_result(
        args,
        endpoint,
        'GetLoadActorAdapterActorId',
        request,
        step_title,
        'NBS load actor adapter id fetched',
        result_formatter=_format_get_actor_result,
    )


async def do_io(args):
    endpoint = await _get_nbs_endpoint(args)
    step_title = f'[bold blue]NBS IO[/] [green]{args.disk_id}[/]'
    if args.operation_type == 'write':
        request = build_write_blocks_request(args.disk_id, args.start_index, args.data)
        return await _invoke_request_as_task_result(
            args,
            endpoint,
            'WriteBlocks',
            request,
            step_title,
            'NBS write completed',
        )
    if args.operation_type == 'read':
        request = build_read_blocks_request(args.disk_id, args.start_index, args.blocks_count)
        return await _invoke_request_as_task_result(
            args,
            endpoint,
            'ReadBlocks',
            request,
            step_title,
            'NBS read completed',
            result_formatter=_format_read_blocks_result,
        )
    raise CliError(f'Unknown NBS IO operation type: {args.operation_type}')


def make_command_step(args):
    actions = {
        'create-disk': (
            f'[bold blue]Create NBS disk[/] [green]{args.disk_id}[/]',
            lambda: do_create_disk(args),
        ),
        'get-load-actor-adapter-id': (
            f'[bold blue]Get NBS load actor adapter id[/] [green]{args.disk_id}[/]',
            lambda: do_get_load_actor_adapter_id(args),
        ),
        'io': (
            f'[bold blue]NBS IO[/] [green]{args.disk_id}[/]',
            lambda: do_io(args),
        ),
    }
    title, action = actions[args.cmd]
    return progress.SimpleStep(title=title, action=action)


def add_arguments(parser):
    subparsers = parser.add_subparsers(help='Commands', dest='cmd', required=True)

    create_disk_parser = subparsers.add_parser('create-disk')
    common.add_common_options(create_disk_parser)
    create_disk_parser.add_argument('--disk-id', required=True)
    create_disk_parser.add_argument('--blocks-count', required=True, type=int)
    create_disk_parser.add_argument('--block-size', default=4096, type=int)
    create_disk_parser.add_argument('--pool', default='ddp1')
    create_disk_parser.add_argument('--type', dest='disk_type', default='ssd')
    create_disk_parser.add_argument('--endpoint', default=None)
    create_disk_parser.add_argument('--sync-requests-batch-size', default=100, type=int)

    get_actor_parser = subparsers.add_parser('get-load-actor-adapter-id')
    common.add_common_options(get_actor_parser)
    get_actor_parser.add_argument('--disk-id', required=True)
    get_actor_parser.add_argument('--endpoint', default=None)

    io_parser = subparsers.add_parser('io')
    common.add_common_options(io_parser)
    io_parser.add_argument('--id', dest='disk_id', required=True)
    io_parser.add_argument('--start-index', '--start_index', dest='start_index', type=int, default=0)
    io_parser.add_argument('--blocks-count', '--blocks_count', dest='blocks_count', type=int, default=1)
    io_parser.add_argument('--data', default='test_data')
    io_parser.add_argument('--type', dest='operation_type', choices=('write', 'read'), default='write')
    io_parser.add_argument('--endpoint', default=None)


async def do(args):
    result = await progress.run_steps([make_command_step(args)], title='[bold]NBS[/]')
    output.get_console().print(result.to_rich_panel(verbose=getattr(args, 'verbose', False)))
    return result
