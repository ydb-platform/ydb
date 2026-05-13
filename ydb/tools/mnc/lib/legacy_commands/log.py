import logging

import ydb.tools.mnc.lib.common as common
import ydb.tools.mnc.lib.deploy_ctx as deploy_ctx
import ydb.tools.mnc.lib.term as term
import ydb.tools.mnc.lib.tools as tools

from ydb.tools.mnc.scheme import multinode


logger = logging.getLogger(__name__)


expected_config = multinode.scheme


async def cmd(host: str, process: str):
    if deploy_ctx.use_services:
        paths = f'{deploy_ctx.deploy_path}/{process}/logs/kikimr.log'
    else:
        paths = f'{deploy_ctx.deploy_path}/{process}/stdout'
    cmd_arg = paths
    result = await term.ssh_run(host, f'cat {cmd_arg}')
    lines = result.stdout.splitlines()
    print(f'Logs from {host} path: "{cmd_arg}"')
    for line in lines:
        print(f'{host} | {line}')
    print(result.stderr)
    return True


async def one_host(host: str, test_kikimr_ids: list[int] = None, node_type: str = None, force=False):
    processes = []
    prefix = 'test_kikimr'
    if node_type == "dynamic":
        prefix = 'test_kikimr_dynamic'
    if node_type == "static":
        prefix = 'test_kikimr_static'
    if test_kikimr_ids is None:
        processes = (
            await term.ssh_run(host, f'cd {deploy_ctx.deploy_path}; ls | grep {prefix}', silent_error=True)
        ).stdout.split()
        processes = [proc for proc in processes if proc]
    else:
        processes = ['{1}_{0}'.format(id, prefix) for id in test_kikimr_ids]
    for process in processes:
        await cmd(host, process)
    return True


async def act(
    hosts: list[str],
    config: dict,
    nodes: list[str] = None,
    exclude_nodes: list[str] = None,
    type: str = None,
):
    nodes_per_host = config['nodes_per_host']
    freehost = config['freehost']
    locations_by_host = common.get_node_locations_by_host(
        hosts, nodes_per_host, freehost, nodes, exclude_nodes
    )
    if type != 'static':
        print('The operation was not implemented for dynamic nodes')
        return False
    return await tools.chain_async(
        *(
            one_host(host)
            for host, _ in locations_by_host.items()
        )
    )


def add_arguments(parser):
    common.add_common_options(parser)
    parser.add_argument('--nodes', '-N', dest='nodes', nargs='*', default=None, help='default: All')
    parser.add_argument('--exclude-nodes', '--exclude_nodes', dest='exclude_nodes', nargs='*', default=None)
    parser.add_argument(
        '--type', '-t', dest='type', choices=('static', 'dynamic', 'all'), default='static', help='default: static'
    )


async def do(args):
    hosts = await common.get_machines(args.config)
    await act(
        list(hosts),
        args.config,
        args.nodes,
        args.exclude_nodes,
        args.type,
    )
