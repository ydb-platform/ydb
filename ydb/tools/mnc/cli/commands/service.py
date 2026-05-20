import rich

from ydb.tools.mnc.lib import common, service
from ydb.tools.mnc.scheme import multinode


expected_config = multinode.scheme


def add_arguments(parser):
    subparsers = parser.add_subparsers(help='Commands', dest='cmd', required=True)

    hosts_parser = subparsers.add_parser('hosts')
    common.add_common_options(hosts_parser)
    hosts_parser.add_argument('operation', choices=tuple(service.allowed_commands))
    hosts_parser.add_argument('--node-type', '--node_type', dest='node_type', choices=tuple(service.allowed_node_types), default=None)

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


async def do_hosts(args):
    hosts = await common.get_machines(args.config)
    return await service.act_hosts(args.operation, hosts, args.node_type)


async def do_nodes(args):
    hosts = await common.get_machines(args.config)
    return await service.act_nodes(
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


async def do(args):
    console = rich.console.Console()
    actions = {
        'hosts': do_hosts,
        'nodes': do_nodes,
    }
    ok = await actions[args.cmd](args)
    console.print('success' if ok else 'operation failed')
    return bool(ok)
