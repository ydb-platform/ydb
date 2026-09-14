import rich

from ydb.tools.mnc.lib import common, init
from ydb.tools.mnc.scheme import multinode


expected_config = multinode.scheme


def add_arguments(parser):
    subparsers = parser.add_subparsers(help='Commands', dest='cmd', required=True)

    static_parser = subparsers.add_parser('static')
    common.add_common_options(static_parser)

    dynamic_parser = subparsers.add_parser('dynamic')
    common.add_common_options(dynamic_parser)


async def do_static(args):
    console = rich.console.Console()
    success = await init.act_static(args.config)
    console.print('success' if success else 'fail')
    return bool(success)


async def do_dynamic(args):
    console = rich.console.Console()
    success = await init.act_dynamic(args.config)
    console.print('success' if success else 'fail')
    return bool(success)


async def do(args):
    actions = {
        'static': do_static,
        'dynamic': do_dynamic,
    }
    return await actions[args.cmd](args)
