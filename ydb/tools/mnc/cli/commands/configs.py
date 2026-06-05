import rich

from ydb.tools.mnc.lib import common, configs
from ydb.tools.mnc.scheme import multinode


expected_config = multinode.scheme


def add_arguments(parser):
    subparsers = parser.add_subparsers(help='Commands', dest='cmd', required=True)

    generate_parser = subparsers.add_parser('generate')
    common.add_common_options(generate_parser)


async def do_generate(args):
    console = rich.console.Console()
    hosts = await common.get_machines(args.config)
    success = await configs.act_generate(hosts, args.config)
    console.print('success' if success else 'fail')
    return bool(success)


async def do(args):
    actions = {
        'generate': do_generate,
    }
    return await actions[args.cmd](args)
