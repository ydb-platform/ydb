import rich

from ydb.tools.mnc.lib import common, deploy, deploy_ctx
from ydb.tools.mnc.scheme import multinode


expected_config = multinode.scheme


def add_arguments(parser):
    subparsers = parser.add_subparsers(help='Commands', dest='cmd', required=True)

    install_parser = subparsers.add_parser('install')
    common.add_common_options(install_parser)
    install_parser.add_argument('--reinstall', dest='reinstall', action='store_const', const=True, default=False)

    uninstall_parser = subparsers.add_parser('uninstall')
    common.add_common_options(uninstall_parser)

    update_cfg_parser = subparsers.add_parser('update_cfg')
    common.add_common_options(update_cfg_parser)
    update_cfg_parser.add_argument('--nodes', '-N', dest='nodes', nargs='*', default=None, help='default: All')
    update_cfg_parser.add_argument('--exclude-nodes', '--exclude_nodes', dest='exclude_nodes', nargs='*', default=None)

    update_bin_parser = subparsers.add_parser('update_bin')
    common.add_common_options(update_bin_parser)
    update_bin_parser.add_argument('--bin-path', '--bin_path', default=None, type=str, help='path to binary file')


async def do_install(args):
    hosts = await common.get_machines(args.config)
    return await deploy.act_install(hosts, args.config, reinstall=args.reinstall)


async def do_uninstall(args):
    hosts = await common.get_machines(args.config)
    return await deploy.act_uninstall(hosts)


async def do_update_cfg(args):
    hosts = await common.get_machines(args.config)
    return await deploy.act_update_cfg(list(hosts), args.config, args.nodes, args.exclude_nodes)


async def do_update_bin(args):
    hosts = await common.get_machines(args.config)
    if args.bin_path is not None:
        deploy_ctx.update_path_to_bin(args.bin_path)
    return await deploy.act_update_bin(hosts, args.config)


async def do(args):
    console = rich.console.Console()
    actions = {
        'install': do_install,
        'uninstall': do_uninstall,
        'update_cfg': do_update_cfg,
        'update_bin': do_update_bin,
    }
    ok = await actions[args.cmd](args)
    console.print('success' if ok else 'failed')
    return bool(ok)
