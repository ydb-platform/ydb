import argparse
import random


def parse_optional_arguments(args):
    kwargs = {}
    # Optional arguments
    if hasattr(args, 'seed') and args.seed is not None:
        random.seed(args.seed)
    if hasattr(args, 'grpc_endpoint') and args.grpc_endpoint is not None:
        kwargs['grpc_endpoint'] = args.grpc_endpoint

    if hasattr(args, 'database') and args.database is not None:
        kwargs['database'] = args.database
    if hasattr(args, 'tenant') and args.tenant is not None:
        kwargs['database'] = args.tenant
    if hasattr(args, 'node_broker_port') and args.node_broker_port is not None:
        kwargs['node_broker_port'] = args.node_broker_port
    if hasattr(args, 'walle_url') and args.walle_url is not None:
        kwargs['walle_url'] = args.walle_url

    kwargs['enable_cores'] = args.enable_cores

    for cmd_arg in ('ic_port', 'grpc_port', 'mbus_port', 'mon_port', 'cfg_home', 'sqs_port', 'binaries_home'):
        if hasattr(args, cmd_arg) and getattr(args, cmd_arg) is not None:
            kwargs[cmd_arg] = getattr(args, cmd_arg)

    if args.local_binary_path is not None:
        kwargs['local_binary_path'] = args.local_binary_path

    return kwargs


def get_parser(generate_func, extra_cfg_arguments=[]):
    parser = argparse.ArgumentParser(
        description='Tool to generate configuration files for YDB cluster',
        epilog="Additional info could be found on a documentation page: https://ydb.tech/docs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(help='sub-command help')
    parser_cfg = subparsers.add_parser('cfg', help='run static config generation')

    parser_cfg.add_argument(
        "cluster_description",
        type=str,
        help="Path to the cluster description",
    )
    parser_cfg.add_argument("binary_path", type=str, help="Path to the KiKiMR's binary")
    parser_cfg.add_argument("output_dir", type=str, help="Path to store configuration files")
    parser_cfg.add_argument("--seed", type=int, help='Force cfg command to use special seed in instantiate random')
    parser_cfg.add_argument("--grpc-endpoint", type=str, help='gRPC endpoint to use in scripts')
    parser_cfg.add_argument(
        '--database', type=str, help='Database to serve (possible options: name of domain or no for pure storage nodes)'
    )
    parser_cfg.add_argument(
        '--tenant', type=str, help='Tenant to serve (possible options: name of domain or no for pure storage nodes)'
    )
    parser_cfg.add_argument('--enable-cores', action='store_true', help='Enables coredumps')
    parser_cfg.add_argument(
        '--dynamic-node', action='store_true', help='Indicates that configuration should be generated for dynamic node'
    )
    parser_cfg.add_argument('--node-broker-port', type=str, help='Node Broker Port to use')
    parser_cfg.add_argument('--cfg-home', type=str, help='Configuration home directory')
    parser_cfg.add_argument('--binaries-home', type=str, help='Binaries home directory')
    parser_cfg.add_argument('--local-binary-path', type=str, help='Path to kikimr binary current host')
    for port in ('grpc-port', 'ic-port', 'mon-port', 'mbus-port', 'sqs-port'):
        parser_cfg.add_argument('--%s' % port, type=str, help='Port to be used during KiKiMR server start')

    for v in extra_cfg_arguments:
        parser_cfg.add_argument(
            v['name'],
            help=v['help'],
        )

    argument_group = parser_cfg.add_mutually_exclusive_group()

    argument_group.add_argument(
        '--static', action='store_true', help='Forces cfg command to generate static configuration'
    )
    argument_group.add_argument(
        '--dynamic', action='store_true', help='Forces cfg command to generate dynamic configuration'
    )
    argument_group.add_argument('--nbs', action='store_true', help='Forces cfg command to generate NBS configuration')
    argument_group.add_argument(
        '--nbs-control', action='store_true', help='Forces cfg command to generate NBS Control configuration'
    )
    argument_group.add_argument('--nfs', action='store_true', help='Forces cfg command to generate NFS configuration')
    argument_group.add_argument(
        '--nfs-control', action='store_true', help='Forces cfg command to generate NFS Control configuration'
    )

    parser_cfg.set_defaults(func=generate_func)
    return parser
