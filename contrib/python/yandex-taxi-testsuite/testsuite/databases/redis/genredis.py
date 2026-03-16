import argparse
import pathlib
import string

MASTER_TPL_FILENAME = 'redis_master.conf.tpl'
SENTINEL_TPL_FILENAME = 'redis_sentinel.conf.tpl'
SLAVE_TPL_FILENAME = 'redis_slave.conf.tpl'
CLUSTER_TPL_FILENAME = 'redis_cluster_node.conf.tpl'

SENTINEL_PARAMS = [
    {
        'down_after_milliseconds': 60000,
        'failover_timeout': 180000,
        'parallel_syncs': 1,
    },
    {
        'down_after_milliseconds': 10000,
        'failover_timeout': 180000,
        'parallel_syncs': 5,
    },
]


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output',
        help='Path to output directory',
        type=pathlib.Path,
    )
    parser.add_argument(
        '--host',
        type=int,
        default='localhost',
        help='Redis host for sentinel config',
    )
    parser.add_argument(
        '--master0-port',
        type=int,
        default=16379,
        help='Redis masters0 port',
    )
    parser.add_argument(
        '--master1-port',
        type=int,
        default=16389,
        help='Redis masters0 port',
    )
    parser.add_argument(
        '--slave0-ports',
        type=int,
        default=16380,
        help='Redis slave0 port',
    )
    parser.add_argument(
        '--slave1-ports',
        type=int,
        default=16390,
        help='Redis slave1 port',
    )
    parser.add_argument(
        '--slave2-ports',
        type=int,
        default=16381,
        help='Redis slave2 port',
    )
    parser.add_argument(
        '--sentinel-port',
        type=int,
        default=26379,
        help='Redis sentinel port',
    )
    parser.add_argument(
        '--cluster-port',
        type=int,
        nargs='+',
        default=[17380, 17381, 17382, 17383, 17384, 17385],
        help='Redis cluster port',
    )
    return parser.parse_args()


def _generate_redis_config(
    input_file: pathlib.Path,
    output_file: pathlib.Path,
    host: str,
    port: int,
    master_port: int | None = None,
) -> None:
    config_tpl = input_file.read_text()
    config_body = string.Template(config_tpl).substitute(
        host=host,
        port=port,
        master_port=master_port,
    )
    output_file.write_text(config_body)


def _generate_master(
    host: str,
    port: int,
    output_path: pathlib.Path,
    index: int,
) -> None:
    input_file = _redis_config_directory() / MASTER_TPL_FILENAME
    output_file = _construct_output_filename(
        output_path,
        MASTER_TPL_FILENAME,
        index,
    )
    _generate_redis_config(
        input_file,
        output_file,
        host,
        port,
    )


def _generate_slave(
    host: str,
    port: int,
    master_port: int,
    output_path: pathlib.Path,
    index: int,
) -> None:
    input_file = _redis_config_directory() / SLAVE_TPL_FILENAME
    output_file = _construct_output_filename(
        output_path,
        SLAVE_TPL_FILENAME,
        index,
    )
    _generate_redis_config(
        input_file,
        output_file,
        host,
        port,
        master_port,
    )


def _generate_sentinel(
    host: str,
    sentinel_port: int,
    ports: list[int],
    output_path: pathlib.Path,
    params: list,
) -> None:
    input_file = _redis_config_directory() / SENTINEL_TPL_FILENAME
    lines = ['daemonize yes', 'port %d' % sentinel_port, '']
    config_tpl = input_file.read_text()

    for index, (port, param) in enumerate(zip(ports, params)):
        config_body = string.Template(config_tpl).substitute(
            index=index,
            host=host,
            port=port,
            **param,
        )
        lines.append(config_body)

    output_path.joinpath('redis_sentinel.conf').write_text('\n'.join(lines))


def _generate_cluster_node(
    host: str,
    port: int,
    output_path: pathlib.Path,
    index: int,
) -> None:
    input_file = _redis_config_directory() / CLUSTER_TPL_FILENAME
    output_file = _construct_output_filename(
        output_path,
        CLUSTER_TPL_FILENAME,
        index,
    )

    _generate_redis_config(
        input_file,
        output_file,
        host,
        port,
    )


def _construct_output_filename(
    output_path: pathlib.Path,
    tpl_filename: str,
    number: int,
) -> pathlib.Path:
    name = tpl_filename.split('.', 1)[0]
    config_filename = ''.join((name, str(number), '.conf'))
    return output_path / config_filename


def _redis_config_directory() -> pathlib.Path:
    return pathlib.Path(__file__).parent / 'configs'


def generate_cluster_redis_configs(
    output_path: pathlib.Path,
    host: str,
    cluster_ports: tuple[int, ...],
) -> None:
    _generate_cluster_node(
        host,
        6379,
        output_path,
        0,
    )


def generate_standalone_redis_config(
    output_path: pathlib.Path,
    host: str,
    port: int,
) -> None:
    input_file = _redis_config_directory() / MASTER_TPL_FILENAME
    output_file = output_path / 'test_standalone_master0.conf'

    _generate_redis_config(
        input_file,
        output_file,
        host,
        port,
    )


def generate_redis_configs(
    output_path: pathlib.Path,
    host: str,
    master0_port: int,
    master1_port: int,
    slave0_port: int,
    slave1_port: int,
    slave2_port: int,
    sentinel_port: int,
) -> None:
    _generate_master(host, master0_port, output_path, 0)
    _generate_master(host, master1_port, output_path, 1)

    _generate_slave(
        host,
        slave0_port,
        master0_port,
        output_path,
        0,
    )
    _generate_slave(
        host,
        slave1_port,
        master1_port,
        output_path,
        1,
    )
    _generate_slave(
        host,
        slave2_port,
        master0_port,
        output_path,
        2,
    )

    _generate_sentinel(
        host,
        sentinel_port,
        [master0_port, master1_port],
        output_path,
        SENTINEL_PARAMS,
    )


def main():
    args = _parse_args()
    generate_redis_configs(
        output_path=args.output,
        host=args.host,
        master0_port=args.master0_port,
        master1_port=args.master1_port,
        slave0_port=args.slave0_port,
        slave1_port=args.slave1_port,
        slave2_port=args.slave2_port,
        sentinel_port=args.sentinel_port,
    )
    generate_cluster_redis_configs(
        output_path=args.output,
        host=args.host,
        cluster_ports=args.cluster_port,
    )


if __name__ == '__main__':
    main()
