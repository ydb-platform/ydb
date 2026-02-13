# -*- coding: utf-8 -*-
import argparse
import logging
import sys
from google.protobuf import json_format, text_format
from ydb.tests.stress.kv_volume.workload import YdbKeyValueVolumeWorkload, generate_preset_configs
import ydb.tests.stress.kv_volume.protos.config_pb2 as config_pb


def load_config(path):
    """Load KeyValueVolumeStressLoad from textproto/json/binary file."""
    try:
        with open(path, 'rb') as f:
            data = f.read()
    except Exception as e:
        logging.error("Failed to read config file %s: %s", path, e)
        sys.exit(1)

    msg = config_pb.KeyValueVolumeStressLoad()

    # Try text (proto text format) or JSON
    try:
        text = data.decode('utf-8')
        if text.lstrip().startswith('{'):
            json_format.Parse(text, msg)
            return msg
        else:
            text_format.Parse(text, msg)
            return msg
    except Exception:
        pass

    # Try binary proto
    try:
        msg.ParseFromString(data)
        return msg
    except Exception as e:
        logging.error("Failed to parse config file %s: %s", path, e)
        sys.exit(1)


if __name__ == '__main__':
    generated_configs = generate_preset_configs()

    parser = argparse.ArgumentParser(
        description="Workload kv wrapper", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--endpoint', default='grpc://localhost:2135', help="YDB endpoint")
    parser.add_argument('--database', default=None, required=True, help='A database to connect')
    parser.add_argument('--duration', default=120, type=lambda x: int(x), help='A duration of workload in seconds')
    parser.add_argument('--in-flight', default=1, type=int, help='In flight')
    parser.add_argument('--log-file', default=None, help='Append log into specified file')
    parser.add_argument('--version', default='v1', choices=['v1', 'v2'], help='Keyvalue grpc api version')

    config_group = parser.add_mutually_exclusive_group(required=True)
    config_group.add_argument('--config', help='Path to KeyValueVolumeStressLoad config (textproto/json/binary)')
    config_group.add_argument('--config-name', choices=generated_configs.keys(), help='Name of preset config to use')

    args = parser.parse_args()

    if args.log_file:
        logging.basicConfig(
            filename=args.log_file,
            filemode='a',
            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
            datefmt='%H:%M:%S',
            level=logging.INFO
        )

    # Resolve parameters, optionally overriding from protobuf config
    cfg = None
    if args.config_name:
        preset_configs = generate_preset_configs()
        if args.config_name not in preset_configs:
            logging.error("Unknown preset config name: %s", args.config_name)
            logging.info("Available presets: %s", ', '.join(preset_configs.keys()))
            sys.exit(1)
        cfg = preset_configs[args.config_name]
    else:
        cfg = load_config(args.config)

    workload = YdbKeyValueVolumeWorkload(
        args.endpoint,
        args.database,
        duration=args.duration,
        worker_count=args.in_flight,
        version=args.version,
        config=cfg,
        verbose=False,
        show_stats=True
    )
    workload.start(use_multiprocessing=True)
    workload.wait_stop()
