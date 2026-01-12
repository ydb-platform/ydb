# -*- coding: utf-8 -*-
import argparse
import logging
import sys
from google.protobuf import json_format, text_format
from ydb.tests.stress.kv_volume.workload import YdbKeyValueVolumeWorkload
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
    parser = argparse.ArgumentParser(
        description="Workload kv wrapper", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--endpoint', default='grpc://localhost:2135', help="YDB endpoint")
    parser.add_argument('--database', default=None, required=True, help='A database to connect')
    parser.add_argument('--duration', default=120, type=lambda x: int(x), help='A duration of workload in seconds')
    parser.add_argument('--load-type', default="read", choices=["read", "read-inline"], help='Load type for kv volumes')
    parser.add_argument('--in-flight', default=1, type=int, help='In flight')
    parser.add_argument('--log-file', default=None, help='Append log into specified file')
    parser.add_argument('--version', default='v1', choices=['v1', 'v2'], help='Keyvalue grpc api version')
    parser.add_argument('--config', required=True, help='Path to KeyValueVolumeStressLoad config (textproto/json/binary)')

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
    if args.config:
        cfg = load_config(args.config)

    workload = YdbKeyValueVolumeWorkload(
        args.endpoint,
        args.database,
        duration=args.duration,
        kv_load_type=args.load_type,
        worker_count=args.in_flight,
        version=args.version,
        config=cfg
    )
    workload.start(use_multiprocessing=True)
    workload.wait_stop()
