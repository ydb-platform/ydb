# -*- coding: utf-8 -*-
import argparse
from ydb.tests.stress.streaming.workload import Workload

if __name__ == '__main__':
    text = """\033[92mStreaming workload\x1b[0m"""
    parser = argparse.ArgumentParser(description=text, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--endpoint', default='localhost:2135', help="An endpoint to be used")
    parser.add_argument('--database', default=None, required=True, help='A database to connect')
    parser.add_argument('--duration', default=60, type=lambda x: int(x), help='A duration of workload in seconds.')
    parser.add_argument('--partitions-count', default=10, type=lambda x: int(x), help='Partitions count.')
    args = parser.parse_args()
    with Workload(args.endpoint, args.database, args.duration, args.partitions_count) as workload:
        workload.loop()
