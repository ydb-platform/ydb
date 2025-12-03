# -*- coding: utf-8 -*-
import argparse
from ydb.tests.stress.streaming.workload import Workload
import logging

logger = logging.getLogger("logger")

if __name__ == '__main__':

    logging.basicConfig(
        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
        datefmt='%H:%M:%S',
        level=logging.INFO)

    text = """\033[92mStreaming workload\x1b[0m"""
    parser = argparse.ArgumentParser(description=text, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--endpoint', default='localhost:2135', type=str, help="An endpoint to be used")
    parser.add_argument('--database', default=None, type=str, required=True, help='A database to connect')
    parser.add_argument('--duration', default=60, type=int, help='A duration of workload in seconds.')
    parser.add_argument('--partitions-count', default=10, type=int, help='Partitions count.')
    parser.add_argument('--prefix', default=None, type=str, help='Topic/source name prefix')
    parser.add_argument('--enable_watermarks', default=False, type=bool, help='watermarks feature flags')
    args = parser.parse_args()

    with Workload(args.endpoint, args.database, args.duration, args.partitions_count, args.prefix, args.enable_watermarks) as workload:
        workload.loop()
