# -*- coding: utf-8 -*-
import argparse
from ydb.tests.stress.simple_queue.workload import Workload

if __name__ == '__main__':
    text = """\033[92mQueue workload\x1b[0m"""
    parser = argparse.ArgumentParser(description=text, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--endpoint', default='localhost:2135', help="An endpoint to be used")
    parser.add_argument('--database', default=None, required=True, help='A database to connect')
    parser.add_argument('--duration', default=10 ** 9, type=lambda x: int(x), help='A duration of workload in seconds.')
    parser.add_argument('--mode', default="row", choices=["row", "column"], help='STORE mode for CREATE TABLE')
    args = parser.parse_args()
    with Workload(args.endpoint, args.database, args.duration, args.mode) as workload:
        workload.start()
