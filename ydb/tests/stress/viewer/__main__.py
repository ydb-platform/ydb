# -*- coding: utf-8 -*-
import argparse
from ydb.tests.stress.viewer.workload import Workload

if __name__ == '__main__':
    text = """\033[92mViewer workload\x1b[0m"""
    parser = argparse.ArgumentParser(description=text, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--mon_endpoint', default='http://localhost:8765', help="An endpoint to be used")
    parser.add_argument('--database', default=None, help='A database to connect')
    parser.add_argument('--duration', default=10 ** 9, type=lambda x: int(x), help='A duration of workload in seconds.')
    args = parser.parse_args()
    with Workload(args.mon_endpoint, args.database, args.duration) as workload:
        workload.start()
