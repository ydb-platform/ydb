# -*- coding: utf-8 -*-
import argparse
import logging
from ydb.tests.stress.vector_workload.workload import YdbVectorWorkload

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Workload vector wrapper", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--endpoint', default='grpc://localhost:2135', help="YDB endpoint")
    parser.add_argument('--database', default=None, required=True, help='A database to connect')
    parser.add_argument('--duration', default=120, type=lambda x: int(x), help='A duration of workload in seconds')
    parser.add_argument('--mode', default='standalone', choices=['standalone', 'generate', 'load'],
                        help='Mode: standalone (default), generate (generate + dump), load (restore + run)')
    parser.add_argument('--data-dir', default=None, help='Directory for dump/restore data (required for generate/load modes)')
    parser.add_argument('--targets', default=10000, type=int, help='Number of query vectors for run select (default: 10000)')
    parser.add_argument('--warmup', default=0, type=int, help='Warmup duration in seconds before measured run (default: 0, disabled)')
    parser.add_argument('--rows', default=100000, type=int, help='Number of rows in generated database (default: 100000)')
    parser.add_argument('--log_file', default=None, help='Append log into specified file')

    args = parser.parse_args()

    if args.mode in ('generate', 'load') and not args.data_dir:
        parser.error(f"--data-dir is required for --mode={args.mode}")

    if args.log_file:
        logging.basicConfig(
            filename=args.log_file,
            filemode='a',
            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
            datefmt='%H:%M:%S',
            level=logging.INFO
        )

    workload = YdbVectorWorkload(args.endpoint, args.database, duration=args.duration,
                                 mode=args.mode, data_dir=args.data_dir, targets=args.targets,
                                 warmup=args.warmup, rows=args.rows)
    workload.start()
    workload.join()
