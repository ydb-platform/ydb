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
    parser.add_argument('--duration', default=120, type=int, help='A duration of workload in seconds')
    parser.add_argument('--mode', default='standalone', choices=['standalone', 'generate', 'load', 's3'],
                        help='Mode: standalone (default), generate (generate + dump), load (restore + run), s3 (import from S3)')
    parser.add_argument('--data-dir', default=None, help='Directory for dump/restore data (required for generate/load modes)')
    parser.add_argument('--targets', default=1000, type=int, help='Number of query vectors for run select (default: 1000)')
    parser.add_argument('--warmup', default=0, type=int, help='Warmup duration in seconds before measured run (default: 0, disabled)')
    parser.add_argument('--rows', default=10000, type=int, help='Number of rows in generated database (default: 10000)')
    parser.add_argument('--threads', default=10, type=int, help='Number of threads for load testing (default: 10)')
    parser.add_argument('--clusters', default=None, type=int, help='Number of clusters in kmeans tree (default: server auto-detect)')
    parser.add_argument('--levels', default=None, type=int, help='Number of levels in kmeans tree (default: server auto-detect)')
    parser.add_argument('--s3-endpoint', default=None, help='S3 endpoint for dataset import (required for --mode=s3)')
    parser.add_argument('--s3-bucket', default=None, help='S3 bucket for dataset import (required for --mode=s3)')
    parser.add_argument('--s3-source', default=None, help='S3 object key prefix for dataset import (required for --mode=s3)')
    parser.add_argument('--s3-destination', default=None, help='Database path for dataset import (required for --mode=s3)')
    parser.add_argument('--s3-query-source', default=None, help='S3 object key prefix for query table import (optional for --mode=s3)')
    parser.add_argument('--s3-query-destination', default=None, help='Database path for query table import (optional for --mode=s3)')
    parser.add_argument('--log_file', default=None, help='Append log into specified file')

    args = parser.parse_args()

    if args.mode in ('generate', 'load') and not args.data_dir:
        parser.error(f"--data-dir is required for --mode={args.mode}")
    if args.mode == 's3':
        if not all([args.s3_endpoint, args.s3_bucket, args.s3_source, args.s3_destination]):
            parser.error("--s3-endpoint, --s3-bucket, --s3-source, and --s3-destination are required for --mode=s3")

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
                                 warmup=args.warmup, rows=args.rows, threads=args.threads,
                                 clusters=args.clusters, levels=args.levels,
                                 s3_endpoint=args.s3_endpoint, s3_bucket=args.s3_bucket,
                                 s3_source=args.s3_source, s3_destination=args.s3_destination,
                                 s3_query_source=args.s3_query_source,
                                 s3_query_destination=args.s3_query_destination)
    workload.start()
    workload.join()
