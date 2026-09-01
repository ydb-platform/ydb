import argparse
from ydb.tests.stress.remove_storage_groups.workload import AlterStorageUnitsWorkload

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Add & remove storage groups workload", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--endpoint', default='grpc://localhost:2135', help="YDB endpoint")
    parser.add_argument('--database', default=None, required=True, help='A database to connect')
    parser.add_argument('--duration', default=120, type=int, help='A duration of workload in seconds')

    args = parser.parse_args()

    workload = AlterStorageUnitsWorkload(args.endpoint, args.database, args.duration)
    workload.start()
    if not workload.wait_stop():
        raise RuntimeError('number of groups did not converge to the expected value')
