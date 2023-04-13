import os
import sys
import time
import signal
import multiprocessing
from argparse import ArgumentParser
import asyncio
from ydb.tests.tools.ydb_serializable.lib import (
    DatabaseChecker,
    DatabaseCheckerOptions,
    DummyLogger,
    SerializabilityError,
)


def main():
    parser = ArgumentParser()
    parser.add_argument('-e', '--endpoint', required=True)
    parser.add_argument('-d', '--database', required=True)
    parser.add_argument('-p', '--path', default=None)
    parser.add_argument('-o', '--output-path', default='')
    parser.add_argument('-r', dest='nreaders', type=int, default=100, help='Number of coroutines with point-key reads, default 100')
    parser.add_argument('-w', dest='nwriters', type=int, default=100, help='Number of coroutines with point-key writes, default 100')
    parser.add_argument('-rw', dest='nreadwriters', type=int, default=100, help='Number of coroutines with point-key read/writes, default 100')
    parser.add_argument('-rt', dest='nreadtablers', type=int, default=100, help='Number of coroutines with read-table transactions, default 100')
    parser.add_argument('-rr', dest='nrangereaders', type=int, default=100, help='Number of coroutines with range-key reads, default 100')
    parser.add_argument('-rwr', dest='nrwrs', type=int, default=0, help='Number of coroutines with read-write-read txs, default 0')
    parser.add_argument('--keys', dest='numkeys', type=int, default=40, help='Number of distinct keys in a table, default 40')
    parser.add_argument('--shards', dest='numshards', type=int, default=4, help='Number of shards for a table, default 4')
    parser.add_argument('--seconds', type=float, default=2.0, help='Minimum number of seconds per iteration, default 2 seconds')
    parser.add_argument('--iterations', type=int, default=None, help='Number of iterations before stopping, default is infinite until Ctrl+C is pressed')
    parser.add_argument('--rt-full', dest='read_table_ranges', action='store_false', default=True, help='Use full table ranges in read-table transactions')
    parser.add_argument('--rt-ranges', dest='read_table_ranges', action='store_true', default=True, help='Use partial table ranges in read-table transactions, this is the default')
    parser.add_argument('--rt-snapshot', dest='read_table_snapshot', action='store_true', default=None, help='Use server-side snapshots for read-table transactions')
    parser.add_argument('--ignore-rt', dest='ignore_read_table', action='store_true', help='Ignore read-table results (e.g. for interference only, legacy option)')
    parser.add_argument('--processes', type=int, default=1, help='Number of processes to fork into, default is 1')
    parser.add_argument('--print-unique-errors', dest='print_unique_errors', action='store_const', const=1, default=0, help='Print unique errors that happen during execution')
    parser.add_argument('--print-unique-traceback', dest='print_unique_errors', action='store_const', const=2, help='Print traceback for unique errors that happen during execution')
    parser.add_argument('--oplog-results', dest='oplog_results', action='store_true', default=False, help='Store operation results in oplog dumps')
    args = parser.parse_args()

    logger = DummyLogger()

    options = DatabaseCheckerOptions()
    options.keys = args.numkeys
    options.shards = args.numshards
    options.readers = args.nreaders
    options.writers = args.nwriters
    options.rwrs = args.nrwrs
    options.readwriters = args.nreadwriters
    options.readtablers = args.nreadtablers
    options.rangereaders = args.nrangereaders
    options.seconds = args.seconds
    options.read_table_ranges = args.read_table_ranges
    options.ignore_read_table = args.ignore_read_table
    options.read_table_snapshot = args.read_table_snapshot
    options.oplog_results = args.oplog_results

    async def async_run_single_inner():
        iterations = args.iterations

        async with DatabaseChecker(args.endpoint, args.database, path=args.path, logger=logger, print_unique_errors=args.print_unique_errors) as checker:
            while iterations is None or iterations > 0:
                try:
                    await checker.async_run(options)
                except SerializabilityError as e:
                    e.history.write_to_file(os.path.join(args.output_path, os.path.basename(e.table) + '_history.json'))
                    e.history.write_log_to_file(os.path.join(args.output_path, os.path.basename(e.table) + '_log.json'))
                    raise

                if iterations is not None:
                    iterations -= 1

    async def async_run_single():
        loop = asyncio.get_event_loop()
        task = asyncio.create_task(async_run_single_inner())

        def handler(signum, frame, sys=sys, logger=logger, loop=loop, task=task):
            logger.warning('Terminating on signal %d', signum)

            def do_cancel():
                if not task.done():
                    task.cancel()

            loop.call_soon_threadsafe(do_cancel)
            sys.exit(1)

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

        await task

    def run_single():
        try:
            asyncio.run(async_run_single())
        except asyncio.exceptions.CancelledError:
            sys.exit(1)

    def run_multiple():
        def handler(signum, frame, sys=sys, logger=logger):
            logger.warning('Terminating on signal %d', signum)
            sys.exit(1)

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

        processes = []
        for _ in range(args.processes):
            processes.append(multiprocessing.Process(target=run_single))

        def reap_children():
            failed = False
            if processes:
                processes[0].join(0.1)
                index = 0
                while index < len(processes):
                    process = processes[index]
                    if process.is_alive():
                        index += 1
                    else:
                        if process.exitcode != 0:
                            failed = True
                        del processes[index]
            return failed

        try:
            # Start all processes
            for process in processes:
                process.daemon = True
                process.start()

            # Periodically filter dead processes
            while processes:
                if reap_children():
                    logger.error('Child process failed')
                    sys.exit(1)

        finally:
            # Wait a short time for processes to finish what they are doing
            deadline = time.time() + 1.0
            while time.time() < deadline and process:
                reap_children()

            # Terminate all processes that are still alive
            for process in processes:
                process.terminate()

            # Wait for all processes to finish
            for process in processes:
                process.join()

    if args.processes > 1:
        run_multiple()
    else:
        run_single()


if __name__ == '__main__':
    main()
