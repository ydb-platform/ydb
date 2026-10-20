import argparse
from pathlib import Path
import signal
import threading

from ydb.tests.tools.fqrun.examples.service.config import write_setup
from ydb.tests.tools.fqrun.examples.service.mock import MockServices


def main():
    parser = argparse.ArgumentParser(description='Local WASM Profile and Echo services for fqrun')
    parser.add_argument('--source-root', type=Path, default=Path.cwd())
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--http-port', type=int, default=0)
    parser.add_argument('--grpc-port', type=int, default=0)
    parser.add_argument('--batch-rows', type=int, default=1, choices=range(1, 65))
    args = parser.parse_args()
    stopped = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stopped.set())
    signal.signal(signal.SIGTERM, lambda *_: stopped.set())
    service = MockServices(args.http_port, args.grpc_port)
    try:
        write_setup(args.source_root.resolve(), args.output_dir, service, args.batch_rows)
        print('Local services ready; configuration and SQL: %s' % args.output_dir.resolve(), flush=True)
        stopped.wait()
    finally:
        service.stop()


if __name__ == '__main__':
    main()
