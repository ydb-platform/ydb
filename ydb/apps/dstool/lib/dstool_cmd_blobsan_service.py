"""CLI adapter for the continuous single-DC repair service."""

import logging
import os
from pathlib import Path
import signal

from ydb.apps.dstool.lib import common


hidden = True

description = 'Continuously scan mirror-3-dc groups and repair single-DC blobs'


def positive(value):
    value = int(value)
    if value <= 0:
        raise ValueError('must be positive')
    return value


def add_options(parser):
    sub = parser.add_subparsers(dest='blobsan_service_action', required=True)
    client = sub.add_parser('client', help='Interactive terminal console (Textual)')
    client.add_argument('--console-sock', required=True)
    client.add_argument('--refresh', type=positive, default=5, help='Dashboard refresh interval in seconds')
    client.add_argument('--request-timeout', type=positive, default=10, help='Console request deadline in seconds')
    master = sub.add_parser('master', help='Coordinator with SQLite state and Unix console')
    master.add_argument('--listen', '-l', type=positive, default=22570)
    master.add_argument('--bind', default='0.0.0.0')
    master.add_argument('--console-sock', required=True)
    master.add_argument('--interval', type=positive, default=3600, help='Rescan clean groups after this many seconds')
    master.add_argument('--retry', type=positive, default=60, help='Rescan candidates/failed groups after this many seconds')
    master.add_argument('--refresh', type=positive, default=300, help='Inventory refresh interval in seconds')
    master.add_argument('--lease', type=positive, default=120)
    worker = sub.add_parser('worker', help='Scan and repair one group at a time')
    worker.add_argument('--master', required=True, help='HOST:PORT or URL of coordinator')
    worker.add_argument('--blobsan', default='~/blobsan', help='Path to blobsan supporting --write2x2-candidates')
    worker.add_argument('--scan-timeout', type=positive, default=3600)
    worker.add_argument('--poll', type=positive, default=5)
    worker.add_argument('--heartbeat', type=positive, default=20, help='Must be shorter than master lease')
    worker.add_argument('--repair-delay', type=float, default=0.0, help='Seconds between repair requests')
    worker.add_argument('--scan-only', action='store_true', help='Report layouts without issuing repair requests')
    for p in (master, worker):
        p.add_argument('--db', required=True, help='Persistent local state directory')
        p.add_argument('--viewer', help='Viewer URL; defaults to this process endpoint/monitoring port')
        p.add_argument('--secret-file', help='Optional shared secret for the worker API')
        p.add_argument('--request-timeout', type=positive, default=20, help='HTTP deadline in seconds')


def do(args):
    if args.dry_run:
        raise common.InvalidParameterError(
            common.connection_params.parser, '--dry-run', True,
            'blobsan-service does not support --dry-run')
    if args.blobsan_service_action == 'client':
        from ydb.apps.dstool.tui.client import ConsoleApp
        ConsoleApp(args.console_sock, refresh=args.refresh, timeout=args.request_timeout).run()
        return

    # Lazy imports keep non-POSIX dstool commands usable on Windows.
    import fcntl
    from ydb.apps.dstool.lib import blobsan_service as svc

    if args.blobsan_service_action == 'worker' and not 0 <= args.repair_delay <= 3600:
        raise common.InvalidParameterError(
            common.connection_params.parser, '--repair-delay', args.repair_delay,
            'Must be in [0, 3600]')
    if args.blobsan_service_action == 'worker':
        binary = Path(args.blobsan).expanduser().resolve()
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise common.InvalidParameterError(
                common.connection_params.parser, '--blobsan', args.blobsan,
                'Not an executable file: ' + str(binary))
        args.blobsan = str(binary)
    if args.blobsan_service_action == 'master' and args.listen > 65535:
        raise common.InvalidParameterError(
            common.connection_params.parser, '--listen', args.listen,
            'Port must be in [1, 65535]')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    directory = Path(args.db).expanduser()
    directory.mkdir(parents=True, exist_ok=True)
    svc.STOP.clear()
    # common.apply_args already parsed/closed token files and configured TLS.
    common.connection_params.http_timeout = args.request_timeout
    with (directory / 'lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        old = {sig: signal.signal(sig, lambda *_: svc.STOP.set()) for sig in (signal.SIGINT, signal.SIGTERM)}
        try:
            (svc.serve if args.blobsan_service_action == 'master' else svc.work)(args, directory)
        finally:
            for sig, handler in old.items():
                signal.signal(sig, handler)
