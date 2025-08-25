#! /usr/bin/python3

import argparse
import atexit
import logging
import os
import requests
import shutil
import sys
import time
import threading

import bridge
import keeper_tui


logger = logging.getLogger(__name__)

LOG_FMT = "%(asctime)s %(levelname)s %(name)s: %(message)s"

MAX_KEEP_LOG_LINES = 200

_GLOBAL_LOG_INTERCEPTOR = None


# Global, thread-safe log interceptor for TUI
class LogInterceptor(logging.Handler):
    def __init__(self, formatter: logging.Formatter):
        super().__init__()
        self._formatter = formatter
        self._lock = threading.Lock()
        self._not_consumed_records = []
        self._all_records = []
        self._truncated_count = 0
        self._disabled = False

    def emit(self, record: logging.LogRecord):
        with self._lock:
            if not self._disabled:
                self._not_consumed_records.append(record)
                return

        # Pass through to normal logging if disabled
        logging.getLogger().handle(record)

    def _process_new_records(self):
        # must be called with lock
        records = list(self._not_consumed_records)

        self._all_records.extend(records)
        all_records_length = len(self._all_records)
        if all_records_length > MAX_KEEP_LOG_LINES:
            self._truncated_count += all_records_length - MAX_KEEP_LOG_LINES
            self._all_records = self._all_records[-MAX_KEEP_LOG_LINES:]

        self._not_consumed_records.clear()
        return records

    def consume_records(self):
        with self._lock:
            return self._process_new_records()

    def disable_and_flush(self):
        # Disable interception and replay kept records via normal logging
        with self._lock:
            self._disabled = True
            self._process_new_records()

            records = list(self._all_records)
            truncated = self._truncated_count
            self._all_records.clear()
            self._truncated_count = 0

        root_logger = logging.getLogger()
        # Detach this interceptor, ensure a normal handler is present
        try:
            root_logger.removeHandler(self)
            _ensure_color_logging_no_tui(root_logger)
        except Exception:
            pass

        if truncated > 0:
            root_logger.info(f"Log records truncated: {truncated}")

        for rec in records:
            root_logger.handle(rec)


def _get_log_interceptor():
    return _GLOBAL_LOG_INTERCEPTOR


# Basic ANSI color support for logs printed to TTY
class _ColorFormatter(logging.Formatter):
    COLORS = {
        logging.DEBUG: "\x1b[37m",      # grey
        logging.INFO: "\x1b[0m",        # reset/default
        logging.WARNING: "\x1b[33m",    # yellow
        logging.ERROR: "\x1b[31m",      # red
        logging.CRITICAL: "\x1b[31;1m", # bright red
    }
    RESET = "\x1b[0m"

    def format(self, record):
        color = self.COLORS.get(record.levelno, self.RESET)
        message = super().format(record)
        return f"{color}{message}{self.RESET}"


def _ensure_color_logging_no_tui(logger_obj):
    if logger_obj.handlers:
        return
    handler = logging.StreamHandler()

    def _supports_color(stream):
        try:
            if os.environ.get('NO_COLOR'):
                return False
            if hasattr(stream, 'isatty') and stream.isatty():
                term = os.environ.get('TERM', '')
                if term and term != 'dumb':
                    return True
        except Exception:
            return False
        return False

    if _supports_color(handler.stream):
        handler.setFormatter(_ColorFormatter(LOG_FMT))
    else:
        handler.setFormatter(logging.Formatter(LOG_FMT))
    logger_obj.addHandler(handler)
    logger_obj.setLevel(logging.INFO)


def _setup_logging(args):
    # Configure root logger with color support so all module logs inherit it
    if args.tui:
        global _GLOBAL_LOG_INTERCEPTOR
        root_logger = logging.getLogger()
        # Remove all handlers so logs are not passed through
        for h in list(root_logger.handlers):
            root_logger.removeHandler(h)
        fmt = "%(asctime)s %(levelname)s %(name)s: %(message)s"
        interceptor = LogInterceptor(logging.Formatter(fmt))
        root_logger.addHandler(interceptor)
        _GLOBAL_LOG_INTERCEPTOR = interceptor

        # Ensure logs are flushed and printed on any normal process exit
        def _flush_logs_on_exit():
            try:
                li = _get_log_interceptor()
                if li:
                    li.disable_and_flush()
            except Exception:
                pass

        atexit.register(_flush_logs_on_exit)
    else:
        _ensure_color_logging_no_tui(logging.getLogger())


    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger(__name__).setLevel(getattr(logging, args.log_level))
    logging.getLogger('bridge').setLevel(getattr(logging, args.log_level))

    # Silence noisy libraries explicitly
    logging.getLogger('urllib3').setLevel(logging.ERROR)
    logging.getLogger('requests').setLevel(logging.ERROR)


def _parse_args():
    log_choices = ['DEBUG','INFO','WARNING','ERROR','CRITICAL']

    parser = argparse.ArgumentParser()

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--endpoint', '-e',
        help='Single endpoint used to resolve piles and their endpoints')
    group.add_argument('--endpoints', nargs='+',
        help='Manual endpoints to check piles. Should include at least three from each pile')

    parser.add_argument('--ydb', required=False, help='Path to ydb cli')

    parser.add_argument('--log-level', default='INFO', choices=log_choices, help='Logging level')

    parser.add_argument("--cluster", default="cluster", help="Cluster name to display")
    parser.add_argument("--tui", action="store_true", help="Enable TUI")
    parser.add_argument("--tui-refresh", type=float, default=1.0, help="Refresh interval in seconds")
    parser.add_argument("--tui-disable-auto-failover", action="store_true", help="Disable automatical failover")

    return parser.parse_args()


def _run_no_tui(endpoints, path_to_cli, piles):
    keeper = bridge.Bridgekeeper(endpoints, path_to_cli, piles)
    keeper.run()


def _run_tui(args, endpoints, path_to_cli, piles):
    auto_failover = not args.tui_disable_auto_failover
    keeper = bridge.Bridgekeeper(endpoints, path_to_cli, piles)
    app = keeper_tui.KeeperApp(
        keeper=keeper,
        cluster_name=args.cluster,
        refresh_seconds=args.tui_refresh,
        auto_failover=auto_failover,
        log_consumer=lambda: _get_log_interceptor().consume_records()
    )

    app.run()

    try:
        keeper.stop_async()
    except Exception as e:
        logger.exception(f"Exception while stopping keeper: {e}")

    log_interceptor = _get_log_interceptor()
    if log_interceptor:
        log_interceptor.disable_and_flush()

    sys.exit(app.return_code)


def main():
    args = _parse_args()

    _setup_logging(args)

    path_to_cli = args.ydb
    if path_to_cli:
        if not os.path.exists(path_to_cli) or not os.access(path_to_cli, os.X_OK):
            logger.error(f"Specified --ydb '{path_to_cli}' not found or not executable")
            sys.exit(2)
    else:
        found = shutil.which('ydb')
        if not found:
            logger.error('ydb cli not found in PATH. Install YDB CLI or specify path to the executable using --ydb')
            sys.exit(2)
        path_to_cli = found
        logger.debug(f'Found ydb CLI: {path_to_cli}')

    endpoints = None
    piles = None

    if args.endpoints is not None:
        endpoints = args.endpoints
        piles = bridge.resolve(args.endpoints[0], path_to_cli)
        if not piles or len(piles) == 0:
            logger.error(f'No piles resolved!')
            sys.exit(1)
        piles_count = len(piles)
        user_endpoints_count = len(endpoints)

        # we want at least 3 endpoints per pile to gather quorum about state
        if user_endpoints_count < piles_count * 3:
            resolved_endpoints = [h for hosts in piles.values() for h in hosts]
            resolved_endpoints_count = len(resolved_endpoints)
            if resolved_endpoints_count > user_endpoints_count:
                logger.warning(f'Too few endpoints {user_endpoints_count} specified, '
                    f'using {resolved_endpoints_count} resolved for {piles_count} piles')
                endpoints = resolved_endpoints
    else:
        try:
            piles = bridge.resolve(args.endpoint, path_to_cli)
            if piles:
                endpoints = [h for hosts in piles.values() for h in hosts]
        except Exception as e:
            # ignore, result is checked below
            logger.debug(f'Resolve throw exception: {e}')

        if not endpoints or len(endpoints) == 0:
            logger.error(f'No endpoints resolved from {args.endpoint}, use --endpoints')
            sys.exit(1)


    # Rest the cluster not in piece

    if args.tui:
        _run_tui(args, endpoints, path_to_cli, piles)
    else:
        _run_no_tui(endpoints, path_to_cli, piles)


if __name__ == '__main__':
    main()
