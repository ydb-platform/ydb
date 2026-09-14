#! /usr/bin/python3
#
#             o7
#            /|
#     .--.  / |        Skipper
#    / _  \   |     "Smile and wave, boys."
#   | (o)(o)  |
#   |   __    |
#   |  (__)   |
#  /|         |\
# /_|  ___    |_\
#   |_/___\__|
#

import bridge
import health
import skipper_tui

import argparse
import atexit
import logging
import os
import requests
import shutil
import sys
import time
import subprocess
import threading
import shlex


logger = logging.getLogger(__name__)

# Add custom TRACE log level (lower than DEBUG)
TRACE_LEVEL_NUM = 5
if not hasattr(logging, "TRACE"):
    logging.TRACE = TRACE_LEVEL_NUM
    logging.addLevelName(TRACE_LEVEL_NUM, "TRACE")

    def trace(self, message, *args, **kws):
        if self.isEnabledFor(TRACE_LEVEL_NUM):
            self._log(TRACE_LEVEL_NUM, message, args, **kws)

    logging.Logger.trace = trace

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
        logging.TRACE: "\x1b[37m",      # grey
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
            if os.environ.get("NO_COLOR"):
                return False
            if hasattr(stream, "isatty") and stream.isatty():
                term = os.environ.get("TERM", "")
                if term and term != "dumb":
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


    logging.getLogger().setLevel(getattr(logging, args.log_level))
    logging.getLogger(__name__).setLevel(getattr(logging, args.log_level))
    logging.getLogger("bridge").setLevel(getattr(logging, args.log_level))
    logging.getLogger("health").setLevel(getattr(logging, args.log_level))

    # Silence noisy libraries explicitly
    logging.getLogger("urllib3").setLevel(logging.ERROR)
    logging.getLogger("requests").setLevel(logging.ERROR)


def _parse_args():
    log_choices = ["TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

    parser = argparse.ArgumentParser()

    parser.add_argument("--endpoint", "-e", required=True,
                        help="Single endpoint used to resolve piles and their endpoints")

    parser.add_argument("--state", "-s", required=True, help="Path to keeper state file (pickle format)")

    parser.add_argument("--ydb", required=False, help="Path to ydb cli")
    parser.add_argument("--ydb-auth-opts", required=False,
                        help="Extra auth/TLS options for ydb CLI (single string, e.g. '--ca-file /path --client-cert-file /path --client-cert-key-file /path')")
    parser.add_argument("--disable-auto-failover", action="store_true", help="Disable automatical failover")

    parser.add_argument("--log-level", default="INFO", choices=log_choices, help="Logging level")

    parser.add_argument("--cluster", default="cluster", help="Cluster name to display")
    parser.add_argument("--tui", action="store_true", help="Enable TUI")
    parser.add_argument("--tui-refresh", type=float, default=1.0, help="Refresh interval in seconds")

    return parser.parse_args()


def _run_no_tui(path_to_cli, piles, auto_failover, state_path, ydb_auth_opts):
    keeper = bridge.BridgeSkipper(path_to_cli, piles, auto_failover=auto_failover, state_path=state_path, ydb_auth_opts=ydb_auth_opts)
    keeper.run()


def _run_tui(args, path_to_cli, piles, ydb_auth_opts):
    auto_failover = not args.disable_auto_failover
    keeper = bridge.BridgeSkipper(path_to_cli, piles, auto_failover=auto_failover, state_path=args.state, ydb_auth_opts=ydb_auth_opts)
    app = skipper_tui.KeeperApp(
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
    # parse auth opts for ydb CLI, if provided
    ydb_auth_opts = None
    try:
        if args.ydb_auth_opts:
            ydb_auth_opts = shlex.split(args.ydb_auth_opts)
    except Exception as e:
        logger.error(f"Failed to parse --ydb-auth-opts: {e}")
        sys.exit(2)

    if path_to_cli:
        if not os.path.exists(path_to_cli) or not os.access(path_to_cli, os.X_OK):
            logger.error(f"Specified --ydb '{path_to_cli}' not found or not executable")
            sys.exit(2)
    else:
        found = shutil.which("ydb")
        if not found:
            logger.error("ydb cli not found in PATH. Install YDB CLI or specify path to the executable using --ydb")
            sys.exit(2)
        path_to_cli = found
        logger.debug(f"Found ydb CLI: {path_to_cli}")

    # ensure that we have a proper cli version by checking presence of "--no-merge"
    try:
        help_proc = subprocess.run([path_to_cli, "monitoring", "healthcheck", "--help"],
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   text=True, timeout=10)
        if help_proc.returncode != 0:
            logger.error(f"Failed to run '{path_to_cli} monitoring healthcheck --help' (exit {help_proc.returncode})")
            sys.exit(2)
        if "--no-merge" not in help_proc.stdout:
            logger.error("ydb CLI is too old: missing '--no-merge' in 'monitoring healthcheck --help'. Please update ydb CLI.")
            sys.exit(2)
    except Exception as e:
        logger.error(f"Failed to verify ydb CLI capabilities: {e}")
        sys.exit(2)

    # check state path
    try:
        state_path = args.state
        # Last component might not exist; parent dir must exist and be writable/executable
        state_dir = os.path.dirname(state_path) or "."
        if not os.path.isdir(state_dir):
            logger.error(f"State directory does not exist: {state_dir}")
            sys.exit(2)
        if not (os.access(state_dir, os.W_OK) and os.access(state_dir, os.X_OK)):
            logger.error(f"Insufficient permissions on state directory: {state_dir}")
            sys.exit(2)
        if os.path.exists(state_path):
            if not os.path.isfile(state_path):
                logger.error(f"State path exists but is not a regular file: {state_path}")
                sys.exit(2)
            if not (os.access(state_path, os.R_OK) and os.access(state_path, os.W_OK)):
                logger.error(f"Insufficient permissions on state file: {state_path}")
                sys.exit(2)
    except Exception as e:
        logger.error(f"Failed to validate state path '{args.state}': {e}")
        sys.exit(2)

    piles = None
    try:
        piles = bridge.resolve(args.endpoint, path_to_cli, ydb_auth_opts=ydb_auth_opts)
    except Exception as e:
        # ignore, result is checked below
        logger.debug(f"Resolve throw exception: {e}")

    if not piles or len(piles) == 0:
        logger.error(f"No piles resolved")
        sys.exit(2)

    resolve_summary = ", ".join(f"{pile}: {len(hosts)}" for pile, hosts in piles.items())
    logger.info(f"Piles host counts: {resolve_summary}")

    pile_count = len(piles)
    if pile_count > 2:
        logger.error(f"This is a demo keeper and more than 2 piles is not supported: you have {pile_count} piles")
        sys.exit(2)

    total_endpoints = 0
    for pile_name, endpoints in piles.items():
        endpoint_count = len(endpoints)
        if endpoint_count == 0:
            logger.error(f"No endpoints resolved for pile '{pile_name}'")
        if endpoint_count < health.MINIMAL_EXPECTED_ENDPOINTS_PER_PILE:
            logger.warning(f"Resolved {endpoint_count} endpoints for pile '{pile_name}', "
                           f"which is less than required {health.MINIMAL_EXPECTED_ENDPOINTS_PER_PILE}")
        total_endpoints += len(endpoints)

    if total_endpoints == 0:
        logger.error(f"No endpoints resolved from {args.endpoint}")
        sys.exit(1)

    if args.tui:
        _run_tui(args, path_to_cli, piles, ydb_auth_opts)
    else:
        auto_failover = not args.disable_auto_failover
        _run_no_tui(path_to_cli, piles, auto_failover, args.state, ydb_auth_opts)


if __name__ == "__main__":
    main()
