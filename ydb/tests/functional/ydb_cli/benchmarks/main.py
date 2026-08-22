import argparse
import json
import logging

from .impl.common import ColorFormatter
from .impl.benchmark_runner import BenchmarkRunner


def main():
    parser = argparse.ArgumentParser(
        description="Tool for benchmarking YDB CLI AI interactive mode",
        epilog="examples:\n"
        "  ./ydb_cli_bench --config config.json --ydb-cli-binary ../ai_interactive/ydb --database /Root --endpoint localhost:12345 --concurrency 3",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("-b", "--ydb-cli-binary", metavar="PATH", help="YDB CLI binary path", required=True)
    parser.add_argument("-c", "--config", metavar="FILE", help="Config file in JSON format", required=True)
    parser.add_argument("-e", "--endpoint", metavar="FQDN", help="Database endpoint", required=True)
    parser.add_argument("-d", "--database", metavar="PATH", help="Shared database name", required=True)
    parser.add_argument(
        "--concurrency", metavar="INT", type=int, default=1, help="Number of parallel samples evaluation"
    )
    parser.add_argument("--cleanup", action="store_true", help="Cleanup databases after benchmark")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Forbid all tools and ask the model to answer the question instantly without inspecting data",
    )

    LOG_LEVELS = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL,
    }
    parser.add_argument(
        "--log-level", choices=LOG_LEVELS.keys(), default="INFO", metavar="LEVEL", help="Tool logging level"
    )
    parser.add_argument("--log-file", metavar="FILE", help="Tool logging file (if not set used stdout)")

    args = parser.parse_args()

    log_format = "%(asctime)s %(levelname)s:%(name)s: %(message)s"
    if args.log_file:
        ColorFormatter.RESET = ""
        handler = logging.FileHandler(args.log_file)
        handler.setFormatter(logging.Formatter(log_format))
    else:
        handler = logging.StreamHandler()
        handler.setFormatter(ColorFormatter(log_format))

    logging.getLogger().handlers.clear()
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(LOG_LEVELS[args.log_level])

    with open(args.config, "r", encoding="utf-8") as f:
        config = json.load(f)
    logging.info(f"Starting benchmark with config:\n{config}")

    runner = BenchmarkRunner(
        config, args.database, args.endpoint, args.concurrency, args.ydb_cli_binary, dry_run=args.dry_run
    )
    try:
        runner.init()
        runner.run()
    finally:
        runner.cleanup(remove_databases=args.cleanup)


if __name__ == "__main__":
    main()
