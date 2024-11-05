import argparse
import logging.handlers

from ydb.library.yql.tools.solomon_emulator.lib.config import Config
from ydb.library.yql.tools.solomon_emulator.lib.webapp import run_web_app

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


def parse_args():
    formatter = argparse.ArgumentDefaultsHelpFormatter
    parser = argparse.ArgumentParser(
        formatter_class=formatter,
    )

    parser.add_argument("--auth", type=str, required=False, help="Allowed value for Authorization header")
    parser.add_argument("--shard", type=str, required=False, action='append',
                        help="Allowed shard id in form $project_name/$service_name/$cluster_name")
    parser.add_argument("--port", type=int, required=False, default=31000, help="Listen HTTP port")
    return parser.parse_args()


def main():
    args = parse_args()
    logger.debug(f"Starting Solomon emulator on local port {args.port}")
    config = Config(args.auth, shards=Config.parse_shards(args.shard))
    run_web_app(config, port=args.port)
