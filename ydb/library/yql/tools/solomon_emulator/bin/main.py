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
    parser.add_argument("--http-port", type=int, required=False, default=31000, help="Listen HTTP port")
    parser.add_argument("--grpc-port", type=int, required=False, default=32000, help="Listen GRPC port")
    return parser.parse_args()


def main():
    args = parse_args()
    logger.debug(f"Starting Solomon emulator on http port {args.http_port}, grpc port {args.grpc_port}")
    config = Config(args.auth, shards=Config.parse_shards(args.shard))
    run_web_app(config, http_port=args.http_port, grpc_port=args.grpc_port)
