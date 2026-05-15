import argparse
import logging
import sys

from aiohttp import web

from ydb.tools.mnc.agent import config
from ydb.tools.mnc.agent.api.disks import routes as disks_routes
from ydb.tools.mnc.agent.api.errors import error_middleware
from ydb.tools.mnc.agent.api.health import routes as health_routes
from ydb.tools.mnc.agent.api.nodes import routes as nodes_routes
from ydb.tools.mnc.agent.api.tasks import routes as tasks_routes
from ydb.tools.mnc.agent.services.database import database_service
from ydb.tools.mnc.agent.services.tasks import task_service


app = web.Application(middlewares=[error_middleware])
logger = None
host = "::"
port = 8999
debug = False


async def startup_event(app):
    await database_service.connect()
    await task_service.start()


async def shutdown_event(app):
    await task_service.stop()
    await database_service.disconnect()


def setup_logging():
    global logger
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)


def setup_routes():
    app.add_routes(health_routes)
    app.add_routes(tasks_routes)
    app.add_routes(nodes_routes)
    app.add_routes(disks_routes)


def parse_args():
    parser = argparse.ArgumentParser(description="MNCAgentServer")
    parser.add_argument("--host", default="localhost", help="Host to bind to (default: localhost)")
    parser.add_argument("--port", type=int, default=8999, help="Port to bind to (default: 8999)")
    parser.add_argument("--mnc-home", default=None, help="MNC home directory (default: /home/user/multinode_home)")
    parser.add_argument("--config", default=None, help="Path to agent YAML config")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    return parser.parse_args()


def main():
    global host, port, debug

    args = parse_args()
    if args.config:
        config.load_config(args.config)
    if args.mnc_home:
        config.mnc_home = args.mnc_home
    config.ensure_mnc_home()

    host = args.host
    port = args.port
    debug = args.debug

    setup_logging()
    setup_routes()
    app.on_startup.append(startup_event)
    app.on_cleanup.append(shutdown_event)

    try:
        web.run_app(app, host=host, port=port, access_log=None)
    except KeyboardInterrupt:
        print("\nServer stopped by user")
        sys.exit(0)
    except Exception as exc:
        print(f"Error starting server: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
