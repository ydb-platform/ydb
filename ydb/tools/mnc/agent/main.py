import sys
import logging
from fastapi import FastAPI
import uvicorn
import argparse

from ydb.tools.mnc.agent.api.health import router as health_router
from ydb.tools.mnc.agent.api.tasks import router as tasks_router
from ydb.tools.mnc.agent.api.nodes import router as nodes_router
from ydb.tools.mnc.agent.api.errors import not_found_handler, internal_error_handler
from ydb.tools.mnc.agent.services.tasks import task_service
from ydb.tools.mnc.agent.services.database import database_service
from ydb.tools.mnc.agent import config

# Global variables
app = FastAPI(
    title="MNCAgentServer",
    version="1.0.0",
    redirect_slashes=False
)
logger = None
host = '::'
port = 8999
debug = False


@app.on_event("startup")
async def startup_event():
    await database_service.connect()
    await task_service.start()


@app.on_event("shutdown")
async def shutdown_event():
    await task_service.stop()
    await database_service.disconnect()


def setup_logging():
    global logger
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)


def setup_routes():
    global app
    # Include routers
    app.include_router(health_router)
    app.include_router(tasks_router)
    app.include_router(nodes_router)

    # Setup exception handlers
    app.add_exception_handler(404, not_found_handler)
    app.add_exception_handler(500, internal_error_handler)


def run_server():
    global host, port, debug, logger

    logger.info(f"Starting server on {host}:{port}")

    try:
        uvicorn.run(
            app,
            host=host,
            port=port,
            log_level="debug" if debug else "info"
        )
    finally:
        pass


def parse_args():
    parser = argparse.ArgumentParser(description='MNCAgentServer')
    parser.add_argument(
        '--host',
        default='::',
        help='Host to bind to (default: :: for IPv6)'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=8999,
        help='Port to bind to (default: 8999)'
    )
    parser.add_argument(
        '--mnc-home',
        default=None,
        help='MNC home directory (default: /home/kruall/multinode_home)'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug mode'
    )
    return parser.parse_args()


def main():
    global host, port, debug

    args = parse_args()

    if args.mnc_home:
        config.mnc_home = args.mnc_home
    config.ensure_mnc_home()

    # Set global variables
    host = args.host
    port = args.port
    debug = args.debug

    # Setup logging
    setup_logging()

    # Create app
    setup_routes()

    try:
        run_server()
    except KeyboardInterrupt:
        print("\nServer stopped by user")
        sys.exit(0)
    except Exception as e:
        print(f"Error starting server: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
