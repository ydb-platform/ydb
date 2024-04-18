import os
import logging
from typing import Final

import json
from aiohttp import web

import yatest.common as yat
from library.python.testing.recipe import declare_recipe, set_env
from library.recipes.common import find_free_ports, start_daemon

logger = logging.getLogger('ydb_mvp_mock.recipe')

YDB_MVP_MOCK_PID_FILE: Final = 'recipe.ydb_mvp_mock.pid'
YDB_HOSTNAME: Final = 'tests-fq-generic-ydb'
YDB_PORT: Final = 2136

async def ydb_handler(request):
    databaseId = request.rel_url.query['databaseId']

    return web.Response(body=json.dumps(
        {
            "endpoint": f"grpc://{YDB_HOSTNAME}:{YDB_PORT}/?database={databaseId}"
        }
    ))

def serve(port: int):
    app = web.Application()
    app.add_routes([web.get('/database', ydb_handler)])
    web.run_app(app, port=port)


def start(argv):
    logger.debug('Start arguments: %s', argv)

    host = "0.0.0.0"
    port = find_free_ports(1)[0]
    _update_environment(host=host, port=port)

    pid = os.fork()
    if pid == 0:
        logger.info('Starting ydb_mvp_mock server...')
        serve(port=port)
    else:
        with open(YDB_MVP_MOCK_PID_FILE, "w") as f:
            f.write(str(pid))


def _update_environment(host: str, port: int):
    variables = {
        'YDB_MVP_MOCK_ENDPOINT': f'http://{host}:{port}',
    }

    for k, v in variables.items():
        set_env(k, v)


def stop(argv):
    logger.debug('Start arguments: %s', argv)
    logger.info('Terminating ydb_mvp_mock server...')
    try:
        with open(yat.work_path(YDB_MVP_MOCK_PID_FILE)) as fin:
            pid = fin.read()
    except IOError:
        logger.error('Can not find server PID')
    else:
        logger.info('Terminate ydb_mvp_mock server PID: %s', pid)
        os.kill(int(pid), 15)
        logger.info('Server terminated.')


if __name__ == "__main__":
    declare_recipe(start, stop)
