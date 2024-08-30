import os
import logging
from typing import Final

import json
from aiohttp import web

import yatest.common as yat
from library.python.testing.recipe import declare_recipe, set_env
from library.recipes.common import find_free_ports

logger = logging.getLogger('mdb_mock.recipe')

MDB_MOCK_PID_FILE: Final = 'recipe.mdb_mock.pid'


async def clickhouse_handler(request):
    cluster_id = request.match_info['cluster_id']

    if cluster_id == 'clickhouse_cluster_id':
        return web.Response(
            body=json.dumps(
                {
                    'hosts': [
                        {'name': 'clickhouse', 'cluster_id': cluster_id, 'health': 'ALIVE', 'type': 'CLICKHOUSE'},
                    ]
                }
            )
        )

    return web.Response(body=json.dumps({}))


async def postgresql_handler(request):
    cluster_id = request.match_info['cluster_id']

    if cluster_id == 'postgresql_cluster_id':
        return web.Response(
            body=json.dumps(
                {
                    'hosts': [
                        {
                            'name': 'postgresql',
                            'services': [
                                {
                                    'health': 'ALIVE',
                                },
                            ],
                        }
                    ]
                }
            )
        )
    return web.Response(body=json.dumps({}))


def serve(port: int):
    app = web.Application()
    app.add_routes([web.get('/managed-clickhouse/v1/clusters/{cluster_id}/hosts', clickhouse_handler)])
    app.add_routes([web.get('/managed-postgresql/v1/clusters/{cluster_id}/hosts', postgresql_handler)])
    web.run_app(app, port=port)


def start(argv):
    logger.debug('Start arguments: %s', argv)

    host = "0.0.0.0"
    port = find_free_ports(1)[0]
    _update_environment(host=host, port=port)

    pid = os.fork()
    if pid == 0:
        logger.info('Starting mdb_mock server...')
        serve(port=port)
    else:
        with open(MDB_MOCK_PID_FILE, "w") as f:
            f.write(str(pid))


def _update_environment(host: str, port: int):
    variables = {
        'MDB_MOCK_ENDPOINT': f'http://{host}:{port}',
    }

    for k, v in variables.items():
        set_env(k, v)


def stop(argv):
    logger.debug('Start arguments: %s', argv)
    logger.info('Terminating mdb_mock server...')
    try:
        with open(yat.work_path(MDB_MOCK_PID_FILE)) as fin:
            pid = fin.read()
    except IOError:
        logger.error('Can not find server PID')
    else:
        logger.info('Terminate mdb_mock server PID: %s', pid)
        os.kill(int(pid), 15)
        logger.info('Server terminated.')


if __name__ == "__main__":
    declare_recipe(start, stop)
