"""
This recipe was inspired by:
https://a.yandex-team.ru/arcadia/library/recipes/clickhouse/recipe/__init__.py
"""

import os
import logging
import socket
from pathlib import Path
from typing import Final
import tempfile

import jinja2

import yatest.common as yat
from library.python.testing.recipe import declare_recipe, set_env
from library.recipes.common import find_free_ports, start_daemon

logger = logging.getLogger('connector.recipe')

CONNECTOR_PID_FILE: Final = 'recipe.connector.pid'


def start(argv):
    logger.debug('Start arguments: %s', argv)

    connector = yat.build_path("ydb/library/yql/providers/generic/connector/app/yq-connector")

    grpc_host = "0.0.0.0"
    grpc_port = find_free_ports(1)[0]

    config_path = _render_config(grpc_host=grpc_host, grpc_port=grpc_port)

    logger.info('Starting connector server...')

    start_daemon(
        command=[connector, 'server', f'--config={config_path}'],
        pid_file_name=yat.work_path(CONNECTOR_PID_FILE),
        is_alive_check=lambda: _is_alive_check(grpc_host, grpc_port),
        environment=_update_environment(grpc_host=grpc_host, grpc_port=grpc_port),
    )

    logger.info('Connector server started')


def _render_config(grpc_host: str, grpc_port: int) -> Path:
    template_ = '''
    connector_server {
        endpoint {
            host: "{{grpc_host}}",
            port: {{grpc_port}}
        }
    }

    logger {
        log_level: TRACE
        enable_sql_query_logging: true
    }
    '''
    template = jinja2.Environment(loader=jinja2.BaseLoader).from_string(template_)

    content = template.render(grpc_host=grpc_host, grpc_port=grpc_port)
    tmp = tempfile.NamedTemporaryFile(delete=False)
    with open(tmp.name, 'w') as f:
        f.write(content)

    return tmp.name


def _update_environment(grpc_host: str, grpc_port: int):
    variables = {
        'YQL_RECIPE_CONNECTOR_GRPC_HOST': grpc_host,
        'YQL_RECIPE_CONNECTOR_GRPC_PORT': str(grpc_port),
    }

    for variable in variables.items():
        (k, v) = variable
        # k = prefix + k
        set_env(k, v)

    environment = os.environ.copy()
    environment.update(variables)

    return environment


def _is_alive_check(host: str, port: str, timeout=2) -> bool:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # presumably
    sock.settimeout(timeout)
    try:
        sock.connect((host, port))
    except Exception as e:
        logger.error(e)
        return False
    else:
        sock.close()
        return True


def stop(argv):
    logger.debug('Start arguments: %s', argv)
    logger.info('Terminating Connector server...')
    try:
        with open(yat.work_path(CONNECTOR_PID_FILE)) as fin:
            pid = fin.read()
    except IOError:
        logger.error('Can not find server PID')
    else:
        logger.info('Terminate Connector server PID: %s', pid)
        os.kill(int(pid), 9)
        logger.info('Server terminated.')


if __name__ == "__main__":
    declare_recipe(start, stop)
