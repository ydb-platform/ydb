import os
import logging
from typing import Final

from concurrent import futures

import yatest.common as yat
from library.python.testing.recipe import declare_recipe, set_env
from library.recipes.common import find_free_ports

import grpc
from ydb.library.yql.providers.common.token_accessor.grpc.token_accessor_pb_pb2_grpc import (
    TokenAccessorServiceServicer,
    add_TokenAccessorServiceServicer_to_server,
)
from ydb.library.yql.providers.common.token_accessor.grpc.token_accessor_pb_pb2 import GetTokenRequest, GetTokenResponse

logger = logging.getLogger('token_accessor_mock.recipe')

TOKEN_ACCESSOR_PID_FILE: Final = 'TOKEN_ACCESSOR_PID_FILE'
TOKEN_ACCESSOR_HMAC_SECRET_FILE: Final = 'TOKEN_ACCESSOR_HMAC_SECRET_FILE'


class TokenAccessor(TokenAccessorServiceServicer):
    def GetToken(self, request: GetTokenRequest, context) -> GetTokenResponse:
        logger.debug('GetToken request: %s', request)
        return GetTokenResponse(token='token'.encode())


def serve(port: int) -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    add_TokenAccessorServiceServicer_to_server(TokenAccessor(), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    logger.info(f'token_accessor_mock server started at {port}')

    server.wait_for_termination()
    logger.info('token_accessor_mock server stopped')


def start(argv):
    logger.debug('Start arguments: %s', argv)

    with open(TOKEN_ACCESSOR_HMAC_SECRET_FILE, "w") as f:
        f.write('hmac_secret')

    port = find_free_ports(1)[0]
    _update_environment(port=port)

    pid = os.fork()
    if pid == 0:
        logger.info('Starting token_accessor_mock server...')
        serve(port=port)
    else:
        with open(TOKEN_ACCESSOR_PID_FILE, "w") as f:
            f.write(str(pid))


def _update_environment(port: int):
    variables = {
        'TOKEN_ACCESSOR_MOCK_ENDPOINT': f'localhost:{port}',
        'TOKEN_ACCESSOR_HMAC_SECRET_FILE': os.path.abspath(TOKEN_ACCESSOR_HMAC_SECRET_FILE),
    }

    for k, v in variables.items():
        set_env(k, v)


def stop(argv):
    logger.debug('Stop arguments: %s', argv)
    logger.info('Terminating token_accessor_mock server...')
    try:
        with open(yat.work_path(TOKEN_ACCESSOR_PID_FILE)) as fin:
            pid = fin.read()
    except IOError:
        logger.error('Can not find server PID')
    else:
        logger.info('Terminate token_accessor_mock server PID: %s', pid)
        os.kill(int(pid), 15)
        logger.info('Server terminated.')


if __name__ == "__main__":
    declare_recipe(start, stop)
