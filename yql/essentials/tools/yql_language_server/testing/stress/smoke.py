import random
import time

import yatest.common

from yql.essentials.tools.yql_language_server.lsp.testing.differential import replay_requests, assert_equivalent
from yql.essentials.tools.yql_language_server.lsp.testing.server import LanguageServer

from .lsp_random import LspRandom

SERVER_PATH = 'yql/essentials/tools/yql_language_server/yql_language_server'
SERVER_PATH = yatest.common.binary_path(SERVER_PATH)

SEED = 1
OPS = 512
DID_CHANGE_PROB = 0.05


def test_parallel():
    trace = list(LspRandom(random.Random(SEED), DID_CHANGE_PROB).generate_trace(OPS))

    with LanguageServer([SERVER_PATH, '-j', '1']) as server:
        seq, _ = replay(trace, server)

    with LanguageServer([SERVER_PATH, '-j', '8']) as server:
        par, _ = replay(trace, server)

    assert_equivalent(seq, par)


def replay(trace, server):
    start = time.perf_counter_ns()
    responses = replay_requests(trace, server)
    end = time.perf_counter_ns()
    return responses, end - start
