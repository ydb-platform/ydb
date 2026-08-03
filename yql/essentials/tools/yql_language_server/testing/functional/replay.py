import os
import json
from pathlib import Path

import pytest
import yatest.common

from yql.essentials.tools.yql_language_server.lsp.testing.replay import replay_trace
from yql.essentials.tools.yql_language_server.lsp.testing.server import LanguageServer
from yql.essentials.tools.yql_language_server.lsp.testing.trace import LspTrace

DATA_PATH = 'yql/essentials/tools/yql_language_server/testing/functional/traces'
DATA_PATH = yatest.common.source_path(DATA_PATH)

SERVER_PATH = 'yql/essentials/tools/yql_language_server/yql_language_server'
SERVER_PATH = yatest.common.binary_path(SERVER_PATH)

PROLOGUE = 'session/initialization.json'
NON_PROLOGUE = (
    'session/empty.json',
    PROLOGUE,
)


def pytest_generate_tests(metafunc: pytest.Metafunc):
    metafunc.parametrize(['path'], discover())


def discover() -> list[(str,)]:
    paths = []

    for path in Path(DATA_PATH).glob("**/*.json"):
        path = os.path.relpath(path, DATA_PATH)
        paths.append((path,))

    return sorted(paths)


def test(path: str):
    trace = lsp_trace_from_file(path)

    if path not in NON_PROLOGUE:
        prologue = lsp_trace_from_file(PROLOGUE)
        prologue.messages.extend(trace.messages)
        trace = prologue

    with LanguageServer([SERVER_PATH, "--stdio"]) as server:
        replay_trace(trace, server)


def lsp_trace_from_file(relpath: str) -> LspTrace:
    with open(os.path.join(DATA_PATH, relpath), 'r', encoding='utf-8') as f:
        trace = json.load(f)
    return LspTrace.from_json(trace)
