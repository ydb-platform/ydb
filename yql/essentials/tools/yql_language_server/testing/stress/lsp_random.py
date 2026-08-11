import random
from typing import Iterator

from yql.essentials.tools.yql_language_server.lsp.testing.trace import LspRequest

URI = 'file:///tmp/smoke.yql'

VOCABULARY = [
    'SELECT 1',
    'SELECT 1 AS a',
    'FROM (SELECT 1 AS a) AS x SELECT x.a',
    'SELECT a, b FROM t',
]


class LspRandom:
    def __init__(self, rng: random.Random, did_change_p: float):
        self._rng = rng
        self.did_change_p = did_change_p
        self._uri = URI
        self._version = 1
        self._text = ''
        self._next_id = 1

    def generate_trace(self, ops: int) -> Iterator[LspRequest]:
        yield self._initialize()
        yield self._initialized()
        self._text = self._random_text()
        yield self._did_open()
        for _ in range(ops):
            yield self.random_message()

    def random_message(self) -> LspRequest:
        if self._rng.random() < self.did_change_p:
            return self.random_did_change()
        return self.random_completion()

    def random_did_change(self) -> LspRequest:
        self._version += 1
        self._text = self._random_text()
        return LspRequest(
            jsonrpc='2.0',
            method='textDocument/didChange',
            params={
                'textDocument': {'uri': self._uri, 'version': self._version},
                'contentChanges': [{'text': self._text}],
            },
        )

    def random_completion(self) -> LspRequest:
        request_id = self._next_id
        self._next_id += 1
        line, character = self._random_position()
        return LspRequest(
            jsonrpc='2.0',
            id=request_id,
            method='textDocument/completion',
            params={
                'textDocument': {'uri': self._uri},
                'position': {'line': line, 'character': character},
                'context': {'triggerKind': 1},
            },
        )

    def _initialize(self) -> LspRequest:
        return LspRequest(
            jsonrpc='2.0',
            id=0,
            method='initialize',
            params={'processId': None, 'capabilities': {}, 'rootUri': None},
        )

    def _initialized(self) -> LspRequest:
        return LspRequest(jsonrpc='2.0', method='initialized', params={})

    def _did_open(self) -> LspRequest:
        return LspRequest(
            jsonrpc='2.0',
            method='textDocument/didOpen',
            params={
                'textDocument': {
                    'uri': self._uri,
                    'languageId': 'yql',
                    'version': self._version,
                    'text': self._text,
                },
            },
        )

    def _random_text(self) -> str:
        return self._rng.choice(VOCABULARY)

    def _random_position(self) -> tuple[int, int]:
        if not self._text:
            return 0, 0
        lines = self._text.split('\n')
        line = self._rng.randrange(len(lines))
        character = self._rng.randrange(len(lines[line]) + 1)
        return line, character
