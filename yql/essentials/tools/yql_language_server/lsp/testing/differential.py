from typing import Iterable
from concurrent.futures import ThreadPoolExecutor

from .server import LanguageServer, carefully
from .trace import LspMessage, LspRequest, LspResponse


def replay_requests(trace: Iterable[LspRequest], server: LanguageServer) -> Iterable[LspResponse]:
    return carefully(lambda s: _replay_requests(trace, s), server)


def assert_equivalent(lhs: Iterable[LspResponse], rhs: Iterable[LspResponse]) -> None:
    assert lhs == rhs, _diff_responses(lhs, rhs)


def _replay_requests(trace: Iterable[LspRequest], server: LanguageServer) -> Iterable[LspResponse]:
    expected_responses = sum(1 for request in trace if request.id is not None)

    messages: list[LspMessage] = []

    def read_responses():
        while len(messages) < expected_responses:
            messages.append(server.recv())

    def send_requests():
        for request in trace:
            server.send(request)

    # NB: avoid stdout buffer overflow (blocking)
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(read_responses)
        send_requests()

    responses: dict[int, LspResponse] = {}
    for message in messages:
        if message.id is None:
            continue
        responses[message.id] = message

    assert len(responses) == expected_responses
    return [responses[i] for i in sorted(responses)]


def _diff_responses(lhs: list[LspResponse], rhs: list[LspResponse]) -> str:
    for i in range(min(len(lhs), len(rhs))):
        if lhs[i] == rhs[i]:
            continue

        return '\n'.join(
            (
                f'response mismatch at id {lhs[i].id}',
                f'lhs: {lhs[i].json!r}',
                f'rhs: {rhs[i].json!r}',
            )
        )

    if len(lhs) != len(rhs):
        return f'response count mismatch: {len(lhs) =} {len(rhs) =}'

    return 'responses differ'
