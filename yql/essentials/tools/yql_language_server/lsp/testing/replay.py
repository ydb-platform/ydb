from .server import LanguageServer, carefully
from .trace import LspMessage, LspRequest, LspResponse, LspTrace


def replay_trace(trace: LspTrace, server: LanguageServer) -> None:
    carefully(lambda s: _replay_messages(trace, s), server)
    x = server.recv()
    assert x is None, f"expected an end of stream, got {x.model_dump_json()}"


def _replay_messages(trace: LspTrace, server: LanguageServer) -> None:
    for message in trace.messages:
        _replay_message(message, server)


def _replay_message(message: LspMessage, server: LanguageServer) -> None:
    if isinstance(message, LspRequest):
        server.send(message)
    elif isinstance(message, LspResponse):
        actual = server.recv()
        assert actual is not None, "expected a response, but the stream ended"
        assert actual == message, (
            f"response mismatch:\n\n"
            f"expected\n\n{message.model_dump_json()}\n\n"
            f"got\n\n{actual.model_dump_json()}"
        )
    else:
        assert False
