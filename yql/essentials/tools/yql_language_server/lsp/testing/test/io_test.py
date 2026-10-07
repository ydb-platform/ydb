import io as _io
import json

from yql.essentials.tools.yql_language_server.lsp.testing.io import (
    lsp_read_message,
    lsp_write_message,
)
from yql.essentials.tools.yql_language_server.lsp.testing.trace import (
    LspRequest,
    LspResponse,
    LspTrace,
)


def _make_request():
    return LspRequest.from_json(
        {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'textDocument/completion',
            'params': {'textDocument': {'uri': 'file:///test.py'}},
        }
    )


def _make_response():
    return LspResponse.from_json(
        {
            'jsonrpc': '2.0',
            'id': 1,
            'result': {'items': []},
        }
    )


def _make_error_response():
    return LspResponse.from_json(
        {
            'jsonrpc': '2.0',
            'id': 2,
            'error': {
                'code': -32601,
                'message': 'Method not found',
            },
        }
    )


def _make_notification():
    return LspRequest.from_json(
        {
            'jsonrpc': '2.0',
            'method': 'textDocument/didOpen',
            'params': {'textDocument': {'uri': 'file:///test.py'}},
        }
    )


def test_write_read_request():
    req = _make_request()

    buf = _io.BytesIO()
    lsp_write_message(buf, req)
    buf.seek(0)
    msg = lsp_read_message(buf)

    assert msg is not None
    assert isinstance(msg, LspRequest)
    assert msg.method == 'textDocument/completion'
    assert msg.id == 1
    assert msg == req


def test_write_read_response():
    resp = _make_response()

    buf = _io.BytesIO()
    lsp_write_message(buf, resp)
    buf.seek(0)
    msg = lsp_read_message(buf)

    assert msg is not None
    assert isinstance(msg, LspResponse)
    assert msg.id == 1
    assert msg.result == {'items': []}
    assert msg == resp


def test_write_read_error_response():
    resp = _make_error_response()

    buf = _io.BytesIO()
    lsp_write_message(buf, resp)
    buf.seek(0)
    msg = lsp_read_message(buf)

    assert msg is not None
    assert isinstance(msg, LspResponse)
    assert msg.id == 2
    assert msg.error == {'code': -32601, 'message': 'Method not found'}
    assert msg == resp


def test_write_read_notification():
    notif = _make_notification()

    buf = _io.BytesIO()
    lsp_write_message(buf, notif)
    buf.seek(0)
    msg = lsp_read_message(buf)

    assert msg is not None
    assert isinstance(msg, LspRequest)
    assert msg.method == 'textDocument/didOpen'
    assert msg.id is None
    assert msg == notif


def test_write_message_format():
    req = _make_request()

    buf = _io.BytesIO()
    lsp_write_message(buf, req)
    data = buf.getvalue()

    assert b'Content-Length: ' in data
    assert b'Content-Type: application/vscode-jsonrpc; charset=utf-8\r\n' in data
    assert b'\r\n\r\n' in data

    header, _, body = data.partition(b'\r\n\r\n')
    obj = json.loads(body)
    assert obj['jsonrpc'] == '2.0'
    assert obj['method'] == 'textDocument/completion'


def test_read_empty_returns_none():
    buf = _io.BytesIO(b'')
    msg = lsp_read_message(buf)
    assert msg is None


def test_lsp_trace_from_json():
    obj = [
        {'jsonrpc': '2.0', 'id': 1, 'method': 'initialize', 'params': {}},
        {'jsonrpc': '2.0', 'id': 1, 'result': {'capabilities': {}}},
        {'jsonrpc': '2.0', 'method': 'initialized', 'params': {}},
    ]

    trace = LspTrace.from_json(obj)

    assert len(trace.messages) == 3
    assert isinstance(trace.messages[0], LspRequest)
    assert isinstance(trace.messages[1], LspResponse)
    assert isinstance(trace.messages[2], LspRequest)


def test_lsp_message_equality():
    req1 = _make_request()
    req2 = _make_request()
    resp = _make_response()

    assert req1 == req2
    assert req1 != resp


def test_lsp_trace_equality():
    obj = [
        {'jsonrpc': '2.0', 'id': 1, 'method': 'initialize'},
        {'jsonrpc': '2.0', 'id': 1, 'result': {}},
    ]

    trace1 = LspTrace.from_json(obj)
    trace2 = LspTrace.from_json(obj)

    assert trace1 == trace2
