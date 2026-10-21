import json
from typing import IO, Any

from .trace import LspMessage


def lsp_read_message(io: IO[bytes]) -> LspMessage | None:
    headers = {}
    while True:
        line = io.readline()
        if not line:
            return None

        line = line.decode('utf-8')
        line = line.rstrip('\r\n')

        if not line:
            break

        if ':' in line:
            key, value = line.split(':', 1)
            headers[key.strip()] = value.strip()

    if 'Content-Length' not in headers:
        return None
    length = int(headers['Content-Length'])

    body = io.read(length)
    if not body:
        return None

    body = body.decode('utf-8')
    obj = json.loads(body)

    return LspMessage.from_json(obj)


def lsp_write_message(io: IO[Any], message: LspMessage):
    body = json.dumps(message.to_json()).encode('utf-8')
    header = f"Content-Length: {len(body)}\r\n" f"Content-Type: application/vscode-jsonrpc; charset=utf-8\r\n\r\n"
    io.write(header.encode('utf-8'))
    io.write(body)
    io.flush()
