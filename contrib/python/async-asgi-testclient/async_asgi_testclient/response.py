from requests.exceptions import StreamConsumedError
from requests.models import Response as _Response
from requests.utils import iter_slices
from requests.utils import stream_decode_response_unicode

import io


class BytesRW(object):
    def __init__(self):
        self._stream = io.BytesIO()
        self._rpos = 0
        self._wpos = 0

    def read(self, size=-1):
        if self._stream is None:
            raise Exception("Stream is closed")
        self._stream.seek(self._rpos)
        bytes_ = self._stream.read(size)
        self._rpos += len(bytes_)
        return bytes_

    def write(self, b):
        if self._stream is None:
            raise Exception("Stream is closed")
        self._stream.seek(self._wpos)
        n = self._stream.write(b)
        self._wpos += n
        return n

    def close(self):
        self._stream = None


class Response(_Response):
    def __init__(self, stream: bool, receive, send):
        super().__init__()

        self.stream = stream
        self.receive_or_fail = receive
        self.send = send
        self._more_body = False
        self.raw = BytesRW()

    async def __aiter__(self):
        async for c in self.iter_content(128):
            yield c

    async def generate(self, n):
        while True:
            val = self.raw.read(n)
            if val == b"":  # EOF
                break
            yield val

        while self._more_body:
            message = await self.receive_or_fail()
            if not isinstance(message, dict):
                raise Exception(f"Unexpected message {message}")
            if message["type"] != "http.response.body":
                raise Exception(
                    f"Excpected message type 'http.response.body'. " f"Found {message}"
                )

            yield message["body"]
            self._more_body = message.get("more_body", False)

        # Send disconnect
        self.send({"type": "http.disconnect"})
        message = await self.receive_or_fail()
        assert message.event == "exit"

    async def iter_content(self, chunk_size=1, decode_unicode=False):
        if self._content_consumed and isinstance(self._content, bool):
            raise StreamConsumedError()
        elif chunk_size is not None and not isinstance(chunk_size, int):
            raise TypeError(
                "chunk_size must be an int, it is instead a %s." % type(chunk_size)
            )

        # simulate reading small chunks of the content
        reused_chunks = iter_slices(self._content, chunk_size)
        stream_chunks = self.generate(chunk_size)
        chunks = reused_chunks if self._content_consumed else stream_chunks

        if decode_unicode:
            chunks = stream_decode_response_unicode(chunks, self)

        async for c in chunks:
            yield c
