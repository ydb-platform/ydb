# pyright: strict
from io import IOBase
import json
from collections import OrderedDict
from typing import Mapping, Optional, AsyncIterable


class StripeResponseBase(object):
    code: int
    headers: Mapping[str, str]

    def __init__(self, code: int, headers: Mapping[str, str]):
        self.code = code
        self.headers = headers

    @property
    def idempotency_key(self) -> Optional[str]:
        try:
            return self.headers["idempotency-key"]
        except KeyError:
            return None

    @property
    def request_id(self) -> Optional[str]:
        try:
            return self.headers["request-id"]
        except KeyError:
            return None


class StripeResponse(StripeResponseBase):
    body: str
    data: object

    def __init__(self, body: str, code: int, headers: Mapping[str, str]):
        StripeResponseBase.__init__(self, code, headers)
        self.body = body
        self.data = json.loads(body, object_pairs_hook=OrderedDict)


class StripeStreamResponse(StripeResponseBase):
    io: IOBase

    def __init__(self, io: IOBase, code: int, headers: Mapping[str, str]):
        StripeResponseBase.__init__(self, code, headers)
        self.io = io


class StripeStreamResponseAsync(StripeResponseBase):
    _stream: AsyncIterable[bytes]

    def __init__(
        self,
        stream: AsyncIterable[bytes],
        code: int,
        headers: Mapping[str, str],
    ):
        StripeResponseBase.__init__(self, code, headers)
        self._stream = stream

    def stream(self) -> AsyncIterable[bytes]:
        return self._stream

    async def read_async(self) -> bytes:
        return b"".join([chunk async for chunk in self._stream])
