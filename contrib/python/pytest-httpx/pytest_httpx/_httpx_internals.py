import base64
from typing import Union, Optional
from collections.abc import Sequence, Iterable, AsyncIterator, Iterator

import httpcore
import httpx

# TODO Get rid of this internal import
from httpx._content import IteratorByteStream, AsyncIteratorByteStream

# Those types are internally defined within httpx._types
HeaderTypes = Union[
    httpx.Headers,
    dict[str, str],
    dict[bytes, bytes],
    Sequence[tuple[str, str]],
    Sequence[tuple[bytes, bytes]],
]


class IteratorStream(AsyncIteratorByteStream, IteratorByteStream):
    def __init__(self, stream: Iterable[bytes]):
        class Stream:
            def __iter__(self) -> Iterator[bytes]:
                yield from stream

            async def __aiter__(self) -> AsyncIterator[bytes]:
                for chunk in stream:
                    yield chunk

        AsyncIteratorByteStream.__init__(self, stream=Stream())
        IteratorByteStream.__init__(self, stream=Stream())


def _to_httpx_url(url: httpcore.URL, headers: list[tuple[bytes, bytes]]) -> httpx.URL:
    for name, value in headers:
        if b"Proxy-Authorization" == name:
            return httpx.URL(
                scheme=url.scheme.decode(),
                host=url.host.decode(),
                port=url.port,
                raw_path=url.target,
                userinfo=base64.b64decode(value[6:]),
            )

    return httpx.URL(
        scheme=url.scheme.decode(),
        host=url.host.decode(),
        port=url.port,
        raw_path=url.target,
    )


def _proxy_url(
    real_transport: Union[httpx.HTTPTransport, httpx.AsyncHTTPTransport],
) -> Optional[httpx.URL]:
    if isinstance(
        real_pool := real_transport._pool, (httpcore.HTTPProxy, httpcore.AsyncHTTPProxy)
    ):
        return _to_httpx_url(real_pool._proxy_url, real_pool._proxy_headers)
