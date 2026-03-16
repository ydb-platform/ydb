from typing import Any, AsyncGenerator, List, Optional

from httpx import AsyncClient, Response

from aiochclient.exceptions import ChClientError
from aiochclient.http_clients.abc import HttpClientABC


class HttpxHttpClient(HttpClientABC):
    line_separator: bytes = b'\n'

    def __init__(self, session: Optional[AsyncClient]):
        if session:
            self._session = session
        else:
            self._session = AsyncClient()

    async def get(self, url: str, params: dict, headers: dict) -> None:
        resp = await self._session.get(url=url, params=params, headers=headers)
        await _check_response(resp)

    async def post_return_lines(
        self, url: str, params: dict, headers: dict, data: Any
    ) -> AsyncGenerator[bytes, None]:
        resp = await self._session.post(
            url=url, params=params, headers=headers, content=data
        )
        await _check_response(resp)

        buffer: bytes = b''
        async for chunk in resp.aiter_bytes():
            lines: List[bytes] = chunk.split(self.line_separator)
            if buffer:
                lines[0] = buffer + lines[0]
            for line in lines[:-1]:
                yield line + self.line_separator
            buffer = lines[-1]
        assert not buffer

    async def post_no_return(
        self, url: str, params: dict, headers: dict, data: Any
    ) -> None:
        resp = await self._session.post(
            url=url, params=params, headers=headers, content=data
        )
        await _check_response(resp)

    async def close(self) -> None:
        await self._session.aclose()


async def _check_response(resp):
    if resp.status_code != 200:
        raise ChClientError(await _read_error_body(resp))


async def _read_error_body(resp: Response):
    return (await resp.aread()).decode(errors='replace')
