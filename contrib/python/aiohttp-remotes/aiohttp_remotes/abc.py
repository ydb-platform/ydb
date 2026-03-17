import abc

from typing_extensions import NoReturn

from aiohttp import web


class ABC(abc.ABC):
    @abc.abstractmethod
    async def setup(self, app: web.Application) -> None:
        pass  # pragma: no cover

    async def raise_error(self, request: web.Request) -> NoReturn:
        raise web.HTTPBadRequest()
