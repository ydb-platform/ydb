from typing import Awaitable, Callable, Iterable, Set, Union

from aiohttp import web

from .abc import ABC


class ANY:
    def __contains__(self, item: object) -> bool:
        return True


class AllowedHosts(ABC):
    def __init__(
        self,
        allowed_hosts: Iterable[str] = ("*",),
        *,
        white_paths: Iterable[str] = (),
    ) -> None:
        real_allowed_hosts: Union[Set[str], ANY] = set(allowed_hosts)

        if "*" in real_allowed_hosts:
            real_allowed_hosts = ANY()

        self._allowed_hosts = real_allowed_hosts
        self._white_paths = set(white_paths)

    async def setup(self, app: web.Application) -> None:
        app.middlewares.append(self.middleware)

    @web.middleware
    async def middleware(
        self,
        request: web.Request,
        handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
    ) -> web.StreamResponse:
        if (
            request.path not in self._white_paths
            and request.host not in self._allowed_hosts
        ):
            return await self.raise_error(request)

        return await handler(request)
