from ipaddress import ip_address
from typing import Awaitable, Callable, Iterable

from aiohttp import web

from .abc import ABC
from .exceptions import IncorrectForwardedCount, RemoteError
from .utils import TrustedOrig, parse_trusted_list, remote_ip


class ForwardedRelaxed(ABC):
    def __init__(self, num: int = 1) -> None:
        self._num = num

    async def setup(self, app: web.Application) -> None:
        app.middlewares.append(self.middleware)

    @web.middleware
    async def middleware(
        self,
        request: web.Request,
        handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
    ) -> web.StreamResponse:
        overrides = {}

        for elem in reversed(request.forwarded[-self._num :]):
            for_ = elem.get("for")
            if for_:
                overrides["remote"] = for_
            proto = elem.get("proto")
            if proto is not None:
                overrides["scheme"] = proto
            host = elem.get("host")
            if host is not None:
                overrides["host"] = host

        request = request.clone(**overrides)  # type: ignore[arg-type]
        return await handler(request)


class ForwardedStrict(ABC):
    def __init__(self, trusted: TrustedOrig, *, white_paths: Iterable[str] = ()):
        self._trusted = parse_trusted_list(trusted)
        self._white_paths = set(white_paths)

    async def setup(self, app: web.Application) -> None:
        app.middlewares.append(self.middleware)

    @web.middleware
    async def middleware(
        self,
        request: web.Request,
        handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
    ) -> web.StreamResponse:
        if request.path in self._white_paths:
            return await handler(request)
        try:
            overrides = {}

            forwarded = request.forwarded
            if len(self._trusted) != len(forwarded):
                raise IncorrectForwardedCount(len(self._trusted), len(forwarded))

            assert request.transport is not None
            peer_ip, *_ = request.transport.get_extra_info("peername")
            ips = [ip_address(peer_ip)]

            for elem in reversed(request.forwarded):
                for_ = elem.get("for")
                if for_:
                    ips.append(ip_address(for_))
                proto = elem.get("proto")
                if proto is not None:
                    overrides["scheme"] = proto
                host = elem.get("host")
                if host is not None:
                    overrides["host"] = host

                overrides["remote"] = str(remote_ip(self._trusted, ips))

            request = request.clone(**overrides)  # type: ignore[arg-type]
            return await handler(request)
        except RemoteError as exc:
            exc.log(request)
            return await self.raise_error(request)
