from abc import abstractmethod
from collections.abc import Container
from ipaddress import ip_address
from typing import Awaitable, Callable, Iterable, List

from multidict import MultiMapping

from aiohttp import hdrs, web

from .abc import ABC
from .exceptions import (
    IncorrectHostCount,
    IncorrectProtoCount,
    IPAddress,
    RemoteError,
    TooManyHeaders,
    UntrustedIP,
)
from .utils import (
    Elem,
    TrustedOrig,
    check_ip,
    parse_trusted_element,
    parse_trusted_list,
    remote_ip,
)


class XForwardedBase(ABC):
    async def setup(self, app: web.Application) -> None:
        app.middlewares.append(self.middleware)

    @web.middleware
    @abstractmethod
    async def middleware(
        self,
        request: web.Request,
        handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
    ) -> web.StreamResponse:
        pass

    def get_forwarded_for(self, headers: MultiMapping[str]) -> List[IPAddress]:
        forwarded_for: List[str] = headers.getall(hdrs.X_FORWARDED_FOR, [])
        if not forwarded_for:
            return []
        if len(forwarded_for) > 1:
            raise TooManyHeaders(hdrs.X_FORWARDED_FOR)
        forwarded_for = forwarded_for[0].split(",")
        valid_ips = []
        for a in forwarded_for:
            addr = a.strip()
            try:
                valid_ips.append(ip_address(addr))
            except ValueError:
                raise web.HTTPBadRequest(
                    reason=f"Invalid {hdrs.X_FORWARDED_FOR} header"
                )
        return valid_ips

    def get_forwarded_proto(self, headers: MultiMapping[str]) -> List[str]:
        forwarded_proto: List[str] = headers.getall(hdrs.X_FORWARDED_PROTO, [])
        if not forwarded_proto:
            return []
        if len(forwarded_proto) > 1:
            raise TooManyHeaders(hdrs.X_FORWARDED_PROTO)
        forwarded_proto = forwarded_proto[0].split(",")
        return [p.strip() for p in forwarded_proto]

    def get_forwarded_host(self, headers: MultiMapping[str]) -> List[str]:
        forwarded_host: List[str] = headers.getall(hdrs.X_FORWARDED_HOST, [])
        if not forwarded_host:
            return []
        if len(forwarded_host) > 1:
            raise TooManyHeaders(hdrs.X_FORWARDED_HOST)
        forwarded_host = forwarded_host[0].split(",")
        return [p.strip() for p in forwarded_host]


class XForwardedRelaxed(XForwardedBase):
    def __init__(self, num: int = 1) -> None:
        self._num = num

    @web.middleware
    async def middleware(
        self,
        request: web.Request,
        handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
    ) -> web.StreamResponse:
        try:
            overrides = {}
            headers = request.headers

            forwarded_for = self.get_forwarded_for(headers)
            if forwarded_for:
                overrides["remote"] = str(forwarded_for[-self._num])

            proto = self.get_forwarded_proto(headers)
            if proto:
                overrides["scheme"] = proto[-self._num]

            host = self.get_forwarded_host(headers)
            if host:
                overrides["host"] = str(host[-self._num])

            request = request.clone(**overrides)  # type: ignore[arg-type]

            return await handler(request)
        except RemoteError as exc:
            exc.log(request)
            return await self.raise_error(request)


class XForwardedFiltered(XForwardedBase):
    def __init__(self, trusted: Elem) -> None:
        if isinstance(trusted, str) or not isinstance(trusted, Container):
            raise TypeError("Trusted list should be a set of aaddresses or networks.")
        self._trusted = parse_trusted_element(trusted)

    @web.middleware
    async def middleware(
        self,
        request: web.Request,
        handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
    ) -> web.StreamResponse:
        try:
            overrides = {}
            headers = request.headers

            forwarded_for = list(reversed(self.get_forwarded_for(headers)))
            if not forwarded_for:
                return await handler(request)

            index = 0
            for ip in forwarded_for:
                try:
                    check_ip(self._trusted, ip)
                    index += 1
                    continue
                except UntrustedIP:
                    overrides["remote"] = str(ip)
                    break

            # If all the IP addresses are from trusted networks, take the
            # left-most.
            if "remote" not in overrides:
                index = -1
                overrides["remote"] = str(forwarded_for[-1])

            # Ideally this should take the scheme corresponding to the entry
            # in X-Forwarded-For that was chosen, but some proxies (the
            # Kubernetes NGINX ingress, for example) only retain one element
            # in X-Forwarded-Proto.  In that case, use what we have.
            proto = list(reversed(self.get_forwarded_proto(headers)))
            if proto:
                if index >= len(proto):
                    index = -1
                overrides["scheme"] = proto[index]

            host = list(reversed(self.get_forwarded_host(headers)))
            if host:
                if index >= len(host):
                    index = -1
                overrides["host"] = host[index]

            request = request.clone(**overrides)  # type: ignore[arg-type]
            return await handler(request)

        except RemoteError as exc:
            exc.log(request)
            return await self.raise_error(request)


class XForwardedStrict(XForwardedBase):
    def __init__(self, trusted: TrustedOrig, *, white_paths: Iterable[str] = ()):
        self._trusted = parse_trusted_list(trusted)
        self._white_paths = set(white_paths)

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
            headers = request.headers

            forwarded_for = self.get_forwarded_for(headers)
            assert request.transport is not None
            peer_ip, *_ = request.transport.get_extra_info("peername")
            ips = [ip_address(peer_ip)] + list(reversed(forwarded_for))
            ip = remote_ip(self._trusted, ips)
            overrides["remote"] = str(ip)

            proto = self.get_forwarded_proto(headers)
            if proto:
                if len(proto) > len(self._trusted):
                    raise IncorrectProtoCount(len(self._trusted), proto)
                overrides["scheme"] = proto[0]

            host = list(reversed(self.get_forwarded_host(headers)))
            if host:
                if len(host) > len(self._trusted):
                    raise IncorrectHostCount(len(self._trusted), host)
                overrides["host"] = host[0]

            request = request.clone(**overrides)  # type: ignore[arg-type]

            return await handler(request)

        except RemoteError as exc:
            exc.log(request)
            return await self.raise_error(request)
