# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import annotations

import asyncio
import contextlib
import logging
import socket

from ..._addressing import (
    Address,
    ResolvedAddress,
)
from ...exceptions import ServiceUnavailable
from ..util import AsyncUtil


log = logging.getLogger("neo4j.io")


def _resolved_addresses_from_info(info, host_name):
    resolved = []
    for fam, _, _, _, addr in info:
        if fam == socket.AF_INET6 and addr[3] != 0:
            # skip any IPv6 addresses with a non-zero scope id
            # as these appear to cause problems on some platforms
            continue
        if addr not in resolved:
            resolved.append(addr)
            yield ResolvedAddress(addr, host_name=host_name)


def _try_get_socket_attributes(*attrs):
    for attr in attrs:
        with contextlib.suppress(AttributeError):
            yield getattr(socket, attr)


_RETRYABLE_DNS_ERRNOS = set(
    _try_get_socket_attributes(
        "EAI_ADDRFAMILY",
        "EAI_AGAIN",
        "EAI_MEMORY",
        "EAI_NODATA",
    )
)
_EAI_NONAME = set(_try_get_socket_attributes("EAI_NONAME"))


class AsyncNetworkUtil:
    @staticmethod
    async def get_address_info(
        host, port, *, family=0, type=0, proto=0, flags=0
    ):
        loop = asyncio.get_event_loop()
        return await loop.getaddrinfo(
            host, port, family=family, type=type, proto=proto, flags=flags
        )

    @staticmethod
    async def _dns_resolver(address, family=0):
        """
        Apply regular DNS resolution.

        Takes an address object and optional address family for filtering.

        :param address:
        :param family:
        :returns:
        """
        try:
            info = await AsyncNetworkUtil.get_address_info(
                address.host,
                address.port,
                family=family,
                type=socket.SOCK_STREAM,
            )
        except OSError as e:
            # note: on some systems like Windows, EAI_NONAME and EAI_NODATA
            #       have the same error-code.
            if e.errno in _EAI_NONAME and (
                address.host is None and address.port is None
            ):
                err_cls = ValueError
            elif e.errno in _RETRYABLE_DNS_ERRNOS or e.errno in _EAI_NONAME:
                err_cls = ServiceUnavailable
            else:
                err_cls = ValueError
            raise err_cls(
                f"Failed to DNS resolve address {address}: {e}"
            ) from e
        return list(_resolved_addresses_from_info(info, address._host_name))

    @staticmethod
    async def resolve_address(address, family=0, resolver=None):
        """
        Carry out domain name resolution on this Address object.

        If a resolver function is supplied, and is callable, this is
        called first, with this object as its argument. This may yield
        multiple output addresses, which are chained into a subsequent
        regular DNS resolution call. If no resolver function is passed,
        the DNS resolution is carried out on the original Address
        object.

        This function returns a list of resolved Address objects.

        :param address: the Address to resolve
        :param family: optional address family to filter resolved
                       addresses by (e.g. ``socket.AF_INET6``)
        :param resolver: optional customer resolver function to be
                         called before regular DNS resolution
        """
        if isinstance(address, ResolvedAddress):
            yield address
            return

        log.debug("[#0000]  _: <RESOLVE> in: %s", address)
        if resolver:
            addresses_resolved = map(
                Address,
                await AsyncUtil.callback(resolver, address),
            )
            for address_resolved in addresses_resolved:
                log.debug(
                    "[#0000]  _: <RESOLVE> custom resolver out: %s",
                    address_resolved,
                )
                addresses_dns_resolved = await AsyncNetworkUtil._dns_resolver(
                    address_resolved, family=family
                )
                for address_dns_resolved in addresses_dns_resolved:
                    log.debug(
                        "[#0000]  _: <RESOLVE> dns resolver out: %s",
                        address_dns_resolved,
                    )
                    yield address_dns_resolved
        else:
            for address_dns_resolved in await AsyncNetworkUtil._dns_resolver(
                address, family=family
            ):
                log.debug(
                    "[#0000]  _: <RESOLVE> dns resolver out: %s",
                    address_dns_resolved,
                )
                yield address_dns_resolved


class NetworkUtil:
    @staticmethod
    def get_address_info(host, port, *, family=0, type=0, proto=0, flags=0):
        return socket.getaddrinfo(host, port, family, type, proto, flags)

    @staticmethod
    def _dns_resolver(address, family=0):
        """
        Apply regular DNS resolution.

        Takes an address object and optional address family for filtering.

        :param address:
        :param family:
        :returns:
        """
        try:
            info = NetworkUtil.get_address_info(
                address.host,
                address.port,
                family=family,
                type=socket.SOCK_STREAM,
            )
        except OSError as e:
            # note: on some systems like Windows, EAI_NONAME and EAI_NODATA
            #       have the same error-code.
            if e.errno in _EAI_NONAME and (
                address.host is None and address.port is None
            ):
                err_cls = ValueError
            elif e.errno in _RETRYABLE_DNS_ERRNOS or e.errno in _EAI_NONAME:
                err_cls = ServiceUnavailable
            else:
                err_cls = ValueError
            raise err_cls(
                f"Failed to DNS resolve address {address}: {e}"
            ) from e
        return _resolved_addresses_from_info(info, address._host_name)

    @staticmethod
    def resolve_address(address, family=0, resolver=None):
        """
        Carry out domain name resolution on this Address object.

        If a resolver function is supplied, and is callable, this is
        called first, with this object as its argument. This may yield
        multiple output addresses, which are chained into a subsequent
        regular DNS resolution call. If no resolver function is passed,
        the DNS resolution is carried out on the original Address
        object.

        This function returns a list of resolved Address objects.

        :param address: the Address to resolve
        :param family: optional address family to filter resolved
                       addresses by (e.g. ``socket.AF_INET6``)
        :param resolver: optional customer resolver function to be
                         called before regular DNS resolution
        """
        if isinstance(address, ResolvedAddress):
            yield address
            return

        log.debug("[#0000]  _: <RESOLVE> in: %s", address)
        if resolver:
            addresses_resolved = map(Address, resolver(address))
            for address_resolved in addresses_resolved:
                log.debug(
                    "[#0000]  _: <RESOLVE> custom resolver out: %s",
                    address_resolved,
                )
                addresses_dns_resolved = NetworkUtil._dns_resolver(
                    address_resolved, family=family
                )
                for address_dns_resolved in addresses_dns_resolved:
                    log.debug(
                        "[#0000]  _: <RESOLVE> dns resolver out: %s",
                        address_dns_resolved,
                    )
                    yield address_dns_resolved
        else:
            for address_dns_resolved in NetworkUtil._dns_resolver(
                address, family=family
            ):
                log.debug(
                    "[#0000]  _: <RESOLVE> dns resolver out: %s",
                    address_dns_resolved,
                )
                yield address_dns_resolved
