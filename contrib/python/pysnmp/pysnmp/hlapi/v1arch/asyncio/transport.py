#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
import asyncio
import socket
from typing import Tuple

from pysnmp.carrier.asyncio.dgram import udp, udp6
from pysnmp.error import PySnmpError
from pysnmp.hlapi.transport import AbstractTransportTarget

__all__ = ["Udp6TransportTarget", "UdpTransportTarget"]


class UdpTransportTarget(AbstractTransportTarget):
    """Represent UDP/IPv4 transport endpoint.

    This object can be used for passing UDP/IPv4 configuration
    information to the
    :py:class:`~pysnmp.hlapi.v1arch.asyncio.AsyncCommandGenerator` and
    :py:class:`~pysnmp.hlapi.v1arch.asyncio.AsyncNotificationOriginator`
    objects scheduled on I/O by :py:class:`~pysnmp.hlapi.SnmpDispatcher`
    class instance.

    See :RFC:`1906#section-3` for more information on the UDP transport mapping.

    Parameters
    ----------
    transportAddr: tuple
        Indicates remote address in Python :py:mod:`socket` module format
        which is a tuple of FQDN, port where FQDN is a string representing
        either hostname or IPv4 address in quad-dotted form, port is an
        integer.
    timeout: :py:class:`int`
        Response timeout in seconds.
    retries: :py:class:`int`
        Maximum number of request retries, 0 retries means just a single
        request.
    tagList: str
        Arbitrary string that contains a list of tag values which are used
        to select target addresses for a particular operation
        (:RFC:`3413#section-4.1.4`).

    Examples
    --------
    >>> from pysnmp.hlapi.v1arch.asyncio import UdpTransportTarget
    >>> await UdpTransportTarget.create(('demo.pysnmp.com', 161))
    UdpTransportTarget(('195.218.195.228', 161), timeout=1, retries=5)
    >>>
    """

    TRANSPORT_DOMAIN: Tuple[int, ...] = udp.DOMAIN_NAME
    PROTO_TRANSPORT = udp.UdpAsyncioTransport

    async def _resolve_address(self, transportAddr: Tuple) -> Tuple[str, int]:
        try:
            loop = asyncio.get_event_loop()
            return (
                await loop.getaddrinfo(
                    transportAddr[0],
                    transportAddr[1],
                    family=socket.AF_INET,
                    type=socket.SOCK_DGRAM,
                    proto=socket.IPPROTO_UDP,
                )
            )[0][4][:2]
        except socket.gaierror as exc:
            raise PySnmpError(
                "Bad IPv4/UDP transport address {}: {}".format(
                    "@".join([str(x) for x in transportAddr]), exc
                )
            )


class Udp6TransportTarget(AbstractTransportTarget):
    """Represent UDP/IPv6 transport endpoint.

    This object can be used for passing UDP/IPv6 configuration
    information to the
    :py:class:`~pysnmp.hlapi.v1arch.asyncio.AsyncCommandGenerator` and
    :py:class:`~pysnmp.hlapi.v1arch.asyncio.AsyncNotificationOriginator`
    objects scheduled on I/O by :py:class:`~pysnmp.hlapi.SnmpDispatcher`
    class instance.

    See :RFC:`1906#section-3`, :RFC:`2851#section-4` for more information
    on the UDP and IPv6 transport mapping.

    Parameters
    ----------
    transportAddr: tuple
        Indicates remote address in Python :py:mod:`socket` module format
        which is a tuple of FQDN, port where FQDN is a string representing
        either hostname or IPv6 address in one of three conventional forms
        (:RFC:`1924#section-3`), port is an integer.
    timeout: int
        Response timeout in seconds.
    retries: int
        Maximum number of request retries, 0 retries means just a single
        request.
    tagList: str
        Arbitrary string that contains a list of tag values which are used
        to select target addresses for a particular operation
        (:RFC:`3413#section-4.1.4`).

    Examples
    --------
    >>> from pysnmp.hlapi.v1arch.asyncio import Udp6TransportTarget
    >>> await Udp6TransportTarget.create(('google.com', 161))
    Udp6TransportTarget(('2a00:1450:4014:80a::100e', 161), timeout=1, retries=5, tagList='')
    >>> await Udp6TransportTarget.create(('FEDC:BA98:7654:3210:FEDC:BA98:7654:3210', 161))
    Udp6TransportTarget(('fedc:ba98:7654:3210:fedc:ba98:7654:3210', 161), timeout=1, retries=5, tagList='')
    >>> await Udp6TransportTarget.create(('1080:0:0:0:8:800:200C:417A', 161))
    Udp6TransportTarget(('1080::8:800:200c:417a', 161), timeout=1, retries=5, tagList='')
    >>> await Udp6TransportTarget.create(('::0', 161))
    Udp6TransportTarget(('::', 161), timeout=1, retries=5, tagList='')
    >>> await Udp6TransportTarget.create(('::', 161))
    Udp6TransportTarget(('::', 161), timeout=1, retries=5, tagList='')
    >>>
    """

    TRANSPORT_DOMAIN: Tuple[int, ...] = udp6.DOMAIN_NAME
    PROTO_TRANSPORT = udp6.Udp6AsyncioTransport

    async def _resolve_address(self, transportAddr: Tuple) -> Tuple[str, int]:
        try:
            loop = asyncio.get_event_loop()
            return (
                await loop.getaddrinfo(
                    transportAddr[0],
                    transportAddr[1],
                    family=socket.AF_INET6,
                    type=socket.SOCK_DGRAM,
                    proto=socket.IPPROTO_UDP,
                )
            )[0][4][:2]
        except socket.gaierror as exc:
            raise PySnmpError(
                "Bad IPv6/UDP transport address {}: {}".format(
                    "@".join([str(x) for x in transportAddr]), exc
                )
            )
