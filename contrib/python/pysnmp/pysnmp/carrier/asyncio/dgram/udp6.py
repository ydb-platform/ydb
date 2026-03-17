#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#

import socket
import warnings
from typing import Tuple

from pysnmp.carrier.asyncio.dgram.base import DgramAsyncioProtocol
from pysnmp.carrier.base import AbstractTransportAddress

DOMAIN_NAME: Tuple[int, ...]
SNMP_UDP6_DOMAIN: Tuple[int, ...]
DOMAIN_NAME = SNMP_UDP6_DOMAIN = (1, 3, 6, 1, 2, 1, 100, 1, 2)


class Udp6TransportAddress(tuple, AbstractTransportAddress):
    """IPv6 transport address."""

    pass


class Udp6AsyncioTransport(DgramAsyncioProtocol):
    """UDP/IPv6 async socket transport."""

    SOCK_FAMILY = socket.has_ipv6 and socket.AF_INET6 or 0
    ADDRESS_TYPE = Udp6TransportAddress

    def normalize_address(self, transportAddress):
        """Normalize IPv6 address."""
        if "%" in transportAddress[0]:  # strip zone ID
            return self.ADDRESS_TYPE(
                (
                    transportAddress[0].split("%")[0],
                    transportAddress[1],
                    0,  # flowinfo
                    0,
                )
            )  # scopeid

        return self.ADDRESS_TYPE((transportAddress[0], transportAddress[1], 0, 0))


Udp6Transport = Udp6AsyncioTransport

# Old to new attribute mapping
deprecated_attributes = {
    "domainName": "DOMAIN_NAME",
    "snmpUDP6Domain": "SNMP_UDP6_DOMAIN",
}


def __getattr__(attr: str):
    if new_attr := deprecated_attributes.get(attr):
        warnings.warn(
            f"{attr} is deprecated. Please use {new_attr} instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return globals()[new_attr]
    raise AttributeError(f"module '{__name__}' has no attribute '{attr}'")
