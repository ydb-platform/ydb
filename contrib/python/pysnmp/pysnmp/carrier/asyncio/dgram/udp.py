#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
# Copyright (C) 2014, Zebra Technologies
# Authors: Matt Hooks <me@matthooks.com>
#          Zachary Lorusso <zlorusso@gmail.com>
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice,
#   this list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright
#   notice, this list of conditions and the following disclaimer in the
#   documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
# IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
# THE POSSIBILITY OF SUCH DAMAGE.
#

import socket
import warnings
from typing import Tuple

from pysnmp.carrier.asyncio.dgram.base import DgramAsyncioProtocol
from pysnmp.carrier.base import AbstractTransportAddress

DOMAIN_NAME: Tuple[int, ...]
SNMP_UDP6_DOMAIN: Tuple[int, ...]
DOMAIN_NAME = SNMP_UDP_DOMAIN = (1, 3, 6, 1, 6, 1, 1)


class UdpTransportAddress(tuple, AbstractTransportAddress):
    """UDP transport address."""

    pass


class UdpAsyncioTransport(DgramAsyncioProtocol):
    """UDP async transport."""

    SOCK_FAMILY = socket.AF_INET
    ADDRESS_TYPE = UdpTransportAddress


UdpTransport = UdpAsyncioTransport

# Old to new attribute mapping
deprecated_attributes = {
    "domainName": "DOMAIN_NAME",
    "snmpUDPDomain": "SNMP_UDP_DOMAIN",
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
