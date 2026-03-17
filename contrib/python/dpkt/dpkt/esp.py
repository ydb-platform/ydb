# $Id: esp.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""Encapsulated Security Protocol."""
from __future__ import absolute_import

from . import dpkt


class ESP(dpkt.Packet):
    """Encapsulated Security Protocol.

    Encapsulating Security Payload (ESP) is a member of the Internet Protocol Security (IPsec) set of protocols that
    encrypt and authenticate the packets of data between computers using a Virtual Private Network (VPN). The focus
    and layer on which ESP operates makes it possible for VPNs to function securely.

    Attributes:
        __hdr__: Header fields of ESP.
            spi: (int): Security Parameters Index. An arbitrary value that, in combination with the destination
                IP address and security protocol (ESP), uniquely identifies the SA for this datagram. (4 bytes)
            spi: (int): Sequence number. This field contains a monotonically increasing counter value. (4 bytes)
    """

    __hdr__ = (
        ('spi', 'I', 0),
        ('seq', 'I', 0)
    )
