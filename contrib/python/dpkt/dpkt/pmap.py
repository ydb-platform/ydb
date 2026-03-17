# $Id: pmap.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""Portmap / rpcbind."""
from __future__ import absolute_import

from . import dpkt

PMAP_PROG = 100000
PMAP_PROCDUMP = 4
PMAP_VERS = 2


class Pmap(dpkt.Packet):
    """Portmap / rpcbind.

    The port mapper (rpc.portmap or just portmap, or rpcbind) is an Open Network Computing Remote Procedure
    Call (ONC RPC) service that runs on network nodes that provide other ONC RPC services. The port mapper service
    always uses TCP or UDP port 111; a fixed port is required for it, as a client would not be able to get the
    port number for the port mapper service from the port mapper itself.

    Attributes:
        __hdr__: Header fields of Pmap.
            prog: (int) Program. (4 bytes)
            vers: (int) Version. (4 bytes)
            prot: (int) Protocol. (4 bytes)
            port: (int) Port. (4 bytes)
    """

    __hdr__ = (
        ('prog', 'I', 0),
        ('vers', 'I', 0),
        ('prot', 'I', 0),
        ('port', 'I', 0),
    )
