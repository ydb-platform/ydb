# $Id: yahoo.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""Yahoo Messenger."""
from __future__ import absolute_import

from . import dpkt


class YHOO(dpkt.Packet):
    """Yahoo Messenger.

    Yahoo! Messenger (sometimes abbreviated Y!M) was an advertisement-supported instant messaging client and associated
     protocol provided by Yahoo!. Yahoo! Messenger was provided free of charge and could be downloaded and used with a
     generic "Yahoo ID" which also allowed access to other Yahoo! services, such as Yahoo! Mail. The service also
     offered VoIP, file transfers, webcam hosting, a text messaging service, and chat rooms in various categories.

    Attributes:
        __hdr__: Header fields of Yahoo Messenger.
            version: (bytes): Version. (8 bytes)
            length: (int): Length. (4 bytes)
            service: (int): Service. (4 bytes)
            connid: (int): Connection ID. (4 bytes)
            magic: (int): Magic. (4 bytes)
            unknown: (int): Unknown. (4 bytes)
            type: (int): Type. (4 bytes)
            nick1: (bytes): Nick1. (36 bytes)
            nick2: (bytes): Nick2. (36 bytes)
    """

    __hdr__ = [
        ('version', '8s', ' ' * 8),
        ('length', 'I', 0),
        ('service', 'I', 0),
        ('connid', 'I', 0),
        ('magic', 'I', 0),
        ('unknown', 'I', 0),
        ('type', 'I', 0),
        ('nick1', '36s', ' ' * 36),
        ('nick2', '36s', ' ' * 36)
    ]
    __byte_order__ = '<'


class YMSG(dpkt.Packet):
    __hdr__ = [
        ('version', '8s', ' ' * 8),
        ('length', 'H', 0),
        ('type', 'H', 0),
        ('unknown1', 'I', 0),
        ('unknown2', 'I', 0)
    ]
