# Defines a copy of the IP protocol as IPIP so the protocol parsing in ip.py
# can decode IPIP packets.
from __future__ import absolute_import

from .ip import IP as IPIP
