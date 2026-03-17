"""A module to allow CAN over UDP on IPv4/IPv6 multicast."""

__all__ = [
    "UdpMulticastBus",
    "bus",
    "utils",
]

from .bus import UdpMulticastBus
