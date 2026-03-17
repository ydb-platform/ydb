from enum import IntEnum, IntFlag


class MessageType(IntEnum):
    BOOTREQUEST = 1  # Client to server
    BOOTREPLY = 2  # Server to client


class HardwareType(IntEnum):
    ETHERNET = 1  # Ethernet (10Mb)
    EXPERIMENTAL_ETHERNET = 2
    AMATEUR_RADIO = 3
    TOKEN_RING = 4
    FDDI = 8
    ATM = 19
    WIRELESS_IEEE_802_11 = 20


class Flag(IntFlag):
    UNICAST = 0x0000  # Unicast response requested
    BROADCAST = 0x8000  # Broadcast response requested
