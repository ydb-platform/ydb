# -*- coding: utf-8 -*-
"""High level wrappers for resources.

This file is part of PyVISA.

:copyright: 2014-2024 by PyVISA Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

from .firewire import FirewireInstrument
from .gpib import GPIBInstrument, GPIBInterface
from .messagebased import MessageBasedResource
from .pxi import PXIInstrument, PXIMemory
from .registerbased import RegisterBasedResource
from .resource import Resource
from .serial import SerialInstrument
from .tcpip import TCPIPInstrument, TCPIPSocket
from .usb import USBInstrument, USBRaw
from .vxi import VXIBackplane, VXIInstrument, VXIMemory

__all__ = [
    "FirewireInstrument",
    "GPIBInstrument",
    "GPIBInterface",
    "MessageBasedResource",
    "PXIInstrument",
    "PXIMemory",
    "RegisterBasedResource",
    "Resource",
    "SerialInstrument",
    "TCPIPInstrument",
    "TCPIPSocket",
    "USBInstrument",
    "USBRaw",
    "VXIBackplane",
    "VXIInstrument",
    "VXIMemory",
]
