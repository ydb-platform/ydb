"""
Ctypes wrapper module for IXXAT Virtual CAN Interface V4 on win32 systems

Copyright (C) 2016 Giuseppe Corbelli <giuseppe.corbelli@weightpack.com>
Copyright (C) 2019 Marcel Kanter <marcel.kanter@googlemail.com>
"""

from can import (
    CanInitializationError,
    CanOperationError,
    CanTimeoutError,
)

__all__ = [
    "VCIBusOffError",
    "VCIDeviceNotFoundError",
    "VCIError",
    "VCIRxQueueEmptyError",
    "VCITimeout",
]


class VCITimeout(CanTimeoutError):
    """Wraps the VCI_E_TIMEOUT error"""


class VCIError(CanOperationError):
    """Try to display errors that occur within the wrapped C library nicely."""


class VCIRxQueueEmptyError(VCIError):
    """Wraps the VCI_E_RXQUEUE_EMPTY error"""

    def __init__(self):
        super().__init__("Receive queue is empty")


class VCIBusOffError(VCIError):
    def __init__(self):
        super().__init__("Controller is in BUSOFF state")


class VCIDeviceNotFoundError(CanInitializationError):
    pass
