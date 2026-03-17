"""
This wrapper is for windows or direct access via CANAL API.
Socket CAN is recommended under Unix/Linux systems.
"""

import logging
from ctypes import *
from enum import IntEnum

import can

from ...exceptions import error_check
from ...typechecking import StringPathLike

log = logging.getLogger("can.usb2can")

# type definitions
flags_t = c_ulong
pConfigureStr = c_char_p
handle_t = c_long
timeout_t = c_ulong
filter_t = c_ulong

# flags mappings
IS_ERROR_FRAME = 4
IS_REMOTE_FRAME = 2
IS_ID_TYPE = 1


class CanalError(IntEnum):
    SUCCESS = 0
    BAUDRATE = 1
    BUS_OFF = 2
    BUS_PASSIVE = 3
    BUS_WARNING = 4
    CAN_ID = 5
    CAN_MESSAGE = 6
    CHANNEL = 7
    FIFO_EMPTY = 8
    FIFO_FULL = 9
    FIFO_SIZE = 10
    FIFO_WAIT = 11
    GENERIC = 12
    HARDWARE = 13
    INIT_FAIL = 14
    INIT_MISSING = 15
    INIT_READY = 16
    NOT_SUPPORTED = 17
    OVERRUN = 18
    RCV_EMPTY = 19
    REGISTER = 20
    TRM_FULL = 21
    ERRFRM_STUFF = 22
    ERRFRM_FORM = 23
    ERRFRM_ACK = 24
    ERRFRM_BIT1 = 25
    ERRFRM_BIT0 = 26
    ERRFRM_CRC = 27
    LIBRARY = 28
    PROCADDRESS = 29
    ONLY_ONE_INSTANCE = 30
    SUB_DRIVER = 31
    TIMEOUT = 32
    NOT_OPEN = 33
    PARAMETER = 34
    MEMORY = 35
    INTERNAL = 36
    COMMUNICATION = 37


class CanalStatistics(Structure):
    _fields_ = [
        ("ReceiveFrams", c_ulong),
        ("TransmistFrams", c_ulong),
        ("ReceiveData", c_ulong),
        ("TransmitData", c_ulong),
        ("Overruns", c_ulong),
        ("BusWarnings", c_ulong),
        ("BusOff", c_ulong),
    ]


stat = CanalStatistics


class CanalStatus(Structure):
    _fields_ = [
        ("channel_status", c_ulong),
        ("lasterrorcode", c_ulong),
        ("lasterrorsubcode", c_ulong),
        ("lasterrorstr", c_byte * 80),
    ]


# data type for the CAN Message
class CanalMsg(Structure):
    _fields_ = [
        ("flags", c_ulong),
        ("obid", c_ulong),
        ("id", c_ulong),
        ("sizeData", c_ubyte),
        ("data", c_ubyte * 8),
        ("timestamp", c_ulong),
    ]


class Usb2CanAbstractionLayer:
    """A low level wrapper around the usb2can library.

    Documentation: http://www.8devices.com/media/products/usb2can/downloads/CANAL_API.pdf
    """

    def __init__(self, dll: StringPathLike = "usb2can.dll") -> None:
        """
        :param dll:
            the path to the usb2can DLL to load

        :raises ~can.exceptions.CanInterfaceNotImplementedError:
            if the DLL could not be loaded
        """
        try:
            self.__m_dllBasic = windll.LoadLibrary(dll)
            if self.__m_dllBasic is None:
                raise Exception("__m_dllBasic is None")

        except Exception as error:
            message = f"DLL failed to load at path: {dll}"
            raise can.CanInterfaceNotImplementedError(message) from error

    def open(self, configuration: str, flags: int):
        """
        Opens a CAN connection using `CanalOpen()`.

        :param configuration:
            the configuration: "device_id; baudrate"
        :param flags:
            the flags to be set
        :returns:
            Valid handle for CANAL API functions on success

        :raises ~can.exceptions.CanInterfaceNotImplementedError:
            if any error occurred
        """
        try:
            # we need to convert this into bytes, since the underlying DLL cannot
            # handle non-ASCII configuration strings
            config_ascii = configuration.encode("ascii", "ignore")
            result = self.__m_dllBasic.CanalOpen(config_ascii, flags)
        except Exception as ex:
            # catch any errors thrown by this call and re-raise
            raise can.CanInitializationError(
                f'CanalOpen() failed, configuration: "{configuration}", error: {ex}'
            ) from ex
        else:
            # any greater-than-zero return value indicates a success
            # (see https://grodansparadis.gitbooks.io/the-vscp-daemon/canal_interface_specification.html)
            # raise an error if the return code is <= 0
            if result <= 0:
                raise can.CanInitializationError(
                    f'CanalOpen() failed, configuration: "{configuration}"',
                    error_code=result,
                )
            else:
                return result

    def close(self, handle) -> CanalError:
        with error_check("Failed to close"):
            return CanalError(self.__m_dllBasic.CanalClose(handle))

    def send(self, handle, msg) -> CanalError:
        with error_check("Failed to transmit frame"):
            return CanalError(self.__m_dllBasic.CanalSend(handle, msg))

    def receive(self, handle, msg) -> CanalError:
        with error_check("Receive error"):
            return CanalError(self.__m_dllBasic.CanalReceive(handle, msg))

    def blocking_send(self, handle, msg, timeout) -> CanalError:
        with error_check("Blocking send error"):
            return CanalError(self.__m_dllBasic.CanalBlockingSend(handle, msg, timeout))

    def blocking_receive(self, handle, msg, timeout) -> CanalError:
        with error_check("Blocking Receive Failed"):
            return CanalError(
                self.__m_dllBasic.CanalBlockingReceive(handle, msg, timeout)
            )

    def get_status(self, handle, status) -> CanalError:
        with error_check("Get status failed"):
            return CanalError(self.__m_dllBasic.CanalGetStatus(handle, status))

    def get_statistics(self, handle, statistics) -> CanalError:
        with error_check("Get Statistics failed"):
            return CanalError(self.__m_dllBasic.CanalGetStatistics(handle, statistics))

    def get_version(self):
        with error_check("Failed to get version info"):
            return self.__m_dllBasic.CanalGetVersion()

    def get_library_version(self):
        with error_check("Failed to get DLL version"):
            return self.__m_dllBasic.CanalGetDllVersion()

    def get_vendor_string(self):
        with error_check("Failed to get vendor string"):
            return self.__m_dllBasic.CanalGetVendorString()
