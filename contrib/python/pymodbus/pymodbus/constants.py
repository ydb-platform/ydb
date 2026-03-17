"""Constants For Modbus Server/Client.

This is the single location for storing default
values for the servers and clients.
"""
import enum


INTERNAL_ERROR = "Pymodbus internal error"


class ModbusStatus(int, enum.Enum):
    """These represent various status codes in the modbus protocol.

    .. attribute:: WAITING

       This indicates that a modbus device is currently
       waiting for a given request to finish some running task.

    .. attribute:: READY

       This indicates that a modbus device is currently
       free to perform the next request task.

    .. attribute:: ON

       This indicates that the given modbus entity is on

    .. attribute:: OFF

       This indicates that the given modbus entity is off

    .. attribute:: SLAVE_ON

       This indicates that the given modbus slave is running

    .. attribute:: SLAVE_OFF

       This indicates that the given modbus slave is not running
    """

    WAITING = 0xFFFF
    READY = 0x0000
    ON = 0xFF00
    OFF = 0x0000
    SLAVE_ON = 0xFF
    SLAVE_OFF = 0x00


class Endian(str, enum.Enum):
    """An enumeration representing the various byte endianness.

    .. attribute:: AUTO

       This indicates that the byte order is chosen by the
       current native environment.

    .. attribute:: BIG

       This indicates that the bytes are in big endian format

    .. attribute:: LITTLE

       This indicates that the bytes are in little endian format

    .. note:: I am simply borrowing the format strings from the
       python struct module for my convenience.
    """

    AUTO = "@"
    BIG = ">"
    LITTLE = "<"


class ModbusPlusOperation(int, enum.Enum):
    """Represents the type of modbus plus request.

    .. attribute:: GET_STATISTICS

       Operation requesting that the current modbus plus statistics
       be returned in the response.

    .. attribute:: CLEAR_STATISTICS

       Operation requesting that the current modbus plus statistics
       be cleared and not returned in the response.
    """

    GET_STATISTICS = 0x0003
    CLEAR_STATISTICS = 0x0004


class DeviceInformation(int, enum.Enum):
    """Represents what type of device information to read.

    .. attribute:: BASIC

       This is the basic (required) device information to be returned.
       This includes VendorName, ProductCode, and MajorMinorRevision
       code.

    .. attribute:: REGULAR

       In addition to basic data objects, the device provides additional
       and optional identification and description data objects. All of
       the objects of this category are defined in the standard but their
       implementation is optional.

    .. attribute:: EXTENDED

       In addition to regular data objects, the device provides additional
       and optional identification and description private data about the
       physical device itself. All of these data are device dependent.

    .. attribute:: SPECIFIC

       Request to return a single data object.
    """

    BASIC = 0x01
    REGULAR = 0x02
    EXTENDED = 0x03
    SPECIFIC = 0x04

    def __str__(self):
        """Override to force int representation for enum members."""
        return str(int(self))


class MoreData(int, enum.Enum):
    """Represents the more follows condition.

    .. attribute:: NOTHING

       This indicates that no more objects are going to be returned.

    .. attribute:: KEEP_READING

       This indicates that there are more objects to be returned.
    """

    NOTHING = 0x00
    KEEP_READING = 0xFF

