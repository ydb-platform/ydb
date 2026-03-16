"""Modbus Device Controller.

These are the device management handlers.  They should be
maintained in the server context and the various methods
should be inserted in the correct locations.
"""
from __future__ import annotations


__all__ = [
    "DeviceInformationFactory",
    "ModbusDeviceIdentification",
    "ModbusPlusStatistics",
]

import struct

# pylint: disable=missing-type-doc
from collections import OrderedDict

from pymodbus.constants import INTERNAL_ERROR, DeviceInformation
from pymodbus.events import ModbusEvent
from pymodbus.utilities import dict_property


# ---------------------------------------------------------------------------#
#  Modbus Plus Statistics
# ---------------------------------------------------------------------------#
class ModbusPlusStatistics:
    """This is used to maintain the current modbus plus statistics count.

    As of right now this is simply a stub to complete the modbus implementation.
    For more information, see the modbus implementation guide page 87.
    """

    __data = OrderedDict(
        {
            "node_type_id": [0x00] * 2,  # 00
            "software_version_number": [0x00] * 2,  # 01
            "network_address": [0x00] * 2,  # 02
            "mac_state_variable": [0x00] * 2,  # 03
            "peer_status_code": [0x00] * 2,  # 04
            "token_pass_counter": [0x00] * 2,  # 05
            "token_rotation_time": [0x00] * 2,  # 06
            "program_master_token_failed": [0x00],  # 07 hi
            "data_master_token_failed": [0x00],  # 07 lo
            "program_master_token_owner": [0x00],  # 08 hi
            "data_master_token_owner": [0x00],  # 08 lo
            "program_slave_token_owner": [0x00],  # 09 hi
            "data_slave_token_owner": [0x00],  # 09 lo
            "data_slave_command_transfer": [0x00],  # 10 hi
            "__unused_10_lowbit": [0x00],  # 10 lo
            "program_slave_command_transfer": [0x00],  # 11 hi
            "program_master_rsp_transfer": [0x00],  # 11 lo
            "program_slave_auto_logout": [0x00],  # 12 hi
            "program_master_connect_status": [0x00],  # 12 lo
            "receive_buffer_dma_overrun": [0x00],  # 13 hi
            "pretransmit_deferral_error": [0x00],  # 13 lo
            "frame_size_error": [0x00],  # 14 hi
            "repeated_command_received": [0x00],  # 14 lo
            "receiver_alignment_error": [0x00],  # 15 hi
            "receiver_collision_abort_error": [0x00],  # 15 lo
            "bad_packet_length_error": [0x00],  # 16 hi
            "receiver_crc_error": [0x00],  # 16 lo
            "transmit_buffer_dma_underrun": [0x00],  # 17 hi
            "bad_link_address_error": [0x00],  # 17 lo
            "bad_mac_function_code_error": [0x00],  # 18 hi
            "internal_packet_length_error": [0x00],  # 18 lo
            "communication_failed_error": [0x00],  # 19 hi
            "communication_retries": [0x00],  # 19 lo
            "no_response_error": [0x00],  # 20 hi
            "good_receive_packet": [0x00],  # 20 lo
            "unexpected_path_error": [0x00],  # 21 hi
            "exception_response_error": [0x00],  # 21 lo
            "forgotten_transaction_error": [0x00],  # 22 hi
            "unexpected_response_error": [0x00],  # 22 lo
            "active_station_bit_map": [0x00] * 8,  # 23-26
            "token_station_bit_map": [0x00] * 8,  # 27-30
            "global_data_bit_map": [0x00] * 8,  # 31-34
            "receive_buffer_use_bit_map": [0x00] * 8,  # 35-37
            "data_master_output_path": [0x00] * 8,  # 38-41
            "data_slave_input_path": [0x00] * 8,  # 42-45
            "program_master_outptu_path": [0x00] * 8,  # 46-49
            "program_slave_input_path": [0x00] * 8,  # 50-53
        }
    )

    def __init__(self):
        """Initialize the modbus plus statistics with the default information."""
        self.reset()

    def __iter__(self):
        """Iterate over the statistics.

        :returns: An iterator of the modbus plus statistics
        """
        return iter(self.__data.items())

    def reset(self):
        """Clear all of the modbus plus statistics."""
        for key in self.__data:
            self.__data[key] = [0x00] * len(self.__data[key])

    def summary(self):
        """Return a summary of the modbus plus statistics.

        :returns: 54 16-bit words representing the status
        """
        return iter(self.__data.values())

    def encode(self):
        """Return a summary of the modbus plus statistics.

        :returns: 54 16-bit words representing the status
        """
        total, values = [], sum(self.__data.values(), [])  # noqa: RUF017
        for i in range(0, len(values), 2):
            total.append((values[i] << 8) | values[i + 1])
        return total


# ---------------------------------------------------------------------------#
#  Device Information Control
# ---------------------------------------------------------------------------#
class ModbusDeviceIdentification:
    """This is used to supply the device identification.

    For the readDeviceIdentification function

    For more information read section 6.21 of the modbus
    application protocol.
    """

    __data = {
        0x00: "",  # VendorName
        0x01: "",  # ProductCode
        0x02: "",  # MajorMinorRevision
        0x03: "",  # VendorUrl
        0x04: "",  # ProductName
        0x05: "",  # ModelName
        0x06: "",  # UserApplicationName
        0x07: "",  # reserved
        0x08: "",  # reserved
        # 0x80 -> 0xFF are privatek
    }

    __names = [
        "VendorName",
        "ProductCode",
        "MajorMinorRevision",
        "VendorUrl",
        "ProductName",
        "ModelName",
        "UserApplicationName",
    ]

    def __init__(self, info=None, info_name=None):
        """Initialize the datastore with the elements you need.

        (note acceptable range is [0x00-0x06,0x80-0xFF] inclusive)

        :param info: A dictionary of {int:string} of values
        :param set: A dictionary of {name:string} of values
        """
        if isinstance(info_name, dict):
            for key in info_name:
                inx = self.__names.index(key)
                self.__data[inx] = info_name[key]

        if isinstance(info, dict):
            for key in info:
                if (0x06 >= key >= 0x00) or (0xFF >= key >= 0x80):
                    self.__data[key] = info[key]

    def __iter__(self):
        """Iterate over the device information.

        :returns: An iterator of the device information
        """
        return iter(self.__data.items())

    def summary(self):
        """Return a summary of the main items.

        :returns: An dictionary of the main items
        """
        return dict(zip(self.__names, iter(self.__data.values())))

    def update(self, value):
        """Update the values of this identity.

        using another identify as the value

        :param value: The value to copy values from
        """
        self.__data.update(value)

    def __setitem__(self, key, value):
        """Access the device information.

        :param key: The register to set
        :param value: The new value for referenced register
        """
        if key not in [0x07, 0x08]:
            self.__data[key] = value

    def __getitem__(self, key):
        """Access the device information.

        :param key: The register to read
        """
        return self.__data.setdefault(key, "")

    def __str__(self):
        """Build a representation of the device.

        :returns: A string representation of the device
        """
        return "DeviceIdentity"

    # -------------------------------------------------------------------------#
    #  Properties
    # -------------------------------------------------------------------------#
    # fmt: off
    VendorName = dict_property(lambda s: s.__data, 0)  # pylint: disable=protected-access
    ProductCode = dict_property(lambda s: s.__data, 1)  # pylint: disable=protected-access
    MajorMinorRevision = dict_property(lambda s: s.__data, 2)  # pylint: disable=protected-access
    VendorUrl = dict_property(lambda s: s.__data, 3)  # pylint: disable=protected-access
    ProductName = dict_property(lambda s: s.__data, 4)  # pylint: disable=protected-access
    ModelName = dict_property(lambda s: s.__data, 5)  # pylint: disable=protected-access
    UserApplicationName = dict_property(lambda s: s.__data, 6)  # pylint: disable=protected-access
    # fmt: on


class DeviceInformationFactory:  # pylint: disable=too-few-public-methods
    """This is a helper.

    That really just hides
    some of the complexity of processing the device information
    requests (function code 0x2b 0x0e).
    """

    __lookup = {
        DeviceInformation.BASIC: lambda c, r, i: c.__gets(  # pylint: disable=protected-access
            r, list(range(i, 0x03))
        ),
        DeviceInformation.REGULAR: lambda c, r, i: c.__gets(  # pylint: disable=protected-access
            r,
            list(range(i, 0x07))
            if c.__get(r, i)[i]  # pylint: disable=protected-access
            else list(range(0, 0x07)),
        ),
        DeviceInformation.EXTENDED: lambda c, r, i: c.__gets(  # pylint: disable=protected-access
            r,
            [x for x in range(i, 0x100) if x not in range(0x07, 0x80)]
            if c.__get(r, i)[i]  # pylint: disable=protected-access
            else [x for x in range(0, 0x100) if x not in range(0x07, 0x80)],
        ),
        DeviceInformation.SPECIFIC: lambda c, r, i: c.__get(  # pylint: disable=protected-access
            r, i
        ),
    }

    @classmethod
    def get(cls, control, read_code=DeviceInformation.BASIC, object_id=0x00):
        """Get the requested device data from the system.

        :param control: The control block to pull data from
        :param read_code: The read code to process
        :param object_id: The specific object_id to read
        :returns: The requested data (id, length, value)
        """
        identity = control.Identity
        return cls.__lookup[read_code](cls, identity, object_id)

    @classmethod
    def __get(cls, identity, object_id):  # pylint: disable=unused-private-member
        """Read a single object_id from the device information.

        :param identity: The identity block to pull data from
        :param object_id: The specific object id to read
        :returns: The requested data (id, length, value)
        """
        return {object_id: identity[object_id]}

    @classmethod
    def __gets(cls, identity, object_ids):  # pylint: disable=unused-private-member
        """Read multiple object_ids from the device information.

        :param identity: The identity block to pull data from
        :param object_ids: The specific object ids to read
        :returns: The requested data (id, length, value)
        """
        return {oid: identity[oid] for oid in object_ids if identity[oid]}

    def __init__(self):
        """Prohibit objects."""
        raise RuntimeError(INTERNAL_ERROR)


# ---------------------------------------------------------------------------#
#  Counters Handler
# ---------------------------------------------------------------------------#
class ModbusCountersHandler:
    """This is a helper class to simplify the properties for the counters.

    0x0B  1  Return Bus Message Count

             Quantity of messages that the remote
             device has detected on the communications system since its
             last restart, clear counters operation, or power-up.  Messages
             with bad CRC are not taken into account.

    0x0C  2  Return Bus Communication Error Count

             Quantity of CRC errors encountered by the remote device since its
             last restart, clear counters operation, or power-up.  In case of
             an error detected on the character level, (overrun, parity error),
             or in case of a message length < 3 bytes, the receiving device is
             not able to calculate the CRC. In such cases, this counter is
             also incremented.

    0x0D  3  Return Slave Exception Error Count

             Quantity of MODBUS exception error detected by the remote device
             since its last restart, clear counters operation, or power-up.
             Exception errors are described and listed in "MODBUS Application
             Protocol Specification" document.

    0xOE  4  Return Slave Message Count

             Quantity of messages addressed to the remote device that the remote
             device has processed since its last restart, clear counters operation,
             or power-up.

    0x0F  5  Return Slave No Response Count

             Quantity of messages received by the remote device for which it
             returned no response (neither a normal response nor an exception
             response), since its last restart, clear counters operation, or
             power-up.

    0x10  6  Return Slave NAK Count

             Quantity of messages addressed to the remote device for which it
             returned a Negative ACKNOWLEDGE (NAK) exception response, since
             its last restart, clear counters operation, or power-up. Exception
             responses are described and listed in "MODBUS Application Protocol
             Specification" document.

    0x11  7  Return Slave Busy Count

             Quantity of messages addressed to the remote device for which it
             returned a Slave Device Busy exception response, since its last
             restart, clear counters operation, or power-up. Exception
             responses are described and listed in "MODBUS Application
             Protocol Specification" document.

    0x12  8  Return Bus Character Overrun Count

             Quantity of messages addressed to the remote device that it could
             not handle due to a character overrun condition, since its last
             restart, clear counters operation, or power-up. A character
             overrun is caused by data characters arriving at the port faster
             than they can.

    .. note:: I threw the event counter in here for convenience
    """

    __data = dict.fromkeys(range(9), 0x00)
    __names = [
        "BusMessage",
        "BusCommunicationError",
        "SlaveExceptionError",
        "SlaveMessage",
        "SlaveNoResponse",
        "SlaveNAK",
        "SLAVE_BUSY",
        "BusCharacterOverrun",
    ]

    def __iter__(self):
        """Iterate over the device counters.

        :returns: An iterator of the device counters
        """
        return zip(self.__names, iter(self.__data.values()))

    def update(self, values):
        """Update the values of this identity.

        using another identify as the value

        :param values: The value to copy values from
        """
        for k, v_item in iter(values.items()):
            v_item += self.__getattribute__(  # pylint: disable=unnecessary-dunder-call
                k
            )
            self.__setattr__(k, v_item)  # pylint: disable=unnecessary-dunder-call

    def reset(self):
        """Clear all of the system counters."""
        self.__data = dict.fromkeys(range(9), 0x00)

    def summary(self):
        """Return a summary of the counters current status.

        :returns: A byte with each bit representing each counter
        """
        count, result = 0x01, 0x00
        for i in iter(self.__data.values()):
            if i != 0x00:  # pylint: disable=compare-to-zero
                result |= count
            count <<= 1
        return result

    # -------------------------------------------------------------------------#
    #  Properties
    # -------------------------------------------------------------------------#
    # fmt: off
    BusMessage = dict_property(lambda s: s.__data, 0)  # pylint: disable=protected-access
    BusCommunicationError = dict_property(lambda s: s.__data, 1)  # pylint: disable=protected-access
    BusExceptionError = dict_property(lambda s: s.__data, 2)  # pylint: disable=protected-access
    SlaveMessage = dict_property(lambda s: s.__data, 3)  # pylint: disable=protected-access
    SlaveNoResponse = dict_property(lambda s: s.__data, 4)  # pylint: disable=protected-access
    SlaveNAK = dict_property(lambda s: s.__data, 5)  # pylint: disable=protected-access
    SLAVE_BUSY = dict_property(lambda s: s.__data, 6)  # pylint: disable=protected-access
    BusCharacterOverrun = dict_property(lambda s: s.__data, 7)  # pylint: disable=protected-access
    Event = dict_property(lambda s: s.__data, 8)  # pylint: disable=protected-access
    # fmt: on


# ---------------------------------------------------------------------------#
#  Main server control block
# ---------------------------------------------------------------------------#
class ModbusControlBlock:
    """This is a global singleton that controls all system information.

    All activity should be logged here and all diagnostic requests
    should come from here.
    """

    _mode = "ASCII"
    _diagnostic = [False] * 16
    _listen_only = False
    _delimiter = b"\r"
    _counters = ModbusCountersHandler()
    _identity = ModbusDeviceIdentification()
    _plus = ModbusPlusStatistics()
    _events: list[ModbusEvent] = []

    # -------------------------------------------------------------------------#
    #  Magic
    # -------------------------------------------------------------------------#
    def __str__(self):
        """Build a representation of the control block.

        :returns: A string representation of the control block
        """
        return "ModbusControl"

    def __iter__(self):
        """Iterate over the device counters.

        :returns: An iterator of the device counters
        """
        return self._counters.__iter__()

    def __new__(cls):
        """Create a new instance."""
        if "_inst" not in vars(cls):
            cls._inst = object.__new__(cls)
        return cls._inst

    # -------------------------------------------------------------------------#
    #  Events
    # -------------------------------------------------------------------------#
    def addEvent(self, event: ModbusEvent):
        """Add a new event to the event log.

        :param event: A new event to add to the log
        """
        self._events.insert(0, event)
        self._events = self._events[0:64]  # chomp to 64 entries
        self.Counter.Event += 1

    def getEvents(self):
        """Return an encoded collection of the event log.

        :returns: The encoded events packet
        """
        events = [event.encode() for event in self._events]
        return b"".join(events)

    def clearEvents(self):
        """Clear the current list of events."""
        self._events = []

    # -------------------------------------------------------------------------#
    #  Other Properties
    # -------------------------------------------------------------------------#
    Identity = property(lambda s: s._identity)
    Counter = property(lambda s: s._counters)
    Events = property(lambda s: s._events)
    Plus = property(lambda s: s._plus)

    def reset(self):
        """Clear all of the system counters and the diagnostic register."""
        self._events = []
        self._counters.reset()
        self._diagnostic = [False] * 16

    # -------------------------------------------------------------------------#
    #  Listen Properties
    # -------------------------------------------------------------------------#
    def _setListenOnly(self, value):
        """Toggle the listen only status.

        :param value: The value to set the listen status to
        """
        self._listen_only = bool(value)

    ListenOnly = property(lambda s: s._listen_only, _setListenOnly)

    # -------------------------------------------------------------------------#
    #  Mode Properties
    # -------------------------------------------------------------------------#
    def _setMode(self, mode):
        """Toggle the current serial mode.

        :param mode: The data transfer method in (RTU, ASCII)
        """
        if mode in {"ASCII", "RTU"}:
            self._mode = mode

    Mode = property(lambda s: s._mode, _setMode)

    # -------------------------------------------------------------------------#
    #  Delimiter Properties
    # -------------------------------------------------------------------------#
    def _setDelimiter(self, char):
        """Change the serial delimiter character.

        :param char: The new serial delimiter character
        """
        if isinstance(char, str):
            self._delimiter = char.encode()
        if isinstance(char, bytes):
            self._delimiter = char
        elif isinstance(char, int):
            self._delimiter = struct.pack(">B", char)

    Delimiter = property(lambda s: s._delimiter, _setDelimiter)

    # -------------------------------------------------------------------------#
    #  Diagnostic Properties
    # -------------------------------------------------------------------------#
    def setDiagnostic(self, mapping):
        """Set the value in the diagnostic register.

        :param mapping: Dictionary of key:value pairs to set
        """
        for entry in iter(mapping.items()):
            if entry[0] >= 0 and entry[0] < len(self._diagnostic):
                self._diagnostic[entry[0]] = bool(entry[1])

    def getDiagnostic(self, bit):
        """Get the value in the diagnostic register.

        :param bit: The bit to get
        :returns: The current value of the requested bit
        """
        try:
            if bit and 0 <= bit < len(self._diagnostic):
                return self._diagnostic[bit]
        except Exception:  # pylint: disable=broad-except
            return None
        return None

    def getDiagnosticRegister(self):
        """Get the entire diagnostic register.

        :returns: The diagnostic register collection
        """
        return self._diagnostic
