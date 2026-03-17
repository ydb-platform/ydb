__all__ = ['Dtc']

import struct
import inspect

from typing import Optional, List, Any, Union


class Dtc:
    """
    Defines a Diagnostic Trouble Code which consist of a 3-byte ID, a status, a severity and some diagnostic data.

    :param dtcid: The 3-byte ID of the DTC
    :type dtcid: int

    """
    class Format:
        """
        Provide a list of DTC formats and their indices. These values are used by the :ref:`The ReadDTCInformation<ReadDtcInformation>` when requesting a number of DTCs.		
        """
        ISO15031_6 = 0                  # 2006
        SAE_J2012_DA_DTCFormat_00 = 0   # 2013 / 2020
        ISO14229_1 = 1
        SAE_J1939_73 = 2
        ISO11992_4 = 3
        SAE_J2012_DA_DTCFormat_04 = 4

        @classmethod
        def get_name(cls, given_id: Optional[int]) -> Optional[str]:
            if given_id is None:
                return ""

            for member in inspect.getmembers(cls):
                if isinstance(member[1], int):
                    if member[1] == given_id:
                        return member[0]

            return None

    class FunctionalGroupIdentifiers:
        """
        Provides a list of FunctionalGroupIdentifiers (Table D.15) which are used by the :ref:`ReadDTCInformation<ReadDtcInformation>` when requesting a number of DTCs.
        """
        EMISSIONS_SYSTEM_GROUP = 0x33
        SAFETY_SYSTEM_GROUP = 0xD0
        VOBD_SYSTEM = 0xFE

    # DTC Status byte
    # This byte is an 8-bit flag indicating how much we are sure that a DTC is active.
    class Status:
        """
        Represents a DTC status which consists of 8 boolean flags (a byte). All flags can be set after instantiation without problems. 

        :param test_failed: DTC is no longer failed at the time of the request
        :type test_failed: bool

        :param test_failed_this_operation_cycle: DTC never failed on the current operation cycle.
        :type test_failed_this_operation_cycle: bool

        :param pending: DTC failed on the current or previous operation cycle.
        :type pending: bool

        :param confirmed: DTC is not confirmed at the time of the request.
        :type confirmed: bool

        :param test_not_completed_since_last_clear: DTC test has been completed since the last codeclear.
        :type test_not_completed_since_last_clear: bool

        :param test_failed_since_last_clear: DTC test failed at least once since last code clear.
        :type test_failed_since_last_clear: bool

        :param test_not_completed_this_operation_cycle: DTC test completed this operation cycle.
        :type test_not_completed_this_operation_cycle: bool

        :param warning_indicator_requested: Server is not requesting warningIndicator to be active.
        :type warning_indicator_requested: bool
        """

        test_failed: bool
        test_failed_this_operation_cycle: bool
        pending: bool
        confirmed: bool
        test_not_completed_since_last_clear: bool
        test_failed_since_last_clear: bool
        test_not_completed_this_operation_cycle: bool
        warning_indicator_requested: bool

        def __init__(self,
                     test_failed: bool = False,
                     test_failed_this_operation_cycle: bool = False,
                     pending: bool = False,
                     confirmed: bool = False,
                     test_not_completed_since_last_clear: bool = False,
                     test_failed_since_last_clear: bool = False,
                     test_not_completed_this_operation_cycle: bool = False,
                     warning_indicator_requested: bool = False):
            self.test_failed = test_failed
            self.test_failed_this_operation_cycle = test_failed_this_operation_cycle
            self.pending = pending
            self.confirmed = confirmed
            self.test_not_completed_since_last_clear = test_not_completed_since_last_clear
            self.test_failed_since_last_clear = test_failed_since_last_clear
            self.test_not_completed_this_operation_cycle = test_not_completed_this_operation_cycle
            self.warning_indicator_requested = warning_indicator_requested

        def get_byte_as_int(self) -> int:  # Returns the status byte as an integer
            byte = 0
            byte |= 0x1 if self.test_failed else 0
            byte |= 0x2 if self.test_failed_this_operation_cycle else 0
            byte |= 0x4 if self.pending else 0
            byte |= 0x8 if self.confirmed else 0
            byte |= 0x10 if self.test_not_completed_since_last_clear else 0
            byte |= 0x20 if self.test_failed_since_last_clear else 0
            byte |= 0x40 if self.test_not_completed_this_operation_cycle else 0
            byte |= 0x80 if self.warning_indicator_requested else 0

            return byte

        def get_byte(self) -> bytes:  # Returns the status byte in "bytes" format for payload creation
            return struct.pack('B', self.get_byte_as_int())

        def set_byte(self, byte: Union[bytes, int]) -> None:  # Set all the status flags from the status byte
            if not isinstance(byte, int) and not isinstance(byte, bytes):
                raise ValueError('Given byte must be an integer or bytes object.')

            if isinstance(byte, bytes):
                if len(byte) != 1:
                    raise ValueError("Expected 1 byte to set the DTC Status")
                byte = int(byte[0])

            self.test_failed = True if byte & 0x01 > 0 else False
            self.test_failed_this_operation_cycle = True if byte & 0x02 > 0 else False
            self.pending = True if byte & 0x04 > 0 else False
            self.confirmed = True if byte & 0x08 > 0 else False
            self.test_not_completed_since_last_clear = True if byte & 0x10 > 0 else False
            self.test_failed_since_last_clear = True if byte & 0x20 > 0 else False
            self.test_not_completed_this_operation_cycle = True if byte & 0x40 > 0 else False
            self.warning_indicator_requested = True if byte & 0x80 > 0 else False

        @classmethod
        def from_byte(cls, byte: Union[bytes, int]) -> "Dtc.Status":
            status = cls()
            status.set_byte(byte)
            return status

    # DTC Severity byte, it's a 3-bit indicator telling how serious a trouble code is.
    class Severity:
        """
        Represents a DTC severity which consists of 3 boolean flags. All flags can be set after instantiation without problems. 

        :param maintenance_only: This value indicates that the failure requests maintenance only
        :type maintenance_only: bool

        :param check_at_next_exit: This value indicates that the failure requires a check of the vehicle at the next halt.
        :type check_at_next_exit: bool

        :param check_immediately: This value indicates that the failure requires an immediate check of the vehicle.
        :type check_immediately: bool
        """

        maintenance_only: bool
        check_at_next_exit: bool
        check_immediately: bool

        def __init__(self, maintenance_only: bool = False, check_at_next_exit: bool = False, check_immediately: bool = False):
            self.maintenance_only = maintenance_only
            self.check_at_next_exit = check_at_next_exit
            self.check_immediately = check_immediately

        def get_byte_as_int(self) -> int:
            byte = 0
            byte |= 0x20 if self.maintenance_only else 0
            byte |= 0x40 if self.check_at_next_exit else 0
            byte |= 0x80 if self.check_immediately else 0

            return byte

        def get_byte(self) -> bytes:
            return struct.pack('B', self.get_byte_as_int())

        def set_byte(self, byte: Union[bytes, int]) -> None:
            if not isinstance(byte, int) and not isinstance(byte, bytes):
                raise ValueError('Given byte must be an integer or bytes object.')

            if isinstance(byte, bytes):
                if len(byte) != 1:
                    raise ValueError("Expected 1 byte to set the DTC Status")
                byte = int(byte[0])

            self.maintenance_only = True if byte & 0x20 > 0 else False
            self.check_at_next_exit = True if byte & 0x40 > 0 else False
            self.check_immediately = True if byte & 0x80 > 0 else False
        
        @classmethod
        def from_byte(cls, byte: Union[bytes, int]) -> "Dtc.Severity":
            severity = cls()
            severity.set_byte(byte)
            return severity

        @property
        def available(self):
            return True if self.get_byte_as_int() > 0 else False

        # A snapshot data. Not defined by ISO14229 and implementation specific.
    # To read this data, the client must have a DID codec set in its config.
    class Snapshot:
        record_number: Optional[int] = None
        did: Optional[int] = None
        data: Optional[bytes] = None
        raw_data: Optional[bytes] = b''

    # Extended data. Not defined by ISO14229 and implementation specific
    # Only raw data can be given to user.
    class ExtendedData:
        record_number: Optional[int] = None
        raw_data: Optional[bytes] = b''

    class DtcClass:
        """
        Represents a DTC class information which consists of 5 boolean flags. All flags can be set after instantiation without problems. 

        :param class0: Bit0 : Unclassified
        :type class0: bool

        :param class1: Bit1 : GTR module B Class A definition.
        :type class1: bool

        :param class2: Bit2: GTR module B Class B1 definition
        :type class2: bool

        :param class3: Bit3: GTR module B Class B2 definition.
        :type class3: bool

        :param class4: Bit4: GTR module B Class C definition.
        :type class4: bool
        """

        class0: bool
        class1: bool
        class2: bool
        class3: bool
        class4: bool

        def __init__(self, 
                     class0: bool = False, 
                     class1: bool = False, 
                     class2: bool = False,
                     class3: bool = False,
                     class4: bool = False
                     ):
            self.class0 = class0
            self.class1 = class1
            self.class2 = class2
            self.class3 = class3
            self.class4 = class4

        def get_byte_as_int(self) -> int:
            byte = 0
            byte |= 0x01 if self.class0 else 0
            byte |= 0x02 if self.class1 else 0
            byte |= 0x04 if self.class2 else 0
            byte |= 0x08 if self.class3 else 0
            byte |= 0x10 if self.class4 else 0

            return byte

        def get_byte(self) -> bytes:
            return struct.pack('B', self.get_byte_as_int())

        def set_byte(self, byte: Union[bytes, int]) -> None:
            if not isinstance(byte, int) and not isinstance(byte, bytes):
                raise ValueError('Given byte must be an integer or bytes object.')

            if isinstance(byte, bytes):
                if len(byte) != 1:
                    raise ValueError("Expected 1 byte to set the DTC Status")
                byte = int(byte[0])

            self.class0 = True if byte & 0x01 > 0 else False
            self.class1 = True if byte & 0x02 > 0 else False
            self.class2 = True if byte & 0x04 > 0 else False
            self.class3 = True if byte & 0x08 > 0 else False
            self.class4 = True if byte & 0x10 > 0 else False

        @property
        def available(self):
            return True if self.get_byte_as_int() > 0 else False

        @classmethod
        def from_byte(cls, byte: Union[bytes, int]) -> "Dtc.DtcClass":
            dtc_class = cls()
            dtc_class.set_byte(byte)
            return dtc_class

    id: int
    status: "Dtc.Status"
    snapshots: List[Union["Dtc.Snapshot", int]]
    extended_data: List["Dtc.ExtendedData"]
    severity: "Dtc.Severity"
    dtc_class:"Dtc.DtcClass"
    functional_unit: Any
    fault_counter: Optional[int]

    def __init__(self, dtcid: int):
        self.id = dtcid
        self.status = Dtc.Status()
        self.snapshots = []  		# . DID codec must be configured
        self.extended_data = []
        self.severity = Dtc.Severity()
        self.dtc_class = Dtc.DtcClass()
        self.functional_unit = None 	# Implementation specific (ISO 14229 D.4)
        # Common practice is to detect a specific failure many times before setting the DTC active. This counter should tell the actual count.
        self.fault_counter = None

    def __repr__(self) -> str:
        return '<DTC ID=0x%06x, Status=0x%02x, Severity=0x%02x, class=%02x at 0x%08x>' % (
            self.id, 
            self.status.get_byte_as_int(), 
            self.severity.get_byte_as_int(), 
            self.dtc_class.get_byte_as_int(),  
            id(self)
            )

    def id_iso(self) -> str:
        return '%c%i%03X-%02X' % (
            ['P', 'C', 'B', 'U'][self.id >> 22],
            (self.id >> 20) & 3,
            (self.id >> 8) & 0xFFF,
            (self.id) & 0xFF
        )
