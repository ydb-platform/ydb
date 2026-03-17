"""Simulator data model classes."""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
from typing import TypeAlias


SimValueType: TypeAlias = int | float | str | bool | bytes
SimAction: TypeAlias = Callable[[SimValueType], SimValueType] | Awaitable[SimValueType]

class SimDataType(Enum):
    """Register types, used to type of a group of registers.

    This is the types pymodbus recognizes, actually the modbus standard do NOT define e.g. INT32,
    but since nearly every device contain e.g. values of type INT32, it is available in pymodbus,
    with automatic conversions to/from registers.
    """

    #: 1 integer == 1 register
    INT16 = 1
    #: 1 positive integer == 1 register
    UINT16 = 2
    #: 1 integer == 2 registers
    INT32 = 3
    #: 1 positive integer == 2 registers
    UINT32 = 4
    #: 1 integer == 4 registers
    INT64 = 5
    #: 1 positive integer == 4 register
    UINT64 = 6
    #: 1 float == 2 registers
    FLOAT32 = 7
    #: 1 float == 4 registers
    FLOAT64 = 8
    #: 1 string == len(string) / 2 registers
    #:
    #: .. tip:: String length must be a multiple of 2 (corresponding to registers).
    STRING = 9
    #: Shared mode: 16 bits == 1 register else 1 bit == 1 "register" (address)
    BITS = 10
    #: Raw registers
    #:
    #: .. warning:: Do not use as default because it fills the memory and block other registrations.
    REGISTERS = 11
    #: Define register address limits and default values
    #:
    #: .. tip:: Implemented a single but special register, and therefore improves speed and memory usage compared to REGISTERS.
    DEFAULT = 12

@dataclass(frozen=True)
class SimData:
    """Configure a group of continuous identical registers.

    **Examples**:

    .. code-block:: python

        SimData(
            address=100,
            count=5,
            value=-123456
            datatype=SimDataType.INT32
        )

    The above code defines 5 INT32, each with the value -123456, in total 10 registers (address 100-109)

    .. code-block:: python

        SimData(
            address=100,
            count=17,
            value=-True
            datatype=SimDataType.BITS
        )

    The above code defines 17 BITS (coils), each with the value True. In non-shared mode addresses are 100-115.

    in shared mode BITS are stored in registers (16bit is one register), the address refer to the register,
    addresses are 100-101 (with register 101 being padded with 15 bits)

    .. tip:: use SimDatatype.DEFAULT to define register limits:

    .. code-block:: python

        SimData(
            address=0,    # First legal registers
            count=1000,   # last legal register is r+count-1
            value=0x1234  # Default register value
            datatype=SimDataType.DEFAULT
        )

    The above code sets the range of legal registers to 0..999 all with the value 0x1234.
    Accessing non-defined registers will cause an exception response.

    Remark that DEFAULT can be overwritten with other definitions:

    .. code-block:: python

        SimData(
            address=0,    # First legal registers
            count=1000,   # last legal register is r+count-1
            value=0x1234  # Default register value
            datatype=SimDataType.DEFAULT
        )
        SimData(
            address=6,
            count=1,
            value=117
            datatype=SimDataType.INT32
        )

    Is a legal and normal combination.

    .. attention:: Using SimDataType.DEFAULT is a LOT more efficient to define all registers, than \
    the other datatypes. This is because default registers are not created unless written to, whereas \
    the registers of other datatypes are each created as objects.
    """

    #: Address of first register, starting with 0.
    address: int

    #: Value of datatype, to initialize the registers (repeated with count, apart from string).
    #:
    #: Depending on in which block the object is used some value types are not legal e.g. float cannot
    #: be used to define coils.
    value: SimValueType = 0

    #: Count of datatype e.g. count=3 datatype=SimdataType.INT32 is 6 registers.
    #:
    #: SimdataType.STR is special, the value string is copied "count" times.
    #:
    #: - count=1, value="ABCD" is 2 registers
    #: - count=3, value="ABCD" is 6 registers, with "ABCD" repeated 3 times.
    count: int = 1

    #: Datatype, used to check access and calculate register count.
    #:
    #: .. note:: Default is SimDataType.REGISTERS
    datatype: SimDataType = SimDataType.REGISTERS

    #: Optional function to call when registers are being read/written.
    #:
    #: **Example function:**
    #:
    #: .. code-block:: python
    #:
    #:     def my_action(
    #:         addr: int,
    #:         value: SimValueType) -> SimValueType:
    #:             return value + 1
    #:
    #:     async def my_action(
    #:         addr: int,
    #:         value: SimValueType) -> SimValueType:
    #:             return value + 1
    #:
    #: .. tip:: use functools.partial to add extra parameters if needed.
    action: SimAction | None = None

    def __check_datatype(self):
        """Check datatype."""
        if self.datatype == SimDataType.STRING and not isinstance(self.value, str):
            raise TypeError("SimDataType.STRING but value not a string")
        if self.datatype in (
                SimDataType.INT16,
                SimDataType.UINT16,
                SimDataType.INT32,
                SimDataType.UINT32,
                SimDataType.INT64,
                SimDataType.UINT64,
            ) and not isinstance(self.value, int):
            raise TypeError("SimDataType.INT variant but value not a int")
        if self.datatype in (
                SimDataType.FLOAT32,
                SimDataType.FLOAT64,
            ) and not isinstance(self.value, float):
            raise TypeError("SimDataType.FLOAT variant but value not a float")
        if self.datatype == SimDataType.BITS and not isinstance(self.value, (bool, int)):
            raise TypeError("SimDataType.BITS but value not a bool or int")
        if self.datatype == SimDataType.REGISTERS and not isinstance(self.value, int):
            raise TypeError("SimDataType.REGISTERS but value not a int")
        if self.datatype == SimDataType.DEFAULT:
            if self.action:
                raise TypeError("SimDataType.DEFAULT cannot have an action")
            if not isinstance(self.value, int):
                raise TypeError("SimDataType.DEFAULT but value not a int")

    def __post_init__(self):
        """Define a group of registers."""
        if not isinstance(self.address, int) or not 0 <= self.address < 65535:
            raise TypeError("0 <= address < 65535")
        if not isinstance(self.count, int) or not 0 < self.count <= 65535:
            raise TypeError("0 < count <= 65535")
        if self.action and not (callable(self.action) or asyncio.iscoroutinefunction(self.action)):
            raise TypeError("action not Callable or Awaitable (async)")
        if not isinstance(self.datatype, SimDataType):
            raise TypeError("datatype not SimDataType")
        if not isinstance(self.value, SimValueType):
            raise TypeError("value not a supported type")
        self.__check_datatype()

@dataclass(frozen=True)
class SimDevice:
    """Configure a device with parameters and registers.

    Registers can be defined as shared or as 4 separate blocks.

    shared_block means all requests access the same registers,
    allowing e.g. input registers to be read with read_holding_register.

    .. warning:: Shared mode cannot be mixed with non-shared mode !

    In shared mode, individual coils/direct input cannot be addressed directly ! Instead
    the register address is used with count. In non-shared mode coils/direct input can be
    addressed directly individually.

    **Device with shared registers**::

        SimDevice(
            id=1,
            block_shared=[SimData(...)]
        )

    **Device with non-shared registers**::

        SimDevice(
            id=1,
            block_coil=[SimData(...)],
            block_direct=[SimData(...)],
            block_holding=[SimData(...)],
            block_input=[SimData(...)],
        )

    A server can contain either a single :class:`SimDevice` or list of :class:`SimDevice` to simulate a
    multipoint line.
    """

    #: Address of device
    #:
    #: Default 0 means accept all devices, except those specifically defined.
    id: int = 0

    #: Enforce type checking, if True access are controlled to be conform with datatypes.
    #:
    #: Used to control that e.g. INT32 are not read as INT16.
    type_check: bool = False

    #: Use this block for shared registers (Modern devices).
    #:
    #: Requests accesses all registers in this block.
    #:
    #: .. warning:: cannot be used together with other block_* parameters!
    block_shared: list[SimData] | None = None

    #: Use this block for non-shared registers (very old devices).
    #:
    #: In this block an address is a single coil, there are no registers.
    #:
    #: Request of type read/write_coil accesses this block.
    #:
    #: .. tip:: block_coil/direct/holding/input must all be defined
    block_coil: list[SimData] | None = None

    #: Use this block for non-shared registers (very old devices).
    #:
    #: In this block an address is a single relay, there are no registers.
    #:
    #: Request of type read/write_direct_input accesses this block.
    #:
    #: .. tip:: block_coil/direct/holding/input must all be defined
    block_direct: list[SimData] | None = None

    #: Use this block for non-shared registers (very old devices).
    #:
    #: In this block an address is a register.
    #:
    #: Request of type read/write_holding accesses this block.
    #:
    #: .. tip:: block_coil/direct/holding/input must all be defined
    block_holding: list[SimData] | None = None

    #: Use this block for non-shared registers (very old devices).
    #:
    #: In this block an address is a register.
    #:
    #: Request of type read/write_input accesses this block.
    #:
    #: .. tip:: block_coil/direct/holding/input must all be defined
    block_input: list[SimData] | None = None

    def __post_init__(self):
        """Define a device."""
        if not isinstance(self.id, int) or not 0 <= self.id < 255:
            raise TypeError("0 <= id < 255")
        blocks = [(self.block_shared, "shared")]
        if self.block_shared:
            if self.block_coil or self.block_direct or self.block_holding or self.block_input:
                raise TypeError("block_* cannot be combined with block_shared")
        else:
            blocks = [
                (self.block_coil, "coil"),
                (self.block_direct, "direct"),
                (self.block_holding, "holding"),
                (self.block_input, "input")]

        for block, name in blocks:
            if not block:
                raise TypeError(f"block_{name} not defined")
            if not isinstance(block, list):
                raise TypeError(f"block_{name} not a list")
            for entry in block:
                if not isinstance(entry, SimData):
                    raise TypeError(f"block_{name} contains non SimData entries")
