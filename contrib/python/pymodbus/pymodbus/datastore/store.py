"""Modbus Server Datastore.

For each server, you will create a ModbusServerContext and pass
in the default address space for each data access.  The class
will create and manage the data.

Further modification of said data accesses should be performed
with [get,set][access]Values(address, count)

Datastore Implementation
-------------------------

There are two ways that the server datastore can be implemented.
The first is a complete range from "address" start to "count"
number of indices.  This can be thought of as a straight array::

    data = range(1, 1 + count)
    [1,2,3,...,count]

The other way that the datastore can be implemented (and how
many devices implement it) is a associate-array::

    data = {1:"1", 3:"3", ..., count:"count"}
    [1,3,...,count]

The difference between the two is that the latter will allow
arbitrary gaps in its datastore while the former will not.
This is seen quite commonly in some modbus implementations.
What follows is a clear example from the field:

Say a company makes two devices to monitor power usage on a rack.
One works with three-phase and the other with a single phase. The
company will dictate a modbus data mapping such that registers::

    n:      phase 1 power
    n+1:    phase 2 power
    n+2:    phase 3 power

Using this, layout, the first device will implement n, n+1, and n+2,
however, the second device may set the latter two values to 0 or
will simply not implemented the registers thus causing a single read
or a range read to fail.

I have both methods implemented, and leave it up to the user to change
based on their preference.
"""
# pylint: disable=missing-type-doc
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Any, Generic, TypeVar

from pymodbus.exceptions import ParameterException


# ---------------------------------------------------------------------------#
#  Datablock Storage
# ---------------------------------------------------------------------------#

V = TypeVar('V', list, dict[int, Any])
class BaseModbusDataBlock(ABC, Generic[V]):
    """Base class for a modbus datastore.

    Derived classes must create the following fields:
            @address The starting address point
            @defult_value The default value of the datastore
            @values The actual datastore values

    Derived classes must implemented the following methods:
            getValues(self, address, count=1)
            setValues(self, address, values)
            reset(self)

    Derived classes can implemented the following async methods:
            async_getValues(self, address, count=1)
            async_setValues(self, address, values)
    but are not needed since these standard call the sync. methods.
    """

    values: V
    address: int
    default_value: Any

    async def async_getValues(self, address: int, count=1) -> Iterable:
        """Return the requested values from the datastore.

        :param address: The starting address
        :param count: The number of values to retrieve
        :raises TypeError:
        """
        return self.getValues(address, count)

    @abstractmethod
    def getValues(self, address:int, count=1) -> Iterable:
        """Return the requested values from the datastore.

        :param address: The starting address
        :param count: The number of values to retrieve
        :raises TypeError:
        """

    async def async_setValues(self, address: int, values: list[int|bool]) -> None:
        """Set the requested values in the datastore.

        :param address: The starting address
        :param values: The values to store
        :raises TypeError:
        """
        self.setValues(address, values)

    @abstractmethod
    def setValues(self, address:int, values) -> None:
        """Set the requested values in the datastore.

        :param address: The starting address
        :param values: The values to store
        :raises TypeError:
        """

    def __str__(self):
        """Build a representation of the datastore.

        :returns: A string representation of the datastore
        """
        return f"DataStore({len(self.values)}, {self.default_value})"

    def __iter__(self):
        """Iterate over the data block data.

        :returns: An iterator of the data block data
        """
        if isinstance(self.values, dict):
            return iter(self.values.items())
        return enumerate(self.values, self.address)


class ModbusSequentialDataBlock(BaseModbusDataBlock[list]):
    """Creates a sequential modbus datastore."""

    def __init__(self, address, values):
        """Initialize the datastore.

        :param address: The starting address of the datastore
        :param values: Either a list or a dictionary of values
        """
        self.address = address
        if hasattr(values, "__iter__"):
            self.values = list(values)
        else:
            self.values = [values]
        self.default_value = self.values[0].__class__()

    @classmethod
    def create(cls):
        """Create a datastore.

        With the full address space initialized to 0x00

        :returns: An initialized datastore
        """
        return cls(0x00, [0x00] * 65536)

    def default(self, count, value=False):
        """Use to initialize a store to one value.

        :param count: The number of fields to set
        :param value: The default value to set to the fields
        """
        self.default_value = value
        self.values = [self.default_value] * count
        self.address = 0x00

    def reset(self):
        """Reset the datastore to the initialized default value."""
        self.values = [self.default_value] * len(self.values)

    def getValues(self, address, count=1):
        """Return the requested values of the datastore.

        :param address: The starting address
        :param count: The number of values to retrieve
        :returns: The requested values from a:a+c
        """
        start = address - self.address
        return self.values[start : start + count]

    def setValues(self, address, values):
        """Set the requested values of the datastore.

        :param address: The starting address
        :param values: The new values to be set
        """
        if not isinstance(values, list):
            values = [values]
        start = address - self.address
        self.values[start : start + len(values)] = values


class ModbusSparseDataBlock(BaseModbusDataBlock[dict[int, Any]]):
    """A sparse modbus datastore.

    E.g Usage.
    sparse = ModbusSparseDataBlock({10: [3, 5, 6, 8], 30: 1, 40: [0]*20})

    This would create a datablock with 3 blocks
    One starts at offset 10 with length 4, one at 30 with length 1, and one at 40 with length 20

    sparse = ModbusSparseDataBlock([10]*100)
    Creates a sparse datablock of length 100 starting at offset 0 and default value of 10

    sparse = ModbusSparseDataBlock() --> Create empty datablock
    sparse.setValues(0, [10]*10)  --> Add block 1 at offset 0 with length 10 (default value 10)
    sparse.setValues(30, [20]*5)  --> Add block 2 at offset 30 with length 5 (default value 20)

    Unless 'mutable' is set to True during initialization, the datablock cannot be altered with
    setValues (new datablocks cannot be added)
    """

    def __init__(self, values=None, mutable=True):
        """Initialize a sparse datastore.

        Will only answer to addresses registered,
        either initially here, or later via setValues()

        :param values: Either a list or a dictionary of values
        :param mutable: Whether the data-block can be altered later with setValues (i.e add more blocks)

        If values is a list, a sequential datablock will be created.

        If values is a dictionary, it should be in {offset: <int | list>} format
        For each list, a sparse datablock is created, starting at 'offset' with the length of the list
        For each integer, the value is set for the corresponding offset.

        """
        self.values = {}
        self._process_values(values)
        self.mutable = mutable
        self.default_value = self.values.copy()

    @classmethod
    def create(cls, values=None):
        """Create sparse datastore.

        Use setValues to initialize registers.

        :param values: Either a list or a dictionary of values
        :returns: An initialized datastore
        """
        return cls(values)

    def reset(self):
        """Reset the store to the initially provided defaults."""
        self.values = self.default_value.copy()

    def getValues(self, address, count=1):
        """Return the requested values of the datastore.

        :param address: The starting address
        :param count: The number of values to retrieve
        :returns: The requested values from a:a+c
        """
        return [self.values[i] for i in range(address, address + count)]

    def _process_values(self, values):
        """Process values."""

        def _process_as_dict(values):
            for idx, val in iter(values.items()):
                if isinstance(val, (list, tuple)):
                    for i, v_item in enumerate(val):
                        self.values[idx + i] = v_item
                else:
                    self.values[idx] = int(val)

        if isinstance(values, dict):
            _process_as_dict(values)
            return
        if hasattr(values, "__iter__"):
            values = dict(enumerate(values))
        elif values is None:
            values = {}  # Must make a new dict here per instance
        else:
            raise ParameterException(
                "Values for datastore must be a list or dictionary"
            )
        _process_as_dict(values)

    def setValues(self, address, values, use_as_default=False):
        """Set the requested values of the datastore.

        :param address: The starting address
        :param values: The new values to be set
        :param use_as_default: Use the values as default
        :raises ParameterException:
        """
        if isinstance(values, dict):
            new_offsets = list(set(values.keys()) - set(self.values.keys()))
            if new_offsets and not self.mutable:
                raise ParameterException(f"Offsets {new_offsets} not in range")
            self._process_values(values)
        else:
            if not isinstance(values, list):
                values = [values]
            for idx, val in enumerate(values):
                if address + idx not in self.values and not self.mutable:
                    raise ParameterException("Offset {address+idx} not in range")
                self.values[address + idx] = val
        if use_as_default:
            for idx, val in iter(self.values.items()):
                self.default_value[idx] = val
