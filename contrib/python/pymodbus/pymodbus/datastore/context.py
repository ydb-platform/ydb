"""Context for datastore."""

from __future__ import annotations

from collections.abc import Sequence

# pylint: disable=missing-type-doc
from pymodbus.datastore.store import ModbusSequentialDataBlock
from pymodbus.exceptions import NoSuchSlaveException
from pymodbus.logging import Log


class ModbusBaseSlaveContext:
    """Interface for a modbus slave data context.

    Derived classes must implemented the following methods:
            reset(self)
            getValues/async_getValues(self, fc_as_hex, address, count=1)
            setValues/async_setValues(self, fc_as_hex, address, values)
    """

    _fx_mapper = {2: "d", 4: "i"}
    _fx_mapper.update([(i, "h") for i in (3, 6, 16, 22, 23)])
    _fx_mapper.update([(i, "c") for i in (1, 5, 15)])

    def decode(self, fx):
        """Convert the function code to the datastore to.

        :param fx: The function we are working with
        :returns: one of [d(iscretes),i(nputs),h(olding),c(oils)
        """
        return self._fx_mapper[fx]

    async def async_getValues(self, fc_as_hex: int, address: int, count: int = 1) -> Sequence[int | bool] | int:
        """Get `count` values from datastore.

        :param fc_as_hex: The function we are working with
        :param address: The starting address
        :param count: The number of values to retrieve
        :returns: The requested values from a:a+c
        """
        return self.getValues(fc_as_hex, address, count)

    async def async_setValues(self, fc_as_hex: int, address: int, values: Sequence[int | bool]) -> None | int:
        """Set the datastore with the supplied values.

        :param fc_as_hex: The function we are working with
        :param address: The starting address
        :param values: The new values to be set
        """
        return self.setValues(fc_as_hex, address, values)

    def getValues(self, fc_as_hex: int, address: int, count: int = 1) -> Sequence[int | bool]:
        """Get `count` values from datastore.

        :param fc_as_hex: The function we are working with
        :param address: The starting address
        :param count: The number of values to retrieve
        :returns: The requested values from a:a+c
        """
        Log.error("getValues({},{},{}) not implemented!", fc_as_hex, address, count)
        return []

    def setValues(self, fc_as_hex: int, address: int, values: Sequence[int | bool]) -> None | int:
        """Set the datastore with the supplied values.

        :param fc_as_hex: The function we are working with
        :param address: The starting address
        :param values: The new values to be set
        """
        Log.error("setValues({},{},{}) not implemented!", fc_as_hex, address, values)
        return 1


# ---------------------------------------------------------------------------#
#  Slave Contexts
# ---------------------------------------------------------------------------#
class ModbusSlaveContext(ModbusBaseSlaveContext):
    """Create a modbus data model with data stored in a block.

    :param di: discrete inputs initializer ModbusDataBlock
    :param co: coils initializer ModbusDataBlock
    :param hr: holding register initializer ModbusDataBlock
    :param ir: input registers initializer ModbusDataBlock
    """

    def __init__(self, *_args,
                    di: ModbusSequentialDataBlock | None = None,
                    co: ModbusSequentialDataBlock | None = None,
                    ir: ModbusSequentialDataBlock | None = None,
                    hr: ModbusSequentialDataBlock | None = None,
                ):
        """Initialize the datastores."""
        self.store = {}
        self.store["d"] = di if di is not None else ModbusSequentialDataBlock.create()
        self.store["c"] = co if di is not None else ModbusSequentialDataBlock.create()
        self.store["i"] = ir if di is not None else ModbusSequentialDataBlock.create()
        self.store["h"] = hr if di is not None else ModbusSequentialDataBlock.create()

    def __str__(self):
        """Return a string representation of the context.

        :returns: A string representation of the context
        """
        return "Modbus Slave Context"

    def reset(self):
        """Reset all the datastores to their default values."""
        for datastore in iter(self.store.values()):
            datastore.reset()

    def getValues(self, fc_as_hex, address, count=1):
        """Get `count` values from datastore.

        :param fc_as_hex: The function we are working with
        :param address: The starting address
        :param count: The number of values to retrieve
        :returns: The requested values from a:a+c
        """
        address += 1
        Log.debug("getValues: fc-[{}] address-{}: count-{}", fc_as_hex, address, count)
        return self.store[self.decode(fc_as_hex)].getValues(address, count)

    def setValues(self, fc_as_hex, address, values):
        """Set the datastore with the supplied values.

        :param fc_as_hex: The function we are working with
        :param address: The starting address
        :param values: The new values to be set
        """
        address += 1
        Log.debug("setValues[{}] address-{}: count-{}", fc_as_hex, address, len(values))
        self.store[self.decode(fc_as_hex)].setValues(address, values)

    def register(self, function_code, fc_as_hex, datablock=None):
        """Register a datablock with the slave context.

        :param function_code: function code (int)
        :param fc_as_hex: string representation of function code (e.g "cf" )
        :param datablock: datablock to associate with this function code
        """
        self.store[fc_as_hex] = datablock or ModbusSequentialDataBlock.create()
        self._fx_mapper[function_code] = fc_as_hex


class ModbusServerContext:
    """This represents a master collection of slave contexts.

    If single is set to true, it will be treated as a single
    context so every device id returns the same context. If single
    is set to false, it will be interpreted as a collection of
    slave contexts.
    """

    def __init__(self, slaves=None, single=True):
        """Initialize a new instance of a modbus server context.

        :param slaves: A dictionary of client contexts
        :param single: Set to true to treat this as a single context
        """
        self.single = single
        self._slaves = slaves or {}
        if self.single:
            self._slaves = {0: self._slaves}

    def __iter__(self):
        """Iterate over the current collection of slave contexts.

        :returns: An iterator over the slave contexts
        """
        return iter(self._slaves.items())

    def __contains__(self, slave):
        """Check if the given slave is in this list.

        :param slave: slave The slave to check for existence
        :returns: True if the slave exists, False otherwise
        """
        if self.single and self._slaves:
            return True
        return slave in self._slaves

    def __setitem__(self, slave, context):
        """Use to set a new slave context.

        :param slave: The slave context to set
        :param context: The new context to set for this slave
        :raises NoSuchSlaveException:
        """
        if self.single:
            slave = 0
        if 0xF7 >= slave >= 0x00:
            self._slaves[slave] = context
        else:
            raise NoSuchSlaveException(f"slave index :{slave} out of range")

    def __delitem__(self, slave):
        """Use to access the slave context.

        :param slave: The slave context to remove
        :raises NoSuchSlaveException:
        """
        if not self.single and (0xF7 >= slave >= 0x00):
            del self._slaves[slave]
        else:
            raise NoSuchSlaveException(f"slave index: {slave} out of range")

    def __getitem__(self, slave):
        """Use to get access to a slave context.

        :param slave: The slave context to get
        :returns: The requested slave context
        :raises NoSuchSlaveException:
        """
        if self.single:
            slave = 0
        if slave in self._slaves:
            return self._slaves.get(slave)
        raise NoSuchSlaveException(
            f"slave - {slave} does not exist, or is out of range"
        )

    def slaves(self):
        """Define slaves."""
        # Python3 now returns keys() as iterable
        return list(self._slaves.keys())
