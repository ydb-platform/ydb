__all__ = ['DynamicDidDefinition']

from udsoncan.common.MemoryLocation import MemoryLocation
from udsoncan.common.AddressAndLengthFormatIdentifier import AddressAndLengthFormatIdentifier

from typing import Union, Type, List, Optional, cast


class DynamicDidDefinition:
    """
    This class serves as a container for the different pieces of a dynamic DID defined by the DynamicallyDefineDataIDentifier service.

    """

    class ByDidDefinition:
        source_did: int
        position: int
        memorysize: int

        def __init__(self, source_did: int, position: int, memorysize: int):
            if not isinstance(source_did, int):
                raise ValueError('source_did must be an integer')
            if source_did > 0xFFFF or source_did < 0:
                raise ValueError('source_did must be an integer between 0 and 0xFFFF')

            if not isinstance(position, int):
                raise ValueError('position must be an integer')
            if position < 0:
                raise ValueError('position must be an integer greater than 0')

            if not isinstance(memorysize, int):
                raise ValueError('memorysize must be an integer')
            if memorysize < 0:
                raise ValueError('memorysize must be an integer greater than 0')

            self.source_did = source_did
            self.position = position
            self.memorysize = memorysize

    class ByMemloc:
        memloc: MemoryLocation

        def __init__(self, memloc: MemoryLocation):
            if not isinstance(memloc, MemoryLocation):
                raise ValueError('memloc must be an instance of MemoryLocation')

            self.memloc = memloc

    entries: Union[List[ByDidDefinition], List[ByMemloc]]
    definition_type: Optional[Union[Type[ByDidDefinition], Type[ByMemloc]]]

    def __init__(self, *args, **kwargs):
        self.entries = list()
        self.definition_type = None

        if len(args) > 0 or len(kwargs) > 0:
            self.add(*args, **kwargs)

    def add(self, *args, **kwargs) -> None:
        """
        Add a piece of definition for a dynamic DID. 

        When defining by memory address, only ``memloc`` can be supplied. 
        Example : ``myDidDefinition.add(myMemLoc)``

        When defining with a source DID, these parameters must be supplied : ``source_did``, ``position``, ``memorysize``. 
        Example : ``myDidDefinition.add(source_did=1234, position=1, memorysize=2)``

        :param source_did: The source DID from which to fetch data from
        :type source_did: int

        :param position: Start position of the data to fetch inside the source DID data
        :type position: int

        :param memorysize: Length of data to fetch inside the source DID data
        :type memorysize: int

        :param memloc: MemoryLocation containing an address, a size and an encoding format
        :type memloc: :ref:`MemoryLocation<MemoryLocation>`

        """
        entry: Optional[Union["DynamicDidDefinition.ByMemloc", "DynamicDidDefinition.ByDidDefinition"]] = None

        if len(args) > 0:
            if isinstance(args[0], MemoryLocation):
                entry = self.ByMemloc(memloc=args[0])
        elif 'memloc' in kwargs:
            entry = self.ByMemloc(memloc=kwargs['memloc'])

        if entry is None:
            entry = self.ByDidDefinition(*args, **kwargs)

        if self.definition_type is None:
            self.definition_type = entry.__class__
        else:
            if entry.__class__ != self.definition_type:
                raise ValueError('It is not possible to define a composite dynamic DID by memory address and by source DID at the same time.')

        self.entries.append(entry)  # type: ignore

    def is_by_source_did(self) -> bool:
        return True if self.definition_type == self.ByDidDefinition else False

    def is_by_memory_address(self) -> bool:
        return True if self.definition_type == self.ByMemloc else False

    def get_alfid(self) -> AddressAndLengthFormatIdentifier:
        if not self.is_by_memory_address():
            raise ValueError('No AddressAndLengthFormatIdentifier available in this DynamicDidDefinition. Only available when defined by memory address')

        alfid: Optional[AddressAndLengthFormatIdentifier] = None
        for entry in cast(List["DynamicDidDefinition.ByMemloc"], self.entries):
            if alfid is None:
                alfid = entry.memloc.alfid
            else:
                if alfid.get_byte() != entry.memloc.alfid.get_byte():
                    raise ValueError(
                        'AddressAndLengthFormatIdentifier of DynamicDidDefinition entries are not consistent. Make sure address_format and memorysize_format are the same accross all entries.')

        if alfid is None:
            raise ValueError('No AddressAndLengthFormatIdentifier avaialble')  # should never happen.

        return alfid

    def get(self) -> Union[List[ByDidDefinition], List[ByMemloc]]:
        return self.entries
