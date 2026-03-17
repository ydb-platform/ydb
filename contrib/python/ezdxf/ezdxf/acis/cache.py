# Copyright (c) 2024, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Sequence
from .entities import Body, load
from .type_hints import EncodedData

if TYPE_CHECKING:
    from ezdxf.entities import DXFEntity

__all__ = ["AcisCache"]
NO_BODIES: Sequence[Body] = tuple()


class AcisCache:
    """This cache manages ACIS bodies created from SAT or SAB data stored in DXF 
    entities.
    
    Each entry is a list of ACIS bodies and is indexed by a hash calculated from the 
    source content of the SAT or SAB data.

    """
    def __init__(self) -> None:
        self._entries: dict[int, Sequence[Body]] = {}
        self.hits: int = 0
        self.misses: int = 0

    @staticmethod
    def hash_data(data: EncodedData) -> int:
        if isinstance(data, list):
            return hash(tuple(data))
        elif isinstance(data, bytearray):
            return hash(bytes(data))
        return hash(data)

    def __len__(self) -> int:
        return len(self._entries)

    def get_bodies(self, data: EncodedData) -> Sequence[Body]:
        if not data:
            return NO_BODIES
        
        hash_value = AcisCache.hash_data(data)
        bodies = self._entries.get(hash_value, NO_BODIES)
        if bodies is not NO_BODIES:
            self.hits += 1
            return bodies
        
        self.misses += 1
        bodies = tuple(load(data))
        self._entries[hash_value] = bodies
        return bodies
    
    def from_dxf_entity(self, entity: DXFEntity) ->  Sequence[Body]:
        from ezdxf.entities import Body as DxfBody

        if not isinstance(entity, DxfBody):
            return NO_BODIES
        return self.get_bodies(entity.acis_data)
