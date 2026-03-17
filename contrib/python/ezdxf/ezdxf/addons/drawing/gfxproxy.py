#  Copyright (c) 2021-2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import Iterable, Iterator
from ezdxf.entities import DXFGraphic, DXFEntity
from ezdxf.lldxf import const
from ezdxf.lldxf.tagwriter import AbstractTagWriter
from ezdxf.protocols import SupportsVirtualEntities
from ezdxf.entities.copy import default_copy, CopyNotSupported


class DXFGraphicProxy(DXFGraphic):
    """DO NOT USE THIS WRAPPER AS REAL DXF ENTITY OUTSIDE THE DRAWING ADD-ON!"""

    def __init__(self, entity: DXFEntity):
        super().__init__()
        self.entity = entity
        self.dxf = self._setup_dxf_namespace(entity)

    def _setup_dxf_namespace(self, entity):
        # copy DXF namespace - modifications do not effect the wrapped entity
        dxf = entity.dxf.copy(self)
        # setup mandatory DXF attributes without default values like layer:
        for k, v in self.DEFAULT_ATTRIBS.items():
            if not dxf.hasattr(k):
                dxf.set(k, v)
        return dxf

    def dxftype(self) -> str:
        return self.entity.dxftype()

    def __virtual_entities__(self) -> Iterator[DXFGraphic]:
        """Implements the SupportsVirtualEntities protocol."""
        if isinstance(self.entity, SupportsVirtualEntities):
            return self.entity.__virtual_entities__()
        if hasattr(self.entity, "virtual_entities"):
            return self.entity.virtual_entities()
        return iter([])

    def virtual_entities(self) -> Iterable[DXFGraphic]:
        return self.__virtual_entities__()

    def copy(self, copy_strategy=default_copy) -> DXFGraphicProxy:
        raise CopyNotSupported(f"Copying of DXFGraphicProxy() not supported.")

    def preprocess_export(self, tagwriter: AbstractTagWriter) -> bool:
        # prevent dxf export
        return False
