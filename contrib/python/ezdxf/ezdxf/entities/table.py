# Copyright (c) 2019-2022 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Optional
from ezdxf.lldxf.attributes import DXFAttr, DXFAttributes, DefSubclass
from ezdxf.lldxf import const
from .dxfentity import SubclassProcessor, DXFEntity
from .factory import register_entity

if TYPE_CHECKING:
    from ezdxf.entities import DXFNamespace
    from ezdxf.lldxf.tagwriter import AbstractTagWriter

__all__ = ["TableHead"]

base_class = DefSubclass(
    None,
    {
        "name": DXFAttr(2),
        "handle": DXFAttr(5),
        "owner": DXFAttr(330),
    },
)

acdb_symbol_table = DefSubclass(
    "AcDbSymbolTable",
    {
        "count": DXFAttr(70, default=0),
    },
)


@register_entity
class TableHead(DXFEntity):
    """The table head structure is only maintained for export and not for
    internal usage, ezdxf ignores an inconsistent table head at runtime.

    """

    DXFTYPE = "TABLE"
    DXFATTRIBS = DXFAttributes(base_class, acdb_symbol_table)

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            dxf.name = processor.base_class.get_first_value(2)
            # Stored max table count is not required:
            dxf.count = 0
        return dxf

    def export_dxf(self, tagwriter: AbstractTagWriter) -> None:
        assert self.dxf.handle, (
            "TABLE needs a handle, maybe loaded from DXF R12 without handle!"
        )
        tagwriter.write_tag2(const.STRUCTURE_MARKER, self.DXFTYPE)
        tagwriter.write_tag2(2, self.dxf.name)
        if tagwriter.dxfversion >= const.DXF2000:
            tagwriter.write_tag2(5, self.dxf.handle)
            if self.has_extension_dict:
                self.extension_dict.export_dxf(tagwriter)  # type: ignore
            tagwriter.write_tag2(const.OWNER_CODE, self.dxf.owner)
            tagwriter.write_tag2(const.SUBCLASS_MARKER, acdb_symbol_table.name)
            tagwriter.write_tag2(70, self.dxf.count)
            # There is always one exception:
            if self.dxf.name == "DIMSTYLE":
                tagwriter.write_tag2(const.SUBCLASS_MARKER, "AcDbDimStyleTable")
        else:  # DXF R12
            # TABLE does not need a handle at all
            tagwriter.write_tag2(70, self.dxf.count)
