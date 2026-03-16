# Copyright (c) 2019-2022 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Optional
from .dxfns import SubclassProcessor, DXFNamespace
from .dxfentity import DXFEntity
from ezdxf.lldxf.attributes import (
    DXFAttr,
    DXFAttributes,
    DefSubclass,
    group_code_mapping,
)
from ezdxf.lldxf.const import DXF2004, DXF2000
from .factory import register_entity

if TYPE_CHECKING:
    from ezdxf.document import Drawing
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf.lldxf.extendedtags import ExtendedTags

__all__ = ["DXFClass"]

class_def = DefSubclass(
    None,
    {
        # Class DXF record name; always unique
        "name": DXFAttr(1),
        # C++ class name. Used to bind with software that defines object class
        # behavior; always unique
        "cpp_class_name": DXFAttr(2),
        # Application name. Posted in Alert box when a class definition listed in
        # this section is not currently loaded
        "app_name": DXFAttr(3),
        # Proxy capabilities flag. Bit-coded value that indicates the capabilities
        # of this object as a proxy:
        # 0 = No operations allowed (0)
        # 1 = Erase allowed (0x1)
        # 2 = Transform allowed (0x2)
        # 4 = Color change allowed (0x4)
        # 8 = Layer change allowed (0x8)
        # 16 = Linetype change allowed (0x10)
        # 32 = Linetype scale change allowed (0x20)
        # 64 = Visibility change allowed (0x40)
        # 128 = Cloning allowed (0x80)
        # 256 = Lineweight change allowed (0x100)
        # 512 = Plot Style Name change allowed (0x200)
        # 895 = All operations except cloning allowed (0x37F)
        # 1023 = All operations allowed (0x3FF)
        # 1024 = Disables proxy warning dialog (0x400)
        # 32768 = R13 format proxy (0x8000)
        "flags": DXFAttr(90, default=0),
        # Instance count for a custom class
        "instance_count": DXFAttr(91, dxfversion=DXF2004, default=0),
        # Was-a-proxy flag. Set to 1 if class was not loaded when this DXF file was
        # created, and 0 otherwise
        "was_a_proxy": DXFAttr(280, default=0),
        # Is-an-entity flag. Set to 1 if class was derived from the AcDbEntity class
        # and can reside in the BLOCKS or ENTITIES section. If 0, instances may
        # appear only in the OBJECTS section
        "is_an_entity": DXFAttr(281, default=0),
    },
)
class_def_group_codes = group_code_mapping(class_def)


@register_entity
class DXFClass(DXFEntity):
    DXFTYPE = "CLASS"
    DXFATTRIBS = DXFAttributes(class_def)
    MIN_DXF_VERSION_FOR_EXPORT = DXF2000

    @classmethod
    def new(
        cls,
        handle: Optional[str] = None,
        owner: Optional[str] = None,
        dxfattribs=None,
        doc: Optional[Drawing] = None,
    ) -> DXFClass:
        """New CLASS constructor - has no handle, no owner and do not need
        document reference .
        """
        dxf_class = cls()
        dxf_class.doc = doc
        dxfattribs = dxfattribs or {}
        dxf_class.update_dxf_attribs(dxfattribs)
        return dxf_class

    def load_tags(
        self, tags: ExtendedTags, dxfversion: Optional[str] = None
    ) -> None:
        """Called by load constructor. CLASS is special."""
        if tags:
            # do not process base class!!!
            self.dxf = DXFNamespace(entity=self)
            processor = SubclassProcessor(tags)
            processor.fast_load_dxfattribs(
                self.dxf, class_def_group_codes, 0, log=False
            )

    def export_dxf(self, tagwriter: AbstractTagWriter):
        """Do complete export here, because CLASS is special."""
        dxfversion = tagwriter.dxfversion
        if dxfversion < DXF2000:
            return
        attribs = self.dxf
        tagwriter.write_tag2(0, self.DXFTYPE)
        attribs.export_dxf_attribs(
            tagwriter,
            [
                "name",
                "cpp_class_name",
                "app_name",
                "flags",
                "instance_count",
                "was_a_proxy",
                "is_an_entity",
            ],
        )

    @property
    def key(self) -> tuple[str, str]:
        return self.dxf.name, self.dxf.cpp_class_name
