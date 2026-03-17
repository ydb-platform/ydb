#  Copyright (c) 2021-2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Optional, Iterator
from ezdxf.lldxf import const
from ezdxf.lldxf.tags import Tags
from ezdxf.query import EntityQuery
from .dxfentity import SubclassProcessor
from .dxfgfx import DXFGraphic
from . import factory
from .copy import default_copy, CopyNotSupported

if TYPE_CHECKING:
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf.entities import DXFNamespace
    from ezdxf.layouts import BaseLayout


# Group Codes of AcDbProxyEntity
# https://help.autodesk.com/view/OARX/2018/ENU/?guid=GUID-89A690F9-E859-4D57-89EA-750F3FB76C6B
# 100 AcDbProxyEntity
# 90  Proxy entity class ID (always 498)
# 91  Application entity's class ID. Class IDs are based on the order of
#     the class in the CLASSES section. The first class is given the ID of
#     500, the next is 501, and so on
#
# 92  Size of graphics data in bytes < R2010; R2010+ = 160
# 310 Binary graphics data (multiple entries can appear) (optional)
#
# 96  Size of unknown data in bytes < R2010; R2010+ = 162
# 311 Binary entity data (multiple entries can appear) (optional)
#
# 93  Size of entity data in bits <R2010; R2010+ = 161
# 310 Binary entity data (multiple entries can appear) (optional)
#
# 330 or 340 or 350 or 360 - An object ID (multiple entries can appear) (optional)
# 94  0 (indicates end of object ID section)
# 95  Object drawing format when it becomes a proxy (a 32-bit unsigned integer):
#     Low word is AcDbDwgVersion
#     High word is MaintenanceReleaseVersion
# 70  Original custom object data format:
#     0 = DWG format
#     1 = DXF format


@factory.register_entity
class ACADProxyEntity(DXFGraphic):
    """READ ONLY ACAD_PROXY_ENTITY CLASS! DO NOT MODIFY!"""

    DXFTYPE = "ACAD_PROXY_ENTITY"
    MIN_DXF_VERSION_FOR_EXPORT = const.DXF2000

    def __init__(self) -> None:
        super().__init__()
        self.acdb_proxy_entity: Optional[Tags] = None

    def copy(self, copy_strategy=default_copy):
        raise CopyNotSupported(f"Copying of {self.dxftype()} not supported.")

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            self.acdb_proxy_entity = processor.subclass_by_index(2)
            self.load_proxy_graphic()
        return dxf

    def load_proxy_graphic(self) -> None:
        if self.acdb_proxy_entity is None:
            return
        for length_code in (92, 160):
            proxy_graphic = load_proxy_data(self.acdb_proxy_entity, length_code, 310)
            if proxy_graphic:
                self.proxy_graphic = proxy_graphic
                return

    def export_dxf(self, tagwriter: AbstractTagWriter) -> None:
        # Proxy graphic is stored in AcDbProxyEntity and not as usual in
        # AcDbEntity!
        preserve_proxy_graphic = self.proxy_graphic
        self.proxy_graphic = None
        super().export_dxf(tagwriter)
        self.proxy_graphic = preserve_proxy_graphic

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags. (internal API)"""
        # Base class and AcDbEntity export is done by parent class
        super().export_entity(tagwriter)
        if self.acdb_proxy_entity is not None:
            tagwriter.write_tags(self.acdb_proxy_entity)
        # XDATA export is done by the parent class

    def __virtual_entities__(self) -> Iterator[DXFGraphic]:
        """Implements the SupportsVirtualEntities protocol."""
        from ezdxf.proxygraphic import ProxyGraphic

        if self.proxy_graphic:
            for e in ProxyGraphic(self.proxy_graphic, doc=self.doc).virtual_entities():
                e.set_source_of_copy(self)
                yield e

    def virtual_entities(self) -> Iterator[DXFGraphic]:
        """Yields proxy graphic as "virtual" entities."""
        return self.__virtual_entities__()

    def explode(self, target_layout: Optional[BaseLayout] = None) -> EntityQuery:
        """Explodes the proxy graphic for the ACAD_PROXY_ENTITY into the target layout,
        if target layout is ``None``, the layout of the ACAD_PROXY_ENTITY will be used.
        This method destroys the source ACAD_PROXY_ENTITY entity.

        Args:
            target_layout: target layout for exploded entities, ``None`` for
                same layout as the source ACAD_PROXY_ENTITY.

        Returns:
            :class:`~ezdxf.query.EntityQuery` container referencing all exploded
            DXF entities.

        """
        if target_layout is None:
            target_layout = self.get_layout()
            if target_layout is None:
                raise const.DXFStructureError(
                    "ACAD_PROXY_ENTITY without layout assignment, specify target layout"
                )
        entities: list[DXFGraphic] = list(self.__virtual_entities__())
        for e in entities:
            target_layout.add_entity(e)
        self.destroy()
        return EntityQuery(entities)


def load_proxy_data(
    tags: Tags, length_code: int, data_code: int = 310
) -> Optional[bytes]:
    try:
        index = tags.tag_index(length_code)
    except const.DXFValueError:
        return None
    binary_data = []
    for code, value in tags[index + 1 :]:
        if code == data_code:
            binary_data.append(value)
        else:
            break  # at first tag with group code != data_code
    return b"".join(binary_data)
