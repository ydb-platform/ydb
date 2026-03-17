# Copyright (c) 2011-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Iterable, Iterator, cast, Optional
from itertools import chain
import logging

from ezdxf.lldxf import const
from ezdxf.entities import entity_linker

if TYPE_CHECKING:
    from ezdxf.document import Drawing
    from ezdxf.entities import DXFEntity, DXFTagStorage, BlockRecord, DXFGraphic
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf.lldxf.tags import Tags


logger = logging.getLogger("ezdxf")


class StoredSection:
    def __init__(self, entities: list[Tags]):
        self.entities = entities

    def export_dxf(self, tagwriter: AbstractTagWriter):
        # (0, SECTION) (2, NAME) is stored in entities
        for entity in self.entities:
            tagwriter.write_tags(entity)
        # ENDSEC not stored in entities !!!
        tagwriter.write_str("  0\nENDSEC\n")


class EntitySection:
    """:class:`EntitiesSection` is just a proxy for :class:`Modelspace` and
    active :class:`Paperspace` linked together.
    """

    def __init__(
        self,
        doc: Optional[Drawing] = None,
        entities: Optional[Iterable[DXFEntity]] = None,
    ):
        self.doc = doc
        if entities is not None:
            self._build(iter(entities))

    def __iter__(self) -> Iterator[DXFEntity]:
        """Returns an iterator for all entities of the modelspace and the active
        paperspace.
        """
        assert self.doc is not None
        layouts = self.doc.layouts
        for entity in chain(layouts.modelspace(), layouts.active_layout()):
            yield entity

    def __len__(self) -> int:
        """Returns the count of all entities in the modelspace and the active paperspace.
        """
        assert self.doc is not None
        layouts = self.doc.layouts
        return len(layouts.modelspace()) + len(layouts.active_layout())

    # none public interface

    def _build(self, entities: Iterator[DXFEntity]) -> None:
        assert self.doc is not None
        section_head = cast("DXFTagStorage", next(entities))
        if section_head.dxftype() != "SECTION" or section_head.base_class[
            1
        ] != (2, "ENTITIES"):
            raise const.DXFStructureError(
                "Critical structure error in ENTITIES section."
            )

        def add(entity: DXFGraphic):
            handle = entity.dxf.owner
            # higher priority for owner handle
            paperspace = 0
            if handle == msp_layout_key:
                paperspace = 0
            elif handle == psp_layout_key:
                paperspace = 1
            elif entity.dxf.hasattr(
                "paperspace"
            ):  # paperspace flag as fallback
                paperspace = entity.dxf.paperspace

            if paperspace:
                psp.add_entity(entity)
            else:
                msp.add_entity(entity)

        msp = cast("BlockRecord", self.doc.block_records.get("*Model_Space"))
        psp = cast("BlockRecord", self.doc.block_records.get("*Paper_Space"))
        msp_layout_key: str = msp.dxf.handle
        psp_layout_key: str = psp.dxf.handle
        linked_entities = entity_linker()
        # Don't store linked entities (VERTEX, ATTRIB, SEQEND) in entity space
        for entity in entities:
            # No check for valid entities here:
            # Use the audit- or the recover module to fix invalid DXF files!
            if not linked_entities(entity):
                add(entity)  # type: ignore

    def export_dxf(self, tagwriter: AbstractTagWriter) -> None:
        assert self.doc is not None
        layouts = self.doc.layouts
        tagwriter.write_str("  0\nSECTION\n  2\nENTITIES\n")
        # Just write *Model_Space and the active *Paper_Space into the
        # ENTITIES section.
        layouts.modelspace().entity_space.export_dxf(tagwriter)
        layouts.active_layout().entity_space.export_dxf(tagwriter)
        tagwriter.write_tag2(0, "ENDSEC")
