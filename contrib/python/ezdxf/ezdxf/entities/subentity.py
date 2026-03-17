# Copyright (c) 2020-2024, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Iterable, Callable, Optional
from typing_extensions import Self

from ezdxf.entities import factory, DXFGraphic, SeqEnd, DXFEntity
from ezdxf.lldxf import const
from .copy import default_copy
import logging

if TYPE_CHECKING:
    from ezdxf.document import Drawing
    from ezdxf.entities import DXFEntity
    from ezdxf.entitydb import EntityDB
    from ezdxf import xref


__all__ = ["entity_linker", "LinkedEntities"]
logger = logging.getLogger("ezdxf")


class LinkedEntities(DXFGraphic):
    """Super class for common features of the INSERT and the POLYLINE entity.
    Both have linked entities like the VERTEX or ATTRIB entity and a
    SEQEND entity.

    """

    def __init__(self) -> None:
        super().__init__()
        self._sub_entities: list[DXFGraphic] = []
        self.seqend: Optional[SeqEnd] = None

    def copy_data(self, entity: Self, copy_strategy=default_copy) -> None:
        """Copy all sub-entities ands SEQEND. (internal API)"""
        assert isinstance(entity, LinkedEntities)
        entity._sub_entities = [
            copy_strategy.copy(e) for e in self._sub_entities
        ]
        if self.seqend:
            entity.seqend = copy_strategy.copy(self.seqend)

    def link_entity(self, entity: DXFEntity) -> None:
        """Link VERTEX to ATTRIB entities."""
        assert isinstance(entity, DXFGraphic)
        entity.set_owner(self.dxf.handle, self.dxf.paperspace)
        self._sub_entities.append(entity)

    def link_seqend(self, seqend: DXFEntity) -> None:
        """Link SEQEND entity. (internal API)"""
        seqend.dxf.owner = self.dxf.owner
        if self.seqend is not None:  # destroy existing seqend
            self.seqend.destroy()
        self.seqend = seqend  # type: ignore

    def post_bind_hook(self):
        """Create always a SEQEND entity."""
        if self.seqend is None:
            self.new_seqend()

    def all_sub_entities(self) -> Iterable[DXFEntity]:
        """Yields all sub-entities and SEQEND. (internal API)"""
        yield from self._sub_entities
        if self.seqend:
            yield self.seqend

    def process_sub_entities(self, func: Callable[[DXFEntity], None]):
        """Call `func` for all sub-entities and SEQEND. (internal API)"""
        for entity in self.all_sub_entities():
            if entity.is_alive:
                func(entity)

    def add_sub_entities_to_entitydb(self, db: EntityDB) -> None:
        """Add sub-entities (VERTEX, ATTRIB, SEQEND) to entity database `db`,
        called from EntityDB. (internal API)
        """

        def add(entity: DXFEntity):
            entity.doc = self.doc  # grant same document
            db.add(entity)

        if not self.seqend or not self.seqend.is_alive:
            self.new_seqend()
        self.process_sub_entities(add)

    def new_seqend(self):
        """Create and bind new SEQEND. (internal API)"""
        attribs = {"layer": self.dxf.layer}
        if self.doc:
            seqend = factory.create_db_entry("SEQEND", attribs, self.doc)
        else:
            seqend = factory.new("SEQEND", attribs)
        self.link_seqend(seqend)

    def set_owner(self, owner: Optional[str], paperspace: int = 0):
        """Set owner of all sub-entities and SEQEND. (internal API)"""
        # Loading from file: POLYLINE/INSERT will be added to layout before
        # vertices/attrib entities are linked, so set_owner() of POLYLINE does
        # not set owner of vertices at loading time.
        super().set_owner(owner, paperspace)
        self.take_ownership()

    def take_ownership(self):
        """Take ownership of all sub-entities and SEQEND. (internal API)"""
        handle = self.dxf.handle
        paperspace = self.dxf.paperspace
        for entity in self.all_sub_entities():
            if entity.is_alive:
                entity.dxf.owner = handle
                entity.dxf.paperspace = paperspace

    def remove_dependencies(self, other: Optional[Drawing] = None):
        """Remove all dependencies from current document to bind entity to
        `other` document. (internal API)
        """
        self.process_sub_entities(lambda e: e.remove_dependencies(other))
        super().remove_dependencies(other)

    def destroy(self) -> None:
        """Destroy all data and references."""
        if not self.is_alive:
            return

        self.process_sub_entities(func=lambda e: e.destroy())
        del self._sub_entities
        del self.seqend
        super().destroy()

    def register_resources(self, registry: xref.Registry) -> None:
        """Register required resources to the resource registry."""
        super().register_resources(registry)
        self.process_sub_entities(lambda e: e.register_resources(registry))

    def map_resources(self, clone: Self, mapping: xref.ResourceMapper) -> None:
        """Translate resources from self to the copied entity."""
        assert isinstance(clone, LinkedEntities)
        super().map_resources(clone, mapping)
        for source, _clone in zip(self.all_sub_entities(), clone.all_sub_entities()):
            source.map_resources(_clone, mapping)


LINKED_ENTITIES = {"INSERT": "ATTRIB", "POLYLINE": "VERTEX"}


def entity_linker() -> Callable[[DXFEntity], bool]:
    """Create an DXF entities linker."""
    main_entity: Optional[DXFEntity] = None
    expected_dxftype = ""

    def entity_linker_(entity: DXFEntity) -> bool:
        """Collect and link entities which are linked to a parent entity:

        - VERTEX -> POLYLINE
        - ATTRIB -> INSERT

        Args:
             entity: examined DXF entity

        Returns:
             True if `entity` is linked to a parent entity

        """
        nonlocal main_entity, expected_dxftype
        dxftype: str = entity.dxftype()
        # INSERT & POLYLINE are not linked entities, they are stored in the
        # entity space.
        are_linked_entities = False
        if main_entity is not None:
            # VERTEX, ATTRIB & SEQEND are linked tags, they are NOT stored in
            # the entity space.
            are_linked_entities = True
            if dxftype == "SEQEND":
                main_entity.link_seqend(entity)  # type: ignore
                # Marks also the end of the main entity
                main_entity = None
            # Check for valid DXF structure:
            #   VERTEX follows POLYLINE
            #   ATTRIB follows INSERT
            elif dxftype == expected_dxftype:
                main_entity.link_entity(entity)  # type: ignore
            else:
                raise const.DXFStructureError(
                    f"Expected DXF entity {dxftype} or SEQEND"
                )

        elif dxftype in LINKED_ENTITIES:
            # Only INSERT and POLYLINE have a linked entities structure:
            if dxftype == "INSERT" and not entity.dxf.get("attribs_follow", 0):
                # INSERT must not have following ATTRIBS:
                #
                #   INSERT with no ATTRIBS, attribs_follow == 0
                #   ATTRIB as a stand alone entity, which is a DXF structure
                #   error, but this error should be handled in the audit
                #   process.
                #   ....
                #   INSERT with ATTRIBS, attribs_follow == 1
                #   ATTRIB as connected entity
                #   SEQEND
                #
                # Therefore a ATTRIB following an INSERT doesn't mean that
                # these entities are linked.
                pass
            else:
                main_entity = entity
                expected_dxftype = LINKED_ENTITIES[dxftype]

        # Attached MTEXT entity - this feature most likely does not exist!
        elif (dxftype == "MTEXT") and (entity.dxf.handle is None):
            logger.error(
                "Found attached MTEXT entity. Please open an issue at github: "
                "https://github.com/mozman/ezdxf/issues and provide a DXF "
                "example file."
            )
        return are_linked_entities

    return entity_linker_
