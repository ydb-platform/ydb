# Copyright (c) 2019-2023, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Iterator, Union, Iterable, Optional

from ezdxf.entities import factory, is_graphic_entity, SortEntsTable
from ezdxf.enums import InsertUnits
from ezdxf.lldxf.const import (
    DXFKeyError,
    DXFValueError,
    DXFStructureError,
    LATEST_DXF_VERSION,
    DXFTypeError,
)
from ezdxf.query import EntityQuery
from ezdxf.groupby import groupby
from ezdxf.entitydb import EntityDB, EntitySpace
from ezdxf.graphicsfactory import CreatorInterface

if TYPE_CHECKING:
    from ezdxf.entities import DXFGraphic, BlockRecord, ExtensionDict
    from ezdxf.eztypes import KeyFunc

SUPPORTED_FOREIGN_ENTITY_TYPES = {
    "ARC",
    "LINE",
    "CIRCLE",
    "ELLIPSE",
    "POINT",
    "LWPOLYLINE",
    "SPLINE",
    "3DFACE",
    "SOLID",
    "TRACE",
    "SHAPE",
    "POLYLINE",
    "MESH",
    "TEXT",
    "MTEXT",
    "HATCH",
    "ATTRIB",
    "ATTDEF",
}


class _AbstractLayout(CreatorInterface):
    entity_space = EntitySpace()

    @property
    def entitydb(self) -> EntityDB:
        """Returns drawing entity database. (internal API)"""
        return self.doc.entitydb

    def __len__(self) -> int:
        """Returns count of entities owned by the layout."""
        return len(self.entity_space)

    def __iter__(self) -> Iterator[DXFGraphic]:
        """Returns iterable of all drawing entities in this layout."""
        return iter(self.entity_space)  # type: ignore

    def __getitem__(self, index):
        """Get entity at `index`.

        The underlying data structure for storing entities is organized like a
        standard Python list, therefore `index` can be any valid list indexing
        or slicing term, like a single index ``layout[-1]`` to get the last
        entity, or an index slice ``layout[:10]`` to get the first 10 or less
        entities as ``list[DXFGraphic]``.

        """
        return self.entity_space[index]

    def query(self, query: str = "*") -> EntityQuery:
        """Get all DXF entities matching the :ref:`entity query string`."""
        return EntityQuery(iter(self), query)

    def groupby(self, dxfattrib: str = "", key: Optional[KeyFunc] = None) -> dict:
        """
        Returns a ``dict`` of entity lists, where entities are grouped by a
        `dxfattrib` or a `key` function.

        Args:
            dxfattrib: grouping by DXF attribute like ``'layer'``
            key: key function, which accepts a :class:`DXFGraphic` entity as
                argument and returns the grouping key of an entity or ``None``
                to ignore the entity. Reason for ignoring: a queried DXF
                attribute is not supported by entity.

        """
        return groupby(iter(self), dxfattrib, key)

    def destroy(self):
        pass

    def purge(self):
        """Remove all destroyed entities from the layout entity space."""
        self.entity_space.purge()


class BaseLayout(_AbstractLayout):
    def __init__(self, block_record: BlockRecord):
        doc = block_record.doc
        assert doc is not None
        super().__init__(doc)
        self.entity_space = block_record.entity_space
        # This is the real central layout management structure:
        self.block_record: BlockRecord = block_record

    @property
    def block_record_handle(self):
        """Returns block record handle. (internal API)"""
        return self.block_record.dxf.handle

    @property
    def layout_key(self) -> str:
        """Returns the layout key as hex string.

        The layout key is the handle of the associated BLOCK_RECORD entry in the
        BLOCK_RECORDS table.

        (internal API)
        """
        return self.block_record.dxf.handle

    @property
    def is_alive(self):
        """``False`` if layout is deleted."""
        return self.block_record.is_alive

    @property
    def is_active_paperspace(self) -> bool:
        """``True`` if is active layout."""
        if self.block_record.is_alive:
            return self.block_record.is_active_paperspace
        return False

    @property
    def is_any_paperspace(self) -> bool:
        """``True`` if is any kind of paperspace layout."""
        return self.block_record.is_any_paperspace

    @property
    def is_modelspace(self) -> bool:
        """``True`` if is modelspace layout."""
        return self.block_record.is_modelspace

    @property
    def is_any_layout(self) -> bool:
        """``True`` if is any kind of modelspace or paperspace layout."""
        return self.block_record.is_any_layout

    @property
    def is_block_layout(self) -> bool:
        """``True`` if not any kind of modelspace or paperspace layout, just a
        regular block definition.
        """
        return self.block_record.is_block_layout

    @property
    def units(self) -> InsertUnits:
        """Get/Set layout/block drawing units as enum, see also :ref:`set
        drawing units`.
        """
        # todo: doesn't care about units stored in XDATA, see ezdxf/units.py
        # Don't know what this units are used for, but header var $INSUNITS
        # are the real units of the model space.
        return self.block_record.dxf.units

    @units.setter
    def units(self, value: InsertUnits) -> None:
        """Set layout/block drawing units as enum."""
        self.block_record.dxf.units = value  # has a DXF validator

    def get_extension_dict(self) -> ExtensionDict:
        """Returns the associated extension dictionary, creates a new one if
        necessary.
        """
        block_record = self.block_record
        if block_record.has_extension_dict:
            return block_record.get_extension_dict()
        else:
            return block_record.new_extension_dict()

    def add_entity(self, entity: DXFGraphic) -> None:
        """Add an existing :class:`DXFGraphic` entity to a layout, but be sure
        to unlink (:meth:`~BaseLayout.unlink_entity`) `entity` from the previous
        owner layout.  Adding entities from a different DXF drawing is not
        supported.

        .. warning:: 
        
            This is a low-level tool - use it with caution and make sure you understand 
            what you are doing! If used improperly, the DXF document may be damaged.

        """
        # bind virtual entities to the DXF document:
        doc = self.doc
        if entity.dxf.handle is None and doc:
            factory.bind(entity, doc)
        handle = entity.dxf.handle
        if handle is None or handle not in self.doc.entitydb:
            raise DXFStructureError(
                "Adding entities from a different DXF drawing is not supported."
            )

        if not is_graphic_entity(entity):
            raise DXFTypeError(f"invalid entity {str(entity)}")
        self.block_record.add_entity(entity)

    def add_foreign_entity(self, entity: DXFGraphic, copy=True) -> None:
        """Add a foreign DXF entity to a layout, this foreign entity could be
        from another DXF document or an entity without an assigned DXF document.
        The intention of this method is to add **simple** entities from another
        DXF document or from a DXF iterator, for more complex operations use the
        :mod:`~ezdxf.addons.importer` add-on. Especially objects with BLOCK
        section (INSERT, DIMENSION, MLEADER) or OBJECTS section dependencies
        (IMAGE, UNDERLAY) can not be supported by this simple method.

        Not all DXF types are supported and every dependency or resource
        reference from another DXF document will be removed except attribute
        layer will be preserved but only with default attributes like
        color ``7`` and linetype ``CONTINUOUS`` because the layer attribute
        doesn't need a layer table entry.

        If the entity is part of another DXF document, it will be unlinked from
        this document and its entity database if argument `copy` is ``False``,
        else the entity will be copied. Unassigned entities like from DXF
        iterators will just be added.

        Supported DXF types:

            - POINT
            - LINE
            - CIRCLE
            - ARC
            - ELLIPSE
            - LWPOLYLINE
            - SPLINE
            - POLYLINE
            - 3DFACE
            - SOLID
            - TRACE
            - SHAPE
            - MESH
            - ATTRIB
            - ATTDEF
            - TEXT
            - MTEXT
            - HATCH

        Args:
            entity: DXF entity to copy or move
            copy: if ``True`` copy entity from other document else unlink from
                other document

        Raises:
            CopyNotSupported: copying of `entity` i not supported
        """
        foreign_doc = entity.doc
        dxftype = entity.dxftype()
        if dxftype not in SUPPORTED_FOREIGN_ENTITY_TYPES:
            raise DXFTypeError(f"unsupported DXF type: {dxftype}")
        if foreign_doc is self.doc:
            raise DXFValueError("entity from same DXF document")

        if foreign_doc is not None:
            if copy:
                entity = entity.copy()
            else:
                # Unbind entity from other document without destruction.
                factory.unbind(entity)

        entity.remove_dependencies(self.doc)
        # add to this layout & bind to document
        self.add_entity(entity)

    def unlink_entity(self, entity: DXFGraphic) -> None:
        """Unlink `entity` from layout but does not delete entity from the
        entity database, this removes `entity` just from the layout entity space.
        """
        self.block_record.unlink_entity(entity)

    def delete_entity(self, entity: DXFGraphic) -> None:
        """Delete `entity` from layout entity space and the entity database,
        this destroys the `entity`.
        """
        self.block_record.delete_entity(entity)

    def delete_all_entities(self) -> None:
        """Delete all entities from this layout and from entity database,
        this destroys all entities in this layout.
        """
        # Create list, because delete modifies the base data structure of
        # the iterator:
        for entity in list(self):
            self.delete_entity(entity)

    def move_to_layout(self, entity: DXFGraphic, layout: BaseLayout) -> None:
        """Move entity to another layout.

        Args:
            entity: DXF entity to move
            layout: any layout (modelspace, paperspace, block) from
                **same** drawing

        """
        if entity.doc != layout.doc:
            raise DXFStructureError(
                "Moving between different DXF drawings is not supported."
            )
        try:
            self.unlink_entity(entity)
        except ValueError:
            raise DXFValueError("Layout does not contain entity.")
        else:
            layout.add_entity(entity)

    def destroy(self) -> None:
        """Delete all linked resources. (internal API)"""
        # block_records table is owner of block_record has to delete it
        # the block_record is the owner of the entities and deletes them all
        self.doc.block_records.remove(self.block_record.dxf.name)

    def get_sortents_table(self, create: bool = True) -> SortEntsTable:
        """Get/Create the SORTENTSTABLE object associated to the layout.

        Args:
            create: new table if table do not exist and `create` is ``True``

        Raises:
            DXFValueError: if table not exist and `create` is ``False``

        (internal API)
        """
        xdict = self.get_extension_dict()
        try:
            sortents_table = xdict["ACAD_SORTENTS"]
        except DXFKeyError:
            if create:
                sortents_table = self.doc.objects.new_entity(
                    "SORTENTSTABLE",
                    dxfattribs={
                        "owner": xdict.handle,
                        "block_record_handle": self.layout_key,
                    },
                )
                xdict["ACAD_SORTENTS"] = sortents_table
            else:
                raise DXFValueError(
                    "Extension dictionary entry ACAD_SORTENTS does not exist."
                )
        return sortents_table

    def set_redraw_order(self, handles: Union[dict, Iterable[tuple[str, str]]]) -> None:
        """If the header variable $SORTENTS `Regen` flag (bit-code value 16)
        is set, AutoCAD regenerates entities in ascending handles order.

        To change redraw order associate a different sort-handle to entities,
        this redefines the order in which the entities are regenerated.
        The `handles` argument can be a dict of entity_handle and sort_handle
        as (k, v) pairs, or an iterable of (entity_handle, sort_handle) tuples.

        The sort-handle doesn't have to be unique, some or all entities can
        share the same sort-handle and a sort-handle can be an existing handle.

        The "0" handle can be used, but this sort-handle will be drawn as
        latest (on top of all other entities) and not as first as expected.

        Args:
            handles: iterable or dict of handle associations; an iterable
                of 2-tuples (entity_handle, sort_handle) or a dict (k, v)
                association as (entity_handle, sort_handle)

        """
        sortents = self.get_sortents_table()
        if isinstance(handles, dict):
            handles = handles.items()
        sortents.set_handles(handles)

    def get_redraw_order(self) -> Iterable[tuple[str, str]]:
        """Returns iterable for all existing table entries as (entity_handle,
        sort_handle) pairs, see also :meth:`~BaseLayout.set_redraw_order`.

        """
        if self.block_record.has_extension_dict:
            xdict = self.get_extension_dict()
        else:
            return tuple()

        try:
            sortents_table = xdict["ACAD_SORTENTS"]
        except DXFKeyError:
            return tuple()
        return iter(sortents_table)

    def entities_in_redraw_order(self, reverse=False) -> Iterable[DXFGraphic]:
        """Yields all entities from layout in ascending redraw order or
        descending redraw order if `reverse` is ``True``.
        """
        from ezdxf import reorder

        redraw_order = self.get_redraw_order()
        if reverse:
            return reorder.descending(self.entity_space, redraw_order)  # type: ignore
        return reorder.ascending(self.entity_space, redraw_order)  # type: ignore


class VirtualLayout(_AbstractLayout):
    """Helper class to disassemble complex entities into basic DXF
    entities by rendering into a virtual layout.

    All entities do not have an assigned DXF document and therefore
    are not stored in any entity database and can not be added to another
    layout by :meth:`add_entity`.

    Deleting entities from this layout does not destroy the entity!

    """

    def __init__(self):
        super().__init__(None)
        self.entity_space = EntitySpace()

    @property
    def dxfversion(self) -> str:
        return LATEST_DXF_VERSION

    def add_entity(self, entity: DXFGraphic) -> None:
        self.entity_space.add(entity)

    def new_entity(self, type_: str, dxfattribs: dict) -> DXFGraphic:
        entity = factory.new(type_, dxfattribs=dxfattribs)
        self.entity_space.add(entity)
        return entity  # type: ignore

    def unlink_entity(self, entity: DXFGraphic) -> None:
        self.entity_space.remove(entity)

    def delete_entity(self, entity: DXFGraphic) -> None:
        self.entity_space.remove(entity)

    def delete_all_entities(self) -> None:
        self.entity_space.clear()

    def copy_all_to_layout(self, layout: BaseLayout) -> None:
        """Copy all entities to a real document layout."""
        doc = layout.doc
        entitydb = doc.entitydb
        for entity in self.entity_space:
            try:
                clone = entity.copy()
            except DXFTypeError:
                continue
            clone.doc = doc
            entitydb.add(clone)
            layout.add_entity(clone)  # type: ignore

    def move_all_to_layout(self, layout: BaseLayout) -> None:
        """Move all entities to a real document layout."""
        doc = layout.doc
        entitydb = doc.entitydb
        for entity in self.entity_space:
            entity.doc = doc
            entitydb.add(entity)
            layout.add_entity(entity)  # type: ignore
        self.delete_all_entities()
