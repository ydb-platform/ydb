# Copyright (c) 2019-2024, Manfred Moitzi
# License: MIT-License
from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Iterable,
    Iterator,
    cast,
    Union,
    Optional,
)
from contextlib import contextmanager
import logging
from ezdxf.lldxf import validator, const
from ezdxf.lldxf.attributes import (
    DXFAttr,
    DXFAttributes,
    DefSubclass,
    RETURN_DEFAULT,
    group_code_mapping,
)
from ezdxf.audit import AuditError
from .dxfentity import base_class, SubclassProcessor, DXFEntity
from .dxfobj import DXFObject
from .factory import register_entity
from .objectcollection import ObjectCollection
from .copy import default_copy, CopyNotSupported

if TYPE_CHECKING:
    from ezdxf.audit import Auditor
    from ezdxf.document import Drawing
    from ezdxf.entities import DXFNamespace, Dictionary
    from ezdxf.entitydb import EntityDB
    from ezdxf.layouts import Layouts
    from ezdxf.lldxf.tagwriter import AbstractTagWriter

__all__ = ["DXFGroup", "GroupCollection"]
logger = logging.getLogger("ezdxf")

acdb_group = DefSubclass(
    "AcDbGroup",
    {
        # Group description
        "description": DXFAttr(300, default=""),
        # 1 = Unnamed
        # 0 = Named
        "unnamed": DXFAttr(
            70,
            default=1,
            validator=validator.is_integer_bool,
            fixer=RETURN_DEFAULT,
        ),
        # 1 = Selectable
        # 0 = Not selectable
        "selectable": DXFAttr(
            71,
            default=1,
            validator=validator.is_integer_bool,
            fixer=RETURN_DEFAULT,
        ),
        # 340: Hard-pointer handle to entity in group (one entry per object)
    },
)
acdb_group_group_codes = group_code_mapping(acdb_group)
GROUP_ITEM_CODE = 340


@register_entity
class DXFGroup(DXFObject):
    """Groups are not allowed in block definitions, and each entity can only
    reside in one group, so cloning of groups creates also new entities.

    """

    DXFTYPE = "GROUP"
    DXFATTRIBS = DXFAttributes(base_class, acdb_group)

    def __init__(self) -> None:
        super().__init__()
        self._handles: set[str] = set()  # only needed at the loading stage
        self._data: list[DXFEntity] = []

    def copy(self, copy_strategy=default_copy):
        raise CopyNotSupported("Copying of GROUP not supported.")

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            tags = processor.fast_load_dxfattribs(
                dxf, acdb_group_group_codes, 1, log=False
            )
            self.load_group(tags)
        return dxf

    def load_group(self, tags):
        for code, value in tags:
            if code == GROUP_ITEM_CODE:
                # First store handles, because at this point, objects
                # are not stored in the EntityDB:
                self._handles.add(value)

    def preprocess_export(self, tagwriter: AbstractTagWriter) -> bool:
        # remove invalid entities
        assert self.doc is not None
        self.purge(self.doc)
        # export GROUP only if all entities reside on the same layout
        if not all_entities_on_same_layout(self._data):
            raise const.DXFStructureError(
                "All entities have to be in the same layout and are not allowed"
                " to be in a block layout."
            )
        return True

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        super().export_entity(tagwriter)
        tagwriter.write_tag2(const.SUBCLASS_MARKER, acdb_group.name)
        self.dxf.export_dxf_attribs(tagwriter, ["description", "unnamed", "selectable"])
        self.export_group(tagwriter)

    def export_group(self, tagwriter: AbstractTagWriter):
        for entity in self._data:
            tagwriter.write_tag2(GROUP_ITEM_CODE, entity.dxf.handle)

    def __iter__(self) -> Iterator[DXFEntity]:
        """Iterate over all DXF entities in :class:`DXFGroup` as instances of
        :class:`DXFGraphic` or inherited (LINE, CIRCLE, ...).

        """
        return (e for e in self._data if e.is_alive)

    def __len__(self) -> int:
        """Returns the count of DXF entities in :class:`DXFGroup`."""
        return len(self._data)

    def __getitem__(self, item):
        """Returns entities by standard Python indexing and slicing."""
        return self._data[item]

    def __contains__(self, item: Union[str, DXFEntity]) -> bool:
        """Returns ``True`` if item is in :class:`DXFGroup`. `item` has to be
        a handle string or an object of type :class:`DXFEntity` or inherited.

        """
        handle = item if isinstance(item, str) else item.dxf.handle
        return handle in set(self.handles())

    def handles(self) -> Iterable[str]:
        """Iterable of handles of all DXF entities in :class:`DXFGroup`."""
        return (entity.dxf.handle for entity in self)

    def post_load_hook(self, doc: "Drawing"):
        super().post_load_hook(doc)
        db_get = doc.entitydb.get

        def set_group_entities():  # post init command
            name = str(self)
            entities = filter_invalid_entities(self._data, self.doc, name)
            if not all_entities_on_same_layout(entities):
                self.clear()
                logger.debug(f"Cleared {name}, had entities from different layouts.")
            else:
                self._data = entities

        def entities():
            for handle in self._handles:
                entity = db_get(handle)
                if entity and entity.is_alive:
                    yield entity

        # Filtering invalid DXF entities is not possible at this stage, just
        # store entities as they are:
        self._data = list(entities())
        del self._handles  # all referenced entities are stored in data
        return set_group_entities

    @contextmanager  # type: ignore
    def edit_data(self) -> list[DXFEntity]:  # type: ignore
        """Context manager which yields all the group entities as
        standard Python list::

            with group.edit_data() as data:
               # add new entities to a group
               data.append(modelspace.add_line((0, 0), (3, 0)))
               # remove last entity from a group
               data.pop()

        """
        data = list(self)
        yield data
        self.set_data(data)

    def _validate_entities(self, entities: Iterable[DXFEntity]) -> list[DXFEntity]:
        assert self.doc is not None
        entities = list(entities)
        valid_entities = filter_invalid_entities(entities, self.doc, str(self))
        if len(valid_entities) != len(entities):
            raise const.DXFStructureError("invalid entities found")
        if not all_entities_on_same_layout(valid_entities):
            raise const.DXFStructureError(
                "All entities have to be in the same layout and are not allowed"
                " to be in a block layout."
            )
        return valid_entities

    def set_data(self, entities: Iterable[DXFEntity]) -> None:
        """Set `entities` as new group content, entities should be an iterable of
        :class:`DXFGraphic` (LINE, CIRCLE, ...).

        Raises:
            DXFValueError: not all entities are located on the same layout (modelspace
                or any paperspace layout but not block)

        """
        valid_entities = self._validate_entities(entities)
        self.clear()
        self._add_group_reactor(valid_entities)
        self._data = valid_entities

    def extend(self, entities: Iterable[DXFEntity]) -> None:
        """Add `entities` to :class:`DXFGroup`, entities should be an iterable of
        :class:`DXFGraphic` (LINE, CIRCLE, ...).

        Raises:
            DXFValueError: not all entities are located on the same layout (modelspace
                or any paperspace layout but not block)

        """
        valid_entities = self._validate_entities(entities)
        handles = set(self.handles())
        valid_entities = [e for e in valid_entities if e.dxf.handle not in handles]
        self._add_group_reactor(valid_entities)
        self._data.extend(valid_entities)

    def clear(self) -> None:
        """Remove all entities from :class:`DXFGroup`, does not delete any
        drawing entities referenced by this group.

        """
        # TODO: remove handle of GROUP entity from reactors of entities #1085
        self._remove_group_reactor(self._data)
        self._data = []

    def _add_group_reactor(self, entities: list[DXFEntity]) -> None:
        group_handle = self.dxf.handle
        for entity in entities:
            entity.append_reactor_handle(group_handle)

    def _remove_group_reactor(self, entities: list[DXFEntity]) -> None:
        group_handle = self.dxf.handle
        for entity in entities:
            entity.discard_reactor_handle(group_handle)

    def audit(self, auditor: Auditor) -> None:
        """Remove invalid entities from :class:`DXFGroup`.

        Invalid entities are:

        - deleted entities
        - all entities which do not reside in model- or paper space
        - all entities if they do not reside in the same layout

        """
        entity_count = len(self)
        assert auditor.doc is not None
        # Remove destroyed or invalid entities:
        self.purge(auditor.doc)
        removed_entity_count = entity_count - len(self)
        if removed_entity_count > 0:
            auditor.fixed_error(
                code=AuditError.INVALID_GROUP_ENTITIES,
                message=f"Removed {removed_entity_count} invalid entities from {str(self)}",
            )
        if not all_entities_on_same_layout(self._data):
            auditor.fixed_error(
                code=AuditError.GROUP_ENTITIES_IN_DIFFERENT_LAYOUTS,
                message=f"Cleared {str(self)}, not all entities are located in "
                f"the same layout.",
            )
            self.clear()

        group_handle = self.dxf.handle
        if not group_handle:
            return
        for entity in self._data:
            if entity.reactors is None or group_handle not in entity.reactors:
                auditor.fixed_error(
                    code=AuditError.MISSING_PERSISTENT_REACTOR,
                    message=f"Entity {entity} in group #{group_handle} does not have "
                    f"group as persistent reactor",
                )
                entity.append_reactor_handle(group_handle)

    def purge(self, doc: Drawing) -> None:
        """Remove invalid group entities."""
        self._data = filter_invalid_entities(
            entities=self._data, doc=doc, group_name=str(self)
        )


def filter_invalid_entities(
    entities: Iterable[DXFEntity],
    doc: Drawing,
    group_name: str = "",
) -> list[DXFEntity]:
    assert doc is not None
    db = doc.entitydb
    valid_owner_handles = valid_layout_handles(doc.layouts)
    valid_entities: list[DXFEntity] = []
    for entity in entities:
        if entity.is_alive and _has_valid_owner(
            entity.dxf.owner, db, valid_owner_handles
        ):
            valid_entities.append(entity)
        elif group_name:
            if entity.is_alive:
                logger.debug(f"{str(entity)} in {group_name} has an invalid owner.")
            else:
                logger.debug(f"Removed deleted entity in {group_name}")
    return valid_entities


def _has_valid_owner(owner: str, db: EntityDB, valid_owner_handles: set[str]) -> bool:
    # no owner -> no layout association
    if owner is None:
        return False
    # The check for owner.dxf.layout != "0" is not sufficient #521
    if valid_owner_handles and owner not in valid_owner_handles:
        return False
    layout = db.get(owner)
    # owner layout does not exist or is destroyed -> no layout association
    if layout is None or not layout.is_alive:
        return False
    # If "valid_owner_handles" is not empty, entities located on BLOCK
    # layouts are already removed.
    # DXF attribute block_record.layout is "0" if entity is located in a
    # block definition, which is invalid:
    return layout.dxf.layout != "0"


def all_entities_on_same_layout(entities: Iterable[DXFEntity]):
    """Check if all entities are on the same layout (model space or any paper
    layout but not block).

    """
    owners = set(entity.dxf.owner for entity in entities)
    # 0 for no entities; 1 for all entities on the same layout
    return len(owners) < 2


def valid_layout_handles(layouts: Layouts) -> set[str]:
    """Returns valid layout keys for group entities."""
    return set(layout.layout_key for layout in layouts if layout.is_any_layout)


class GroupCollection(ObjectCollection[DXFGroup]):
    def __init__(self, doc: Drawing):
        super().__init__(doc, dict_name="ACAD_GROUP", object_type="GROUP")
        self._next_unnamed_number = 0

    def groups(self) -> Iterator[DXFGroup]:
        """Iterable of all existing groups."""
        for name, group in self:
            yield group

    def next_name(self) -> str:
        name = self._next_name()
        while name in self:
            name = self._next_name()
        return name

    def _next_name(self) -> str:
        self._next_unnamed_number += 1
        return f"*A{self._next_unnamed_number}"

    def new(
        self,
        name: Optional[str] = None,
        description: str = "",
        selectable: bool = True,
    ) -> DXFGroup:
        r"""Creates a new group. If `name` is ``None`` an unnamed group is
        created, which has an automatically generated name like "\*Annnn".
        Group names are case-insensitive.

        Args:
            name: group name as string
            description: group description as string
            selectable: group is selectable if ``True``

        """
        if name is not None and name in self:
            raise const.DXFValueError(f"GROUP '{name}' already exists.")

        if name is None:
            name = self.next_name()
            unnamed = 1
        else:
            unnamed = 0
        # The group name isn't stored in the group entity itself.
        dxfattribs = {
            "description": description,
            "unnamed": unnamed,
            "selectable": int(bool(selectable)),
        }
        return cast(DXFGroup, self._new(name, dxfattribs))

    def delete(self, group: Union[DXFGroup, str]) -> None:
        """Delete `group`, `group` can be an object of type :class:`DXFGroup`
        or a group name as string.

        """
        entitydb = self.doc.entitydb
        assert entitydb is not None
        # Delete group by name:
        if isinstance(group, str):
            name = group
        elif group.dxftype() == "GROUP":
            name = get_group_name(group, entitydb)
        else:
            raise TypeError(group.dxftype())

        if name in self:
            super().delete(name)
        else:
            raise const.DXFValueError("GROUP not in group table registered.")

    def audit(self, auditor: Auditor) -> None:
        """Removes empty groups and invalid handles from all groups."""
        trash = []
        for name, group in self:
            group = cast(DXFGroup, group)
            group.audit(auditor)
            if not len(group):  # remove empty group
                # do not delete groups while iterating over groups!
                trash.append(name)

        # now delete empty groups
        for name in trash:
            auditor.fixed_error(
                code=AuditError.REMOVE_EMPTY_GROUP,
                message=f'Removed empty group "{name}".',
            )
            self.delete(name)


def get_group_name(group: DXFGroup, db: EntityDB) -> str:
    """Get name of `group`."""
    group_table = cast("Dictionary", db[group.dxf.owner])
    for name, entity in group_table.items():
        if entity is group:
            return name
    return ""
