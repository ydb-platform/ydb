# Copyright (c) 2019-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from ezdxf.document import Drawing
    from ezdxf.entities import DXFEntity
    from ezdxf.lldxf.extendedtags import ExtendedTags


__all__ = [
    "register_entity",
    "ENTITY_CLASSES",
    "replace_entity",
    "new",
    "cls",
    "is_bound",
    "create_db_entry",
    "load",
    "bind",
]
# Stores all registered classes:
ENTITY_CLASSES = {}
# use @set_default_class to register the default entity class:
DEFAULT_CLASS = None


def set_default_class(cls):
    global DEFAULT_CLASS
    DEFAULT_CLASS = cls
    return cls


def replace_entity(cls):
    name = cls.DXFTYPE
    ENTITY_CLASSES[name] = cls
    return cls


def register_entity(cls):
    name = cls.DXFTYPE
    if name in ENTITY_CLASSES:
        raise TypeError(f"Double registration for DXF type {name}.")
    ENTITY_CLASSES[name] = cls
    return cls


def new(
    dxftype: str, dxfattribs=None, doc: Optional[Drawing] = None
) -> DXFEntity:
    """Create a new entity, does not require an instantiated DXF document."""
    entity = cls(dxftype).new(
        handle=None,
        owner=None,
        dxfattribs=dxfattribs,
        doc=doc,
    )
    return entity.cast() if hasattr(entity, "cast") else entity


def create_db_entry(dxftype, dxfattribs, doc: Drawing) -> DXFEntity:
    entity = new(dxftype=dxftype, dxfattribs=dxfattribs)
    bind(entity, doc)
    return entity


def load(tags: ExtendedTags, doc: Optional[Drawing] = None) -> DXFEntity:
    entity = cls(tags.dxftype()).load(tags, doc)
    return entity.cast() if hasattr(entity, "cast") else entity


def cls(dxftype: str) -> DXFEntity:
    """Returns registered class for `dxftype`."""
    # DEFAULT_CLASS is not None at runtime!
    return ENTITY_CLASSES.get(dxftype, DEFAULT_CLASS)  # type: ignore


def bind(entity: DXFEntity, doc: Drawing) -> None:
    """Bind `entity` to the DXF document `doc`.

    The bind process stores the DXF `entity` in the entity database of the DXF
    document.

    """
    assert entity.is_alive, "Can not bind destroyed entity."
    assert doc.entitydb is not None, "Missing entity database."
    entity.doc = doc
    doc.entitydb.add(entity)

    # Do not call the post_bind_hook() while loading from external sources,
    # not all entities and resources are loaded at this point of time!
    if not doc.is_loading:  # type: ignore
        # bind extension dictionary
        if entity.extension_dict is not None:
            xdict = entity.extension_dict
            if xdict.has_valid_dictionary:
                xdict.update_owner(entity.dxf.handle)
                dictionary = xdict.dictionary
                if not is_bound(dictionary, doc):
                    bind(dictionary, doc)
                    doc.objects.add_object(dictionary)
        entity.post_bind_hook()


def unbind(entity: DXFEntity):
    """Unbind `entity` from document and layout, but does not destroy the
    entity.

    Turns `entity` into a virtual entity: no handle, no owner, no document.
    """
    if entity.is_alive and not entity.is_virtual:
        doc = entity.doc
        if entity.dxf.owner is not None:
            try:
                layout = doc.layouts.get_layout_for_entity(entity)  # type: ignore
            except KeyError:
                pass
            else:
                layout.unlink_entity(entity)  # type: ignore

        process_sub_entities = getattr(entity, "process_sub_entities", None)
        if process_sub_entities:
            process_sub_entities(lambda e: unbind(e))

        doc.entitydb.discard(entity)  # type: ignore
        entity.doc = None


def is_bound(entity: DXFEntity, doc: Drawing) -> bool:
    """Returns ``True`` if `entity`is bound to DXF document `doc`."""
    if not entity.is_alive:
        return False
    if entity.is_virtual or entity.doc is not doc:
        return False
    assert doc.entitydb, "Missing entity database."
    return entity.dxf.handle in doc.entitydb
