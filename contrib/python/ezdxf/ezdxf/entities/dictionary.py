# Copyright (c) 2019-2025, Manfred Moitzi
# License: MIT-License
from __future__ import annotations
from typing import TYPE_CHECKING, Union, Optional
from typing_extensions import Self
import logging
from ezdxf.lldxf import validator
from ezdxf.lldxf.const import (
    SUBCLASS_MARKER,
    DXFKeyError,
    DXFValueError,
    DXFTypeError,
    DXFStructureError,
)
from ezdxf.lldxf.attributes import (
    DXFAttr,
    DXFAttributes,
    DefSubclass,
    RETURN_DEFAULT,
    group_code_mapping,
)
from ezdxf.lldxf.types import is_valid_handle
from ezdxf.audit import AuditError
from ezdxf.entities import factory, DXFGraphic
from .dxfentity import base_class, SubclassProcessor, DXFEntity
from .dxfobj import DXFObject
from .copy import default_copy, CopyNotSupported

if TYPE_CHECKING:
    from ezdxf.entities import DXFNamespace, XRecord
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf.document import Drawing
    from ezdxf.audit import Auditor
    from ezdxf import xref

__all__ = ["Dictionary", "DictionaryWithDefault", "DictionaryVar"]
logger = logging.getLogger("ezdxf")

acdb_dictionary = DefSubclass(
    "AcDbDictionary",
    {
        # DXF Reference: 280 - Hard-owner flag.
        # If set to 1, indicates that elements of the dictionary are to be treated as
        # hard-owned
        # No definition of the default state in the DXF reference!
        #
        # 2025-04-27:
        #   AutoCAD creates the root DICTIONARY and the top level DICTIONARY entries
        #   without group code 280 tags.
        #   Extension dicts are created with the hard_owned flag set to 1.
        #   See exploration/dict-analyzer.py
        #   Conclusion: default state is 0
        "hard_owned": DXFAttr(
            280,
            # 2025-04-27: changed to 0
            default=0,
            # 2024-11-18: changed to False because of issue #1203
            # 2025-04-09: changed back to True because of issue #1279
            optional=True,
            validator=validator.is_integer_bool,
            fixer=RETURN_DEFAULT,
        ),
        # Duplicate record cloning flag (determines how to merge duplicate entries):
        # 0 = not applicable
        # 1 = keep existing
        # 2 = use clone
        # 3 = <xref>$0$<name>
        # 4 = $0$<name>
        # 5 = Unmangle name
        "cloning": DXFAttr(
            281,
            default=1,
            validator=validator.is_in_integer_range(0, 6),
            fixer=RETURN_DEFAULT,
        ),
        # 3: entry name
        # 350: entry handle, some DICTIONARY objects have 360 as handle group code,
        # this is accepted by AutoCAD but not documented by the DXF reference!
        # ezdxf replaces group code 360 by 350.
        # - group code 350 is a soft-owner handle
        # - group code 360 is a hard-owner handle
    },
)
acdb_dictionary_group_codes = group_code_mapping(acdb_dictionary)
KEY_CODE = 3
VALUE_CODE = 350
# Some DICTIONARY use group code 360:
SEARCH_CODES = (VALUE_CODE, 360)


@factory.register_entity
class Dictionary(DXFObject):
    """AutoCAD maintains items such as mline styles and group definitions as
    objects in dictionaries. Other applications are free to create and use
    their own dictionaries as they see fit. The prefix "ACAD_" is reserved
    for use by AutoCAD applications.

    Dictionary entries are (key, DXFEntity) pairs. DXFEntity could be a string,
    because at loading time not all objects are already stored in the EntityDB,
    and have to be acquired later.

    """

    DXFTYPE = "DICTIONARY"
    DXFATTRIBS = DXFAttributes(base_class, acdb_dictionary)

    def __init__(self) -> None:
        super().__init__()
        self._data: dict[str, Union[str, DXFObject]] = dict()
        self._value_code = VALUE_CODE

    def copy_data(self, entity: Self, copy_strategy=default_copy) -> None:
        """Copy hard owned entities but do not store the copies in the entity
        database, this is a second step (factory.bind), this is just real copying.
        """
        assert isinstance(entity, Dictionary)
        entity._value_code = self._value_code
        if self.dxf.hard_owned:
            # Reactors are removed from the cloned DXF objects.
            data: dict[str, DXFEntity] = dict()
            for key, ent in self.items():
                # ignore strings and None - these entities do not exist
                # in the entity database
                if isinstance(ent, DXFEntity):
                    try:  # todo: follow CopyStrategy.ignore_copy_errors_in_linked entities
                        data[key] = ent.copy(copy_strategy=copy_strategy)
                    except CopyNotSupported:
                        if copy_strategy.settings.ignore_copy_errors_in_linked_entities:
                            logger.warning(
                                f"copy process ignored {str(ent)} - this may cause problems in AutoCAD"
                            )
                        else:
                            raise
            entity._data = data  # type: ignore
        else:
            entity._data = dict(self._data)

    def get_handle_mapping(self, clone: Dictionary) -> dict[str, str]:
        """Returns handle mapping for in-object copies."""
        handle_mapping: dict[str, str] = dict()
        if not self.is_hard_owner:
            return handle_mapping

        for key, entity in self.items():
            if not isinstance(entity, DXFEntity):
                continue
            copied_entry = clone.get(key)
            if copied_entry:
                handle_mapping[entity.dxf.handle] = copied_entry.dxf.handle
        return handle_mapping

    def map_resources(self, clone: Self, mapping: xref.ResourceMapper) -> None:
        """Translate resources from self to the copied entity."""
        assert isinstance(clone, Dictionary)
        super().map_resources(clone, mapping)
        if self.is_hard_owner:
            return
        data = dict()
        for key, entity in self.items():
            if not isinstance(entity, DXFEntity):
                continue
            entity_copy = mapping.get_reference_of_copy(entity.dxf.handle)
            if entity_copy:
                data[key] = entity
        clone._data = data  # type: ignore

    def del_source_of_copy(self) -> None:
        super().del_source_of_copy()
        for _, entity in self.items():
            if isinstance(entity, DXFEntity) and entity.is_alive:
                entity.del_source_of_copy()

    def post_bind_hook(self) -> None:
        """Called by binding a new or copied dictionary to the document,
        bind hard owned sub-entities to the same document and add them to the
        objects section.
        """
        if not self.dxf.hard_owned:
            return

        # copied or new dictionary:
        doc = self.doc
        assert doc is not None
        object_section = doc.objects
        owner_handle = self.dxf.handle
        for _, entity in self.items():
            entity.dxf.owner = owner_handle
            factory.bind(entity, doc)
            # For a correct DXF export add entities to the objects section:
            object_section.add_object(entity)

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            tags = processor.fast_load_dxfattribs(
                dxf, acdb_dictionary_group_codes, 1, log=False
            )
            self.load_dict(tags)
        return dxf

    def load_dict(self, tags):
        entry_handle = None
        dict_key = None
        value_code = VALUE_CODE
        for code, value in tags:
            if code in SEARCH_CODES:
                # First store handles, because at this point, NOT all objects
                # are stored in the EntityDB, at first access convert the handle
                # to a DXFEntity object.
                value_code = code
                entry_handle = value
            elif code == KEY_CODE:
                dict_key = value
            if dict_key and entry_handle:
                # Store entity as handle string:
                self._data[dict_key] = entry_handle
                entry_handle = None
                dict_key = None
        # Use same value code as loaded:
        self._value_code = value_code

    def post_load_hook(self, doc: Drawing) -> None:
        super().post_load_hook(doc)
        db = doc.entitydb

        def items():
            for key, handle in self.items():
                entity = db.get(handle)
                if entity is not None and entity.is_alive:
                    yield key, entity

        if len(self):
            for k, v in list(items()):
                self.__setitem__(k, v)

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        super().export_entity(tagwriter)
        tagwriter.write_tag2(SUBCLASS_MARKER, acdb_dictionary.name)
        self.dxf.export_dxf_attribs(tagwriter, ["hard_owned", "cloning"])
        self.export_dict(tagwriter)

    def export_dict(self, tagwriter: AbstractTagWriter):
        # key: dict key string
        # value: DXFEntity or handle as string
        # Ignore invalid handles at export, because removing can create an empty
        # dictionary, which is more a problem for AutoCAD than invalid handles,
        # and removing the whole dictionary is maybe also a problem.
        for key, value in self._data.items():
            tagwriter.write_tag2(KEY_CODE, key)
            # Value can be a handle string or a DXFEntity object:
            if isinstance(value, DXFEntity):
                if value.is_alive:
                    value = value.dxf.handle
                else:
                    logger.debug(
                        f'Key "{key}" points to a destroyed entity '
                        f'in {str(self)}, target replaced by "0" handle.'
                    )
                    value = "0"
            # Use same value code as loaded:
            tagwriter.write_tag2(self._value_code, value)

    @property
    def is_hard_owner(self) -> bool:
        """Returns ``True`` if the dictionary is hard owner of entities.
        Hard owned entities will be destroyed by deleting the dictionary.
        """
        return bool(self.dxf.hard_owned)

    def keys(self):
        """Returns a :class:`KeysView` of all dictionary keys."""
        return self._data.keys()

    def items(self):
        """Returns an :class:`ItemsView` for all dictionary entries as
        (key, entity) pairs. An entity can be a handle string if the entity
        does not exist.
        """
        for key in self.keys():
            yield key, self.get(key)  # maybe handle -> DXFEntity

    def __getitem__(self, key: str) -> DXFEntity:
        """Return self[`key`].

        The returned value can be a handle string if the entity does not exist.

        Raises:
            DXFKeyError: `key` does not exist

        """
        if key in self._data:
            return self._data[key]  # type: ignore
        else:
            raise DXFKeyError(key)

    def __setitem__(self, key: str, entity: DXFObject) -> None:
        """Set self[`key`] = `entity`.

        Only DXF objects stored in the OBJECTS section are allowed as content
        of :class:`Dictionary` objects. DXF entities stored in layouts are not
        allowed.

        Raises:
            DXFTypeError: invalid DXF type

        """
        return self.add(key, entity)

    def __delitem__(self, key: str) -> None:
        """Delete self[`key`].

        Raises:
            DXFKeyError: `key` does not exist

        """
        return self.remove(key)

    def __contains__(self, key: str) -> bool:
        """Returns `key` ``in`` self."""
        return key in self._data

    def __len__(self) -> int:
        """Returns count of dictionary entries."""
        return len(self._data)

    count = __len__

    def get(self, key: str, default: Optional[DXFObject] = None) -> Optional[DXFObject]:
        """Returns the :class:`DXFEntity` for `key`, if `key` exist else
        `default`. An entity can be a handle string if the entity
        does not exist.

        """
        return self._data.get(key, default)  # type: ignore

    def find_key(self, entity: DXFEntity) -> str:
        """Returns the DICTIONARY key string for `entity` or an empty string if not
        found.
        """
        for key, entry in self._data.items():
            if entry is entity:
                return key
        return ""

    def add(self, key: str, entity: DXFObject) -> None:
        """Add entry (key, value).

        If the DICTIONARY is hard owner of its entries, the :meth:`add` does NOT take
        ownership of the entity automatically.

        Raises:
            DXFValueError: invalid entity handle
            DXFTypeError: invalid DXF type

        """
        if isinstance(entity, str):
            if not is_valid_handle(entity):
                raise DXFValueError(f"Invalid entity handle #{entity} for key {key}")
        elif isinstance(entity, DXFGraphic):
            if self.doc is not None and self.doc.is_loading:  # type: ignore
                # AutoCAD add-ons can store graphical entities in DICTIONARIES
                # in the OBJECTS section and AutoCAD does not complain - so just
                # preserve them!
                # Example "ZJMC-288.dxf" in issue #585, add-on: "acdgnlsdraw.crx"?
                logger.warning(f"Invalid entity {str(entity)} in {str(self)}")
            else:
                # Do not allow ezdxf users to add graphical entities to a
                # DICTIONARY object!
                raise DXFTypeError(f"Graphic entities not allowed: {entity.dxftype()}")
        self._data[key] = entity

    def take_ownership(self, key: str, entity: DXFObject):
        """Add entry (key, value) and take ownership."""
        self.add(key, entity)
        entity.dxf.owner = self.dxf.handle

    def remove(self, key: str) -> None:
        """Delete entry `key`. Raises :class:`DXFKeyError`, if `key` does not
        exist. Destroys hard owned DXF entities.

        """
        data = self._data
        if key not in data:
            raise DXFKeyError(key)

        if self.is_hard_owner:
            assert self.doc is not None
            entity = self.__getitem__(key)
            # Presumption: hard owned DXF objects always reside in the OBJECTS
            # section.
            self.doc.objects.delete_entity(entity)  # type: ignore
        del data[key]

    def discard(self, key: str) -> None:
        """Delete entry `key` if exists. Does not raise an exception if `key`
        doesn't exist and does not destroy hard owned DXF entities.

        """
        try:
            del self._data[key]
        except KeyError:
            pass

    def clear(self) -> None:
        """Delete all entries from the dictionary and destroys hard owned
        DXF entities.
        """
        if self.is_hard_owner:
            self._delete_hard_owned_entries()
        self._data.clear()

    def _delete_hard_owned_entries(self) -> None:
        # Presumption: hard owned DXF objects always reside in the OBJECTS section
        objects = self.doc.objects  # type: ignore
        for key, entity in self.items():
            if isinstance(entity, DXFEntity):
                objects.delete_entity(entity)  # type: ignore

    def add_new_dict(self, key: str, hard_owned: bool = False) -> Dictionary:
        """Create a new sub-dictionary of type :class:`Dictionary`.

        Args:
            key: name of the sub-dictionary
            hard_owned: entries of the new dictionary are hard owned

        """
        dxf_dict = self.doc.objects.add_dictionary(  # type: ignore
            owner=self.dxf.handle, hard_owned=hard_owned
        )
        self.add(key, dxf_dict)
        return dxf_dict

    def add_dict_var(self, key: str, value: str) -> DictionaryVar:
        """Add a new :class:`DictionaryVar`.

        Args:
             key: entry name as string
             value: entry value as string

        """
        new_var = self.doc.objects.add_dictionary_var(  # type: ignore
            owner=self.dxf.handle, value=value
        )
        self.add(key, new_var)
        return new_var

    def add_xrecord(self, key: str) -> XRecord:
        """Add a new :class:`XRecord`.

        Args:
             key: entry name as string

        """
        new_xrecord = self.doc.objects.add_xrecord(  # type: ignore
            owner=self.dxf.handle,
        )
        self.add(key, new_xrecord)
        return new_xrecord

    def set_or_add_dict_var(self, key: str, value: str) -> DictionaryVar:
        """Set or add new :class:`DictionaryVar`.

        Args:
             key: entry name as string
             value: entry value as string

        """
        if key not in self:
            dict_var = self.doc.objects.add_dictionary_var(  # type: ignore
                owner=self.dxf.handle, value=value
            )
            self.add(key, dict_var)
        else:
            dict_var = self.get(key)
            dict_var.dxf.value = str(value)  # type: ignore
        return dict_var

    def link_dxf_object(self, name: str, obj: DXFObject) -> None:
        """Add `obj` and set owner of `obj` to this dictionary.

        Graphical DXF entities have to reside in a layout and therefore can not
        be owned by a :class:`Dictionary`.

        Raises:
            DXFTypeError: `obj` has invalid DXF type

        """
        if not isinstance(obj, DXFObject):
            raise DXFTypeError(f"invalid DXF type: {obj.dxftype()}")
        self.add(name, obj)
        obj.dxf.owner = self.dxf.handle

    def get_required_dict(self, key: str, hard_owned=False) -> Dictionary:
        """Get entry `key` or create a new :class:`Dictionary`,
        if `Key` not exist.
        """
        dxf_dict = self.get(key)
        if dxf_dict is None:
            dxf_dict = self.add_new_dict(key, hard_owned=hard_owned)
        elif not isinstance(dxf_dict, Dictionary):
            raise DXFStructureError(
                f"expected a DICTIONARY entity, got {str(dxf_dict)} for key: {key}"
            )
        return dxf_dict

    def audit(self, auditor: Auditor) -> None:
        if not self.is_alive:
            return
        super().audit(auditor)
        self._remove_keys_to_missing_entities(auditor)

    def _remove_keys_to_missing_entities(self, auditor: Auditor):
        trash: list[str] = []
        append = trash.append
        db = auditor.entitydb
        for key, entry in self._data.items():
            if isinstance(entry, str):
                if entry not in db:
                    append(key)
            elif entry.is_alive:
                if entry.dxf.handle not in db:
                    append(key)
                    continue
            else:  # entity is destroyed, remove key
                append(key)
        for key in trash:
            del self._data[key]
            auditor.fixed_error(
                code=AuditError.INVALID_DICTIONARY_ENTRY,
                message=f'Removed entry "{key}" with invalid handle in {str(self)}',
                dxf_entity=self,
                data=key,
            )

    def destroy(self) -> None:
        if not self.is_alive:
            return

        if self.is_hard_owner:
            self._delete_hard_owned_entries()
        super().destroy()


acdb_dict_with_default = DefSubclass(
    "AcDbDictionaryWithDefault",
    {
        "default": DXFAttr(340),
    },
)
acdb_dict_with_default_group_codes = group_code_mapping(acdb_dict_with_default)


@factory.register_entity
class DictionaryWithDefault(Dictionary):
    DXFTYPE = "ACDBDICTIONARYWDFLT"
    DXFATTRIBS = DXFAttributes(base_class, acdb_dictionary, acdb_dict_with_default)

    def __init__(self) -> None:
        super().__init__()
        self._default: Optional[DXFObject] = None

    def copy_data(self, entity: Self, copy_strategy=default_copy) -> None:
        super().copy_data(entity, copy_strategy=copy_strategy)
        assert isinstance(entity, DictionaryWithDefault)
        entity._default = self._default

    def post_load_hook(self, doc: Drawing) -> None:
        # Set _default to None if default object not exist - audit() replaces
        # a not existing default object by a placeholder object.
        # AutoCAD ignores not existing default objects!
        self._default = doc.entitydb.get(self.dxf.default)  # type: ignore
        super().post_load_hook(doc)

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            processor.fast_load_dxfattribs(dxf, acdb_dict_with_default_group_codes, 2)
        return dxf

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        super().export_entity(tagwriter)
        tagwriter.write_tag2(SUBCLASS_MARKER, acdb_dict_with_default.name)
        self.dxf.export_dxf_attribs(tagwriter, "default")

    def __getitem__(self, key: str):
        return self.get(key)

    def get(self, key: str, default: Optional[DXFObject] = None) -> Optional[DXFObject]:
        # `default` argument is ignored, exist only for API compatibility,
        """Returns :class:`DXFEntity` for `key` or the predefined dictionary
        wide :attr:`dxf.default` entity if `key` does not exist or ``None``
        if default value also not exist.

        """
        return super().get(key, default=self._default)

    def set_default(self, default: DXFObject) -> None:
        """Set dictionary wide default entry.

        Args:
            default: default entry as :class:`DXFEntity`

        """
        self._default = default
        self.dxf.default = self._default.dxf.handle

    def audit(self, auditor: Auditor) -> None:
        def create_missing_default_object():
            placeholder = self.doc.objects.add_placeholder(owner=self.dxf.handle)
            self.set_default(placeholder)
            auditor.fixed_error(
                code=AuditError.CREATED_MISSING_OBJECT,
                message=f"Created missing default object in {str(self)}.",
            )

        if self._default is None or not self._default.is_alive:
            if auditor.entitydb.locked:
                auditor.add_post_audit_job(create_missing_default_object)
            else:
                create_missing_default_object()
        super().audit(auditor)


acdb_dict_var = DefSubclass(
    "DictionaryVariables",
    {
        "schema": DXFAttr(280, default=0),
        # Object schema number (currently set to 0)
        "value": DXFAttr(1, default=""),
    },
)
acdb_dict_var_group_codes = group_code_mapping(acdb_dict_var)


@factory.register_entity
class DictionaryVar(DXFObject):
    """
    DICTIONARYVAR objects are used by AutoCAD as a means to store named values
    in the database for setvar / getvar purposes without the need to add entries
    to the DXF HEADER section. System variables that are stored as
    DICTIONARYVAR objects are the following:

        - DEFAULTVIEWCATEGORY
        - DIMADEC
        - DIMASSOC
        - DIMDSEP
        - DRAWORDERCTL
        - FIELDEVAL
        - HALOGAP
        - HIDETEXT
        - INDEXCTL
        - INDEXCTL
        - INTERSECTIONCOLOR
        - INTERSECTIONDISPLAY
        - MSOLESCALE
        - OBSCOLOR
        - OBSLTYPE
        - OLEFRAME
        - PROJECTNAME
        - SORTENTS
        - UPDATETHUMBNAIL
        - XCLIPFRAME
        - XCLIPFRAME

    """

    DXFTYPE = "DICTIONARYVAR"
    DXFATTRIBS = DXFAttributes(base_class, acdb_dict_var)

    @property
    def value(self) -> str:
        """Get/set the value of the :class:`DictionaryVar` as string."""
        return self.dxf.get("value", "")

    @value.setter
    def value(self, data: str) -> None:
        self.dxf.set("value", str(data))

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            processor.fast_load_dxfattribs(dxf, acdb_dict_var_group_codes, 1)
        return dxf

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        super().export_entity(tagwriter)
        tagwriter.write_tag2(SUBCLASS_MARKER, acdb_dict_var.name)
        self.dxf.export_dxf_attribs(tagwriter, ["schema", "value"])
