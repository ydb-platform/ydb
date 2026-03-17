# Copyright (c) 2019-2024 Manfred Moitzi
# License: MIT License
""" :class:`DXFEntity` is the super class of all DXF entities.

The current entity system uses the features of the latest supported DXF version.

The stored DXF version of the document is used to warn users if they use
unsupported DXF features of the current DXF version.

The DXF version of the document can be changed at runtime or overridden by
exporting, but unsupported DXF features are just ignored by exporting.

Ezdxf does no conversion between different DXF versions, this package is
still not a CAD application.

"""
from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Iterator,
    Optional,
    Type,
    TypeVar,
    Callable,
)
from typing_extensions import Self

import logging
import uuid
from ezdxf import options
from ezdxf.lldxf import const
from ezdxf.lldxf.tags import Tags
from ezdxf.lldxf.types import DXFTag
from ezdxf.lldxf.extendedtags import ExtendedTags
from ezdxf.lldxf.attributes import DXFAttr, DXFAttributes, DefSubclass
from ezdxf.tools import set_flag_state
from . import factory
from .appdata import AppData, Reactors
from .dxfns import DXFNamespace, SubclassProcessor
from .xdata import XData
from .xdict import ExtensionDict
from .copy import default_copy, CopyNotSupported

if TYPE_CHECKING:
    from ezdxf.audit import Auditor
    from ezdxf.document import Drawing
    from ezdxf.entities import DXFGraphic, Insert
    from ezdxf.lldxf.attributes import DXFAttr
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf.math import Matrix44
    from ezdxf import xref


__all__ = ["DXFEntity", "DXFTagStorage", "base_class", "SubclassProcessor"]
logger = logging.getLogger("ezdxf")

# Dynamic attributes created only at request:
# Source entity of a copy or None if not a copy:
DYN_SOURCE_OF_COPY_ATTRIBUTE = "_source_of_copy"
# UUID created on demand by uuid.uuid4()
DYN_UUID_ATTRIBUTE = "_uuid"
# Source block reference, which created the virtual entity, bound entities can
# not have such an attribute:
DYN_SOURCE_BLOCK_REFERENCE_ATTRIBUTE = "_source_block_reference"

base_class: DefSubclass = DefSubclass(
    None,
    {
        "handle": DXFAttr(5),
        # owner: Soft-pointer ID/handle to owner BLOCK_RECORD object
        # This tag is not supported by DXF R12, but is used intern to unify entity
        # handling between DXF R12 and DXF R2000+
        # Do not write this tag into DXF R12 files!
        "owner": DXFAttr(330),
        # Application defined data can only appear here:
        # 102, {APPID ... multiple entries possible DXF R12?
        # 102, {ACAD_REACTORS ... one entry DXF R2000+, optional
        # 102, {ACAD_XDICTIONARY  ... one entry DXF R2000+, optional
    },
)

T = TypeVar("T", bound="DXFEntity")


class DXFEntity:
    """Common super class for all DXF entities."""

    DXFTYPE = "DXFENTITY"  # storing as class var needs less memory
    DXFATTRIBS = DXFAttributes(base_class)  # DXF attribute definitions

    # Default DXF attributes are set at instantiating a new object, the
    # difference to attribute default values is, that this attributes are
    # really set, this means there is an real object in the dxf namespace
    # defined, where default attribute values get returned on access without
    # an existing object in the dxf namespace.
    DEFAULT_ATTRIBS: dict[str, Any] = {}
    MIN_DXF_VERSION_FOR_EXPORT = const.DXF12

    def __init__(self) -> None:
        """Default constructor. (internal API)"""
        # Public attributes for package users
        self.doc: Optional[Drawing] = None
        self.dxf: DXFNamespace = DXFNamespace(entity=self)

        # None public attributes for package users
        # create extended data only if needed:
        self.appdata: Optional[AppData] = None
        self.reactors: Optional[Reactors] = None
        self.extension_dict: Optional[ExtensionDict] = None
        self.xdata: Optional[XData] = None
        self.proxy_graphic: Optional[bytes] = None

        # For documentation:
        # Dynamic attributes, created only at request:
        # DYN_SOURCE_OF_COPY_ATTRIBUTE
        # DYN_UUID_ATTRIBUTE
        # DYN_SOURCE_BLOCK_REFERENCE_ATTRIBUTE

    @property
    def uuid(self) -> uuid.UUID:
        """Returns a UUID, which allows to distinguish even
        virtual entities without a handle.

        Dynamic attribute: this UUID will be created at the first request.

        """
        uuid_ = getattr(self, DYN_UUID_ATTRIBUTE, None)
        if uuid_ is None:
            uuid_ = uuid.uuid4()
            setattr(self, DYN_UUID_ATTRIBUTE, uuid_)
        return uuid_

    @classmethod
    def new(
        cls: Type[T],
        handle: Optional[str] = None,
        owner: Optional[str] = None,
        dxfattribs=None,
        doc: Optional[Drawing] = None,
    ) -> T:
        """Constructor for building new entities from scratch by ezdxf.

        NEW process:

        This is a trusted environment where everything is under control of
        ezdxf respectively the package-user, it is okay to raise exception
        to show implementation errors in ezdxf or usage errors of the
        package-user.

        The :attr:`Drawing.is_loading` flag can be checked to distinguish the
        NEW and the LOAD process.

        Args:
            handle: unique DXF entity handle or None
            owner: owner handle if entity has an owner else None or '0'
            dxfattribs: DXF attributes
            doc: DXF document

        (internal API)
        """
        entity = cls()
        entity.doc = doc
        entity.dxf.handle = handle
        entity.dxf.owner = owner
        attribs = dict(cls.DEFAULT_ATTRIBS)
        attribs.update(dxfattribs or {})
        entity.update_dxf_attribs(attribs)
        # Only this method triggers the post_new_hook()
        entity.post_new_hook()
        return entity

    def post_new_hook(self):
        """Post-processing and integrity validation after entity creation.

        Called only if created by ezdxf (see :meth:`DXFEntity.new`),
        not if loaded from an external source.

        (internal API)
        """
        pass

    def post_bind_hook(self):
        """Post-processing and integrity validation after binding entity to a
        DXF Document. This method is triggered by the :func:`factory.bind`
        function only when the entity was created by ezdxf.

        If the entity was loaded in the 1st loading stage, the
        :func:`factory.load` functions also calls the :func:`factory.bind`
        to bind entities to the loaded document, but not all entities are
        loaded at this time. To avoid problems this method will not be called
        when loading content from DXF file, but :meth:`post_load_hook` will be
        triggered for loaded entities at a later and safer point in time.

        (internal API)
        """
        pass

    @classmethod
    def load(cls: Type[T], tags: ExtendedTags, doc: Optional[Drawing] = None) -> T:
        """Constructor to generate entities loaded from an external source.

        LOAD process:

        This is an untrusted environment where valid structure are not
        guaranteed and errors should be fixed, because the package-user is not
        responsible for the problems and also can't fix them, raising
        exceptions should only be done for unrecoverable issues.
        Log fixes for debugging!

            Be more like BricsCAD and not as mean as AutoCAD!

        The :attr:`Drawing.is_loading` flag can be checked to distinguish the
        NEW and the LOAD process.

        Args:
            tags: DXF tags as :class:`ExtendedTags`
            doc: DXF Document

        (internal API)
        """
        # This method does not trigger the post_new_hook()
        entity = cls()
        entity.doc = doc
        dxfversion = doc.dxfversion if doc else None
        entity.load_tags(tags, dxfversion=dxfversion)
        return entity

    def load_tags(self, tags: ExtendedTags, dxfversion: Optional[str] = None) -> None:
        """Generic tag loading interface, called if DXF document is loaded
        from external sources.

        1. Loading stage which set the basic DXF attributes, additional
           resources (DXF objects) are not loaded yet. References to these
           resources have to be stored as handles and can be resolved in the
        2. Loading stage: :meth:`post_load_hook`.

        (internal API)
        """
        if tags:
            if len(tags.appdata):
                self.setup_app_data(tags.appdata)
            if len(tags.xdata):
                try:  # the common case - fast path
                    self.xdata = XData(tags.xdata)
                except const.DXFValueError:  # contains invalid group codes
                    self.xdata = XData.safe_init(tags.xdata)
                    logger.debug(f"removed invalid XDATA from {tags.entity_name()}")

            processor = SubclassProcessor(tags, dxfversion=dxfversion)
            self.dxf = self.load_dxf_attribs(processor)

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        """Load DXF attributes into DXF namespace."""
        return DXFNamespace(processor, self)

    def post_load_hook(self, doc: Drawing) -> Optional[Callable]:
        """The 2nd loading stage when loading DXF documents from an external
        source, for the 1st loading stage see :meth:`load_tags`.

        This stage is meant to convert resource handles into :class:`DXFEntity`
        objects. This is an untrusted environment where valid structure are not
        guaranteed, raise exceptions only for unrecoverable structure errors
        and fix everything else. Log fixes for debugging!

        Some fixes can not be applied at this stage, because some structures
        like the OBJECTS section are not initialized, in this case return a
        callable, which will be executed after the DXF document is fully
        initialized, for an example see :class:`Image`.

        Triggered in method: :meth:`Drawing._2nd_loading_stage`

        Examples for two stage loading:
        Image, Underlay, DXFGroup, Dictionary, Dimstyle, MText

        """
        if self.extension_dict is not None:
            self.extension_dict.load_resources(doc)
        return None

    @classmethod
    def from_text(cls: Type[T], text: str, doc: Optional[Drawing] = None) -> T:
        """Load constructor from text for testing. (internal API)"""
        return cls.load(ExtendedTags.from_text(text), doc)

    @classmethod
    def shallow_copy(cls: Type[T], other: DXFEntity) -> T:
        """Copy constructor for type casting e.g. Polyface and Polymesh.
        (internal API)
        """
        entity = cls()
        entity.doc = other.doc
        entity.dxf = other.dxf
        entity.extension_dict = other.extension_dict
        entity.reactors = other.reactors
        entity.appdata = other.appdata
        entity.xdata = other.xdata
        entity.proxy_graphic = other.proxy_graphic
        entity.dxf.rewire(entity)
        # Do not set copy state, this is not a real copy!
        return entity

    def copy(self, copy_strategy=default_copy) -> Self:
        """Internal entity copy for usage in the same document or as virtual entity.

        Returns a copy of `self` but without handle, owner and reactors.
        This copy is NOT stored in the entity database and does NOT reside
        in any layout, block, table or objects section!
        The extension dictionary will be copied for entities bound to a valid
        DXF document. The reactors are not copied.

        (internal API)
        """
        return copy_strategy.copy(self)

    def copy_data(self, entity: Self, copy_strategy=default_copy) -> None:
        """Copy entity data like vertices or attribs to the copy of the entity.

        This is the second stage of the copy process, see copy() method.

        (internal API)
        """
        pass

    def set_source_of_copy(self, source: Optional[DXFEntity]):
        """Set immediate source entity of a copy.

        Also used from outside to set the source of sub-entities
        of disassembled entities (POLYLINE, LWPOLYLINE, ...).

        (Internal API)
        """
        if isinstance(source, DXFEntity) and not source.is_alive:
            source = None
        # dynamic attribute: exist only in copies:
        setattr(self, DYN_SOURCE_OF_COPY_ATTRIBUTE, source)

    def del_source_of_copy(self) -> None:
        """Delete source of copy reference.

        (Internal API)
        """
        if hasattr(self, DYN_SOURCE_OF_COPY_ATTRIBUTE):
            delattr(self, DYN_SOURCE_OF_COPY_ATTRIBUTE)

    @property
    def is_copy(self) -> bool:
        """Is ``True`` if the entity is a copy."""
        return self.source_of_copy is not None

    @property
    def source_of_copy(self) -> Optional[DXFEntity]:
        """The immediate source entity if this entity is a copy else
        ``None``. Never references a destroyed entity.
        """
        # attribute only exist in copies:
        source = getattr(self, DYN_SOURCE_OF_COPY_ATTRIBUTE, None)
        if isinstance(source, DXFEntity) and not source.is_alive:
            return None
        return source

    @property
    def origin_of_copy(self) -> Optional[DXFEntity]:
        """The origin source entity if this entity is a copy else
        ``None``. References the first non-virtual source entity and never
        references a destroyed entity.
        """
        source = self.source_of_copy
        # follow source entities references until the first non-virtual entity:
        while isinstance(source, DXFEntity) and source.is_alive and source.is_virtual:
            source = source.source_of_copy
        return source

    def update_dxf_attribs(self, dxfattribs: dict) -> None:
        """Set DXF attributes by a ``dict`` like :code:`{'layer': 'test',
        'color': 4}`.
        """
        setter = self.dxf.set
        for key, value in dxfattribs.items():
            setter(key, value)

    def setup_app_data(self, appdata: list[Tags]) -> None:
        """Setup data structures from APP data. (internal API)"""
        for data in appdata:
            code, appid = data[0]
            if appid == const.ACAD_REACTORS:
                self.reactors = Reactors.from_tags(data)
            elif appid == const.ACAD_XDICTIONARY:
                self.extension_dict = ExtensionDict.from_tags(data)
            else:
                self.set_app_data(appid, data)

    def update_handle(self, handle: str) -> None:
        """Update entity handle. (internal API)"""
        self.dxf.handle = handle
        if self.extension_dict:
            self.extension_dict.update_owner(handle)

    @property
    def is_alive(self) -> bool:
        """Is ``False`` if entity has been deleted."""
        return hasattr(self, "dxf")

    @property
    def is_virtual(self) -> bool:
        """Is ``True`` if entity is a virtual entity."""
        return self.doc is None or self.dxf.handle is None

    @property
    def is_bound(self) -> bool:
        """Is ``True`` if entity is bound to DXF document."""
        if self.is_alive and not self.is_virtual:
            return factory.is_bound(self, self.doc)  # type: ignore
        return False

    @property
    def has_source_block_reference(self) -> bool:
        """Is ``True`` if this virtual entity was created by a block reference."""
        return hasattr(self, DYN_SOURCE_BLOCK_REFERENCE_ATTRIBUTE)

    @property
    def source_block_reference(self) -> Optional[Insert]:
        """The source block reference (INSERT) which created
        this virtual entity. The property is ``None`` if this entity was not
        created by a block reference.
        """
        blockref = getattr(self, DYN_SOURCE_BLOCK_REFERENCE_ATTRIBUTE, None)
        if blockref is not None and blockref.is_alive:
            return blockref
        return None

    def set_source_block_reference(self, blockref: Insert) -> None:
        """Set the immediate source block reference which created this virtual
        entity.

        The source block reference can only be set once by the immediate INSERT
        entity and does not change if the entity is passed through multiple
        nested INSERT entities.

        (Internal API)
        """
        assert self.is_virtual, "instance has to be a virtual entity"
        if self.has_source_block_reference:
            return
        setattr(self, DYN_SOURCE_BLOCK_REFERENCE_ATTRIBUTE, blockref)

    def del_source_block_reference(self) -> None:
        """Delete source block reference.

        (Internal API)
        """
        if hasattr(self, DYN_SOURCE_BLOCK_REFERENCE_ATTRIBUTE):
            delattr(self, DYN_SOURCE_BLOCK_REFERENCE_ATTRIBUTE)

    def get_dxf_attrib(self, key: str, default: Any = None) -> Any:
        """Get DXF attribute `key`, returns `default` if key doesn't exist, or
        raise :class:`DXFValueError` if `default` is :class:`DXFValueError`
        and no DXF default value is defined::

            layer = entity.get_dxf_attrib("layer")
            # same as
            layer = entity.dxf.layer

        Raises :class:`DXFAttributeError` if `key` is not an supported DXF
        attribute.

        """
        return self.dxf.get(key, default)

    def set_dxf_attrib(self, key: str, value: Any) -> None:
        """Set new `value` for DXF attribute `key`::

           entity.set_dxf_attrib("layer", "MyLayer")
           # same as
           entity.dxf.layer = "MyLayer"

        Raises :class:`DXFAttributeError` if `key` is not an supported DXF
        attribute.

        """
        self.dxf.set(key, value)

    def del_dxf_attrib(self, key: str) -> None:
        """Delete DXF attribute `key`, does not raise an error if attribute is
        supported but not present.

        Raises :class:`DXFAttributeError` if `key` is not an supported DXF
        attribute.

        """
        self.dxf.discard(key)

    def has_dxf_attrib(self, key: str) -> bool:
        """Returns ``True`` if DXF attribute `key` really exist.

        Raises :class:`DXFAttributeError` if `key` is not an supported DXF
        attribute.

        """
        return self.dxf.hasattr(key)

    dxf_attrib_exists = has_dxf_attrib

    def is_supported_dxf_attrib(self, key: str) -> bool:
        """Returns ``True`` if DXF attrib `key` is supported by this entity.
        Does not grant that attribute `key` really exist.

        """
        if key in self.DXFATTRIBS:
            if self.doc:
                return (
                    self.doc.dxfversion
                    >= self.DXFATTRIBS.get(key).dxfversion  # type: ignore
                )
            else:
                return True
        else:
            return False

    def dxftype(self) -> str:
        """Get DXF type as string, like ``LINE`` for the line entity."""
        return self.DXFTYPE

    def __str__(self) -> str:
        """Returns a simple string representation."""
        return "{}(#{})".format(self.dxftype(), self.dxf.handle)

    def __repr__(self) -> str:
        """Returns a simple string representation including the class."""
        return str(self.__class__) + " " + str(self)

    def dxfattribs(self, drop: Optional[set[str]] = None) -> dict:
        """Returns a ``dict`` with all existing DXF attributes and their
        values and exclude all DXF attributes listed in set `drop`.

        """
        all_attribs = self.dxf.all_existing_dxf_attribs()
        if drop:
            return {k: v for k, v in all_attribs.items() if k not in drop}
        else:
            return all_attribs

    def set_flag_state(
        self, flag: int, state: bool = True, name: str = "flags"
    ) -> None:
        """Set binary coded `flag` of DXF attribute `name` to 1 (on)
        if `state` is ``True``, set `flag` to 0 (off)
        if `state` is ``False``.
        """
        flags = self.dxf.get(name, 0)
        self.dxf.set(name, set_flag_state(flags, flag, state=state))

    def get_flag_state(self, flag: int, name: str = "flags") -> bool:
        """Returns ``True`` if any `flag` of DXF attribute is 1 (on), else
        ``False``. Always check only one flag state at the time.
        """
        return bool(self.dxf.get(name, 0) & flag)

    def remove_dependencies(self, other: Optional[Drawing] = None):
        """Remove all dependencies from current document.

        Intended usage is to remove dependencies from the current document to
        move or copy the entity to `other` DXF document.

        An error free call of this method does NOT guarantee that this entity
        can be moved/copied to the `other` document, some entities like
        DIMENSION have too many dependencies to a document to move or copy
        them, but to check this is not the domain of this method!

        (internal API)
        """
        if self.is_alive:
            self.dxf.owner = None
            self.dxf.handle = None
            self.reactors = None
            self.extension_dict = None
            self.appdata = None
            self.xdata = None
            # remove dynamic attributes if exist:
            self.del_source_of_copy()
            self.del_source_block_reference()

    def destroy(self) -> None:
        """Delete all data and references. Does not delete entity from
        structures like layouts or groups.

        (internal API)
        """
        if not self.is_alive:
            return

        if self.extension_dict is not None:
            self.extension_dict.destroy()
            del self.extension_dict
        del self.appdata
        del self.reactors
        del self.xdata
        del self.doc
        del self.dxf  # check mark for is_alive
        # Remove dynamic attributes, which reference other entities:
        self.del_source_of_copy()
        self.del_source_block_reference()

    def _silent_kill(self):  # final - do not override this method!
        """Delete entity but not the referenced content!

        DANGER! DON'T USE THIS METHOD!

        (internal API)
        """
        if not self.is_alive:
            return
        del self.extension_dict
        del self.appdata
        del self.reactors
        del self.xdata
        del self.doc
        del self.dxf  # check mark for is_alive

    def notify(self, message_type: int, data: Any = None) -> None:
        """Internal messaging system.  (internal API)"""
        pass

    def preprocess_export(self, tagwriter: AbstractTagWriter) -> bool:
        """Pre requirement check and pre-processing for export.

        Returns ``False``  if entity should not be exported at all.

        (internal API)
        """
        return True

    def export_dxf(self, tagwriter: AbstractTagWriter) -> None:
        """Export DXF entity by `tagwriter`.

        This is the first key method for exporting DXF entities:

            - has to know the group codes for each attribute
            - has to add subclass tags in correct order
            - has to integrate extended data: ExtensionDict, Reactors, AppData
            - has to maintain the correct tag order (because sometimes order matters)

        (internal API)

        """
        if tagwriter.dxfversion < self.MIN_DXF_VERSION_FOR_EXPORT:
            return
        if not self.preprocess_export(tagwriter):
            return
        # write handle, AppData, Reactors, ExtensionDict, owner
        self.export_base_class(tagwriter)

        # this is the entity specific part
        self.export_entity(tagwriter)

        # write xdata at the end of the entity
        self.export_xdata(tagwriter)

    def export_base_class(self, tagwriter: AbstractTagWriter) -> None:
        """Export base class DXF attributes and structures. (internal API)"""
        dxftype = self.DXFTYPE
        _handle_code = 105 if dxftype == "DIMSTYLE" else 5
        # 1. tag: (0, DXFTYPE)
        tagwriter.write_tag2(const.STRUCTURE_MARKER, dxftype)

        if tagwriter.dxfversion >= const.DXF2000:
            tagwriter.write_tag2(_handle_code, self.dxf.handle)
            if self.appdata:
                self.appdata.export_dxf(tagwriter)
            if self.has_extension_dict:
                self.extension_dict.export_dxf(tagwriter)  # type: ignore
            if self.reactors:
                self.reactors.export_dxf(tagwriter)
            tagwriter.write_tag2(const.OWNER_CODE, self.dxf.get("owner", "0"))
        else:  # DXF R12
            if tagwriter.write_handles:
                tagwriter.write_tag2(_handle_code, self.dxf.handle)
                # do not write owner handle - not supported by DXF R12

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export DXF entity specific data by `tagwriter`.

        This is the second key method for exporting DXF entities:

            - has to know the group codes for each attribute
            - has to add subclass tags in correct order
            - has to maintain the correct tag order (because sometimes order matters)

        (internal API)
        """
        # base class (handle, appid, reactors, xdict, owner) export is done by parent class
        pass
        # xdata and embedded objects  export is also done by parent

    def export_xdata(self, tagwriter: AbstractTagWriter) -> None:
        """Export DXF XDATA by `tagwriter`. (internal API)"""
        if self.xdata:
            self.xdata.export_dxf(tagwriter)

    def audit(self, auditor: Auditor) -> None:
        """Validity check. (internal API)"""
        # Important: do not check owner handle! -> DXFGraphic(), DXFObject()
        # check app data
        # check reactors
        # check extension dict
        # check XDATA

    @property
    def has_extension_dict(self) -> bool:
        """Returns ``True`` if entity has an attached
        :class:`~ezdxf.entities.xdict.ExtensionDict` instance.
        """
        xdict = self.extension_dict
        # Don't use None check: bool(xdict) for an empty extension dict is False
        if xdict is not None and xdict.is_alive:
            return xdict.dictionary.is_alive
        return False

    def get_extension_dict(self) -> ExtensionDict:
        """Returns the existing :class:`~ezdxf.entities.xdict.ExtensionDict`
        instance.

        Raises:
            AttributeError: extension dict does not exist

        """
        if self.has_extension_dict:
            return self.extension_dict  # type: ignore
        else:
            raise AttributeError("Entity has no extension dictionary.")

    def new_extension_dict(self) -> ExtensionDict:
        """Create a new :class:`~ezdxf.entities.xdict.ExtensionDict` instance."""
        assert self.doc is not None
        xdict = ExtensionDict.new(self.dxf.handle, self.doc)
        self.extension_dict = xdict
        return xdict

    def discard_extension_dict(self) -> None:
        """Delete :class:`~ezdxf.entities.xdict.ExtensionDict` instance."""
        if isinstance(self.extension_dict, ExtensionDict):
            self.extension_dict.destroy()
        self.extension_dict = None

    def discard_empty_extension_dict(self) -> None:
        """Delete :class:`~ezdxf.entities.xdict.ExtensionDict` instance when empty."""
        if (
            isinstance(self.extension_dict, ExtensionDict)
            and len(self.extension_dict) == 0
        ):
            self.discard_extension_dict()

    def has_app_data(self, appid: str) -> bool:
        """Returns ``True`` if application defined data for `appid` exist."""
        if self.appdata:
            return appid in self.appdata
        else:
            return False

    def get_app_data(self, appid: str) -> Tags:
        """Returns application defined data for `appid`.

        Args:
            appid: application name as defined in the APPID table.

        Raises:
            DXFValueError: no data for `appid` found

        """
        if self.appdata:
            return Tags(self.appdata.get(appid)[1:-1])
        else:
            raise const.DXFValueError(appid)

    def set_app_data(self, appid: str, tags: Iterable) -> None:
        """Set application defined data for `appid` as iterable of tags.

        Args:
             appid: application name as defined in the APPID table.
             tags: iterable of (code, value) tuples or :class:`~ezdxf.lldxf.types.DXFTag`

        """
        if self.appdata is None:
            self.appdata = AppData()
        self.appdata.add(appid, tags)

    def discard_app_data(self, appid: str):
        """Discard application defined data for `appid`. Does not raise an
        exception if no data for `appid` exist.
        """
        if self.appdata:
            self.appdata.discard(appid)

    def has_xdata(self, appid: str) -> bool:
        """Returns ``True`` if extended data for `appid` exist."""
        if self.xdata:
            return appid in self.xdata
        else:
            return False

    def get_xdata(self, appid: str) -> Tags:
        """Returns extended data for `appid`.

        Args:
            appid: application name as defined in the APPID table.

        Raises:
            DXFValueError: no extended data for `appid` found

        """
        if self.xdata:
            return Tags(self.xdata.get(appid)[1:])
        else:
            raise const.DXFValueError(appid)

    def set_xdata(self, appid: str, tags: Iterable) -> None:
        """Set extended data for `appid` as iterable of tags.

        Args:
             appid: application name as defined in the APPID table.
             tags: iterable of (code, value) tuples or :class:`~ezdxf.lldxf.types.DXFTag`

        """
        if self.xdata is None:
            self.xdata = XData()
        self.xdata.add(appid, tags)

    def discard_xdata(self, appid: str) -> None:
        """Discard extended data for `appid`. Does not raise an exception if
        no extended data for `appid` exist.
        """
        if self.xdata:
            self.xdata.discard(appid)

    def has_xdata_list(self, appid: str, name: str) -> bool:
        """Returns ``True`` if a tag list `name` for extended data `appid`
        exist.
        """
        if self.has_xdata(appid):
            return self.xdata.has_xlist(appid, name)  # type: ignore
        else:
            return False

    def get_xdata_list(self, appid: str, name: str) -> Tags:
        """Returns tag list `name` for extended data `appid`.

        Args:
            appid: application name as defined in the APPID table.
            name: extended data list name

        Raises:
            DXFValueError: no extended data for `appid` found or no data list `name` not found

        """
        if self.xdata:
            return Tags(self.xdata.get_xlist(appid, name))
        else:
            raise const.DXFValueError(appid)

    def set_xdata_list(self, appid: str, name: str, tags: Iterable) -> None:
        """Set tag list `name` for extended data `appid` as iterable of tags.

        Args:
             appid: application name as defined in the APPID table.
             name: extended data list name
             tags: iterable of (code, value) tuples or :class:`~ezdxf.lldxf.types.DXFTag`

        """
        if self.xdata is None:
            self.xdata = XData()
        self.xdata.set_xlist(appid, name, tags)

    def discard_xdata_list(self, appid: str, name: str) -> None:
        """Discard tag list `name` for extended data `appid`. Does not raise
        an exception if no extended data for `appid` or no tag list `name`
        exist.
        """
        if self.xdata:
            self.xdata.discard_xlist(appid, name)

    def replace_xdata_list(self, appid: str, name: str, tags: Iterable) -> None:
        """
        Replaces tag list `name` for existing extended data `appid` by `tags`.
        Appends new list if tag list `name` do not exist, but raises
        :class:`DXFValueError` if extended data `appid` do not exist.

        Args:
             appid: application name as defined in the APPID table.
             name: extended data list name
             tags: iterable of (code, value) tuples or :class:`~ezdxf.lldxf.types.DXFTag`

        Raises:
            DXFValueError: no extended data for `appid` found

        """
        assert self.xdata is not None
        self.xdata.replace_xlist(appid, name, tags)

    def has_reactors(self) -> bool:
        """Returns ``True`` if entity has reactors."""
        return bool(self.reactors)

    def get_reactors(self) -> list[str]:
        """Returns associated reactors as list of handles."""
        return self.reactors.get() if self.reactors else []

    def set_reactors(self, handles: Iterable[str]) -> None:
        """Set reactors as list of handles."""
        if self.reactors is None:
            self.reactors = Reactors()
        self.reactors.set(handles)

    def append_reactor_handle(self, handle: str) -> None:
        """Append `handle` to reactors."""
        if self.reactors is None:
            self.reactors = Reactors()
        self.reactors.add(handle)

    def discard_reactor_handle(self, handle: str) -> None:
        """Discard `handle` from reactors. Does not raise an exception if
        `handle` does not exist.
        """
        if self.reactors:
            self.reactors.discard(handle)

    def register_resources(self, registry: xref.Registry) -> None:
        """Register required resources to the resource registry."""
        if self.xdata:
            for name in self.xdata.data.keys():
                registry.add_appid(name)
        if self.appdata:  # add hard owned entities
            for tags in self.appdata.tags():
                for tag in tags.get_hard_owner_handles():
                    registry.add_handle(tag.value)

    def map_resources(self, clone: Self, mapping: xref.ResourceMapper) -> None:
        """Translate resources from self to the copied entity."""

        def map_xdata_resources():
            for index, (code, value) in enumerate(tags):
                if code == 1005:  # map soft-pointer handles
                    tags[index] = DXFTag(code, mapping.get_handle(value))
                elif code == 1003:  # map layer name
                    tags[index] = DXFTag(code, mapping.get_layer(value))

        if clone.xdata:
            for tags in clone.xdata.data.values():
                map_xdata_resources()

        if clone.appdata:
            for tags in clone.appdata.tags():
                mapping.map_pointers(tags, new_owner_handle=clone.dxf.handle)

        if clone.extension_dict:
            assert self.extension_dict is not None
            self.extension_dict.dictionary.map_resources(
                clone.extension_dict.dictionary, mapping
            )
        # reactors are not copied automatically, clone.reactors is always None:
        if self.reactors:
            # reactors are soft-pointers (group code 330)
            mapped_handles = [mapping.get_handle(h) for h in self.reactors.reactors]
            mapped_handles = [h for h in mapped_handles if h != "0"]
            if mapped_handles:
                clone.set_reactors(mapped_handles)


@factory.set_default_class
class DXFTagStorage(DXFEntity):
    """Just store all the tags as they are. (internal class)"""

    def __init__(self) -> None:
        """Default constructor"""
        super().__init__()
        self.xtags = ExtendedTags()
        self.embedded_objects: Optional[list[Tags]] = None

    def copy(self, copy_strategy=default_copy) -> Self:
        raise CopyNotSupported(
            f"Copying of tag storage {self.dxftype()} not supported."
        )

    def transform(self, m: Matrix44) -> Self:
        raise NotImplementedError("cannot transform DXF tag storage")

    @property
    def base_class(self):
        return self.xtags.subclasses[0]

    @property
    def is_graphic_entity(self) -> bool:
        """Returns ``True`` if the entity has a graphical representations and
        can reside in the model space, a paper space or a block layout,
        otherwise the entity is a table or class entry or a DXF object from the
        OBJECTS section.
        """
        return self.xtags.has_subclass("AcDbEntity")

    def graphic_properties(self) -> dict[str, Any]:
        """Returns the graphical properties like layer, color, linetype, ... as
        `dxfattribs` dict if the :class:`TagStorage` object represents a graphical
        entity otherwise returns an empty dictionary.

        These are all the DXF attributes which are stored in the ``AcDbEntity``
        subclass.

        """
        from ezdxf.entities.dxfgfx import acdb_entity_group_codes

        attribs: dict[str, Any] = dict()
        try:
            tags = self.xtags.get_subclass("AcDbEntity")
        except const.DXFKeyError:
            return attribs

        for code, value in tags:
            attrib_name = acdb_entity_group_codes.get(code)
            if isinstance(attrib_name, str):
                attribs[attrib_name] = value
        return attribs

    @classmethod
    def load(cls, tags: ExtendedTags, doc: Optional[Drawing] = None) -> DXFTagStorage:
        assert isinstance(tags, ExtendedTags)
        entity = cls.new(doc=doc)
        dxfversion = doc.dxfversion if doc else None
        entity.load_tags(tags, dxfversion=dxfversion)
        entity.store_tags(tags)
        entity.store_embedded_objects(tags)
        if options.load_proxy_graphics:
            entity.load_proxy_graphic()
        return entity

    def load_proxy_graphic(self) -> None:
        try:
            tags = self.xtags.get_subclass("AcDbEntity")
        except const.DXFKeyError:
            return
        binary_data = [tag.value for tag in tags.find_all(310)]
        if len(binary_data):
            self.proxy_graphic = b"".join(binary_data)

    def store_tags(self, tags: ExtendedTags) -> None:
        # store DXFTYPE, overrides class member
        # 1. tag of 1. subclass is the structure tag (0, DXFTYPE)
        self.xtags = tags
        self.DXFTYPE = self.base_class[0].value
        try:
            acdb_entity = tags.get_subclass("AcDbEntity")
            self.dxf.__dict__["paperspace"] = acdb_entity.get_first_value(67, 0)
        except const.DXFKeyError:
            # just fake it
            self.dxf.__dict__["paperspace"] = 0

    def store_embedded_objects(self, tags: ExtendedTags) -> None:
        self.embedded_objects = tags.embedded_objects

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Write subclass tags as they are."""
        for subclass in self.xtags.subclasses[1:]:
            tagwriter.write_tags(subclass)

        if self.embedded_objects:
            for tags in self.embedded_objects:
                tagwriter.write_tags(tags)

    def destroy(self) -> None:
        if not self.is_alive:
            return

        del self.xtags
        super().destroy()

    def __virtual_entities__(self) -> Iterator[DXFGraphic]:
        """Implements the SupportsVirtualEntities protocol."""
        from ezdxf.proxygraphic import ProxyGraphic

        if self.proxy_graphic:
            for e in ProxyGraphic(self.proxy_graphic, self.doc).virtual_entities():
                e.set_source_of_copy(self)
                yield e

    def virtual_entities(self) -> Iterator[DXFGraphic]:
        """Yields proxy graphic as "virtual" entities."""
        return self.__virtual_entities__()
