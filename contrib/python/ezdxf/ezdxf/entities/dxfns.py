# Copyright (c) 2020-2025, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Any, Optional, Union, Iterable, TYPE_CHECKING, Set
import logging
import itertools
from ezdxf import options
from ezdxf.lldxf import const
from ezdxf.lldxf.attributes import XType, DXFAttributes, DXFAttr
from ezdxf.lldxf.types import cast_value, dxftag
from ezdxf.lldxf.tags import Tags

if TYPE_CHECKING:
    from ezdxf.lldxf.extendedtags import ExtendedTags
    from ezdxf.entities import DXFEntity
    from ezdxf.lldxf.tagwriter import AbstractTagWriter


__all__ = ["DXFNamespace", "SubclassProcessor"]
logger = logging.getLogger("ezdxf")

ERR_INVALID_DXF_ATTRIB = 'Invalid DXF attribute "{}" for entity {}'
ERR_DXF_ATTRIB_NOT_EXITS = 'DXF attribute "{}" does not exist'

# supported event handler called by setting DXF attributes
# for usage, implement a method named like the dict-value, that accepts the new
# value as argument e.g.:
#   Polyline.on_layer_change(name) -> changes also layers of all vertices

SETTER_EVENTS = {
    "layer": "on_layer_change",
    "linetype": "on_linetype_change",
    "style": "on_style_change",
    "dimstyle": "on_dimstyle_change",
}
EXCLUDE_FROM_UPDATE = frozenset(["_entity", "handle", "owner"])


class DXFNamespace:
    """:class:`DXFNamespace` manages all named DXF attributes of an entity.

    The DXFNamespace.__dict__ is used as DXF attribute storage, therefore only
    valid Python names can be used as attrib name.

    The namespace can only contain immutable objects: string, int, float, bool,
    Vec3. Because of the immutability, copy and deepcopy are the same.

    (internal class)
    """

    def __init__(
        self,
        processor: Optional[SubclassProcessor] = None,
        entity: Optional[DXFEntity] = None,
    ):
        if processor:
            base_class = processor.base_class
            handle_code = 105 if base_class[0].value == "DIMSTYLE" else 5
            # CLASS entities have no handle.
            # TABLE entities have no handle if loaded from a DXF R12 file.
            # Owner tag is None if loaded from a DXF R12 file
            handle = None
            owner = None
            for tag in base_class:
                group_code = tag.code
                if group_code == handle_code:
                    handle = tag.value
                    if owner:
                        break
                elif group_code == 330:
                    owner = tag.value
                    if handle:
                        break
            self.rewire(entity, handle, owner)
        else:
            self.reset_handles()
            self.rewire(entity)

    def copy(self, entity: DXFEntity):
        namespace = self.__class__()
        for k, v in self.__dict__.items():
            namespace.__dict__[k] = v
        namespace.rewire(entity)
        return namespace

    def __deepcopy__(self, memodict: Optional[dict] = None):
        return self.copy(self._entity)

    def __getstate__(self) -> object:
        return self.__dict__

    def __setstate__(self, state: object) -> None:
        if not isinstance(state, dict):
            raise TypeError(f"invalid state: {type(state).__name__}")
        # bypass __setattr__
        object.__setattr__(self, "__dict__", state)

    def reset_handles(self):
        """Reset handle and owner to None."""
        self.__dict__["handle"] = None
        self.__dict__["owner"] = None

    def rewire(
        self,
        entity: Optional[DXFEntity],
        handle: Optional[str] = None,
        owner: Optional[str] = None,
    ) -> None:
        """Rewire DXF namespace with parent entity

        Args:
            entity: new associated entity
            handle: new handle or None
            owner:  new entity owner handle or None

        """
        # bypass __setattr__()
        self.__dict__["_entity"] = entity
        if handle is not None:
            self.__dict__["handle"] = handle
        if owner is not None:
            self.__dict__["owner"] = owner

    def __getattr__(self, key: str) -> Any:
        """Called if DXF attribute `key` does not exist, returns the DXF
        default value or ``None``.

        Raises:
            DXFAttributeError: attribute `key` is not supported

        """
        attrib_def: Optional[DXFAttr] = self.dxfattribs.get(key)
        if attrib_def:
            if attrib_def.xtype == XType.callback:
                return attrib_def.get_callback_value(self._entity)
            else:
                return attrib_def.default
        else:
            raise const.DXFAttributeError(
                ERR_INVALID_DXF_ATTRIB.format(key, self.dxftype)
            )

    def __setattr__(self, key: str, value: Any) -> None:
        """Set DXF attribute `key` to `value`.

        Raises:
            DXFAttributeError: attribute `key` is not supported

        """

        def entity() -> str:
            # DXFNamespace is maybe not assigned to the entity yet:
            handle = self.get("handle")
            _entity = self._entity
            if _entity:
                return _entity.dxftype() + f"(#{handle})"
            else:
                return f"#{handle}"

        def check(value):
            value = cast_value(attrib_def.code, value)
            if not attrib_def.is_valid_value(value):
                if attrib_def.fixer:
                    value = attrib_def.fixer(value)
                    logger.debug(
                        f'Fixed invalid attribute "{key}" in entity'
                        f' {entity()} to "{str(value)}".'
                    )
                else:
                    raise const.DXFValueError(
                        f'Invalid value {str(value)} for attribute "{key}" in '
                        f"entity {entity()}."
                    )
            return value

        attrib_def: Optional[DXFAttr] = self.dxfattribs.get(key)
        if attrib_def:
            if attrib_def.xtype == XType.callback:
                attrib_def.set_callback_value(self._entity, value)
            else:
                self.__dict__[key] = check(value)
        else:
            raise const.DXFAttributeError(
                ERR_INVALID_DXF_ATTRIB.format(key, self.dxftype)
            )

        if key in SETTER_EVENTS:
            handler = getattr(self._entity, SETTER_EVENTS[key], None)
            if handler:
                handler(value)

    def __delattr__(self, key: str) -> None:
        """Delete DXF attribute `key`.

        Raises:
            DXFAttributeError: attribute `key` does not exist

        """
        if self.hasattr(key):
            del self.__dict__[key]
        else:
            raise const.DXFAttributeError(ERR_DXF_ATTRIB_NOT_EXITS.format(key))

    def get(self, key: str, default: Any = None) -> Any:
        """Returns value of DXF attribute `key` or the given `default` value
        not DXF default value for unset attributes.

        Raises:
            DXFAttributeError: attribute `key` is not supported

        """
        # callback values should not exist as attribute in __dict__
        if self.hasattr(key):
            # do not return the DXF default value
            return self.__dict__[key]
        attrib_def: Optional["DXFAttr"] = self.dxfattribs.get(key)
        if attrib_def:
            if attrib_def.xtype == XType.callback:
                return attrib_def.get_callback_value(self._entity)
            else:
                return default  # return give default
        else:
            raise const.DXFAttributeError(
                ERR_INVALID_DXF_ATTRIB.format(key, self.dxftype)
            )

    def get_default(self, key: str) -> Any:
        """Returns DXF default value for unset DXF attribute `key`."""
        value = self.get(key, None)
        return self.dxf_default_value(key) if value is None else value

    def set(self, key: str, value: Any) -> None:
        """Set DXF attribute `key` to `value`.

        Raises:
            DXFAttributeError: attribute `key` is not supported

        """
        self.__setattr__(key, value)

    def unprotected_set(self, key: str, value: Any) -> None:
        """Set DXF attribute `key` to `value` without any validity checks.

        Used for fast attribute setting without validity checks at loading time.

        (internal API)
        """
        self.__dict__[key] = value

    def all_existing_dxf_attribs(self) -> dict:
        """Returns all existing DXF attributes, except DXFEntity back-link."""
        attribs = dict(self.__dict__)
        del attribs["_entity"]
        return attribs

    def update(
        self,
        dxfattribs: dict[str, Any],
        *,
        exclude: Optional[Set[str]] = None,
        ignore_errors=False,
    ) -> None:
        """Update DXF namespace attributes from a dict."""
        if exclude is None:
            exclude = EXCLUDE_FROM_UPDATE  # type: ignore
        else:  # always exclude "_entity" back-link
            exclude = {"_entity"} | exclude

        set_attribute = self.__setattr__
        for k, v in dxfattribs.items():
            if k not in exclude:  # type: ignore
                try:
                    set_attribute(k, v)
                except (AttributeError, ValueError):
                    if not ignore_errors:
                        raise

    def discard(self, key: str) -> None:
        """Delete DXF attribute `key` silently without any exception."""
        try:
            del self.__dict__[key]
        except KeyError:
            pass

    def is_supported(self, key: str) -> bool:
        """Returns True if DXF attribute `key` is supported else False.
        Does not grant that attribute `key` really exists and does not
        check if the actual DXF version of the document supports this
        attribute, unsupported attributes will be ignored at export.

        """
        return key in self.dxfattribs

    def hasattr(self, key: str) -> bool:
        """Returns True if attribute `key` really exists else False."""
        return key in self.__dict__

    @property
    def dxftype(self) -> str:
        """Returns the DXF entity type."""
        return self._entity.DXFTYPE

    @property
    def dxfattribs(self) -> DXFAttributes:
        """Returns the DXF attribute definition."""
        return self._entity.DXFATTRIBS

    def dxf_default_value(self, key: str) -> Any:
        """Returns the default value as defined in the DXF standard."""
        attrib: Optional[DXFAttr] = self.dxfattribs.get(key)
        if attrib:
            return attrib.default
        else:
            return None

    def export_dxf_attribs(
        self, tagwriter: AbstractTagWriter, attribs: Union[str, Iterable]
    ) -> None:
        """Exports DXF attribute `name` by `tagwriter`. Non-optional attributes
        are forced and optional tags are only written if different to default
        value. DXF version check is always on: does not export DXF attribs
        which are not supported by tagwriter.dxfversion.

        Args:
            tagwriter: tag writer object
            attribs: DXF attribute name as string or an iterable of names

        """
        if isinstance(attribs, str):
            self._export_dxf_attribute_optional(tagwriter, attribs)
        else:
            for name in attribs:
                self._export_dxf_attribute_optional(tagwriter, name)

    def _export_dxf_attribute_optional(
        self, tagwriter: AbstractTagWriter, name: str
    ) -> None:
        """Exports DXF attribute `name` by `tagwriter`.

        Optional tags are only written if they differ from the default value.

        """
        attrib: Optional[DXFAttr] = self.dxfattribs.get(name)
        if attrib is None:
            raise const.DXFAttributeError(
                ERR_INVALID_DXF_ATTRIB.format(name, self.dxftype)
            )

        optional = attrib.optional
        default = attrib.default
        value = self.get(name, None)

        # Force default value e.g. layer
        if value is None and not optional:
            # default value can be None!
            value = default

        if value is None:
            logger.debug(
                f"DXF attribute '{name}' not written because its a None value."
            )
            return

        # Do not write explicit optional attribs if equal to the default value
        if (
            optional
            and (not tagwriter.force_optional)
            and default is not None
            and default == value
        ):
            return
        _export_group_codes(tagwriter, attrib, value)

    def export_dxf_attribute_if_exists(
        self, tagwriter: AbstractTagWriter, name: str
    ) -> None:
        """Exports DXF attribute `name` by `tagwriter` if exists.

        If the attribute exists, and it's not None it will be written, the optional-flag
        is ignored.

        No default value will be written if the attribute doesn't exist!
        This method can not be used for attributes that are required (e.g. layer)!

        """
        if not self.hasattr(name):
            return

        attrib: Optional[DXFAttr] = self.dxfattribs.get(name)
        assert (
            attrib is not None
        ), f"existing DXF attribute '{name}' has no definition class - internal error"

        value = self.get(name)
        if value is None:
            logger.debug(
                f"DXF attribute '{name}' not written because its a None value."
            )
            return
        _export_group_codes(tagwriter, attrib, value)


def _export_group_codes(
    tagwriter: AbstractTagWriter, attrib: DXFAttr, value: Any
) -> None:
    assert attrib is not None
    assert value is not None

    # Do write the attribute if the export DXF version is lower than the minimal
    # required DXF version for the attribute.
    if tagwriter.dxfversion < attrib.dxfversion:
        return

    # For explicit 2D points export only x- and y-coordinates.
    if attrib.xtype == XType.point2d and len(value) > 2:
        try:  # Vec3, Vec2
            value = (value.x, value.y)
        except AttributeError:
            value = value[:2]

    if isinstance(value, str):
        assert "\n" not in value, "line break '\\n' not allowed"
        assert "\r" not in value, "line break '\\r' not allowed"
    tag = dxftag(attrib.code, value)
    tagwriter.write_tag(tag)


BASE_CLASS_CODES = {0, 5, 102, 330}


class SubclassProcessor:
    """Helper class for loading tags into entities. (internal class)"""

    def __init__(self, tags: ExtendedTags, dxfversion: Optional[str] = None):
        if len(tags.subclasses) == 0:
            raise ValueError("Invalid tags.")
        self.subclasses: list[Tags] = list(tags.subclasses)  # copy subclasses
        self.embedded_objects: list[Tags] = tags.embedded_objects or []
        self.dxfversion: Optional[str] = dxfversion
        # DXF R12 and prior have no subclass marker system, all tags of an
        # entity in one flat list.
        # Later DXF versions have at least 2 subclasses base_class and
        # AcDbEntity.
        # Exception: CLASS has also only one subclass and no subclass marker,
        # handled as DXF R12 entity
        self.r12: bool = (dxfversion == const.DXF12) or (len(self.subclasses) == 1)
        self.name: str = tags.dxftype()
        self.handle: str
        try:
            self.handle = tags.get_handle()
        except const.DXFValueError:
            self.handle = "<?>"

    @property
    def base_class(self):
        return self.subclasses[0]

    def log_unprocessed_tags(
        self,
        unprocessed_tags: Iterable,
        subclass="<?>",
        handle: Optional[str] = None,
    ) -> None:
        if options.log_unprocessed_tags:
            for tag in unprocessed_tags:
                entity = ""
                if handle:
                    entity = f" in entity #{handle}"
                logger.info(f"ignored {repr(tag)} in subclass {subclass}" + entity)

    def find_subclass(self, name: str) -> Optional[Tags]:
        for subclass in self.subclasses:
            if len(subclass) and subclass[0].value == name:
                return subclass
        return None

    def subclass_by_index(self, index: int) -> Optional[Tags]:
        try:
            return self.subclasses[index]
        except IndexError:
            return None

    def detect_implementation_version(
        self, subclass_index: int, group_code: int, default: int
    ) -> int:
        subclass = self.subclass_by_index(subclass_index)
        if subclass and len(subclass) > 1:
            # the version tag has to be the 2nd tag after the subclass marker
            tag = subclass[1]
            if tag.code == group_code:
                return tag.value
        return default

    # TODO: rename to complex_dxfattribs_loader()
    def fast_load_dxfattribs(
        self,
        dxf: DXFNamespace,
        group_code_mapping: dict[int, Union[str, list]],
        subclass: Union[int, str, Tags],
        *,
        recover=False,
        log=True,
    ) -> Tags:
        """Load DXF attributes into the DXF namespace and returns the
        unprocessed tags without leading subclass marker(100, AcDb...).
        Bypasses the DXF attribute validity checks.

        Args:
            dxf: entity DXF namespace
            group_code_mapping: group code to DXF attribute name mapping,
                callback attributes have to be marked with a leading "*"
            subclass: subclass by index, by name or as Tags()
            recover: recover graphic attributes
            log: enable/disable logging of unprocessed tags

        """
        if self.r12:
            tags = self.subclasses[0]
        else:
            if isinstance(subclass, int):
                tags = self.subclass_by_index(subclass)  # type: ignore
            elif isinstance(subclass, str):
                tags = self.find_subclass(subclass)  # type: ignore
            else:
                tags = subclass

        unprocessed_tags = Tags()
        if tags is None or len(tags) == 0:
            return unprocessed_tags

        processed_names: set[str] = set()
        # Localize attributes:
        get_attrib_name = group_code_mapping.get
        append_unprocessed_tag = unprocessed_tags.append
        unprotected_set_attrib = dxf.unprotected_set
        mark_attrib_as_processed = processed_names.add

        # Ignore (100, "AcDb...") or (0, "ENTITY") tag in case of DXF R12
        start = 1 if tags[0].code in (0, 100) else 0
        for tag in tags[start:]:
            name = get_attrib_name(tag.code)
            if isinstance(name, list):  # process group code duplicates:
                names = name
                # If all existing attrib names are used, treat this tag
                # like an unprocessed tag.
                name = None
                # The attribute names are processed in the order of their
                # definition:
                for name_ in names:
                    if name_ not in processed_names:
                        name = name_
                        mark_attrib_as_processed(name_)
                        break
            if name:
                # Ignore callback attributes and group codes explicit marked
                # as "*IGNORE":
                if name[0] != "*":
                    unprotected_set_attrib(
                        name, cast_value(tag.code, tag.value)  # type: ignore
                    )
            else:
                append_unprocessed_tag(tag)

        if self.r12:
            # R12 has always unprocessed tags, because there are all tags in one
            # subclass and one subclass definition never covers all tags e.g.
            # handle is processed in DXFEntity, so it is an unprocessed tag in
            # AcDbEntity.
            return unprocessed_tags

        # Only DXF R13+
        if recover and len(unprocessed_tags):
            # TODO: maybe obsolete if simple_dxfattribs_loader() is used for
            #  most old DXF R12 entities
            unprocessed_tags = recover_graphic_attributes(unprocessed_tags, dxf)
        if len(unprocessed_tags) and log:
            # First tag is the subclass specifier (100, "AcDb...")
            name = tags[0].value
            self.log_unprocessed_tags(
                unprocessed_tags, subclass=name, handle=dxf.get("handle")
            )
        return unprocessed_tags

    def append_base_class_to_acdb_entity(self) -> None:
        """It is valid to mix up the base class with AcDbEntity class.
        This method appends all none base class group codes to the
        AcDbEntity class.
        """
        # This is only needed for DXFEntity, so applying this method
        # automatically to all entities is waste of runtime
        # -> DXFGraphic.load_dxf_attribs()
        # TODO: maybe obsolete if simple_dxfattribs_loader() is used for
        #  most old DXF R12 entities
        if self.r12:
            return

        acdb_entity_tags = self.subclasses[1]
        if acdb_entity_tags[0] == (100, "AcDbEntity"):
            acdb_entity_tags.extend(
                tag for tag in self.subclasses[0] if tag.code not in BASE_CLASS_CODES
            )

    def simple_dxfattribs_loader(
        self, dxf: DXFNamespace, group_code_mapping: dict[int, str]
    ) -> None:
        # tested in test suite 201 for the POINT entity
        """Load DXF attributes from all subclasses into the DXF namespace.

        Can not handle same group codes in different subclasses, does not remove
        processed tags or log unprocessed tags and bypasses the DXF attribute
        validity checks.

        This method ignores the subclass structure and can load data from
        very malformed DXF files, like such in issue #604.
        This method works only for very simple DXF entities with unique group
        codes in all subclasses, the old DXF R12 entities:

            - POINT
            - LINE
            - CIRCLE
            - ARC
            - INSERT
            - SHAPE
            - SOLID/TRACE/3DFACE
            - TEXT (ATTRIB/ATTDEF bypasses TEXT loader)
            - BLOCK/ENDBLK
            - POLYLINE/VERTEX/SEQEND
            - DIMENSION and subclasses
            - all table entries: LAYER, LTYPE, ...

        And the newer DXF entities:

            - ELLIPSE
            - RAY/XLINE

        The recover mode for graphical attributes is automatically included.
        Logging of unprocessed tags is not possible but also not required for
        this simple and well known entities.

        Args:
            dxf: entity DXF namespace
            group_code_mapping: group code name mapping for all DXF attributes
                from all subclasses, callback attributes have to be marked with
                a leading "*"

        """
        tags = itertools.chain.from_iterable(self.subclasses)
        get_attrib_name = group_code_mapping.get
        unprotected_set_attrib = dxf.unprotected_set
        for tag in tags:
            name = get_attrib_name(tag.code)
            if isinstance(name, str) and not name.startswith("*"):
                unprotected_set_attrib(name, cast_value(tag.code, tag.value))


GRAPHIC_ATTRIBUTES_TO_RECOVER = {
    8: "layer",
    6: "linetype",
    62: "color",
    67: "paperspace",
    370: "lineweight",
    48: "ltscale",
    60: "invisible",
    420: "true_color",
    430: "color_name",
    440: "transparency",
    284: "shadow_mode",
    347: "material_handle",
    348: "visualstyle_handle",
    380: "plotstyle_enum",
    390: "plotstyle_handle",
}


# TODO: maybe obsolete if simple_dxfattribs_loader() is used for
#  most old DXF R12 entities
def recover_graphic_attributes(tags: Tags, dxf: DXFNamespace) -> Tags:
    unprocessed_tags = Tags()
    for tag in tags:
        attrib_name = GRAPHIC_ATTRIBUTES_TO_RECOVER.get(tag.code)
        # Don't know if the unprocessed tag is really a misplaced tag,
        # so check if the attribute already exist!
        if attrib_name and not dxf.hasattr(attrib_name):
            dxf.set(attrib_name, tag.value)
        else:
            unprocessed_tags.append(tag)
    return unprocessed_tags
