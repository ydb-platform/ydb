# Copyright (c) 2011-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Iterable, Optional, Iterator
from itertools import chain
import logging
from .types import tuples_to_tags, NONE_TAG
from .tags import Tags, DXFTag
from .const import DXFStructureError, DXFValueError, DXFKeyError
from .types import (
    APP_DATA_MARKER,
    SUBCLASS_MARKER,
    XDATA_MARKER,
    EMBEDDED_OBJ_MARKER,
    EMBEDDED_OBJ_STR,
)
from .types import is_app_data_marker, is_embedded_object_marker
from .tagger import internal_tag_compiler

if TYPE_CHECKING:
    from ezdxf.eztypes import IterableTags

logger = logging.getLogger("ezdxf")


class ExtendedTags:
    """Manages DXF tags located in sub structures:

        - Subclasses
        - AppData
        - Extended Data (XDATA)
        - Embedded objects

    Args:
        tags: iterable of type DXFTag()
        legacy: flag for DXF R12 tags

    """

    __slots__ = ("subclasses", "appdata", "xdata", "embedded_objects")

    def __init__(self, tags: Optional[Iterable[DXFTag]] = None, legacy=False):
        if isinstance(tags, str):
            raise DXFValueError(
                "use ExtendedTags.from_text() to create tags from a string."
            )

        # code == 102, keys are "{<arbitrary name>", values are Tags()
        self.appdata: list[Tags] = list()

        # code == 100, keys are "subclass-name", values are Tags()
        self.subclasses: list[Tags] = list()

        # code >= 1000, keys are "APPID", values are Tags()
        self.xdata: list[Tags] = list()

        # Store embedded objects as list, but embedded objects are rare, so
        # storing an empty list for every DXF entity is waste of memory.
        # Support for multiple embedded objects is maybe future proof, but
        # for now only one embedded object per entity is used.
        self.embedded_objects: Optional[list[Tags]] = None

        if tags is not None:
            self._setup(iter(tags))
            if legacy:
                self.legacy_repair()

    def legacy_repair(self):
        """Legacy (DXF R12) tags handling and repair."""
        self.flatten_subclasses()
        # ... and we can do some checks:
        # DXF R12 does not support (102, '{APPID') ... structures
        if len(self.appdata):
            # Just a debug message, do not delete appdata, this would corrupt
            # the data structure.
            self.debug("Found application defined entity data in DXF R12.")

        # That is really unlikely, but...
        if self.embedded_objects is not None:
            # Removing embedded objects from DXF R12 does not corrupt the
            # data structure:
            self.embedded_objects = None
            self.debug("Found embedded object in DXF R12.")

    def flatten_subclasses(self):
        """Flatten subclasses in legacy mode (DXF R12).

        There exists DXF R12 with subclass markers, technical incorrect but
        works if the reader ignore subclass marker tags, unfortunately ezdxf
        tries to use this subclass markers and therefore R12 parsing by ezdxf
        does not work without removing these subclass markers.

        This method removes all subclass markers and flattens all subclasses
        into ExtendedTags.noclass.

        """
        if len(self.subclasses) < 2:
            return
        noclass = self.noclass
        for subclass in self.subclasses[1:]:
            # Exclude first tag (100, subclass marker):
            noclass.extend(subclass[1:])
        self.subclasses = [noclass]
        self.debug("Removed subclass marker from entity for DXF R12.")

    def debug(self, msg: str) -> None:
        msg += f" <{self.entity_name()}>"
        logger.debug(msg)

    def entity_name(self) -> str:
        try:
            handle = f"(#{self.get_handle()})"
        except DXFValueError:
            handle = ""
        return self.dxftype() + handle

    def __copy__(self) -> ExtendedTags:
        """Shallow copy."""

        def copy(tag_lists):
            return [tags.clone() for tags in tag_lists]

        clone = self.__class__()
        clone.appdata = copy(self.appdata)
        clone.subclasses = copy(self.subclasses)
        clone.xdata = copy(self.xdata)
        if self.embedded_objects is not None:
            clone.embedded_objects = copy(self.embedded_objects)
        return clone

    clone = __copy__

    def __getitem__(self, index) -> Tags:
        return self.noclass[index]

    @property
    def noclass(self) -> Tags:
        """Short cut to access first subclass."""
        return self.subclasses[0]

    def get_handle(self) -> str:
        """Returns handle as hex string."""
        return self.noclass.get_handle()

    def dxftype(self) -> str:
        """Returns DXF type as string like "LINE"."""
        return self.noclass[0].value

    def replace_handle(self, handle: str) -> None:
        """Replace the existing entity handle by a new value."""
        self.noclass.replace_handle(handle)

    def _setup(self, tags: Iterator[DXFTag]) -> None:
        def is_end_of_class(tag):
            # fast path
            if tag.code not in {
                SUBCLASS_MARKER,
                EMBEDDED_OBJ_MARKER,
                XDATA_MARKER,
            }:
                return False
            else:
                # really an embedded object
                if (
                    tag.code == EMBEDDED_OBJ_MARKER
                    and tag.value != EMBEDDED_OBJ_STR
                ):
                    return False
                else:
                    return True

        def collect_base_class() -> DXFTag:
            """The base class contains AppData, but not XDATA and ends with
            SUBCLASS_MARKER, XDATA_MARKER or EMBEDDED_OBJ_MARKER.
            """
            # All subclasses begin with (100, subclass name) EXCEPT DIMASSOC
            # has a subclass starting with: (1, AcDbOsnapPointRef)
            # This special subclass is ignored by ezdxf, content is included in
            # the preceding subclass: (100, AcDbDimAssoc)
            # TEXT contains 2x the (100, AcDbText).
            #
            # Therefore it is not possible to use an (ordered) dict with
            # the subclass name as key, but usual use case is access by
            # numerical index.

            data = Tags()
            try:
                while True:
                    tag = next(tags)
                    if is_app_data_marker(tag):
                        app_data_pos = len(self.appdata)
                        data.append(DXFTag(tag.code, app_data_pos))
                        collect_app_data(tag)
                    elif is_end_of_class(tag):
                        self.subclasses.append(data)
                        return tag
                    else:
                        data.append(tag)
            except StopIteration:
                pass
            self.subclasses.append(data)
            return NONE_TAG

        def collect_subclass(starttag: DXFTag) -> DXFTag:
            """A subclass does NOT contain AppData or XDATA, and ends with
            SUBCLASS_MARKER, XDATA_MARKER or EMBEDDED_OBJ_MARKER.
            """
            # All subclasses begin with (100, subclass name)
            # for exceptions and rant see: collect_base_class()

            data = Tags([starttag])
            try:
                while True:
                    tag = next(tags)
                    # removed app data collection in subclasses
                    if is_end_of_class(tag):
                        self.subclasses.append(data)
                        return tag
                    else:
                        data.append(tag)
            except StopIteration:
                pass
            self.subclasses.append(data)
            return NONE_TAG

        def collect_app_data(starttag: DXFTag) -> None:
            """AppData can't contain XDATA or subclasses.

            AppData can only appear in the first unnamed subclass
            """
            data = Tags([starttag])
            # Alternative closing tag 'APPID}':
            closing_strings = ("}", starttag.value[1:] + "}")
            while True:
                try:
                    tag = next(tags)
                except StopIteration:
                    raise DXFStructureError(
                        "Missing closing (102, '}') tag in appdata structure."
                    )
                data.append(tag)
                if (tag.code == APP_DATA_MARKER) and (
                    tag.value in closing_strings
                ):
                    break
                    # Other (102, ) tags are treated as usual DXF tags.
            self.appdata.append(data)

        def collect_xdata(starttag: DXFTag) -> DXFTag:
            """XDATA is always at the end of the entity even if an embedded
            object is present and can not contain AppData or subclasses.

            """
            data = Tags([starttag])
            try:
                while True:
                    tag = next(tags)
                    if tag.code == XDATA_MARKER:
                        self.xdata.append(data)
                        return tag
                    else:
                        data.append(tag)
            except StopIteration:
                pass
            self.xdata.append(data)
            return NONE_TAG

        def collect_embedded_object(starttag: DXFTag) -> DXFTag:
            """Since AutoCAD 2018, DXF entities can contain embedded objects
            starting with a (101, 'Embedded Object') tag.

            All embedded object data is collected in a simple Tags() object,
            no subclass app data or XDATA processing is done.
            ezdxf does not use or modify the embedded object data, the data is
            just stored and written out as it is.

            self.embedded_objects = [
                1. embedded object as Tags(),
                2. embedded object as Tags(),
                ...
            ]

            Support for multiple embedded objects is maybe future proof, but
            for now only one embedded object per entity is used.

            """
            if self.embedded_objects is None:
                self.embedded_objects = list()
            data = Tags([starttag])
            try:
                while True:
                    tag = next(tags)
                    if (
                        is_embedded_object_marker(tag)
                        or tag.code == XDATA_MARKER
                    ):
                        # Another embedded object found: maybe in the future
                        # DXF entities can contain more than one embedded
                        # object.
                        self.embedded_objects.append(data)
                        return tag
                    else:
                        data.append(tag)
            except StopIteration:
                pass
            self.embedded_objects.append(data)
            return NONE_TAG

        # Preceding tags without a subclass
        tag = collect_base_class()
        while tag.code == SUBCLASS_MARKER:
            tag = collect_subclass(tag)

        while is_embedded_object_marker(tag):
            tag = collect_embedded_object(tag)

        # XDATA appear after an embedded object
        while tag.code == XDATA_MARKER:
            tag = collect_xdata(tag)

        if tag is not NONE_TAG:
            raise DXFStructureError(
                "Unexpected tag '%r' at end of entity." % tag
            )

    def __iter__(self) -> Iterator[DXFTag]:
        for subclass in self.subclasses:
            for tag in subclass:
                if tag.code == APP_DATA_MARKER and isinstance(tag.value, int):
                    yield from self.appdata[tag.value]
                else:
                    yield tag
        yield from chain.from_iterable(self.xdata)
        if self.embedded_objects is not None:
            yield from chain.from_iterable(self.embedded_objects)

    def get_subclass(self, name: str, pos: int = 0) -> Tags:
        """Get subclass `name`.

        Args:
            name: subclass name as string like "AcDbEntity"
            pos: start searching at subclass `pos`.

        """
        for index, subclass in enumerate(self.subclasses):
            try:
                if (index >= pos) and (subclass[0].value == name):
                    return subclass
            except IndexError:
                pass  # subclass[0]: ignore empty subclasses

        raise DXFKeyError(f'Subclass "{name}" does not exist.')

    def has_subclass(self, name: str) -> bool:
        for subclass in self.subclasses:
            try:
                if subclass[0].value == name:
                    return True
            except IndexError:
                pass  # ignore empty subclasses
        return False

    def has_xdata(self, appid: str) -> bool:
        """``True`` if has XDATA for `appid`."""
        return any(xdata[0].value == appid for xdata in self.xdata)

    def get_xdata(self, appid: str) -> Tags:
        """Returns XDATA for `appid` as :class:`Tags`."""
        for xdata in self.xdata:
            if xdata[0].value == appid:
                return xdata
        raise DXFValueError(f'No extended data for APPID "{appid}".')

    def set_xdata(self, appid: str, tags: IterableTags) -> None:
        """Set `tags` as XDATA for `appid`."""
        xdata = self.get_xdata(appid)
        xdata[1:] = tuples_to_tags(tags)

    def new_xdata(
        self, appid: str, tags: Optional[IterableTags] = None
    ) -> Tags:
        """Append a new XDATA block.

        Assumes that no XDATA block with the same `appid` already exist::

            try:
                xdata = tags.get_xdata('EZDXF')
            except ValueError:
                xdata = tags.new_xdata('EZDXF')
        """
        xtags = Tags([DXFTag(XDATA_MARKER, appid)])
        if tags is not None:
            xtags.extend(tuples_to_tags(tags))
        self.xdata.append(xtags)
        return xtags

    def has_app_data(self, appid: str) -> bool:
        """``True`` if has application defined data for `appid`."""
        return any(appdata[0].value == appid for appdata in self.appdata)

    def get_app_data(self, appid: str) -> Tags:
        """Returns application defined data for `appid` as :class:`Tags`
        including marker tags."""
        for appdata in self.appdata:
            if appdata[0].value == appid:
                return appdata
        raise DXFValueError(
            f'Application defined group "{appid}" does not exist.'
        )

    def get_app_data_content(self, appid: str) -> Tags:
        """Returns application defined data for `appid` as :class:`Tags`
        without first and last marker tag.
        """
        return Tags(self.get_app_data(appid)[1:-1])

    def set_app_data_content(self, appid: str, tags: IterableTags) -> None:
        """Set application defined data for `appid` for already exiting data."""
        app_data = self.get_app_data(appid)
        app_data[1:-1] = tuples_to_tags(tags)

    def new_app_data(
        self,
        appid: str,
        tags: Optional[IterableTags] = None,
        subclass_name: Optional[str] = None,
    ) -> Tags:
        """Append a new application defined data to subclass `subclass_name`.

        Assumes that no app data block with the same `appid` already exist::

            try:
                app_data = tags.get_app_data('{ACAD_REACTORS', tags)
            except ValueError:
                app_data = tags.new_app_data('{ACAD_REACTORS', tags)

        """
        if not appid.startswith("{"):
            raise DXFValueError("Appid has to start with '{'.")

        app_tags = Tags(
            [
                DXFTag(APP_DATA_MARKER, appid),
                DXFTag(APP_DATA_MARKER, "}"),
            ]
        )
        if tags is not None:
            app_tags[1:1] = tuples_to_tags(tags)

        if subclass_name is None:
            subclass = self.noclass
        else:
            # raises KeyError, if not exist
            subclass = self.get_subclass(subclass_name, 1)
        app_data_pos = len(self.appdata)
        subclass.append(DXFTag(APP_DATA_MARKER, app_data_pos))
        self.appdata.append(app_tags)
        return app_tags

    @classmethod
    def from_text(cls, text: str, legacy=False) -> ExtendedTags:
        """Create :class:`ExtendedTags` from DXF text."""
        return cls(internal_tag_compiler(text), legacy=legacy)
