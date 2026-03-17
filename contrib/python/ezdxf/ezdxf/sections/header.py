# Copyright (c) 2011-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    Any,
    Iterable,
    Iterator,
    KeysView,
    Optional,
    Sequence,
    TYPE_CHECKING,
    Union,
)
import logging
from collections import OrderedDict

from ezdxf.lldxf import const
from ezdxf.lldxf.tags import group_tags, Tags, DXFTag
from ezdxf.lldxf.types import strtag
from ezdxf.lldxf.validator import header_validator
from ezdxf.sections.headervars import (
    HEADER_VAR_MAP,
    version_specific_group_code,
)

if TYPE_CHECKING:
    from ezdxf.lldxf.tagwriter import AbstractTagWriter

logger = logging.getLogger("ezdxf")

MIN_HEADER_TEXT = """  0
SECTION
  2
HEADER
  9
$ACADVER
  1
AC1009
  9
$DWGCODEPAGE
  3
ANSI_1252
  9
$HANDSEED
  5
FF
"""

# Additional variables may be stored as DICTIONARYVAR in the OBJECTS
# section in the DICTIONARY "AcDbVariableDictionary" of the root dict.
# - CANNOSCALE
# - CENTEREXE
# - CENTERLTYPEFILE
# - CETRANSPARENCY
# - CMLEADERSTYLE
# - CTABLESTYLE
# - CVIEWDETAILSTYLE
# - CVIEWSECTIONSTYLE
# - LAYEREVAL
# - LAYERNOTIFY
# - LIGHTINGUNITS
# - MSLTSCALE


class CustomVars:
    """The :class:`CustomVars` class stores custom properties in the DXF header as
    $CUSTOMPROPERTYTAG and $CUSTOMPROPERTY values. Custom properties require DXF R2004
    or later, `ezdxf` can create custom properties for older DXF versions as well, but
    AutoCAD will not show that properties.

    """

    def __init__(self) -> None:
        self.properties: list[tuple[str, str]] = list()

    def __len__(self) -> int:
        """Count of custom properties."""
        return len(self.properties)

    def __iter__(self) -> Iterator[tuple[str, str]]:
        """Iterate over all custom properties as ``(tag, value)`` tuples."""
        return iter(self.properties)

    def clear(self) -> None:
        """Remove all custom properties."""
        self.properties.clear()

    def append(self, tag: str, value: str) -> None:
        """Add custom property as ``(tag, value)`` tuple."""
        # custom properties always stored as strings
        self.properties.append((tag, str(value)))

    def get(self, tag: str, default: Optional[str] = None):
        """Returns the value of the first custom property `tag`."""
        for key, value in self.properties:
            if key == tag:
                return value
        else:
            return default

    def has_tag(self, tag: str) -> bool:
        """Returns ``True`` if custom property `tag` exist."""
        return self.get(tag) is not None

    def remove(self, tag: str, all: bool = False) -> None:
        """Removes the first occurrence of custom property `tag`, removes all
        occurrences if `all` is ``True``.

        Raises `:class:`DXFValueError` if `tag`  does not exist.

        """
        found_tag = False
        for item in self.properties:
            if item[0] == tag:
                self.properties.remove(item)
                found_tag = True
                if not all:
                    return
        if not found_tag:
            raise const.DXFValueError(f"Tag '{tag}' does not exist")

    def replace(self, tag: str, value: str) -> None:
        """Replaces the value of the first custom property `tag` by a new
        `value`.

        Raises :class:`DXFValueError` if `tag`  does not exist.

        """
        properties = self.properties
        for index in range(len(properties)):
            name = properties[index][0]
            if name == tag:
                properties[index] = (name, value)
                return

        raise const.DXFValueError(f"Tag '{tag}' does not exist")

    def write(self, tagwriter: AbstractTagWriter) -> None:
        """Export custom properties as DXF tags. (internal API)"""
        for tag, value in self.properties:
            s = f"  9\n$CUSTOMPROPERTYTAG\n  1\n{tag}\n  9\n$CUSTOMPROPERTY\n  1\n{value}\n"
            tagwriter.write_str(s)


def default_vars() -> OrderedDict:
    vars = OrderedDict()
    for vardef in HEADER_VAR_MAP.values():
        vars[vardef.name] = HeaderVar(DXFTag(vardef.code, vardef.default))
    return vars


class HeaderSection:
    MIN_HEADER_TAGS = Tags.from_text(MIN_HEADER_TEXT)
    name = "HEADER"

    def __init__(self) -> None:
        self.hdrvars: dict[str, HeaderVar] = OrderedDict()
        self.custom_vars = CustomVars()

    @classmethod
    def load(cls, tags: Optional[Iterable[DXFTag]] = None) -> HeaderSection:
        """Constructor to generate header variables loaded from DXF files
        (untrusted environment).

        Args:
            tags: DXF tags as Tags() or ExtendedTags()

        (internal API)
        """
        if tags is None:  # create default header
            # files without a header have the default version: R12
            return cls.new(dxfversion=const.DXF12)
        section = cls()
        section.load_tags(iter(tags))
        return section

    @classmethod
    def new(cls, dxfversion=const.LATEST_DXF_VERSION) -> HeaderSection:
        section = HeaderSection()
        section.hdrvars = default_vars()
        section["$ACADVER"] = dxfversion
        return section

    def load_tags(self, tags: Iterable[DXFTag]) -> None:
        """Constructor to generate header variables loaded from DXF files
        (untrusted environment).

        Args:
            tags: DXF tags as Tags() or ExtendedTags()

        """
        _tags = iter(tags) or iter(self.MIN_HEADER_TAGS)
        section_tag = next(_tags)
        name_tag = next(_tags)

        if section_tag != (0, "SECTION") or name_tag != (2, "HEADER"):
            raise const.DXFStructureError("Critical structure error in HEADER section.")

        groups = group_tags(header_validator(_tags), splitcode=9)
        custom_property_stack = []  # collect $CUSTOMPROPERTY/TAG
        for group in groups:
            name = group[0].value
            value = group[1]
            if name in ("$CUSTOMPROPERTYTAG", "$CUSTOMPROPERTY"):
                custom_property_stack.append(value.value)
            else:
                self.hdrvars[name] = HeaderVar(value)

        custom_property_stack.reverse()
        while len(custom_property_stack):
            try:
                self.custom_vars.append(
                    tag=custom_property_stack.pop(),
                    value=custom_property_stack.pop(),
                )
            except IndexError:  # internal exception
                break

    @classmethod
    def from_text(cls, text: str) -> HeaderSection:
        """Load constructor from text for testing"""
        return cls.load(Tags.from_text(text))

    def _headervar_factory(self, key: str, value: Any) -> DXFTag:
        if key in HEADER_VAR_MAP:
            factory = HEADER_VAR_MAP[key].factory
            return factory(value)
        else:
            raise const.DXFKeyError(f"Invalid header variable {key}.")

    def __len__(self) -> int:
        """Returns count of header variables."""
        return len(self.hdrvars)

    def __contains__(self, key) -> bool:
        """Returns ``True`` if header variable `key` exist."""
        return key in self.hdrvars

    def varnames(self) -> KeysView:
        """Returns an iterable of all header variable names."""
        return self.hdrvars.keys()

    def export_dxf(self, tagwriter: AbstractTagWriter) -> None:
        """Exports header section as DXF tags. (internal API)"""

        def _write(name: str, value: Any) -> None:
            if value.value is None:
                logger.info(f"did not write header var {name}, value is None.")
                return
            tagwriter.write_tag2(9, name)
            group_code = version_specific_group_code(name, dxfversion)
            # fix invalid group codes
            if group_code != value.code:
                value = HeaderVar((group_code, value.value))
            tagwriter.write_str(str(value))

        dxfversion: str = tagwriter.dxfversion
        write_handles = tagwriter.write_handles

        tagwriter.write_str("  0\nSECTION\n  2\nHEADER\n")
        save = self["$ACADVER"]
        self["$ACADVER"] = dxfversion
        for name, value in header_vars_by_priority(self.hdrvars, dxfversion):
            if not write_handles and name == "$HANDSEED":
                continue  # skip $HANDSEED
            _write(name, value)
            if name == "$LASTSAVEDBY":  # ugly hack, but necessary for AutoCAD
                self.custom_vars.write(tagwriter)
        self["$ACADVER"] = save
        tagwriter.write_str("  0\nENDSEC\n")

    def get(self, key: str, default: Any = None) -> Any:
        """Returns value of header variable `key` if exist, else the `default`
        value.

        """
        if key in self.hdrvars:
            return self.__getitem__(key)
        else:
            return default

    def __getitem__(self, key: str) -> Any:
        """Get header variable `key` by index operator like:
        :code:`drawing.header['$ACADVER']`

        """
        try:
            return self.hdrvars[key].value
        except KeyError:  # map exception
            raise const.DXFKeyError(str(key))

    def __setitem__(self, key: str, value: Any) -> None:
        """Set header variable `key` to `value` by index operator like:
        :code:`drawing.header['$ANGDIR'] = 1`

        """
        try:
            tags = self._headervar_factory(key, value)
        except (IndexError, ValueError):
            raise const.DXFValueError(str(value))
        self.hdrvars[key] = HeaderVar(tags)

    def __delitem__(self, key: str) -> None:
        """Delete header variable `key` by index operator like:
        :code:`del drawing.header['$ANGDIR']`

        """
        try:
            del self.hdrvars[key]
        except KeyError:  # map exception
            raise const.DXFKeyError(str(key))

    def reset_wcs(self):
        """Reset the current UCS settings to the :ref:`WCS`."""
        self["$UCSBASE"] = ""
        self["$UCSNAME"] = ""
        self["$UCSORG"] = (0, 0, 0)
        self["$UCSXDIR"] = (1, 0, 0)
        self["$UCSYDIR"] = (0, 1, 0)
        self["$UCSORTHOREF"] = ""
        self["$UCSORTHOVIEW"] = 0
        self["$UCSORGTOP"] = (0, 0, 0)
        self["$UCSORGBOTTOM"] = (0, 0, 0)
        self["$UCSORGLEFT"] = (0, 0, 0)
        self["$UCSORGRIGHT"] = (0, 0, 0)
        self["$UCSORGFRONT"] = (0, 0, 0)
        self["$UCSORGBACK"] = (0, 0, 0)


def header_vars_by_priority(
    header_vars: dict[str, HeaderVar], dxfversion: str
) -> Iterable[tuple]:
    order = []
    for name, value in header_vars.items():
        vardef = HEADER_VAR_MAP.get(name, None)
        if vardef is None:
            logger.info(f"Header variable {name} ignored, dxfversion={dxfversion}.")
            continue
        if vardef.mindxf <= dxfversion <= vardef.maxdxf:
            order.append((vardef.priority, (name, value)))
    order.sort()
    for priority, tag in order:
        yield tag


class HeaderVar:
    def __init__(self, tag: Union[DXFTag, Sequence]):
        self.tag = tag

    @property
    def code(self) -> int:
        return self.tag[0]

    @property
    def value(self) -> Any:
        return self.tag[1]

    @property
    def ispoint(self) -> bool:
        return self.code == 10

    def __str__(self) -> str:
        if self.ispoint:
            code, value = self.tag
            s = []
            for coord in value:
                s.append(strtag((code, coord)))
                code += 10
            return "".join(s)
        else:
            return strtag(self.tag)  # type: ignore
