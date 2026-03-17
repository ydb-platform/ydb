"""Base classes and other objects used by enumerations."""

from __future__ import annotations

import enum
import textwrap
from typing import TYPE_CHECKING, Any, Type, TypeVar

if TYPE_CHECKING:
    from typing_extensions import Self

_T = TypeVar("_T", bound="BaseXmlEnum")


class BaseEnum(int, enum.Enum):
    """Base class for Enums that do not map XML attr values.

    The enum's value will be an integer, corresponding to the integer assigned the
    corresponding member in the MS API enum of the same name.
    """

    def __new__(cls, ms_api_value: int, docstr: str):
        self = int.__new__(cls, ms_api_value)
        self._value_ = ms_api_value
        self.__doc__ = docstr.strip()
        return self

    def __str__(self):
        """The symbolic name and string value of this member, e.g. 'MIDDLE (3)'."""
        return f"{self.name} ({self.value})"


class BaseXmlEnum(int, enum.Enum):
    """Base class for Enums that also map XML attr values.

    The enum's value will be an integer, corresponding to the integer assigned the
    corresponding member in the MS API enum of the same name.
    """

    xml_value: str | None

    def __new__(cls, ms_api_value: int, xml_value: str | None, docstr: str):
        self = int.__new__(cls, ms_api_value)
        self._value_ = ms_api_value
        self.xml_value = xml_value
        self.__doc__ = docstr.strip()
        return self

    def __str__(self):
        """The symbolic name and string value of this member, e.g. 'MIDDLE (3)'."""
        return f"{self.name} ({self.value})"

    @classmethod
    def from_xml(cls, xml_value: str) -> Self:
        """Enumeration member corresponding to XML attribute value `xml_value`.

        Raises `ValueError` if `xml_value` is the empty string ("") or is not an XML attribute
        value registered on the enumeration. Note that enum members that do not correspond to one
        of the defined values for an XML attribute have `xml_value == ""`. These
        "return-value only" members cannot be automatically mapped from an XML attribute value and
        must be selected explicitly by code, based on the appropriate conditions.

        Example::

            >>> WD_PARAGRAPH_ALIGNMENT.from_xml("center")
            WD_PARAGRAPH_ALIGNMENT.CENTER

        """
        # -- the empty string never maps to a member --
        member = (
            next((member for member in cls if member.xml_value == xml_value), None)
            if xml_value
            else None
        )

        if member is None:
            raise ValueError(f"{cls.__name__} has no XML mapping for {repr(xml_value)}")

        return member

    @classmethod
    def to_xml(cls: Type[_T], value: int | _T) -> str:
        """XML value of this enum member, generally an XML attribute value."""
        # -- presence of multi-arg `__new__()` method fools type-checker, but getting a
        # -- member by its value using EnumCls(val) works as usual.
        member = cls(value)
        xml_value = member.xml_value
        if not xml_value:
            raise ValueError(f"{cls.__name__}.{member.name} has no XML representation")
        return xml_value

    @classmethod
    def validate(cls: Type[_T], value: _T):
        """Raise |ValueError| if `value` is not an assignable value."""
        if value not in cls:
            raise ValueError(f"{value} not a member of {cls.__name__} enumeration")


class DocsPageFormatter(object):
    """Formats a reStructuredText documention page (string) for an enumeration."""

    def __init__(self, clsname: str, clsdict: dict[str, Any]):
        self._clsname = clsname
        self._clsdict = clsdict

    @property
    def page_str(self):
        """
        The RestructuredText documentation page for the enumeration. This is
        the only API member for the class.
        """
        tmpl = ".. _%s:\n\n%s\n\n%s\n\n----\n\n%s"
        components = (
            self._ms_name,
            self._page_title,
            self._intro_text,
            self._member_defs,
        )
        return tmpl % components

    @property
    def _intro_text(self):
        """
        The docstring of the enumeration, formatted for use at the top of the
        documentation page
        """
        try:
            cls_docstring = self._clsdict["__doc__"]
        except KeyError:
            cls_docstring = ""

        if cls_docstring is None:
            return ""

        return textwrap.dedent(cls_docstring).strip()

    def _member_def(self, member: BaseEnum | BaseXmlEnum):
        """Return an individual member definition formatted as an RST glossary entry.

        Output is wrapped to fit within 78 columns.
        """
        member_docstring = textwrap.dedent(member.__doc__ or "").strip()
        member_docstring = textwrap.fill(
            member_docstring,
            width=78,
            initial_indent=" " * 4,
            subsequent_indent=" " * 4,
        )
        return "%s\n%s\n" % (member.name, member_docstring)

    @property
    def _member_defs(self):
        """
        A single string containing the aggregated member definitions section
        of the documentation page
        """
        members = self._clsdict["__members__"]
        member_defs = [self._member_def(member) for member in members if member.name is not None]
        return "\n".join(member_defs)

    @property
    def _ms_name(self):
        """
        The Microsoft API name for this enumeration
        """
        return self._clsdict["__ms_name__"]

    @property
    def _page_title(self):
        """
        The title for the documentation page, formatted as code (surrounded
        in double-backtics) and underlined with '=' characters
        """
        title_underscore = "=" * (len(self._clsname) + 4)
        return "``%s``\n%s" % (self._clsname, title_underscore)
