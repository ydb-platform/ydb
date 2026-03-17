"""OPC-local oxml module to handle OPC-local concerns like relationship parsing."""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, cast

from lxml import etree

from pptx.opc.constants import NAMESPACE as NS
from pptx.opc.constants import RELATIONSHIP_TARGET_MODE as RTM
from pptx.oxml import parse_xml, register_element_cls
from pptx.oxml.simpletypes import (
    ST_ContentType,
    ST_Extension,
    ST_TargetMode,
    XsdAnyUri,
    XsdId,
)
from pptx.oxml.xmlchemy import (
    BaseOxmlElement,
    OptionalAttribute,
    RequiredAttribute,
    ZeroOrMore,
)

if TYPE_CHECKING:
    from pptx.opc.packuri import PackURI

nsmap = {
    "ct": NS.OPC_CONTENT_TYPES,
    "pr": NS.OPC_RELATIONSHIPS,
    "r": NS.OFC_RELATIONSHIPS,
}


def oxml_to_encoded_bytes(
    element: BaseOxmlElement,
    encoding: str = "utf-8",
    pretty_print: bool = False,
    standalone: bool | None = None,
) -> bytes:
    return etree.tostring(
        element, encoding=encoding, pretty_print=pretty_print, standalone=standalone
    )


def oxml_tostring(
    elm: BaseOxmlElement,
    encoding: str | None = None,
    pretty_print: bool = False,
    standalone: bool | None = None,
):
    return etree.tostring(elm, encoding=encoding, pretty_print=pretty_print, standalone=standalone)


def serialize_part_xml(part_elm: BaseOxmlElement) -> bytes:
    """Produce XML-file bytes for `part_elm`, suitable for writing directly to a `.xml` file.

    Includes XML-declaration header.
    """
    return etree.tostring(part_elm, encoding="UTF-8", standalone=True)


class CT_Default(BaseOxmlElement):
    """`<Default>` element.

    Specifies the default content type to be applied to a part with the specified extension.
    """

    extension: str = RequiredAttribute(  # pyright: ignore[reportAssignmentType]
        "Extension", ST_Extension
    )
    contentType: str = RequiredAttribute(  # pyright: ignore[reportAssignmentType]
        "ContentType", ST_ContentType
    )


class CT_Override(BaseOxmlElement):
    """`<Override>` element.

    Specifies the content type to be applied for a part with the specified partname.
    """

    partName: str = RequiredAttribute(  # pyright: ignore[reportAssignmentType]
        "PartName", XsdAnyUri
    )
    contentType: str = RequiredAttribute(  # pyright: ignore[reportAssignmentType]
        "ContentType", ST_ContentType
    )


class CT_Relationship(BaseOxmlElement):
    """`<Relationship>` element.

    Represents a single relationship from a source to a target part.
    """

    rId: str = RequiredAttribute("Id", XsdId)  # pyright: ignore[reportAssignmentType]
    reltype: str = RequiredAttribute("Type", XsdAnyUri)  # pyright: ignore[reportAssignmentType]
    target_ref: str = RequiredAttribute(  # pyright: ignore[reportAssignmentType]
        "Target", XsdAnyUri
    )
    targetMode: str = OptionalAttribute(  # pyright: ignore[reportAssignmentType]
        "TargetMode", ST_TargetMode, default=RTM.INTERNAL
    )

    @classmethod
    def new(
        cls, rId: str, reltype: str, target_ref: str, target_mode: str = RTM.INTERNAL
    ) -> CT_Relationship:
        """Return a new `<Relationship>` element.

        `target_ref` is either a partname or a URI.
        """
        relationship = cast(CT_Relationship, parse_xml(f'<Relationship xmlns="{nsmap["pr"]}"/>'))
        relationship.rId = rId
        relationship.reltype = reltype
        relationship.target_ref = target_ref
        relationship.targetMode = target_mode
        return relationship


class CT_Relationships(BaseOxmlElement):
    """`<Relationships>` element, the root element in a .rels file."""

    relationship_lst: list[CT_Relationship]
    _insert_relationship: Callable[[CT_Relationship], CT_Relationship]

    relationship = ZeroOrMore("pr:Relationship")

    def add_rel(
        self, rId: str, reltype: str, target: str, is_external: bool = False
    ) -> CT_Relationship:
        """Add a child `<Relationship>` element with attributes set as specified."""
        target_mode = RTM.EXTERNAL if is_external else RTM.INTERNAL
        relationship = CT_Relationship.new(rId, reltype, target, target_mode)
        return self._insert_relationship(relationship)

    @classmethod
    def new(cls) -> CT_Relationships:
        """Return a new `<Relationships>` element."""
        return cast(CT_Relationships, parse_xml(f'<Relationships xmlns="{nsmap["pr"]}"/>'))

    @property
    def xml_file_bytes(self) -> bytes:
        """Return XML bytes, with XML-declaration, for this `<Relationships>` element.

        Suitable for saving in a .rels stream, not pretty printed and with an XML declaration at
        the top.
        """
        return oxml_to_encoded_bytes(self, encoding="UTF-8", standalone=True)


class CT_Types(BaseOxmlElement):
    """`<Types>` element.

    The container element for Default and Override elements in [Content_Types].xml.
    """

    default_lst: list[CT_Default]
    override_lst: list[CT_Override]

    _add_default: Callable[..., CT_Default]
    _add_override: Callable[..., CT_Override]

    default = ZeroOrMore("ct:Default")
    override = ZeroOrMore("ct:Override")

    def add_default(self, ext: str, content_type: str) -> CT_Default:
        """Add a child `<Default>` element with attributes set to parameter values."""
        return self._add_default(extension=ext, contentType=content_type)

    def add_override(self, partname: PackURI, content_type: str) -> CT_Override:
        """Add a child `<Override>` element with attributes set to parameter values."""
        return self._add_override(partName=partname, contentType=content_type)

    @classmethod
    def new(cls) -> CT_Types:
        """Return a new `<Types>` element."""
        return cast(CT_Types, parse_xml(f'<Types xmlns="{nsmap["ct"]}"/>'))


register_element_cls("ct:Default", CT_Default)
register_element_cls("ct:Override", CT_Override)
register_element_cls("ct:Types", CT_Types)

register_element_cls("pr:Relationship", CT_Relationship)
register_element_cls("pr:Relationships", CT_Relationships)
