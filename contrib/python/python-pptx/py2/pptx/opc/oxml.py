# encoding: utf-8

"""OPC-local oxml module to handle OPC-local concerns like relationship parsing."""

from lxml import etree

from .constants import NAMESPACE as NS, RELATIONSHIP_TARGET_MODE as RTM
from ..oxml import parse_xml, register_element_cls
from ..oxml.simpletypes import (
    ST_ContentType,
    ST_Extension,
    ST_TargetMode,
    XsdAnyUri,
    XsdId,
)
from ..oxml.xmlchemy import (
    BaseOxmlElement,
    OptionalAttribute,
    RequiredAttribute,
    ZeroOrMore,
)


nsmap = {
    "ct": NS.OPC_CONTENT_TYPES,
    "pr": NS.OPC_RELATIONSHIPS,
    "r": NS.OFC_RELATIONSHIPS,
}


def oxml_tostring(elm, encoding=None, pretty_print=False, standalone=None):
    return etree.tostring(
        elm, encoding=encoding, pretty_print=pretty_print, standalone=standalone
    )


def serialize_part_xml(part_elm):
    xml = etree.tostring(part_elm, encoding="UTF-8", standalone=True)
    return xml


class CT_Default(BaseOxmlElement):
    """
    ``<Default>`` element, specifying the default content type to be applied
    to a part with the specified extension.
    """

    extension = RequiredAttribute("Extension", ST_Extension)
    contentType = RequiredAttribute("ContentType", ST_ContentType)


class CT_Override(BaseOxmlElement):
    """
    ``<Override>`` element, specifying the content type to be applied for a
    part with the specified partname.
    """

    partName = RequiredAttribute("PartName", XsdAnyUri)
    contentType = RequiredAttribute("ContentType", ST_ContentType)


class CT_Relationship(BaseOxmlElement):
    """
    ``<Relationship>`` element, representing a single relationship from a
    source to a target part.
    """

    rId = RequiredAttribute("Id", XsdId)
    reltype = RequiredAttribute("Type", XsdAnyUri)
    target_ref = RequiredAttribute("Target", XsdAnyUri)
    targetMode = OptionalAttribute("TargetMode", ST_TargetMode, default=RTM.INTERNAL)

    @classmethod
    def new(cls, rId, reltype, target, target_mode=RTM.INTERNAL):
        """
        Return a new ``<Relationship>`` element.
        """
        xml = '<Relationship xmlns="%s"/>' % nsmap["pr"]
        relationship = parse_xml(xml)
        relationship.rId = rId
        relationship.reltype = reltype
        relationship.target_ref = target
        relationship.targetMode = target_mode
        return relationship


class CT_Relationships(BaseOxmlElement):
    """`<Relationships>` element, the root element in a .rels file."""

    relationship = ZeroOrMore("pr:Relationship")

    def add_rel(self, rId, reltype, target, is_external=False):
        """
        Add a child ``<Relationship>`` element with attributes set according
        to parameter values.
        """
        target_mode = RTM.EXTERNAL if is_external else RTM.INTERNAL
        relationship = CT_Relationship.new(rId, reltype, target, target_mode)
        self._insert_relationship(relationship)

    @classmethod
    def new(cls):
        """Return a new ``<Relationships>`` element."""
        return parse_xml('<Relationships xmlns="%s"/>' % nsmap["pr"])

    @property
    def xml(self):
        """
        Return XML string for this element, suitable for saving in a .rels
        stream, not pretty printed and with an XML declaration at the top.
        """
        return oxml_tostring(self, encoding="UTF-8", standalone=True)


class CT_Types(BaseOxmlElement):
    """
    ``<Types>`` element, the container element for Default and Override
    elements in [Content_Types].xml.
    """

    default = ZeroOrMore("ct:Default")
    override = ZeroOrMore("ct:Override")

    def add_default(self, ext, content_type):
        """
        Add a child ``<Default>`` element with attributes set to parameter
        values.
        """
        return self._add_default(extension=ext, contentType=content_type)

    def add_override(self, partname, content_type):
        """
        Add a child ``<Override>`` element with attributes set to parameter
        values.
        """
        return self._add_override(partName=partname, contentType=content_type)

    @classmethod
    def new(cls):
        """
        Return a new ``<Types>`` element.
        """
        xml = '<Types xmlns="%s"/>' % nsmap["ct"]
        types = parse_xml(xml)
        return types


register_element_cls("ct:Default", CT_Default)
register_element_cls("ct:Override", CT_Override)
register_element_cls("ct:Types", CT_Types)

register_element_cls("pr:Relationship", CT_Relationship)
register_element_cls("pr:Relationships", CT_Relationships)
