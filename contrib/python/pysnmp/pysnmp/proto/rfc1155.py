#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from pyasn1.error import PyAsn1Error
from pyasn1.type import constraint, namedtype, tag, univ
from pysnmp.proto import error
from pysnmp.smi.error import SmiError

__all__ = [
    "Opaque",
    "NetworkAddress",
    "ObjectName",
    "TimeTicks",
    "Counter",
    "Gauge",
    "IpAddress",
]


class IpAddress(univ.OctetString):
    """IP address."""

    tagSet = univ.OctetString.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassApplication, tag.tagFormatSimple, 0x00)
    )
    subtypeSpec = univ.OctetString.subtypeSpec + constraint.ValueSizeConstraint(
        4, 4
    )  # noqa: N815

    def prettyIn(self, value):  # noqa: N802
        """Return a pretty-formatted IP address."""
        if isinstance(value, str) and len(value) != 4:
            try:
                value = [int(x) for x in value.split(".")]
            except:
                raise error.ProtocolError("Bad IP address syntax %s" % value)
        if len(value) != 4:
            raise error.ProtocolError("Bad IP address syntax")
        return univ.OctetString.prettyIn(self, value)

    def prettyOut(self, value):  # noqa: N802
        """Return a pretty-printed IP address."""
        if value:
            return ".".join(["%d" % x for x in self.__class__(value).asNumbers()])
        else:
            return ""


class Counter(univ.Integer):
    """Counter."""

    tagSet = univ.Integer.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassApplication, tag.tagFormatSimple, 0x01)
    )
    subtypeSpec = univ.Integer.subtypeSpec + constraint.ValueRangeConstraint(
        0, 4294967295
    )  # noqa: N815


class NetworkAddress(univ.Choice):
    """Network address."""

    componentType = namedtype.NamedTypes(  # noqa: N815
        namedtype.NamedType("internet", IpAddress())
    )

    def clone(self, value=univ.noValue, **kwargs):
        """Clone this instance.

        If *value* is specified, use its tag as the component type selector,
        and itself as the component value.

        :param value: (Optional) the component value.
        :type value: :py:obj:`pyasn1.type.base.Asn1ItemBase`
        :return: the cloned instance.
        :rtype: :py:obj:`pysnmp.proto.rfc1155.NetworkAddress`
        :raise: :py:obj:`pysnmp.smi.error.SmiError`:
            if the type of *value* is not allowed for this Choice instance.
        """
        cloned = univ.Choice.clone(self, **kwargs)
        if value is not univ.noValue:
            if isinstance(value, NetworkAddress):
                value = value.getComponent()
            elif not isinstance(value, IpAddress):
                # IpAddress is the only supported type, perhaps forever because
                # this is SNMPv1.
                value = IpAddress(value)
            try:
                tagSet = value.tagSet
            except AttributeError:
                raise PyAsn1Error(f"component value {value!r} has no tag set")
            cloned.setComponentByType(tagSet, value)
        return cloned

    # RFC 1212, section 4.1.6:
    #
    #    "(5)  NetworkAddress-valued: `n+1' sub-identifiers, where `n'
    #          depends on the kind of address being encoded (the first
    #          sub-identifier indicates the kind of address, value 1
    #          indicates an IpAddress);"

    def clone_from_name(self, value, impliedFlag, parentRow, parentIndices):
        """Clone this instance from a tuple of sub-identifiers."""
        kind = value[0]
        clone = self.clone()
        if kind == 1:
            clone["internet"] = tuple(value[1:5])
            return clone, value[5:]
        else:
            raise SmiError(f"unknown NetworkAddress type {kind!r}")

    def clone_as_name(self, impliedFlag, parentRow, parentIndices):
        """Return a tuple of sub-identifiers representing this instance.

        :param bool impliedFlag: (Unused) whether the value is implied.
        :param parentRow: (Unused) the parent row.
        :param parentIndices: (Unused) the parent indices.
        :return: the sub-identifiers.
        :rtype: :py:obj:`tuple`
        :raise: :py:obj:`pysnmp.smi.error.SmiError`:
            if the type of the component value is not allowed for this Choice instance.
        """
        kind = self.getName()
        component = self.getComponent()
        if kind == "internet":
            return (1,) + tuple(component.asNumbers())
        else:
            raise SmiError(f"unknown NetworkAddress type {kind!r}")


class Gauge(univ.Integer):
    """Gauge."""

    tagSet = univ.Integer.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassApplication, tag.tagFormatSimple, 0x02)
    )
    subtypeSpec = univ.Integer.subtypeSpec + constraint.ValueRangeConstraint(
        0, 4294967295
    )  # noqa: N815


class TimeTicks(univ.Integer):
    """TimeTicks."""

    tagSet = univ.Integer.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassApplication, tag.tagFormatSimple, 0x03)
    )
    subtypeSpec = univ.Integer.subtypeSpec + constraint.ValueRangeConstraint(
        0, 4294967295
    )  # noqa: N815


class Opaque(univ.OctetString):
    """Opaque."""

    tagSet = univ.OctetString.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassApplication, tag.tagFormatSimple, 0x04)
    )


class ObjectName(univ.ObjectIdentifier):
    """Object name."""

    pass


class TypeCoercionHackMixIn:  # XXX keep this old-style class till pyasn1 types becomes new-style
    # Reduce ASN1 type check to simple tag check as SMIv2 objects may
    # not be constraints-compatible with those used in SNMP PDU.
    def _verify_component(self, idx, value, **kwargs):
        componentType = self._componentType  # noqa: N806
        if componentType:
            if idx >= len(componentType):
                raise PyAsn1Error("Component type error out of range")
            t = componentType[idx].getType()
            if not t.getTagSet().isSuperTagSetOf(value.getTagSet()):
                raise PyAsn1Error(f"Component type error {t!r} vs {value!r}")


class SimpleSyntax(TypeCoercionHackMixIn, univ.Choice):
    componentType = namedtype.NamedTypes(  # noqa: N815
        namedtype.NamedType("number", univ.Integer()),
        namedtype.NamedType("string", univ.OctetString()),
        namedtype.NamedType("object", univ.ObjectIdentifier()),
        namedtype.NamedType("empty", univ.Null()),
    )


class ApplicationSyntax(TypeCoercionHackMixIn, univ.Choice):
    componentType = namedtype.NamedTypes(  # noqa: N815
        namedtype.NamedType("address", NetworkAddress()),
        namedtype.NamedType("counter", Counter()),
        namedtype.NamedType("gauge", Gauge()),
        namedtype.NamedType("ticks", TimeTicks()),
        namedtype.NamedType("arbitrary", Opaque()),
    )


class ObjectSyntax(univ.Choice):
    componentType = namedtype.NamedTypes(  # noqa: N815
        namedtype.NamedType("simple", SimpleSyntax()),
        namedtype.NamedType("application-wide", ApplicationSyntax()),
    )
