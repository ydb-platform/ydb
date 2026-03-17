#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from pyasn1.type import constraint, namedtype, namedval, tag, univ
from pysnmp.proto import rfc1902

__all__ = [
    "unSpecified",
    "EndOfMibView",
    "ReportPDU",
    "UnSpecified",
    "BulkPDU",
    "SNMPv2TrapPDU",
    "GetRequestPDU",
    "NoSuchObject",
    "GetNextRequestPDU",
    "GetBulkRequestPDU",
    "NoSuchInstance",
    "ResponsePDU",
    "noSuchObject",
    "InformRequestPDU",
    "endOfMibView",
    "SetRequestPDU",
    "noSuchInstance",
]

# Value reference -- max bindings in VarBindList
max_bindings = rfc1902.Integer(2147483647)

# Take SNMP exception values out of BindValue structure for convenience

UnSpecified = univ.Null
unSpecified = UnSpecified("")  # noqa: N816


class NoSuchObject(univ.Null):
    """No Such Object exception."""

    tagSet = univ.Null.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassContext, tag.tagFormatSimple, 0x00)
    )

    def prettyPrint(self, scope=0):  # noqa: N802
        """Return a human-readable string representation of the exception."""
        return "No Such Object currently exists at this OID"


noSuchObject = NoSuchObject("")  # noqa: N816


class NoSuchInstance(univ.Null):
    """No Such Instance exception."""

    tagSet = univ.Null.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassContext, tag.tagFormatSimple, 0x01)
    )

    def prettyPrint(self, scope=0):  # noqa: N802
        """Return a human-readable string representation of the exception."""
        return "No Such Instance currently exists at this OID"


noSuchInstance = NoSuchInstance("")  # noqa: N816


class EndOfMibView(univ.Null):
    """End of MIB View exception."""

    tagSet = univ.Null.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassContext, tag.tagFormatSimple, 0x02)
    )

    def prettyPrint(self, scope=0):  # noqa: N802
        """Return a human-readable string representation of the exception."""
        return "No more variables left in this MIB View"


endOfMibView = EndOfMibView("")  # noqa: N816


# Made a separate class for better readability
class _BindValue(univ.Choice):
    componentType = namedtype.NamedTypes(  # noqa: N815
        namedtype.NamedType("value", rfc1902.ObjectSyntax()),
        namedtype.NamedType("unSpecified", unSpecified),
        namedtype.NamedType("noSuchObject", noSuchObject),
        namedtype.NamedType("noSuchInstance", noSuchInstance),
        namedtype.NamedType("endOfMibView", endOfMibView),
    )


class VarBind(univ.Sequence):
    """VarBind class."""

    componentType = namedtype.NamedTypes(  # noqa: N815
        namedtype.NamedType("name", rfc1902.ObjectName()),
        namedtype.NamedType("", _BindValue()),
    )


class VarBindList(univ.SequenceOf):
    """VarBindList class."""

    componentType = VarBind()  # noqa: N815
    subtypeSpec = univ.SequenceOf.subtypeSpec + constraint.ValueSizeConstraint(
        0, max_bindings
    )  # noqa: N815


errorStatus = univ.Integer(  # noqa: N816
    namedValues=namedval.NamedValues(
        ("noError", 0),
        ("tooBig", 1),
        ("noSuchName", 2),
        ("badValue", 3),
        ("readOnly", 4),
        ("genErr", 5),
        ("noAccess", 6),
        ("wrongType", 7),
        ("wrongLength", 8),
        ("wrongEncoding", 9),
        ("wrongValue", 10),
        ("noCreation", 11),
        ("inconsistentValue", 12),
        ("resourceUnavailable", 13),
        ("commitFailed", 14),
        ("undoFailed", 15),
        ("authorizationError", 16),
        ("notWritable", 17),
        ("inconsistentName", 18),
    )
)


# Base class for a non-bulk PDU
class PDU(univ.Sequence):
    """PDU class."""

    componentType = namedtype.NamedTypes(  # noqa: N815
        namedtype.NamedType("request-id", rfc1902.Integer32()),
        namedtype.NamedType("error-status", errorStatus),
        namedtype.NamedType(
            "error-index",
            univ.Integer().subtype(
                subtypeSpec=constraint.ValueRangeConstraint(0, max_bindings)
            ),
        ),
        namedtype.NamedType("variable-bindings", VarBindList()),
    )


nonRepeaters = univ.Integer().subtype(  # noqa: N816
    subtypeSpec=constraint.ValueRangeConstraint(0, max_bindings)
)
maxRepetitions = univ.Integer().subtype(  # noqa: N816
    subtypeSpec=constraint.ValueRangeConstraint(0, max_bindings)
)


# Base class for bulk PDU
class BulkPDU(univ.Sequence):
    """BulkPDU class."""

    componentType = namedtype.NamedTypes(  # noqa: N815
        namedtype.NamedType("request-id", rfc1902.Integer32()),
        namedtype.NamedType("non-repeaters", nonRepeaters),
        namedtype.NamedType("max-repetitions", maxRepetitions),
        namedtype.NamedType("variable-bindings", VarBindList()),
    )


class GetRequestPDU(PDU):
    """GetRequest PDU."""

    tagSet = PDU.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassContext, tag.tagFormatConstructed, 0)
    )


class GetNextRequestPDU(PDU):
    """GetNextRequest PDU."""

    tagSet = PDU.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassContext, tag.tagFormatConstructed, 1)
    )


class ResponsePDU(PDU):
    """Response PDU."""

    tagSet = PDU.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassContext, tag.tagFormatConstructed, 2)
    )


class SetRequestPDU(PDU):
    """SetRequest PDU."""

    tagSet = PDU.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassContext, tag.tagFormatConstructed, 3)
    )


class GetBulkRequestPDU(BulkPDU):
    """GetBulkRequest PDU."""

    tagSet = PDU.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassContext, tag.tagFormatConstructed, 5)
    )


class InformRequestPDU(PDU):
    """InformRequest PDU."""

    tagSet = PDU.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassContext, tag.tagFormatConstructed, 6)
    )


class SNMPv2TrapPDU(PDU):
    """SNMPv2Trap PDU."""

    tagSet = PDU.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassContext, tag.tagFormatConstructed, 7)
    )


class ReportPDU(PDU):
    """Report PDU."""

    tagSet = PDU.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassContext, tag.tagFormatConstructed, 8)
    )


class PDUs(univ.Choice):
    """PDUs class."""

    componentType = namedtype.NamedTypes(  # noqa: N815
        namedtype.NamedType("get-request", GetRequestPDU()),
        namedtype.NamedType("get-next-request", GetNextRequestPDU()),
        namedtype.NamedType("get-bulk-request", GetBulkRequestPDU()),
        namedtype.NamedType("response", ResponsePDU()),
        namedtype.NamedType("set-request", SetRequestPDU()),
        namedtype.NamedType("inform-request", InformRequestPDU()),
        namedtype.NamedType("snmpV2-trap", SNMPv2TrapPDU()),
        namedtype.NamedType("report", ReportPDU()),
    )
