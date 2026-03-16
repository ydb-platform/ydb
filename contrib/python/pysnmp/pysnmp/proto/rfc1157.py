#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from pyasn1.type import namedtype, namedval, tag, univ
from pysnmp.proto import rfc1155

__all__ = [
    "GetNextRequestPDU",
    "GetResponsePDU",
    "SetRequestPDU",
    "TrapPDU",
    "GetRequestPDU",
]


class VarBind(univ.Sequence):
    componentType = namedtype.NamedTypes(  # noqa: N815
        namedtype.NamedType("name", rfc1155.ObjectName()),
        namedtype.NamedType("value", rfc1155.ObjectSyntax()),
    )


class VarBindList(univ.SequenceOf):
    componentType = VarBind()  # noqa: N815


errorStatus = univ.Integer(  # noqa: N816
    namedValues=namedval.NamedValues(
        ("noError", 0),
        ("tooBig", 1),
        ("noSuchName", 2),
        ("badValue", 3),
        ("readOnly", 4),
        ("genErr", 5),
    )
)


class _RequestBase(univ.Sequence):
    componentType = namedtype.NamedTypes(  # noqa: N815
        namedtype.NamedType("request-id", univ.Integer()),
        namedtype.NamedType("error-status", errorStatus),
        namedtype.NamedType("error-index", univ.Integer()),
        namedtype.NamedType("variable-bindings", VarBindList()),
    )


class GetRequestPDU(_RequestBase):
    """GetRequest PDU."""

    tagSet = _RequestBase.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassContext, tag.tagFormatConstructed, 0)
    )


class GetNextRequestPDU(_RequestBase):
    """GetNextRequest PDU."""

    tagSet = _RequestBase.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassContext, tag.tagFormatConstructed, 1)
    )


class GetResponsePDU(_RequestBase):
    """GetResponse PDU."""

    tagSet = _RequestBase.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassContext, tag.tagFormatConstructed, 2)
    )


class SetRequestPDU(_RequestBase):
    """SetRequest PDU."""

    tagSet = _RequestBase.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassContext, tag.tagFormatConstructed, 3)
    )


genericTrap = univ.Integer().clone(  # noqa: N816
    namedValues=namedval.NamedValues(
        ("coldStart", 0),
        ("warmStart", 1),
        ("linkDown", 2),
        ("linkUp", 3),
        ("authenticationFailure", 4),
        ("egpNeighborLoss", 5),
        ("enterpriseSpecific", 6),
    )
)


class TrapPDU(univ.Sequence):
    """Trap PDU."""

    tagSet = univ.Sequence.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassContext, tag.tagFormatConstructed, 4)
    )
    componentType = namedtype.NamedTypes(  # noqa: N815
        namedtype.NamedType("enterprise", univ.ObjectIdentifier()),
        namedtype.NamedType("agent-addr", rfc1155.NetworkAddress()),
        namedtype.NamedType("generic-trap", genericTrap),
        namedtype.NamedType("specific-trap", univ.Integer()),
        namedtype.NamedType("time-stamp", rfc1155.TimeTicks()),
        namedtype.NamedType("variable-bindings", VarBindList()),
    )


class PDUs(univ.Choice):
    componentType = namedtype.NamedTypes(  # noqa: N815
        namedtype.NamedType("get-request", GetRequestPDU()),
        namedtype.NamedType("get-next-request", GetNextRequestPDU()),
        namedtype.NamedType("get-response", GetResponsePDU()),
        namedtype.NamedType("set-request", SetRequestPDU()),
        namedtype.NamedType("trap", TrapPDU()),
    )


version = univ.Integer(namedValues=namedval.NamedValues(("version-1", 0)))


class Message(univ.Sequence):
    componentType = namedtype.NamedTypes(  # noqa: N815
        namedtype.NamedType("version", version),
        namedtype.NamedType("community", univ.OctetString()),
        namedtype.NamedType("data", PDUs()),
    )
