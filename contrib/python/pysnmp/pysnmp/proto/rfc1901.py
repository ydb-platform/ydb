#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from pyasn1.type import namedtype, namedval, univ
from pysnmp.proto import rfc1905

version = univ.Integer(namedValues=namedval.NamedValues(("version-2c", 1)))


class Message(univ.Sequence):
    """Create a new SNMP message."""

    componentType = namedtype.NamedTypes(  # noqa: N815
        namedtype.NamedType("version", version),
        namedtype.NamedType("community", univ.OctetString()),
        namedtype.NamedType("data", rfc1905.PDUs()),
    )
