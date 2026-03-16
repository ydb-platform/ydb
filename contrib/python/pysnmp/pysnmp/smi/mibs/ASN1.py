#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from pyasn1.type import univ
from pysnmp.proto import rfc1902

mibBuilder.export_symbols(
    "ASN1",
    ObjectIdentifier=univ.ObjectIdentifier,
    # Instead of using base ASN,1 types we use SNMPv2 SMI ones to make
    # SMI objects type-compatible with SNMP protocol values
    Integer=rfc1902.Integer32,
    OctetString=rfc1902.OctetString,
)
