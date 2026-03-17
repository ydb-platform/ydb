#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
"""
This module initializes the SNMP USM Security Model for the pysnmp library.

It imports the `SnmpUSMSecurityModel` from the `service` module within the
`pysnmp.proto.secmod.rfc3414` package.

Classes:
    SnmpUSMSecurityModel: A class representing the SNMP User-based Security Model (USM).

"""
from pysnmp.proto.secmod.rfc3414 import service

SnmpUSMSecurityModel = service.SnmpUSMSecurityModel
