#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
(MibScalarInstance,) = mibBuilder.import_symbols("SNMPv2-SMI", "MibScalarInstance")

(vacmViewSpinLock,) = mibBuilder.import_symbols(
    "SNMP-VIEW-BASED-ACM-MIB", "vacmViewSpinLock"
)

__vacmViewSpinLock = MibScalarInstance(
    vacmViewSpinLock.name, (0,), vacmViewSpinLock.syntax
)

mibBuilder.export_symbols(
    "__SNMP-VIEW-BASED-ACM-MIB", vacmViewSpinLock=__vacmViewSpinLock
)
