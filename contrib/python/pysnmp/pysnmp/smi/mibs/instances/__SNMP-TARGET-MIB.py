#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
(MibScalarInstance,) = mibBuilder.import_symbols("SNMPv2-SMI", "MibScalarInstance")

(
    snmpTargetSpinLock,
    snmpUnavailableContexts,
    snmpUnknownContexts,
) = mibBuilder.import_symbols(
    "SNMP-TARGET-MIB",
    "snmpTargetSpinLock",
    "snmpUnavailableContexts",
    "snmpUnknownContexts",
)

__snmpTargetSpinLock = MibScalarInstance(
    snmpTargetSpinLock.name, (0,), snmpTargetSpinLock.syntax.clone(0)
)
__snmpUnavailableContexts = MibScalarInstance(
    snmpUnavailableContexts.name, (0,), snmpUnavailableContexts.syntax.clone(0)
)
__snmpUnknownContexts = MibScalarInstance(
    snmpUnknownContexts.name, (0,), snmpUnknownContexts.syntax.clone(0)
)

mibBuilder.export_symbols(
    "__SNMP-TARGET-MIB",
    snmpTargetSpinLock=__snmpTargetSpinLock,
    snmpUnavailableContexts=__snmpUnavailableContexts,
    snmpUnknownContexts=__snmpUnknownContexts,
)
