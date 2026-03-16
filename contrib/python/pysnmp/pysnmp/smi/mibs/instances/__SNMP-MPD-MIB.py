#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
(MibScalarInstance,) = mibBuilder.import_symbols("SNMPv2-SMI", "MibScalarInstance")

(
    snmpUnknownSecurityModels,
    snmpInvalidMsgs,
    snmpUnknownPDUHandlers,
) = mibBuilder.import_symbols(
    "SNMP-MPD-MIB",
    "snmpUnknownSecurityModels",
    "snmpInvalidMsgs",
    "snmpUnknownPDUHandlers",
)

__snmpUnknownSecurityModels = MibScalarInstance(
    snmpUnknownSecurityModels.name, (0,), snmpUnknownSecurityModels.syntax.clone(0)
)
__snmpInvalidMsgs = MibScalarInstance(
    snmpInvalidMsgs.name, (0,), snmpInvalidMsgs.syntax.clone(0)
)
__snmpUnknownPDUHandlers = MibScalarInstance(
    snmpUnknownPDUHandlers.name, (0,), snmpUnknownPDUHandlers.syntax.clone(0)
)

mibBuilder.export_symbols(
    "__SNMP-MPD-MIB",
    snmpUnknownSecurityModels=__snmpUnknownSecurityModels,
    snmpInvalidMsgs=__snmpInvalidMsgs,
    snmpUnknownPDUHandlers=__snmpUnknownPDUHandlers,
)
