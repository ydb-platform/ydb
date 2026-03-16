#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
import time

(MibScalarInstance,) = mibBuilder.import_symbols("SNMPv2-SMI", "MibScalarInstance")

(
    snmpEngineID,
    snmpEngineBoots,
    snmpEngineTime,
    snmpEngineMaxMessageSize,
) = mibBuilder.import_symbols(
    "SNMP-FRAMEWORK-MIB",
    "snmpEngineID",
    "snmpEngineBoots",
    "snmpEngineTime",
    "snmpEngineMaxMessageSize",
)

__snmpEngineID = MibScalarInstance(snmpEngineID.name, (0,), snmpEngineID.syntax)
__snmpEngineBoots = MibScalarInstance(
    snmpEngineBoots.name, (0,), snmpEngineBoots.syntax.clone(1)
)
__snmpEngineTime = MibScalarInstance(
    snmpEngineTime.name, (0,), snmpEngineTime.syntax.clone(int(time.time()))
)
__snmpEngineMaxMessageSize = MibScalarInstance(
    snmpEngineMaxMessageSize.name, (0,), snmpEngineMaxMessageSize.syntax.clone(4096)
)

mibBuilder.export_symbols(
    "__SNMP-FRAMEWORK-MIB",
    snmpEngineID=__snmpEngineID,
    snmpEngineBoots=__snmpEngineBoots,
    snmpEngineTime=__snmpEngineTime,
    snmpEngineMaxMessageSize=__snmpEngineMaxMessageSize,
)
