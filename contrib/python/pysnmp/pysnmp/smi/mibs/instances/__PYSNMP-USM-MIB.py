#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
(MibScalarInstance,) = mibBuilder.import_symbols("SNMPv2-SMI", "MibScalarInstance")

(
    pysnmpUsmDiscoverable,
    pysnmpUsmDiscovery,
    pysnmpUsmKeyType,
) = mibBuilder.import_symbols(
    "PYSNMP-USM-MIB", "pysnmpUsmDiscoverable", "pysnmpUsmDiscovery", "pysnmpUsmKeyType"
)

__pysnmpUsmDiscoverable = MibScalarInstance(
    pysnmpUsmDiscoverable.name, (0,), pysnmpUsmDiscoverable.syntax
)
__pysnmpUsmDiscovery = MibScalarInstance(
    pysnmpUsmDiscovery.name, (0,), pysnmpUsmDiscovery.syntax
)
__pysnmpUsmKeyType = MibScalarInstance(
    pysnmpUsmKeyType.name, (0,), pysnmpUsmKeyType.syntax
)

mibBuilder.export_symbols(
    "__PYSNMP-USM-MIB",
    pysnmpUsmDiscoverable=__pysnmpUsmDiscoverable,
    pysnmpUsmDiscovery=__pysnmpUsmDiscovery,
    pysnmpUsmKeyType=__pysnmpUsmKeyType,
)
