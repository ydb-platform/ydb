#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
(MibScalarInstance,) = mibBuilder.import_symbols("SNMPv2-SMI", "MibScalarInstance")

(
    usmStatsUnsupportedSecLevels,
    usmStatsNotInTimeWindows,
    usmStatsUnknownUserNames,
    usmStatsUnknownEngineIDs,
    usmStatsWrongDigests,
    usmStatsDecryptionErrors,
    usmUserSpinLock,
) = mibBuilder.import_symbols(
    "SNMP-USER-BASED-SM-MIB",
    "usmStatsUnsupportedSecLevels",
    "usmStatsNotInTimeWindows",
    "usmStatsUnknownUserNames",
    "usmStatsUnknownEngineIDs",
    "usmStatsWrongDigests",
    "usmStatsDecryptionErrors",
    "usmUserSpinLock",
)

__usmStatsUnsupportedSecLevels = MibScalarInstance(
    usmStatsUnsupportedSecLevels.name,
    (0,),
    usmStatsUnsupportedSecLevels.syntax.clone(0),
)
__usmStatsNotInTimeWindows = MibScalarInstance(
    usmStatsNotInTimeWindows.name, (0,), usmStatsNotInTimeWindows.syntax.clone(0)
)
__usmStatsUnknownUserNames = MibScalarInstance(
    usmStatsUnknownUserNames.name, (0,), usmStatsUnknownUserNames.syntax.clone(0)
)
__usmStatsUnknownEngineIDs = MibScalarInstance(
    usmStatsUnknownEngineIDs.name, (0,), usmStatsUnknownEngineIDs.syntax.clone(0)
)
__usmStatsWrongDigests = MibScalarInstance(
    usmStatsWrongDigests.name, (0,), usmStatsWrongDigests.syntax.clone(0)
)
__usmStatsDecryptionErrors = MibScalarInstance(
    usmStatsDecryptionErrors.name, (0,), usmStatsDecryptionErrors.syntax.clone(0)
)
__usmUserSpinLock = MibScalarInstance(
    usmUserSpinLock.name, (0,), usmUserSpinLock.syntax.clone(0)
)

mibBuilder.export_symbols(
    "__SNMP-USER-BASED-SM-MIB",
    usmStatsUnsupportedSecLevels=__usmStatsUnsupportedSecLevels,
    usmStatsNotInTimeWindows=__usmStatsNotInTimeWindows,
    usmStatsUnknownUserNames=__usmStatsUnknownUserNames,
    usmStatsUnknownEngineIDs=__usmStatsUnknownEngineIDs,
    usmStatsWrongDigests=__usmStatsWrongDigests,
    usmStatsDecryptionErrors=__usmStatsDecryptionErrors,
    usmUserSpinLock=__usmUserSpinLock,
)
