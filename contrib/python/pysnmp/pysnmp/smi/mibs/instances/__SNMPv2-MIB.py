#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from sys import version
from time import time

from pysnmp import __version__

(MibScalarInstance, TimeTicks) = mibBuilder.import_symbols(
    "SNMPv2-SMI", "MibScalarInstance", "TimeTicks"
)

(
    sysDescr,
    sysObjectID,
    sysUpTime,
    sysContact,
    sysName,
    sysLocation,
    sysServices,
    sysORLastChange,
    snmpInPkts,
    snmpOutPkts,
    snmpInBadVersions,
    snmpInBadCommunityNames,
    snmpInBadCommunityUses,
    snmpInASNParseErrs,
    snmpInTooBigs,
    snmpInNoSuchNames,
    snmpInBadValues,
    snmpInReadOnlys,
    snmpInGenErrs,
    snmpInTotalReqVars,
    snmpInTotalSetVars,
    snmpInGetRequests,
    snmpInGetNexts,
    snmpInSetRequests,
    snmpInGetResponses,
    snmpInTraps,
    snmpOutTooBigs,
    snmpOutNoSuchNames,
    snmpOutBadValues,
    snmpOutGenErrs,
    snmpOutSetRequests,
    snmpOutGetResponses,
    snmpOutTraps,
    snmpEnableAuthenTraps,
    snmpSilentDrops,
    snmpProxyDrops,
    snmpTrapOID,
    coldStart,
    snmpSetSerialNo,
) = mibBuilder.import_symbols(
    "SNMPv2-MIB",
    "sysDescr",
    "sysObjectID",
    "sysUpTime",
    "sysContact",
    "sysName",
    "sysLocation",
    "sysServices",
    "sysORLastChange",
    "snmpInPkts",
    "snmpOutPkts",
    "snmpInBadVersions",
    "snmpInBadCommunityNames",
    "snmpInBadCommunityUses",
    "snmpInASNParseErrs",
    "snmpInTooBigs",
    "snmpInNoSuchNames",
    "snmpInBadValues",
    "snmpInReadOnlys",
    "snmpInGenErrs",
    "snmpInTotalReqVars",
    "snmpInTotalSetVars",
    "snmpInGetRequests",
    "snmpInGetNexts",
    "snmpInSetRequests",
    "snmpInGetResponses",
    "snmpInTraps",
    "snmpOutTooBigs",
    "snmpOutNoSuchNames",
    "snmpOutBadValues",
    "snmpOutGenErrs",
    "snmpOutSetRequests",
    "snmpOutGetResponses",
    "snmpOutTraps",
    "snmpEnableAuthenTraps",
    "snmpSilentDrops",
    "snmpProxyDrops",
    "snmpTrapOID",
    "coldStart",
    "snmpSetSerialNo",
)

__sysDescr = MibScalarInstance(
    sysDescr.name,
    (0,),
    sysDescr.syntax.clone(
        "PySNMP engine version {}, Python {}".format(
            __version__, version.replace("\n", " ").replace("\r", " ")
        )
    ),
)
__sysObjectID = MibScalarInstance(
    sysObjectID.name, (0,), sysObjectID.syntax.clone((1, 3, 6, 1, 4, 1, 20408))
)


class SysUpTime(TimeTicks):
    createdAt = time()

    def clone(self, **kwargs):
        if "value" not in kwargs:
            kwargs["value"] = int((time() - self.createdAt) * 100)
        return TimeTicks.clone(self, **kwargs)


__sysUpTime = MibScalarInstance(sysUpTime.name, (0,), SysUpTime(0))
__sysContact = MibScalarInstance(sysContact.name, (0,), sysContact.syntax.clone(""))
__sysName = MibScalarInstance(sysName.name, (0,), sysName.syntax.clone(""))
__sysLocation = MibScalarInstance(sysLocation.name, (0,), sysLocation.syntax.clone(""))
__sysServices = MibScalarInstance(sysServices.name, (0,), sysServices.syntax.clone(0))
__sysORLastChange = MibScalarInstance(
    sysORLastChange.name, (0,), sysORLastChange.syntax.clone(0)
)
__snmpInPkts = MibScalarInstance(snmpInPkts.name, (0,), snmpInPkts.syntax.clone(0))
__snmpOutPkts = MibScalarInstance(snmpOutPkts.name, (0,), snmpOutPkts.syntax.clone(0))
__snmpInBadVersions = MibScalarInstance(
    snmpInBadVersions.name, (0,), snmpInBadVersions.syntax.clone(0)
)
__snmpInBadCommunityNames = MibScalarInstance(
    snmpInBadCommunityNames.name, (0,), snmpInBadCommunityNames.syntax.clone(0)
)
__snmpInBadCommunityUses = MibScalarInstance(
    snmpInBadCommunityUses.name, (0,), snmpInBadCommunityUses.syntax.clone(0)
)
__snmpInASNParseErrs = MibScalarInstance(
    snmpInASNParseErrs.name, (0,), snmpInASNParseErrs.syntax.clone(0)
)
__snmpInTooBigs = MibScalarInstance(
    snmpInTooBigs.name, (0,), snmpInTooBigs.syntax.clone(0)
)
__snmpInNoSuchNames = MibScalarInstance(
    snmpInNoSuchNames.name, (0,), snmpInNoSuchNames.syntax.clone(0)
)
__snmpInBadValues = MibScalarInstance(
    snmpInBadValues.name, (0,), snmpInBadValues.syntax.clone(0)
)
__snmpInReadOnlys = MibScalarInstance(
    snmpInReadOnlys.name, (0,), snmpInReadOnlys.syntax.clone(0)
)
__snmpInGenErrs = MibScalarInstance(
    snmpInGenErrs.name, (0,), snmpInGenErrs.syntax.clone(0)
)
__snmpInTotalReqVars = MibScalarInstance(
    snmpInTotalReqVars.name, (0,), snmpInTotalReqVars.syntax.clone(0)
)
__snmpInTotalSetVars = MibScalarInstance(
    snmpInTotalSetVars.name, (0,), snmpInTotalSetVars.syntax.clone(0)
)
__snmpInGetRequests = MibScalarInstance(
    snmpInGetRequests.name, (0,), snmpInGetRequests.syntax.clone(0)
)
__snmpInGetNexts = MibScalarInstance(
    snmpInGetNexts.name, (0,), snmpInGetNexts.syntax.clone(0)
)
__snmpInSetRequests = MibScalarInstance(
    snmpInSetRequests.name, (0,), snmpInSetRequests.syntax.clone(0)
)
__snmpInGetResponses = MibScalarInstance(
    snmpInGetResponses.name, (0,), snmpInGetResponses.syntax.clone(0)
)
__snmpInTraps = MibScalarInstance(snmpInTraps.name, (0,), snmpInTraps.syntax.clone(0))
__snmpOutTooBigs = MibScalarInstance(
    snmpOutTooBigs.name, (0,), snmpOutTooBigs.syntax.clone(0)
)
__snmpOutNoSuchNames = MibScalarInstance(
    snmpOutNoSuchNames.name, (0,), snmpOutNoSuchNames.syntax.clone(0)
)
__snmpOutBadValues = MibScalarInstance(
    snmpOutBadValues.name, (0,), snmpOutBadValues.syntax.clone(0)
)
__snmpOutGenErrs = MibScalarInstance(
    snmpOutGenErrs.name, (0,), snmpOutGenErrs.syntax.clone(0)
)
__snmpOutSetRequests = MibScalarInstance(
    snmpOutSetRequests.name, (0,), snmpOutSetRequests.syntax.clone(0)
)
__snmpOutGetResponses = MibScalarInstance(
    snmpOutGetResponses.name, (0,), snmpOutGetResponses.syntax.clone(0)
)
__snmpOutTraps = MibScalarInstance(
    snmpOutTraps.name, (0,), snmpOutTraps.syntax.clone(0)
)
__snmpEnableAuthenTraps = MibScalarInstance(
    snmpEnableAuthenTraps.name, (0,), snmpEnableAuthenTraps.syntax.clone(1)
)
__snmpSilentDrops = MibScalarInstance(
    snmpSilentDrops.name, (0,), snmpSilentDrops.syntax.clone(0)
)
__snmpProxyDrops = MibScalarInstance(
    snmpProxyDrops.name, (0,), snmpProxyDrops.syntax.clone(0)
)
__snmpTrapOID = MibScalarInstance(
    snmpTrapOID.name, (0,), snmpTrapOID.syntax.clone(coldStart.name)
)
__snmpSetSerialNo = MibScalarInstance(
    snmpSetSerialNo.name, (0,), snmpSetSerialNo.syntax.clone(0)
)

mibBuilder.export_symbols(
    "__SNMPv2-MIB",
    sysDescr=__sysDescr,
    sysObjectID=__sysObjectID,
    sysUpTime=__sysUpTime,
    sysContact=__sysContact,
    sysName=__sysName,
    sysLocation=__sysLocation,
    sysServices=__sysServices,
    sysORLastChange=__sysORLastChange,
    snmpInPkts=__snmpInPkts,
    snmpOutPkts=__snmpOutPkts,
    snmpInBadVersions=__snmpInBadVersions,
    snmpInBadCommunityNames=__snmpInBadCommunityNames,
    snmpInBadCommunityUses=__snmpInBadCommunityUses,
    snmpInASNParseErrs=__snmpInASNParseErrs,
    snmpInTooBigs=__snmpInTooBigs,
    snmpInNoSuchNames=__snmpInNoSuchNames,
    snmpInBadValues=__snmpInBadValues,
    snmpInReadOnlys=__snmpInReadOnlys,
    snmpInGenErrs=__snmpInGenErrs,
    snmpInTotalReqVars=__snmpInTotalReqVars,
    snmpInTotalSetVars=__snmpInTotalSetVars,
    snmpInGetRequests=__snmpInGetRequests,
    snmpInGetNexts=__snmpInGetNexts,
    snmpInSetRequests=__snmpInSetRequests,
    snmpInGetResponses=__snmpInGetResponses,
    snmpInTraps=__snmpInTraps,
    snmpOutTooBigs=__snmpOutTooBigs,
    snmpOutNoSuchNames=__snmpOutNoSuchNames,
    snmpOutBadValues=__snmpOutBadValues,
    snmpOutGenErrs=__snmpOutGenErrs,
    snmpOutSetRequests=__snmpOutSetRequests,
    snmpOutGetResponses=__snmpOutGetResponses,
    snmpOutTraps=__snmpOutTraps,
    snmpEnableAuthenTraps=__snmpEnableAuthenTraps,
    snmpSilentDrops=__snmpSilentDrops,
    snmpProxyDrops=__snmpProxyDrops,
    snmpTrapOID=__snmpTrapOID,
    snmpSetSerialNo=__snmpSetSerialNo,
)
