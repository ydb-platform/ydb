#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
# PySNMP MIB module PYSNMP-SOURCE-MIB (https://www.pysnmp.com/pysnmp)
# ASN.1 source http://mibs.pysnmp.com:80/asn1/PYSNMP-SOURCE-MIB
# Produced by pysmi-0.1.3 at Mon Apr 17 11:46:02 2017
# On host grommit.local platform Darwin version 16.4.0 by user ilya
# Using Python version 3.4.2 (v3.4.2:ab2c023a9432, Oct  5 2014, 20:42:22)
#
Integer, OctetString, ObjectIdentifier = mibBuilder.import_symbols(
    "ASN1", "Integer", "OctetString", "ObjectIdentifier"
)
(NamedValues,) = mibBuilder.import_symbols("ASN1-ENUMERATION", "NamedValues")
(
    SingleValueConstraint,
    ValueRangeConstraint,
    ConstraintsIntersection,
    ValueSizeConstraint,
    ConstraintsUnion,
) = mibBuilder.import_symbols(
    "ASN1-REFINEMENT",
    "SingleValueConstraint",
    "ValueRangeConstraint",
    "ConstraintsIntersection",
    "ValueSizeConstraint",
    "ConstraintsUnion",
)
(pysnmpModuleIDs,) = mibBuilder.import_symbols("PYSNMP-MIB", "pysnmpModuleIDs")
(snmpTargetAddrEntry,) = mibBuilder.import_symbols(
    "SNMP-TARGET-MIB", "snmpTargetAddrEntry"
)
NotificationGroup, ModuleCompliance = mibBuilder.import_symbols(
    "SNMPv2-CONF", "NotificationGroup", "ModuleCompliance"
)
(
    ModuleIdentity,
    iso,
    MibScalar,
    MibTable,
    MibTableRow,
    MibTableColumn,
    Gauge32,
    NotificationType,
    IpAddress,
    MibIdentifier,
    Unsigned32,
    Counter32,
    ObjectIdentity,
    Counter64,
    Bits,
    Integer32,
    TimeTicks,
) = mibBuilder.import_symbols(
    "SNMPv2-SMI",
    "ModuleIdentity",
    "iso",
    "MibScalar",
    "MibTable",
    "MibTableRow",
    "MibTableColumn",
    "Gauge32",
    "NotificationType",
    "IpAddress",
    "MibIdentifier",
    "Unsigned32",
    "Counter32",
    "ObjectIdentity",
    "Counter64",
    "Bits",
    "Integer32",
    "TimeTicks",
)
TextualConvention, DisplayString, TAddress = mibBuilder.import_symbols(
    "SNMPv2-TC", "TextualConvention", "DisplayString", "TAddress"
)
pysnmpSourceMIB = ModuleIdentity((1, 3, 6, 1, 4, 1, 20408, 3, 1, 8))
if mibBuilder.loadTexts:
    pysnmpSourceMIB.setRevisions(
        (
            "2017-04-14 00:00",
            "2015-01-16 00:00",
        )
    )
if mibBuilder.loadTexts:
    pysnmpSourceMIB.setLastUpdated("201704140000Z")
if mibBuilder.loadTexts:
    pysnmpSourceMIB.setOrganization("The PySNMP Project")
if mibBuilder.loadTexts:
    pysnmpSourceMIB.setContactInfo(
        "E-mail: LeXtudio Inc. <support@lextudio.com> GitHub: https://github.com/lextudio/pysnmp"
    )
if mibBuilder.loadTexts:
    pysnmpSourceMIB.setDescription(
        "This MIB module defines implementation specific objects that provide variable source transport endpoints feature to SNMP Engine and Standard SNMP Applications."
    )
pysnmpSourceMIBObjects = MibIdentifier((1, 3, 6, 1, 4, 1, 20408, 3, 1, 8, 1))
pysnmpSourceMIBConformance = MibIdentifier((1, 3, 6, 1, 4, 1, 20408, 3, 1, 8, 2))
snmpSourceAddrTable = MibTable(
    (1, 3, 6, 1, 4, 1, 20408, 3, 1, 8, 1, 1),
)
if mibBuilder.loadTexts:
    snmpSourceAddrTable.setStatus("current")
if mibBuilder.loadTexts:
    snmpSourceAddrTable.setDescription(
        "A table of transport addresses to be used as a source in the generation of SNMP messages. This table contains additional objects for the SNMP-TRANSPORT-ADDRESS::snmpSourceAddressTable."
    )
snmpSourceAddrEntry = MibTableRow(
    (1, 3, 6, 1, 4, 1, 20408, 3, 1, 8, 1, 1, 1),
)
snmpTargetAddrEntry.registerAugmentions(("PYSNMP-SOURCE-MIB", "snmpSourceAddrEntry"))
snmpSourceAddrEntry.setIndexNames(*snmpTargetAddrEntry.getIndexNames())
if mibBuilder.loadTexts:
    snmpSourceAddrEntry.setStatus("current")
if mibBuilder.loadTexts:
    snmpSourceAddrEntry.setDescription(
        "A transport address to be used as a source in the generation of SNMP operations. An entry containing additional management information applicable to a particular target."
    )
snmpSourceAddrTAddress = MibTableColumn(
    (1, 3, 6, 1, 4, 1, 20408, 3, 1, 8, 1, 1, 1, 1), TAddress()
).setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpSourceAddrTAddress.setStatus("current")
if mibBuilder.loadTexts:
    snmpSourceAddrTAddress.setDescription(
        "This object contains a transport address. The format of this address depends on the value of the snmpSourceAddrTDomain object."
    )
pysnmpSourceMIBCompliances = MibIdentifier((1, 3, 6, 1, 4, 1, 20408, 3, 1, 8, 2, 1))
pysnmpSourceMIBGroups = MibIdentifier((1, 3, 6, 1, 4, 1, 20408, 3, 1, 8, 2, 2))
mibBuilder.export_symbols(
    "PYSNMP-SOURCE-MIB",
    pysnmpSourceMIBConformance=pysnmpSourceMIBConformance,
    pysnmpSourceMIB=pysnmpSourceMIB,
    snmpSourceAddrTable=snmpSourceAddrTable,
    snmpSourceAddrEntry=snmpSourceAddrEntry,
    pysnmpSourceMIBGroups=pysnmpSourceMIBGroups,
    PYSNMP_MODULE_ID=pysnmpSourceMIB,
    snmpSourceAddrTAddress=snmpSourceAddrTAddress,
    pysnmpSourceMIBObjects=pysnmpSourceMIBObjects,
    pysnmpSourceMIBCompliances=pysnmpSourceMIBCompliances,
)
