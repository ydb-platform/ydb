#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
# PySNMP MIB module PYSNMP-MIB (https://www.pysnmp.com/pysnmp)
# ASN.1 source http://mibs.pysnmp.com:80/asn1/PYSNMP-MIB
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
    enterprises,
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
    "enterprises",
    "TimeTicks",
)
TextualConvention, DisplayString = mibBuilder.import_symbols(
    "SNMPv2-TC", "TextualConvention", "DisplayString"
)
pysnmp = ModuleIdentity((1, 3, 6, 1, 4, 1, 20408))
if mibBuilder.loadTexts:
    pysnmp.setRevisions(
        (
            "2017-04-14 00:00",
            "2005-05-14 00:00",
        )
    )
if mibBuilder.loadTexts:
    pysnmp.setLastUpdated("201704140000Z")
if mibBuilder.loadTexts:
    pysnmp.setOrganization("The PySNMP Project")
if mibBuilder.loadTexts:
    pysnmp.setContactInfo(
        "E-mail: LeXtudio Inc. <support@lextudio.com> GitHub: https://github.com/lextudio/pysnmp"
    )
if mibBuilder.loadTexts:
    pysnmp.setDescription("PySNMP top-level MIB tree infrastructure")
pysnmpObjects = MibIdentifier((1, 3, 6, 1, 4, 1, 20408, 1))
pysnmpExamples = MibIdentifier((1, 3, 6, 1, 4, 1, 20408, 2))
pysnmpEnumerations = MibIdentifier((1, 3, 6, 1, 4, 1, 20408, 3))
pysnmpModuleIDs = MibIdentifier((1, 3, 6, 1, 4, 1, 20408, 3, 1))
pysnmpAgentOIDs = MibIdentifier((1, 3, 6, 1, 4, 1, 20408, 3, 2))
pysnmpDomains = MibIdentifier((1, 3, 6, 1, 4, 1, 20408, 3, 3))
pysnmpExperimental = MibIdentifier((1, 3, 6, 1, 4, 1, 20408, 9999))
pysnmpNotificationPrefix = MibIdentifier((1, 3, 6, 1, 4, 1, 20408, 4))
pysnmpNotifications = MibIdentifier((1, 3, 6, 1, 4, 1, 20408, 4, 0))
pysnmpNotificationObjects = MibIdentifier((1, 3, 6, 1, 4, 1, 20408, 4, 1))
pysnmpConformance = MibIdentifier((1, 3, 6, 1, 4, 1, 20408, 5))
pysnmpCompliances = MibIdentifier((1, 3, 6, 1, 4, 1, 20408, 5, 1))
pysnmpGroups = MibIdentifier((1, 3, 6, 1, 4, 1, 20408, 5, 2))
mibBuilder.export_symbols(
    "PYSNMP-MIB",
    pysnmpCompliances=pysnmpCompliances,
    pysnmpObjects=pysnmpObjects,
    pysnmpNotificationPrefix=pysnmpNotificationPrefix,
    pysnmpModuleIDs=pysnmpModuleIDs,
    pysnmpGroups=pysnmpGroups,
    pysnmpNotificationObjects=pysnmpNotificationObjects,
    pysnmp=pysnmp,
    pysnmpExperimental=pysnmpExperimental,
    pysnmpNotifications=pysnmpNotifications,
    PYSNMP_MODULE_ID=pysnmp,
    pysnmpEnumerations=pysnmpEnumerations,
    pysnmpDomains=pysnmpDomains,
    pysnmpAgentOIDs=pysnmpAgentOIDs,
    pysnmpConformance=pysnmpConformance,
    pysnmpExamples=pysnmpExamples,
)
