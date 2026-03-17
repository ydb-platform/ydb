#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
#
# PySNMP MIB module SNMP-MPD-MIB (https://www.pysnmp.com/pysnmp)
# ASN.1 source file://asn1/SNMP-MPD-MIB
# Produced by pysmi-1.5.8 at Sat Nov  2 15:25:32 2024
# On host MacBook-Pro.local platform Darwin version 24.1.0 by user lextm
# Using Python version 3.12.0 (main, Nov 14 2023, 23:52:11) [Clang 15.0.0 (clang-1500.0.40.1)]
#

# Import base ASN.1 objects even if this MIB does not use it

(Integer,
 OctetString,
 ObjectIdentifier) = mibBuilder.import_symbols(
    "ASN1",
    "Integer",
    "OctetString",
    "ObjectIdentifier")

(NamedValues,) = mibBuilder.import_symbols(
    "ASN1-ENUMERATION",
    "NamedValues")
(ConstraintsIntersection,
 ConstraintsUnion,
 SingleValueConstraint,
 ValueRangeConstraint,
 ValueSizeConstraint) = mibBuilder.import_symbols(
    "ASN1-REFINEMENT",
    "ConstraintsIntersection",
    "ConstraintsUnion",
    "SingleValueConstraint",
    "ValueRangeConstraint",
    "ValueSizeConstraint")

# Import SMI symbols from the MIBs this MIB depends on

(ModuleCompliance,
 NotificationGroup,
 ObjectGroup) = mibBuilder.import_symbols(
    "SNMPv2-CONF",
    "ModuleCompliance",
    "NotificationGroup",
    "ObjectGroup")

(Bits,
 Counter32,
 Counter64,
 Gauge32,
 Integer32,
 IpAddress,
 ModuleIdentity,
 MibIdentifier,
 NotificationType,
 ObjectIdentity,
 MibScalar,
 MibTable,
 MibTableRow,
 MibTableColumn,
 TimeTicks,
 Unsigned32,
 iso,
 snmpModules) = mibBuilder.import_symbols(
    "SNMPv2-SMI",
    "Bits",
    "Counter32",
    "Counter64",
    "Gauge32",
    "Integer32",
    "IpAddress",
    "ModuleIdentity",
    "MibIdentifier",
    "NotificationType",
    "ObjectIdentity",
    "MibScalar",
    "MibTable",
    "MibTableRow",
    "MibTableColumn",
    "TimeTicks",
    "Unsigned32",
    "iso",
    "snmpModules")

(DisplayString,
 TextualConvention) = mibBuilder.import_symbols(
    "SNMPv2-TC",
    "DisplayString",
    "TextualConvention")


# MODULE-IDENTITY

snmpMPDMIB = ModuleIdentity(
    (1, 3, 6, 1, 6, 3, 11)
)
if mibBuilder.loadTexts:
    snmpMPDMIB.setRevisions(
        ("2002-10-14 00:00",
         "1999-05-04 16:36",
         "1997-09-30 00:00")
    )
if mibBuilder.loadTexts:
    snmpMPDMIB.setLastUpdated("200210140000Z")
if mibBuilder.loadTexts:
    snmpMPDMIB.setOrganization("SNMPv3 Working Group")
if mibBuilder.loadTexts:
    snmpMPDMIB.setContactInfo("WG-EMail: snmpv3@lists.tislabs.com Subscribe: snmpv3-request@lists.tislabs.com Co-Chair: Russ Mundy Network Associates Laboratories postal: 15204 Omega Drive, Suite 300 Rockville, MD 20850-4601 USA EMail: mundy@tislabs.com phone: +1 301-947-7107 Co-Chair & Co-editor: David Harrington Enterasys Networks postal: 35 Industrial Way P. O. Box 5005 Rochester NH 03866-5005 USA EMail: dbh@enterasys.com phone: +1 603-337-2614 Co-editor: Jeffrey Case SNMP Research, Inc. postal: 3001 Kimberlin Heights Road Knoxville, TN 37920-9716 USA EMail: case@snmp.com phone: +1 423-573-1434 Co-editor: Randy Presuhn BMC Software, Inc. postal: 2141 North First Street San Jose, CA 95131 USA EMail: randy_presuhn@bmc.com phone: +1 408-546-1006 Co-editor: Bert Wijnen Lucent Technologies postal: Schagen 33 3461 GL Linschoten Netherlands EMail: bwijnen@lucent.com phone: +31 348-680-485 ")
if mibBuilder.loadTexts:
    snmpMPDMIB.setDescription("The MIB for Message Processing and Dispatching Copyright (C) The Internet Society (2002). This version of this MIB module is part of RFC 3412; see the RFC itself for full legal notices. ")


# Types definitions


# TEXTUAL-CONVENTIONS



# MIB Managed Objects in the order of their OIDs

_SnmpMPDAdmin_ObjectIdentity = ObjectIdentity
snmpMPDAdmin = _SnmpMPDAdmin_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 11, 1)
)
_SnmpMPDMIBObjects_ObjectIdentity = ObjectIdentity
snmpMPDMIBObjects = _SnmpMPDMIBObjects_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 11, 2)
)
_SnmpMPDStats_ObjectIdentity = ObjectIdentity
snmpMPDStats = _SnmpMPDStats_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 11, 2, 1)
)
_SnmpUnknownSecurityModels_Type = Counter32
_SnmpUnknownSecurityModels_Object = MibScalar
snmpUnknownSecurityModels = _SnmpUnknownSecurityModels_Object(
    (1, 3, 6, 1, 6, 3, 11, 2, 1, 1),
    _SnmpUnknownSecurityModels_Type()
)
snmpUnknownSecurityModels.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpUnknownSecurityModels.setStatus("current")
if mibBuilder.loadTexts:
    snmpUnknownSecurityModels.setDescription("The total number of packets received by the SNMP engine which were dropped because they referenced a securityModel that was not known to or supported by the SNMP engine. ")
_SnmpInvalidMsgs_Type = Counter32
_SnmpInvalidMsgs_Object = MibScalar
snmpInvalidMsgs = _SnmpInvalidMsgs_Object(
    (1, 3, 6, 1, 6, 3, 11, 2, 1, 2),
    _SnmpInvalidMsgs_Type()
)
snmpInvalidMsgs.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpInvalidMsgs.setStatus("current")
if mibBuilder.loadTexts:
    snmpInvalidMsgs.setDescription("The total number of packets received by the SNMP engine which were dropped because there were invalid or inconsistent components in the SNMP message. ")
_SnmpUnknownPDUHandlers_Type = Counter32
_SnmpUnknownPDUHandlers_Object = MibScalar
snmpUnknownPDUHandlers = _SnmpUnknownPDUHandlers_Object(
    (1, 3, 6, 1, 6, 3, 11, 2, 1, 3),
    _SnmpUnknownPDUHandlers_Type()
)
snmpUnknownPDUHandlers.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpUnknownPDUHandlers.setStatus("current")
if mibBuilder.loadTexts:
    snmpUnknownPDUHandlers.setDescription("The total number of packets received by the SNMP engine which were dropped because the PDU contained in the packet could not be passed to an application responsible for handling the pduType, e.g. no SNMP application had registered for the proper combination of the contextEngineID and the pduType. ")
_SnmpMPDMIBConformance_ObjectIdentity = ObjectIdentity
snmpMPDMIBConformance = _SnmpMPDMIBConformance_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 11, 3)
)
_SnmpMPDMIBCompliances_ObjectIdentity = ObjectIdentity
snmpMPDMIBCompliances = _SnmpMPDMIBCompliances_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 11, 3, 1)
)
_SnmpMPDMIBGroups_ObjectIdentity = ObjectIdentity
snmpMPDMIBGroups = _SnmpMPDMIBGroups_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 11, 3, 2)
)

# Managed Objects groups

snmpMPDGroup = ObjectGroup(
    (1, 3, 6, 1, 6, 3, 11, 3, 2, 1)
)
snmpMPDGroup.setObjects(
      *(("SNMP-MPD-MIB", "snmpUnknownSecurityModels"),
        ("SNMP-MPD-MIB", "snmpInvalidMsgs"),
        ("SNMP-MPD-MIB", "snmpUnknownPDUHandlers"))
)
if mibBuilder.loadTexts:
    snmpMPDGroup.setStatus("current")
if mibBuilder.loadTexts:
    snmpMPDGroup.setDescription("A collection of objects providing for remote monitoring of the SNMP Message Processing and Dispatching process. ")


# Notification objects


# Notifications groups


# Agent capabilities


# Module compliance

snmpMPDCompliance = ModuleCompliance(
    (1, 3, 6, 1, 6, 3, 11, 3, 1, 1)
)
snmpMPDCompliance.setObjects(
    ("SNMP-MPD-MIB", "snmpMPDGroup")
)
if mibBuilder.loadTexts:
    snmpMPDCompliance.setStatus(
        "current"
    )
if mibBuilder.loadTexts:
    snmpMPDCompliance.setDescription("The compliance statement for SNMP entities which implement the SNMP-MPD-MIB. ")


# Export all MIB objects to the MIB builder

mibBuilder.export_symbols(
    "SNMP-MPD-MIB",
    **{"snmpMPDMIB": snmpMPDMIB,
       "snmpMPDAdmin": snmpMPDAdmin,
       "snmpMPDMIBObjects": snmpMPDMIBObjects,
       "snmpMPDStats": snmpMPDStats,
       "snmpUnknownSecurityModels": snmpUnknownSecurityModels,
       "snmpInvalidMsgs": snmpInvalidMsgs,
       "snmpUnknownPDUHandlers": snmpUnknownPDUHandlers,
       "snmpMPDMIBConformance": snmpMPDMIBConformance,
       "snmpMPDMIBCompliances": snmpMPDMIBCompliances,
       "snmpMPDCompliance": snmpMPDCompliance,
       "snmpMPDMIBGroups": snmpMPDMIBGroups,
       "snmpMPDGroup": snmpMPDGroup,
       "PYSNMP_MODULE_ID": snmpMPDMIB}
)
