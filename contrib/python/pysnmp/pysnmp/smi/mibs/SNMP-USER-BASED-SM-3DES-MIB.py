#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
# PySNMP MIB module SNMP-USER-BASED-SM-3DES-MIB (https://www.pysnmp.com/pysnmp)
# ASN.1 source file://asn1/SNMP-USER-BASED-SM-3DES-MIB
# Produced by pysmi-1.5.8 at Sat Nov  2 15:25:36 2024
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

(snmpPrivProtocols,) = mibBuilder.import_symbols(
    "SNMP-FRAMEWORK-MIB",
    "snmpPrivProtocols")

(ModuleCompliance,
 NotificationGroup) = mibBuilder.import_symbols(
    "SNMPv2-CONF",
    "ModuleCompliance",
    "NotificationGroup")

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

(AutonomousType,
 DisplayString,
 TextualConvention) = mibBuilder.import_symbols(
    "SNMPv2-TC",
    "AutonomousType",
    "DisplayString",
    "TextualConvention")


# MODULE-IDENTITY

snmpUsmMIB = ModuleIdentity(
    (1, 3, 6, 1, 6, 3, 15)
)
if mibBuilder.loadTexts:
    snmpUsmMIB.setRevisions(
        ("1999-10-06 00:00",)
    )
if mibBuilder.loadTexts:
    snmpUsmMIB.setLastUpdated("9910060000Z")
if mibBuilder.loadTexts:
    snmpUsmMIB.setOrganization("SNMPv3 Working Group")
if mibBuilder.loadTexts:
    snmpUsmMIB.setContactInfo("WG-email: snmpv3@lists.tislabs.com Subscribe: majordomo@lists.tislabs.com In msg body: subscribe snmpv3 Chair: Russ Mundy NAI Labs postal: 3060 Washington Rd Glenwood MD 21738 USA email: mundy@tislabs.com phone: +1-443-259-2307 Co-editor: David Reeder NAI Labs postal: 3060 Washington Road (Route 97) Glenwood, MD 21738 USA email: dreeder@tislabs.com phone: +1-443-259-2348 Co-editor: Olafur Gudmundsson NAI Labs postal: 3060 Washington Road (Route 97) Glenwood, MD 21738 USA email: ogud@tislabs.com phone: +1-443-259-2389 ")
if mibBuilder.loadTexts:
    snmpUsmMIB.setDescription("Extension to the SNMP User-based Security Model to support Triple-DES EDE in 'Outside' CBC (cipher-block chaining) Mode. ")


# Types definitions


# TEXTUAL-CONVENTIONS



# MIB Managed Objects in the order of their OIDs

_Usm3DESEDEPrivProtocol_ObjectIdentity = ObjectIdentity
usm3DESEDEPrivProtocol = _Usm3DESEDEPrivProtocol_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 10, 1, 2, 3)
)
if mibBuilder.loadTexts:
    usm3DESEDEPrivProtocol.setStatus("current")
if mibBuilder.loadTexts:
    usm3DESEDEPrivProtocol.setReference("- Data Encryption Standard, National Institute of Standards and Technology. Federal Information Processing Standard (FIPS) Publication 46-3, (1999, pending approval). Will supersede FIPS Publication 46-2. - Data Encryption Algorithm, American National Standards Institute. ANSI X3.92-1981, (December, 1980). - DES Modes of Operation, National Institute of Standards and Technology. Federal Information Processing Standard (FIPS) Publication 81, (December, 1980). - Data Encryption Algorithm - Modes of Operation, American National Standards Institute. ANSI X3.106-1983, (May 1983). ")
if mibBuilder.loadTexts:
    usm3DESEDEPrivProtocol.setDescription("The 3DES-EDE Symmetric Encryption Protocol.")

# Managed Objects groups


# Notification objects


# Notifications groups


# Agent capabilities


# Module compliance


# Export all MIB objects to the MIB builder

mibBuilder.export_symbols(
    "SNMP-USER-BASED-SM-3DES-MIB",
    **{"usm3DESEDEPrivProtocol": usm3DESEDEPrivProtocol,
       "snmpUsmMIB": snmpUsmMIB,
       "PYSNMP_MODULE_ID": snmpUsmMIB}
)
