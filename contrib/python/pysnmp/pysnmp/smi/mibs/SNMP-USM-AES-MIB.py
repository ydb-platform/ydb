#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
# PySNMP MIB module SNMP-USM-AES-MIB (https://www.pysnmp.com/pysnmp)
# ASN.1 source file://asn1/SNMP-USM-AES-MIB
# Produced by pysmi-1.5.8 at Sat Nov  2 15:25:39 2024
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

(DisplayString,
 TextualConvention) = mibBuilder.import_symbols(
    "SNMPv2-TC",
    "DisplayString",
    "TextualConvention")


# MODULE-IDENTITY

snmpUsmAesMIB = ModuleIdentity(
    (1, 3, 6, 1, 6, 3, 20)
)
if mibBuilder.loadTexts:
    snmpUsmAesMIB.setRevisions(
        ("2004-06-14 00:00",)
    )
if mibBuilder.loadTexts:
    snmpUsmAesMIB.setLastUpdated("200406140000Z")
if mibBuilder.loadTexts:
    snmpUsmAesMIB.setOrganization("IETF")
if mibBuilder.loadTexts:
    snmpUsmAesMIB.setContactInfo("Uri Blumenthal Lucent Technologies / Bell Labs 67 Whippany Rd. 14D-318 Whippany, NJ 07981, USA 973-386-2163 uri@bell-labs.com Fabio Maino Andiamo Systems, Inc. 375 East Tasman Drive San Jose, CA 95134, USA 408-853-7530 fmaino@andiamo.com Keith McCloghrie Cisco Systems, Inc. 170 West Tasman Drive San Jose, CA 95134-1706, USA 408-526-5260 kzm@cisco.com")
if mibBuilder.loadTexts:
    snmpUsmAesMIB.setDescription("Definitions of Object Identities needed for the use of AES by SNMP's User-based Security Model. Copyright (C) The Internet Society (2004). This version of this MIB module is part of RFC 3826; see the RFC itself for full legal notices. Supplementary information may be available on http://www.ietf.org/copyrights/ianamib.html.")


# Types definitions


# TEXTUAL-CONVENTIONS



# MIB Managed Objects in the order of their OIDs

_UsmAesCfb128Protocol_ObjectIdentity = ObjectIdentity
usmAesCfb128Protocol = _UsmAesCfb128Protocol_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 10, 1, 2, 4)
)
if mibBuilder.loadTexts:
    usmAesCfb128Protocol.setStatus("current")
if mibBuilder.loadTexts:
    usmAesCfb128Protocol.setReference("- Specification for the ADVANCED ENCRYPTION STANDARD. Federal Information Processing Standard (FIPS) Publication 197. (November 2001). - Dworkin, M., NIST Recommendation for Block Cipher Modes of Operation, Methods and Techniques. NIST Special Publication 800-38A (December 2001). ")
if mibBuilder.loadTexts:
    usmAesCfb128Protocol.setDescription("The CFB128-AES-128 Privacy Protocol.")

# Managed Objects groups


# Notification objects


# Notifications groups


# Agent capabilities


# Module compliance


# Export all MIB objects to the MIB builder

mibBuilder.export_symbols(
    "SNMP-USM-AES-MIB",
    **{"usmAesCfb128Protocol": usmAesCfb128Protocol,
       "snmpUsmAesMIB": snmpUsmAesMIB,
       "PYSNMP_MODULE_ID": snmpUsmAesMIB}
)
