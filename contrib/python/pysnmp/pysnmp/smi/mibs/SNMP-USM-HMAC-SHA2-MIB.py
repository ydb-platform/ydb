#
# PySNMP MIB module SNMP-USM-HMAC-SHA2-MIB (https://www.pysnmp.com/pysnmp)
# ASN.1 source file://asn1/SNMP-USM-HMAC-SHA2-MIB
# Produced by pysmi-1.5.8 at Sat Nov  2 15:25:40 2024
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

(snmpAuthProtocols,) = mibBuilder.import_symbols(
    "SNMP-FRAMEWORK-MIB",
    "snmpAuthProtocols")

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
 mib_2) = mibBuilder.import_symbols(
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
    "mib-2")

(DisplayString,
 TextualConvention) = mibBuilder.import_symbols(
    "SNMPv2-TC",
    "DisplayString",
    "TextualConvention")


# MODULE-IDENTITY

snmpUsmHmacSha2MIB = ModuleIdentity(
    (1, 3, 6, 1, 2, 1, 235)
)
if mibBuilder.loadTexts:
    snmpUsmHmacSha2MIB.setRevisions(
        ("2016-04-18 00:00",
         "2015-10-14 00:00")
    )
if mibBuilder.loadTexts:
    snmpUsmHmacSha2MIB.setLastUpdated("201604180000Z")
if mibBuilder.loadTexts:
    snmpUsmHmacSha2MIB.setOrganization("SNMPv3 Working Group")
if mibBuilder.loadTexts:
    snmpUsmHmacSha2MIB.setContactInfo("WG email: OPSAWG@ietf.org Subscribe: https://www.ietf.org/mailman/listinfo/opsawg Editor: Johannes Merkle secunet Security Networks Postal: Mergenthaler Allee 77 D-65760 Eschborn Germany Phone: +49 20154543091 Email: johannes.merkle@secunet.com Co-Editor: Manfred Lochter Bundesamt fuer Sicherheit in der Informationstechnik (BSI) Postal: Postfach 200363 D-53133 Bonn Germany Phone: +49 228 9582 5643 Email: manfred.lochter@bsi.bund.de")
if mibBuilder.loadTexts:
    snmpUsmHmacSha2MIB.setDescription("Definitions of Object Identities needed for the use of HMAC-SHA2 Authentication Protocols by SNMP's User-based Security Model. Copyright (c) 2016 IETF Trust and the persons identified as authors of the code. All rights reserved. Redistribution and use in source and binary forms, with or without modification, is permitted pursuant to, and subject to the license terms contained in, the Simplified BSD License set forth in Section 4.c of the IETF Trust's Legal Provisions Relating to IETF Documents (http://trustee.ietf.org/license-info).")


# Types definitions


# TEXTUAL-CONVENTIONS



# MIB Managed Objects in the order of their OIDs

_UsmHMAC128SHA224AuthProtocol_ObjectIdentity = ObjectIdentity
usmHMAC128SHA224AuthProtocol = _UsmHMAC128SHA224AuthProtocol_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 10, 1, 1, 4)
)
if mibBuilder.loadTexts:
    usmHMAC128SHA224AuthProtocol.setStatus("current")
if mibBuilder.loadTexts:
    usmHMAC128SHA224AuthProtocol.setReference("- Krawczyk, H., Bellare, M., and R. Canetti, HMAC: Keyed-Hashing for Message Authentication, RFC 2104. - National Institute of Standards and Technology, Secure Hash Standard (SHS), FIPS PUB 180-4, 2012.")
if mibBuilder.loadTexts:
    usmHMAC128SHA224AuthProtocol.setDescription("The Authentication Protocol usmHMAC128SHA224AuthProtocol uses HMAC-SHA-224 and truncates output to 128 bits.")
_UsmHMAC192SHA256AuthProtocol_ObjectIdentity = ObjectIdentity
usmHMAC192SHA256AuthProtocol = _UsmHMAC192SHA256AuthProtocol_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 10, 1, 1, 5)
)
if mibBuilder.loadTexts:
    usmHMAC192SHA256AuthProtocol.setStatus("current")
if mibBuilder.loadTexts:
    usmHMAC192SHA256AuthProtocol.setReference("- Krawczyk, H., Bellare, M., and R. Canetti, HMAC: Keyed-Hashing for Message Authentication, RFC 2104. - National Institute of Standards and Technology, Secure Hash Standard (SHS), FIPS PUB 180-4, 2012.")
if mibBuilder.loadTexts:
    usmHMAC192SHA256AuthProtocol.setDescription("The Authentication Protocol usmHMAC192SHA256AuthProtocol uses HMAC-SHA-256 and truncates output to 192 bits.")
_UsmHMAC256SHA384AuthProtocol_ObjectIdentity = ObjectIdentity
usmHMAC256SHA384AuthProtocol = _UsmHMAC256SHA384AuthProtocol_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 10, 1, 1, 6)
)
if mibBuilder.loadTexts:
    usmHMAC256SHA384AuthProtocol.setStatus("current")
if mibBuilder.loadTexts:
    usmHMAC256SHA384AuthProtocol.setReference("- Krawczyk, H., Bellare, M., and R. Canetti, HMAC: Keyed-Hashing for Message Authentication, RFC 2104. - National Institute of Standards and Technology, Secure Hash Standard (SHS), FIPS PUB 180-4, 2012.")
if mibBuilder.loadTexts:
    usmHMAC256SHA384AuthProtocol.setDescription("The Authentication Protocol usmHMAC256SHA384AuthProtocol uses HMAC-SHA-384 and truncates output to 256 bits.")
_UsmHMAC384SHA512AuthProtocol_ObjectIdentity = ObjectIdentity
usmHMAC384SHA512AuthProtocol = _UsmHMAC384SHA512AuthProtocol_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 10, 1, 1, 7)
)
if mibBuilder.loadTexts:
    usmHMAC384SHA512AuthProtocol.setStatus("current")
if mibBuilder.loadTexts:
    usmHMAC384SHA512AuthProtocol.setReference("- Krawczyk, H., Bellare, M., and R. Canetti, HMAC: Keyed-Hashing for Message Authentication, RFC 2104. - National Institute of Standards and Technology, Secure Hash Standard (SHS), FIPS PUB 180-4, 2012.")
if mibBuilder.loadTexts:
    usmHMAC384SHA512AuthProtocol.setDescription("The Authentication Protocol usmHMAC384SHA512AuthProtocol uses HMAC-SHA-512 and truncates output to 384 bits.")

# Managed Objects groups


# Notification objects


# Notifications groups


# Agent capabilities


# Module compliance


# Export all MIB objects to the MIB builder

mibBuilder.export_symbols(
    "SNMP-USM-HMAC-SHA2-MIB",
    **{"snmpUsmHmacSha2MIB": snmpUsmHmacSha2MIB,
       "usmHMAC128SHA224AuthProtocol": usmHMAC128SHA224AuthProtocol,
       "usmHMAC192SHA256AuthProtocol": usmHMAC192SHA256AuthProtocol,
       "usmHMAC256SHA384AuthProtocol": usmHMAC256SHA384AuthProtocol,
       "usmHMAC384SHA512AuthProtocol": usmHMAC384SHA512AuthProtocol,
       "PYSNMP_MODULE_ID": snmpUsmHmacSha2MIB}
)
