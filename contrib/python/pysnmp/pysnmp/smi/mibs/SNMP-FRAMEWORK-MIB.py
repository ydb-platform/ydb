#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
# PySNMP MIB module SNMP-FRAMEWORK-MIB (https://www.pysnmp.com/pysnmp)
# ASN.1 source file://asn1/SNMP-FRAMEWORK-MIB
# Produced by pysmi-1.5.8 at Sat Nov  2 15:25:25 2024
# On host MacBook-Pro.local platform Darwin version 24.1.0 by user lextm
# Using Python version 3.12.0 (main, Nov 14 2023, 23:52:11) [Clang 15.0.0 (clang-1500.0.40.1)]
#
# IMPORTANT: customization

import platform
import time

try:
    import os
except ImportError:
    pass

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

snmpFrameworkMIB = ModuleIdentity(
    (1, 3, 6, 1, 6, 3, 10)
)
if mibBuilder.loadTexts:
    snmpFrameworkMIB.setRevisions(
        ("2002-10-14 00:00",
         "1999-01-19 00:00",
         "1997-11-20 00:00")
    )
if mibBuilder.loadTexts:
    snmpFrameworkMIB.setLastUpdated("200210140000Z")
if mibBuilder.loadTexts:
    snmpFrameworkMIB.setOrganization("SNMPv3 Working Group")
if mibBuilder.loadTexts:
    snmpFrameworkMIB.setContactInfo("WG-EMail: snmpv3@lists.tislabs.com Subscribe: snmpv3-request@lists.tislabs.com Co-Chair: Russ Mundy Network Associates Laboratories postal: 15204 Omega Drive, Suite 300 Rockville, MD 20850-4601 USA EMail: mundy@tislabs.com phone: +1 301-947-7107 Co-Chair & Co-editor: David Harrington Enterasys Networks postal: 35 Industrial Way P. O. Box 5005 Rochester, New Hampshire 03866-5005 USA EMail: dbh@enterasys.com phone: +1 603-337-2614 Co-editor: Randy Presuhn BMC Software, Inc. postal: 2141 North First Street San Jose, California 95131 USA EMail: randy_presuhn@bmc.com phone: +1 408-546-1006 Co-editor: Bert Wijnen Lucent Technologies postal: Schagen 33 3461 GL Linschoten Netherlands EMail: bwijnen@lucent.com phone: +31 348-680-485 ")
if mibBuilder.loadTexts:
    snmpFrameworkMIB.setDescription("The SNMP Management Architecture MIB Copyright (C) The Internet Society (2002). This version of this MIB module is part of RFC 3411; see the RFC itself for full legal notices. ")


# Types definitions


# TEXTUAL-CONVENTIONS



class SnmpEngineID(TextualConvention, OctetString):
    status = "current"
    subtypeSpec = OctetString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(5, 32),
    )

    if mibBuilder.loadTexts:
        description = "An SNMP engine's administratively-unique identifier. Objects of this type are for identification, not for addressing, even though it is possible that an address may have been used in the generation of a specific value. The value for this object may not be all zeros or all 'ff'H or the empty (zero length) string. The initial value for this object may be configured via an operator console entry or via an algorithmic function. In the latter case, the following example algorithm is recommended. In cases where there are multiple engines on the same system, the use of this algorithm is NOT appropriate, as it would result in all of those engines ending up with the same ID value. 1) The very first bit is used to indicate how the rest of the data is composed. 0 - as defined by enterprise using former methods that existed before SNMPv3. See item 2 below. 1 - as defined by this architecture, see item 3 below. Note that this allows existing uses of the engineID (also known as AgentID [RFC1910]) to co-exist with any new uses. 2) The snmpEngineID has a length of 12 octets. The first four octets are set to the binary equivalent of the agent's SNMP management private enterprise number as assigned by the Internet Assigned Numbers Authority (IANA). For example, if Acme Networks has been assigned { enterprises 696 }, the first four octets would be assigned '000002b8'H. The remaining eight octets are determined via one or more enterprise-specific methods. Such methods must be designed so as to maximize the possibility that the value of this object will be unique in the agent's administrative domain. For example, it may be the IP address of the SNMP entity, or the MAC address of one of the interfaces, with each address suitably padded with random octets. If multiple methods are defined, then it is recommended that the first octet indicate the method being used and the remaining octets be a function of the method. 3) The length of the octet string varies. The first four octets are set to the binary equivalent of the agent's SNMP management private enterprise number as assigned by the Internet Assigned Numbers Authority (IANA). For example, if Acme Networks has been assigned { enterprises 696 }, the first four octets would be assigned '000002b8'H. The very first bit is set to 1. For example, the above value for Acme Networks now changes to be '800002b8'H. The fifth octet indicates how the rest (6th and following octets) are formatted. The values for the fifth octet are: 0 - reserved, unused. 1 - IPv4 address (4 octets) lowest non-special IP address 2 - IPv6 address (16 octets) lowest non-special IP address 3 - MAC address (6 octets) lowest IEEE MAC address, canonical order 4 - Text, administratively assigned Maximum remaining length 27 5 - Octets, administratively assigned Maximum remaining length 27 6-127 - reserved, unused 128-255 - as defined by the enterprise Maximum remaining length 27 "

    # IMPORTANT: customization
    defaultValue = [128, 0, 79, 184, 5]
    try:
        # Attempt to base engine ID on local system name and properties
        defaultValue += [ord(x) for x in platform.node()[:16]]
    except Exception:
        pass
    try:
        # Attempt to base engine ID on PID
        defaultValue += [os.getpid() >> 8 & 0xFF, os.getpid() & 0xFF]
    except Exception:
        pass
    # add pseudo-random text ID
    defaultValue += [id(defaultValue) >> 8 & 0xFF, id(defaultValue) & 0xFF]
    defaultValue = OctetString(defaultValue).asOctets()

class SnmpSecurityModel(TextualConvention, Integer32):
    status = "current"
    subtypeSpec = Integer32.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueRangeConstraint(0, 2147483647),
    )

    if mibBuilder.loadTexts:
        description = "An identifier that uniquely identifies a Security Model of the Security Subsystem within this SNMP Management Architecture. The values for securityModel are allocated as follows: - The zero value does not identify any particular security model. - Values between 1 and 255, inclusive, are reserved for standards-track Security Models and are managed by the Internet Assigned Numbers Authority (IANA). - Values greater than 255 are allocated to enterprise-specific Security Models. An enterprise-specific securityModel value is defined to be: enterpriseID * 256 + security model within enterprise For example, the fourth Security Model defined by the enterprise whose enterpriseID is 1 would be 259. This scheme for allocation of securityModel values allows for a maximum of 255 standards- based Security Models, and for a maximum of 256 Security Models per enterprise. It is believed that the assignment of new securityModel values will be rare in practice because the larger the number of simultaneously utilized Security Models, the larger the chance that interoperability will suffer. Consequently, it is believed that such a range will be sufficient. In the unlikely event that the standards committee finds this number to be insufficient over time, an enterprise number can be allocated to obtain an additional 256 possible values. Note that the most significant bit must be zero; hence, there are 23 bits allocated for various organizations to design and define non-standard securityModels. This limits the ability to define new proprietary implementations of Security Models to the first 8,388,608 enterprises. It is worthwhile to note that, in its encoded form, the securityModel value will normally require only a single byte since, in practice, the leftmost bits will be zero for most messages and sign extension is suppressed by the encoding rules. As of this writing, there are several values of securityModel defined for use with SNMP or reserved for use with supporting MIB objects. They are as follows: 0 reserved for 'any' 1 reserved for SNMPv1 2 reserved for SNMPv2c 3 User-Based Security Model (USM) "


class SnmpMessageProcessingModel(TextualConvention, Integer32):
    status = "current"
    subtypeSpec = Integer32.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueRangeConstraint(0, 2147483647),
    )

    if mibBuilder.loadTexts:
        description = "An identifier that uniquely identifies a Message Processing Model of the Message Processing Subsystem within this SNMP Management Architecture. The values for messageProcessingModel are allocated as follows: - Values between 0 and 255, inclusive, are reserved for standards-track Message Processing Models and are managed by the Internet Assigned Numbers Authority (IANA). - Values greater than 255 are allocated to enterprise-specific Message Processing Models. An enterprise messageProcessingModel value is defined to be: enterpriseID * 256 + messageProcessingModel within enterprise For example, the fourth Message Processing Model defined by the enterprise whose enterpriseID is 1 would be 259. This scheme for allocating messageProcessingModel values allows for a maximum of 255 standards- based Message Processing Models, and for a maximum of 256 Message Processing Models per enterprise. It is believed that the assignment of new messageProcessingModel values will be rare in practice because the larger the number of simultaneously utilized Message Processing Models, the larger the chance that interoperability will suffer. It is believed that such a range will be sufficient. In the unlikely event that the standards committee finds this number to be insufficient over time, an enterprise number can be allocated to obtain an additional 256 possible values. Note that the most significant bit must be zero; hence, there are 23 bits allocated for various organizations to design and define non-standard messageProcessingModels. This limits the ability to define new proprietary implementations of Message Processing Models to the first 8,388,608 enterprises. It is worthwhile to note that, in its encoded form, the messageProcessingModel value will normally require only a single byte since, in practice, the leftmost bits will be zero for most messages and sign extension is suppressed by the encoding rules. As of this writing, there are several values of messageProcessingModel defined for use with SNMP. They are as follows: 0 reserved for SNMPv1 1 reserved for SNMPv2c 2 reserved for SNMPv2u and SNMPv2* 3 reserved for SNMPv3 "


class SnmpSecurityLevel(TextualConvention, Integer32):
    status = "current"
    subtypeSpec = Integer32.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        SingleValueConstraint(
            *(1,
              2,
              3)
        )
    )
    namedValues = NamedValues(
        *(("noAuthNoPriv", 1),
          ("authNoPriv", 2),
          ("authPriv", 3))
    )

    if mibBuilder.loadTexts:
        description = "A Level of Security at which SNMP messages can be sent or with which operations are being processed; in particular, one of: noAuthNoPriv - without authentication and without privacy, authNoPriv - with authentication but without privacy, authPriv - with authentication and with privacy. These three values are ordered such that noAuthNoPriv is less than authNoPriv and authNoPriv is less than authPriv. "


class SnmpAdminString(TextualConvention, OctetString):
    status = "current"
    displayHint = "255t"
    subtypeSpec = OctetString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(0, 255),
    )

    if mibBuilder.loadTexts:
        description = "An octet string containing administrative information, preferably in human-readable form. To facilitate internationalization, this information is represented using the ISO/IEC IS 10646-1 character set, encoded as an octet string using the UTF-8 transformation format described in [RFC2279]. Since additional code points are added by amendments to the 10646 standard from time to time, implementations must be prepared to encounter any code point from 0x00000000 to 0x7fffffff. Byte sequences that do not correspond to the valid UTF-8 encoding of a code point or are outside this range are prohibited. The use of control codes should be avoided. When it is necessary to represent a newline, the control code sequence CR LF should be used. The use of leading or trailing white space should be avoided. For code points not directly supported by user interface hardware or software, an alternative means of entry and display, such as hexadecimal, may be provided. For information encoded in 7-bit US-ASCII, the UTF-8 encoding is identical to the US-ASCII encoding. UTF-8 may require multiple bytes to represent a single character / code point; thus the length of this object in octets may be different from the number of characters encoded. Similarly, size constraints refer to the number of encoded octets, not the number of characters represented by an encoding. Note that when this TC is used for an object that is used or envisioned to be used as an index, then a SIZE restriction MUST be specified so that the number of sub-identifiers for any object instance does not exceed the limit of 128, as defined by [RFC3416]. Note that the size of an SnmpAdminString object is measured in octets, not characters. "

    # IMPORTANT: customization
    encoding = "utf-8"

# MIB Managed Objects in the order of their OIDs

_SnmpFrameworkAdmin_ObjectIdentity = ObjectIdentity
snmpFrameworkAdmin = _SnmpFrameworkAdmin_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 10, 1)
)
_SnmpAuthProtocols_ObjectIdentity = ObjectIdentity
snmpAuthProtocols = _SnmpAuthProtocols_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 10, 1, 1)
)
if mibBuilder.loadTexts:
    snmpAuthProtocols.setStatus("current")
if mibBuilder.loadTexts:
    snmpAuthProtocols.setDescription("Registration point for standards-track authentication protocols used in SNMP Management Frameworks. ")
_SnmpPrivProtocols_ObjectIdentity = ObjectIdentity
snmpPrivProtocols = _SnmpPrivProtocols_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 10, 1, 2)
)
if mibBuilder.loadTexts:
    snmpPrivProtocols.setStatus("current")
if mibBuilder.loadTexts:
    snmpPrivProtocols.setDescription("Registration point for standards-track privacy protocols used in SNMP Management Frameworks. ")
_SnmpFrameworkMIBObjects_ObjectIdentity = ObjectIdentity
snmpFrameworkMIBObjects = _SnmpFrameworkMIBObjects_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 10, 2)
)
_SnmpEngine_ObjectIdentity = ObjectIdentity
snmpEngine = _SnmpEngine_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 10, 2, 1)
)
_SnmpEngineID_Type = SnmpEngineID
_SnmpEngineID_Object = MibScalar
snmpEngineID = _SnmpEngineID_Object(
    (1, 3, 6, 1, 6, 3, 10, 2, 1, 1),
    _SnmpEngineID_Type()
)
snmpEngineID.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpEngineID.setStatus("current")
if mibBuilder.loadTexts:
    snmpEngineID.setDescription("An SNMP engine's administratively-unique identifier. This information SHOULD be stored in non-volatile storage so that it remains constant across re-initializations of the SNMP engine. ")


class _SnmpEngineBoots_Type(Integer32):
    """Custom type snmpEngineBoots based on Integer32"""
    subtypeSpec = Integer32.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueRangeConstraint(1, 2147483647),
    )


_SnmpEngineBoots_Type.__name__ = "Integer32"
_SnmpEngineBoots_Object = MibScalar
snmpEngineBoots = _SnmpEngineBoots_Object(
    (1, 3, 6, 1, 6, 3, 10, 2, 1, 2),
    _SnmpEngineBoots_Type()
)
snmpEngineBoots.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpEngineBoots.setStatus("current")
if mibBuilder.loadTexts:
    snmpEngineBoots.setDescription("The number of times that the SNMP engine has (re-)initialized itself since snmpEngineID was last configured. ")


class _SnmpEngineTime_Type(Integer32):
    """Custom type snmpEngineTime based on Integer32"""
    subtypeSpec = Integer32.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueRangeConstraint(0, 2147483647),
    )

    # IMPORTANT: customization
    def clone(self, *args, **kwargs):
        if not args:
            try:
                args = (time.time() - self,)
            except Exception:
                pass
        return Integer32.clone(self, *args, **kwargs)


_SnmpEngineTime_Type.__name__ = "Integer32"
_SnmpEngineTime_Object = MibScalar
snmpEngineTime = _SnmpEngineTime_Object(
    (1, 3, 6, 1, 6, 3, 10, 2, 1, 3),
    _SnmpEngineTime_Type()
)
snmpEngineTime.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpEngineTime.setStatus("current")
if mibBuilder.loadTexts:
    snmpEngineTime.setUnits("seconds")
if mibBuilder.loadTexts:
    snmpEngineTime.setDescription("The number of seconds since the value of the snmpEngineBoots object last changed. When incrementing this object's value would cause it to exceed its maximum, snmpEngineBoots is incremented as if a re-initialization had occurred, and this object's value consequently reverts to zero. ")


class _SnmpEngineMaxMessageSize_Type(Integer32):
    """Custom type snmpEngineMaxMessageSize based on Integer32"""
    subtypeSpec = Integer32.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueRangeConstraint(484, 2147483647),
    )


_SnmpEngineMaxMessageSize_Type.__name__ = "Integer32"
_SnmpEngineMaxMessageSize_Object = MibScalar
snmpEngineMaxMessageSize = _SnmpEngineMaxMessageSize_Object(
    (1, 3, 6, 1, 6, 3, 10, 2, 1, 4),
    _SnmpEngineMaxMessageSize_Type()
)
snmpEngineMaxMessageSize.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpEngineMaxMessageSize.setStatus("current")
if mibBuilder.loadTexts:
    snmpEngineMaxMessageSize.setDescription("The maximum length in octets of an SNMP message which this SNMP engine can send or receive and process, determined as the minimum of the maximum message size values supported among all of the transports available to and supported by the engine. ")
_SnmpFrameworkMIBConformance_ObjectIdentity = ObjectIdentity
snmpFrameworkMIBConformance = _SnmpFrameworkMIBConformance_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 10, 3)
)
_SnmpFrameworkMIBCompliances_ObjectIdentity = ObjectIdentity
snmpFrameworkMIBCompliances = _SnmpFrameworkMIBCompliances_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 10, 3, 1)
)
_SnmpFrameworkMIBGroups_ObjectIdentity = ObjectIdentity
snmpFrameworkMIBGroups = _SnmpFrameworkMIBGroups_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 10, 3, 2)
)

# Managed Objects groups

snmpEngineGroup = ObjectGroup(
    (1, 3, 6, 1, 6, 3, 10, 3, 2, 1)
)
snmpEngineGroup.setObjects(
      *(("SNMP-FRAMEWORK-MIB", "snmpEngineID"),
        ("SNMP-FRAMEWORK-MIB", "snmpEngineBoots"),
        ("SNMP-FRAMEWORK-MIB", "snmpEngineTime"),
        ("SNMP-FRAMEWORK-MIB", "snmpEngineMaxMessageSize"))
)
if mibBuilder.loadTexts:
    snmpEngineGroup.setStatus("current")
if mibBuilder.loadTexts:
    snmpEngineGroup.setDescription("A collection of objects for identifying and determining the configuration and current timeliness values of an SNMP engine. ")


# Notification objects


# Notifications groups


# Agent capabilities


# Module compliance

snmpFrameworkMIBCompliance = ModuleCompliance(
    (1, 3, 6, 1, 6, 3, 10, 3, 1, 1)
)
snmpFrameworkMIBCompliance.setObjects(
    ("SNMP-FRAMEWORK-MIB", "snmpEngineGroup")
)
if mibBuilder.loadTexts:
    snmpFrameworkMIBCompliance.setStatus(
        "current"
    )
if mibBuilder.loadTexts:
    snmpFrameworkMIBCompliance.setDescription("The compliance statement for SNMP engines which implement the SNMP Management Framework MIB. ")


# Export all MIB objects to the MIB builder

mibBuilder.export_symbols(
    "SNMP-FRAMEWORK-MIB",
    **{"SnmpEngineID": SnmpEngineID,
       "SnmpSecurityModel": SnmpSecurityModel,
       "SnmpMessageProcessingModel": SnmpMessageProcessingModel,
       "SnmpSecurityLevel": SnmpSecurityLevel,
       "SnmpAdminString": SnmpAdminString,
       "snmpFrameworkMIB": snmpFrameworkMIB,
       "snmpFrameworkAdmin": snmpFrameworkAdmin,
       "snmpAuthProtocols": snmpAuthProtocols,
       "snmpPrivProtocols": snmpPrivProtocols,
       "snmpFrameworkMIBObjects": snmpFrameworkMIBObjects,
       "snmpEngine": snmpEngine,
       "snmpEngineID": snmpEngineID,
       "snmpEngineBoots": snmpEngineBoots,
       "snmpEngineTime": snmpEngineTime,
       "snmpEngineMaxMessageSize": snmpEngineMaxMessageSize,
       "snmpFrameworkMIBConformance": snmpFrameworkMIBConformance,
       "snmpFrameworkMIBCompliances": snmpFrameworkMIBCompliances,
       "snmpFrameworkMIBCompliance": snmpFrameworkMIBCompliance,
       "snmpFrameworkMIBGroups": snmpFrameworkMIBGroups,
       "snmpEngineGroup": snmpEngineGroup,
       "PYSNMP_MODULE_ID": snmpFrameworkMIB}
)
