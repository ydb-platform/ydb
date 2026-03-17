#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
# PySNMP MIB module SNMP-COMMUNITY-MIB (https://www.pysnmp.com/pysnmp)
# ASN.1 source file://asn1/SNMP-COMMUNITY-MIB
# Produced by pysmi-1.5.8 at Sat Nov  2 15:25:30 2024
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

(SnmpAdminString,
 SnmpEngineID) = mibBuilder.import_symbols(
    "SNMP-FRAMEWORK-MIB",
    "SnmpAdminString",
    "SnmpEngineID")

(SnmpTagValue,
 snmpTargetAddrEntry) = mibBuilder.import_symbols(
    "SNMP-TARGET-MIB",
    "SnmpTagValue",
    "snmpTargetAddrEntry")

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
 RowStatus,
 StorageType,
 TextualConvention) = mibBuilder.import_symbols(
    "SNMPv2-TC",
    "DisplayString",
    "RowStatus",
    "StorageType",
    "TextualConvention")


# MODULE-IDENTITY

snmpCommunityMIB = ModuleIdentity(
    (1, 3, 6, 1, 6, 3, 18)
)
if mibBuilder.loadTexts:
    snmpCommunityMIB.setRevisions(
        ("2003-08-06 00:00",
         "2000-03-06 00:00")
    )
if mibBuilder.loadTexts:
    snmpCommunityMIB.setLastUpdated("200308060000Z")
if mibBuilder.loadTexts:
    snmpCommunityMIB.setOrganization("SNMPv3 Working Group")
if mibBuilder.loadTexts:
    snmpCommunityMIB.setContactInfo("WG-email: snmpv3@lists.tislabs.com Subscribe: majordomo@lists.tislabs.com In msg body: subscribe snmpv3 Co-Chair: Russ Mundy SPARTA, Inc Postal: 7075 Samuel Morse Drive Columbia, MD 21045 USA EMail: mundy@tislabs.com Phone: +1 410-872-1515 Co-Chair: David Harrington Enterasys Networks Postal: 35 Industrial Way P. O. Box 5005 Rochester, New Hampshire 03866-5005 USA EMail: dbh@enterasys.com Phone: +1 603-337-2614 Co-editor: Rob Frye Vibrant Solutions Postal: 2711 Prosperity Ave Fairfax, Virginia 22031 USA E-mail: rfrye@vibrant-1.com Phone: +1-703-270-2000 Co-editor: David B. Levi Nortel Networks Postal: 3505 Kesterwood Drive Knoxville, Tennessee 37918 E-mail: dlevi@nortelnetworks.com Phone: +1 865 686 0432 Co-editor: Shawn A. Routhier Wind River Systems, Inc. Postal: 500 Wind River Way Alameda, CA 94501 E-mail: sar@epilogue.com Phone: +1 510 749 2095 Co-editor: Bert Wijnen Lucent Technologies Postal: Schagen 33 3461 GL Linschoten Netherlands Email: bwijnen@lucent.com Phone: +31-348-407-775 ")
if mibBuilder.loadTexts:
    snmpCommunityMIB.setDescription("This MIB module defines objects to help support coexistence between SNMPv1, SNMPv2c, and SNMPv3. Copyright (C) The Internet Society (2003) This version of this MIB module is part of RFC 3584; see the RFC itself for full legal notices.")


# Types definitions


# TEXTUAL-CONVENTIONS



# MIB Managed Objects in the order of their OIDs

_SnmpCommunityMIBObjects_ObjectIdentity = ObjectIdentity
snmpCommunityMIBObjects = _SnmpCommunityMIBObjects_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 18, 1)
)
_SnmpCommunityTable_Object = MibTable
snmpCommunityTable = _SnmpCommunityTable_Object(
    (1, 3, 6, 1, 6, 3, 18, 1, 1)
)
if mibBuilder.loadTexts:
    snmpCommunityTable.setStatus("current")
if mibBuilder.loadTexts:
    snmpCommunityTable.setDescription("The table of community strings configured in the SNMP engine's Local Configuration Datastore (LCD).")
_SnmpCommunityEntry_Object = MibTableRow
snmpCommunityEntry = _SnmpCommunityEntry_Object(
    (1, 3, 6, 1, 6, 3, 18, 1, 1, 1)
)
snmpCommunityEntry.setIndexNames(
    (1, "SNMP-COMMUNITY-MIB", "snmpCommunityIndex"),
)
if mibBuilder.loadTexts:
    snmpCommunityEntry.setStatus("current")
if mibBuilder.loadTexts:
    snmpCommunityEntry.setDescription("Information about a particular community string.")


class _SnmpCommunityIndex_Type(SnmpAdminString):
    """Custom type snmpCommunityIndex based on SnmpAdminString"""
    subtypeSpec = SnmpAdminString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(1, 32),
    )


_SnmpCommunityIndex_Type.__name__ = "SnmpAdminString"
_SnmpCommunityIndex_Object = MibTableColumn
snmpCommunityIndex = _SnmpCommunityIndex_Object(
    (1, 3, 6, 1, 6, 3, 18, 1, 1, 1, 1),
    _SnmpCommunityIndex_Type()
)
snmpCommunityIndex.setMaxAccess("not-accessible")
if mibBuilder.loadTexts:
    snmpCommunityIndex.setStatus("current")
if mibBuilder.loadTexts:
    snmpCommunityIndex.setDescription("The unique index value of a row in this table.")
_SnmpCommunityName_Type = OctetString
_SnmpCommunityName_Object = MibTableColumn
snmpCommunityName = _SnmpCommunityName_Object(
    (1, 3, 6, 1, 6, 3, 18, 1, 1, 1, 2),
    _SnmpCommunityName_Type()
)
snmpCommunityName.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpCommunityName.setStatus("current")
if mibBuilder.loadTexts:
    snmpCommunityName.setDescription("The community string for which a row in this table represents a configuration. There is no SIZE constraint specified for this object because RFC 1157 does not impose any explicit limitation on the length of community strings (their size is constrained indirectly by the SNMP message size).")


class _SnmpCommunitySecurityName_Type(SnmpAdminString):
    """Custom type snmpCommunitySecurityName based on SnmpAdminString"""
    subtypeSpec = SnmpAdminString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(1, 32),
    )


_SnmpCommunitySecurityName_Type.__name__ = "SnmpAdminString"
_SnmpCommunitySecurityName_Object = MibTableColumn
snmpCommunitySecurityName = _SnmpCommunitySecurityName_Object(
    (1, 3, 6, 1, 6, 3, 18, 1, 1, 1, 3),
    _SnmpCommunitySecurityName_Type()
)
snmpCommunitySecurityName.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpCommunitySecurityName.setStatus("current")
if mibBuilder.loadTexts:
    snmpCommunitySecurityName.setDescription("A human readable string representing the corresponding value of snmpCommunityName in a Security Model independent format.")
_SnmpCommunityContextEngineID_Type = SnmpEngineID
_SnmpCommunityContextEngineID_Object = MibTableColumn
snmpCommunityContextEngineID = _SnmpCommunityContextEngineID_Object(
    (1, 3, 6, 1, 6, 3, 18, 1, 1, 1, 4),
    _SnmpCommunityContextEngineID_Type()
)
snmpCommunityContextEngineID.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpCommunityContextEngineID.setStatus("current")
if mibBuilder.loadTexts:
    snmpCommunityContextEngineID.setDescription("The contextEngineID indicating the location of the context in which management information is accessed when using the community string specified by the corresponding instance of snmpCommunityName. The default value is the snmpEngineID of the entity in which this object is instantiated.")


class _SnmpCommunityContextName_Type(SnmpAdminString):
    """Custom type snmpCommunityContextName based on SnmpAdminString"""
    defaultHexValue = ""

    subtypeSpec = SnmpAdminString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(0, 32),
    )


_SnmpCommunityContextName_Type.__name__ = "SnmpAdminString"
_SnmpCommunityContextName_Object = MibTableColumn
snmpCommunityContextName = _SnmpCommunityContextName_Object(
    (1, 3, 6, 1, 6, 3, 18, 1, 1, 1, 5),
    _SnmpCommunityContextName_Type()
)
snmpCommunityContextName.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpCommunityContextName.setStatus("current")
if mibBuilder.loadTexts:
    snmpCommunityContextName.setDescription("The context in which management information is accessed when using the community string specified by the corresponding instance of snmpCommunityName.")


class _SnmpCommunityTransportTag_Type(SnmpTagValue):
    """Custom type snmpCommunityTransportTag based on SnmpTagValue"""
    defaultHexValue = ""


_SnmpCommunityTransportTag_Type.__name__ = "SnmpTagValue"
_SnmpCommunityTransportTag_Object = MibTableColumn
snmpCommunityTransportTag = _SnmpCommunityTransportTag_Object(
    (1, 3, 6, 1, 6, 3, 18, 1, 1, 1, 6),
    _SnmpCommunityTransportTag_Type()
)
snmpCommunityTransportTag.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpCommunityTransportTag.setStatus("current")
if mibBuilder.loadTexts:
    snmpCommunityTransportTag.setDescription("This object specifies a set of transport endpoints which are used in two ways: - to specify the transport endpoints from which an SNMP entity will accept management requests, and - to specify the transport endpoints to which a notification may be sent using the community string matching the corresponding instance of snmpCommunityName. In either case, if the value of this object has zero-length, transport endpoints are not checked when either authenticating messages containing this community string, nor when generating notifications. The transports identified by this object are specified in the snmpTargetAddrTable. Entries in that table whose snmpTargetAddrTagList contains this tag value are identified. If a management request containing a community string that matches the corresponding instance of snmpCommunityName is received on a transport endpoint other than the transport endpoints identified by this object the request is deemed unauthentic. When a notification is to be sent using an entry in this table, if the destination transport endpoint of the notification does not match one of the transport endpoints selected by this object, the notification is not sent.")
_SnmpCommunityStorageType_Type = StorageType
_SnmpCommunityStorageType_Object = MibTableColumn
snmpCommunityStorageType = _SnmpCommunityStorageType_Object(
    (1, 3, 6, 1, 6, 3, 18, 1, 1, 1, 7),
    _SnmpCommunityStorageType_Type()
)
snmpCommunityStorageType.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpCommunityStorageType.setStatus("current")
if mibBuilder.loadTexts:
    snmpCommunityStorageType.setDescription("The storage type for this conceptual row in the snmpCommunityTable. Conceptual rows having the value 'permanent' need not allow write-access to any columnar object in the row.")
_SnmpCommunityStatus_Type = RowStatus
_SnmpCommunityStatus_Object = MibTableColumn
snmpCommunityStatus = _SnmpCommunityStatus_Object(
    (1, 3, 6, 1, 6, 3, 18, 1, 1, 1, 8),
    _SnmpCommunityStatus_Type()
)
snmpCommunityStatus.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpCommunityStatus.setStatus("current")
if mibBuilder.loadTexts:
    snmpCommunityStatus.setDescription("The status of this conceptual row in the snmpCommunityTable. An entry in this table is not qualified for activation until instances of all corresponding columns have been initialized, either through default values, or through Set operations. The snmpCommunityName and snmpCommunitySecurityName objects must be explicitly set. There is no restriction on setting columns in this table when the value of snmpCommunityStatus is active(1).")
_SnmpTargetAddrExtTable_Object = MibTable
snmpTargetAddrExtTable = _SnmpTargetAddrExtTable_Object(
    (1, 3, 6, 1, 6, 3, 18, 1, 2)
)
if mibBuilder.loadTexts:
    snmpTargetAddrExtTable.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetAddrExtTable.setDescription("The table of mask and maximum message size (mms) values associated with the snmpTargetAddrTable. The snmpTargetAddrExtTable augments the snmpTargetAddrTable with a transport address mask value and a maximum message size value. The transport address mask allows entries in the snmpTargetAddrTable to define a set of addresses instead of just a single address. The maximum message size value allows the maximum message size of another SNMP entity to be configured for use in SNMPv1 (and SNMPv2c) transactions, where the message format does not specify a maximum message size.")
_SnmpTargetAddrExtEntry_Object = MibTableRow
snmpTargetAddrExtEntry = _SnmpTargetAddrExtEntry_Object(
    (1, 3, 6, 1, 6, 3, 18, 1, 2, 1)
)
if mibBuilder.loadTexts:
    snmpTargetAddrExtEntry.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetAddrExtEntry.setDescription("Information about a particular mask and mms value.")


class _SnmpTargetAddrTMask_Type(OctetString):
    """Custom type snmpTargetAddrTMask based on OctetString"""
    defaultHexValue = ""

    subtypeSpec = OctetString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(0, 255),
    )


_SnmpTargetAddrTMask_Type.__name__ = "OctetString"
_SnmpTargetAddrTMask_Object = MibTableColumn
snmpTargetAddrTMask = _SnmpTargetAddrTMask_Object(
    (1, 3, 6, 1, 6, 3, 18, 1, 2, 1, 1),
    _SnmpTargetAddrTMask_Type()
)
snmpTargetAddrTMask.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpTargetAddrTMask.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetAddrTMask.setDescription("The mask value associated with an entry in the snmpTargetAddrTable. The value of this object must have the same length as the corresponding instance of snmpTargetAddrTAddress, or must have length 0. An attempt to set it to any other value will result in an inconsistentValue error. The value of this object allows an entry in the snmpTargetAddrTable to specify multiple addresses. The mask value is used to select which bits of a transport address must match bits of the corresponding instance of snmpTargetAddrTAddress, in order for the transport address to match a particular entry in the snmpTargetAddrTable. Bits which are 1 in the mask value indicate bits in the transport address which must match bits in the snmpTargetAddrTAddress value. Bits which are 0 in the mask indicate bits in the transport address which need not match. If the length of the mask is 0, the mask should be treated as if all its bits were 1 and its length were equal to the length of the corresponding value of snmpTargetAddrTable. This object may not be modified while the value of the corresponding instance of snmpTargetAddrRowStatus is active(1). An attempt to set this object in this case will result in an inconsistentValue error.")


class _SnmpTargetAddrMMS_Type(Integer32):
    """Custom type snmpTargetAddrMMS based on Integer32"""
    defaultValue = 484

    subtypeSpec = Integer32.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueRangeConstraint(0, 0),
        ValueRangeConstraint(484, 2147483647),
    )


_SnmpTargetAddrMMS_Type.__name__ = "Integer32"
_SnmpTargetAddrMMS_Object = MibTableColumn
snmpTargetAddrMMS = _SnmpTargetAddrMMS_Object(
    (1, 3, 6, 1, 6, 3, 18, 1, 2, 1, 2),
    _SnmpTargetAddrMMS_Type()
)
snmpTargetAddrMMS.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpTargetAddrMMS.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetAddrMMS.setDescription("The maximum message size value associated with an entry in the snmpTargetAddrTable. Note that a value of 0 means that the maximum message size is unknown.")
_SnmpTrapAddress_Type = IpAddress
_SnmpTrapAddress_Object = MibScalar
snmpTrapAddress = _SnmpTrapAddress_Object(
    (1, 3, 6, 1, 6, 3, 18, 1, 3),
    _SnmpTrapAddress_Type()
)
snmpTrapAddress.setMaxAccess("accessible-for-notify")
if mibBuilder.loadTexts:
    snmpTrapAddress.setStatus("current")
if mibBuilder.loadTexts:
    snmpTrapAddress.setDescription("The value of the agent-addr field of a Trap PDU which is forwarded by a proxy forwarder application using an SNMP version other than SNMPv1. The value of this object SHOULD contain the value of the agent-addr field from the original Trap PDU as generated by an SNMPv1 agent.")
_SnmpTrapCommunity_Type = OctetString
_SnmpTrapCommunity_Object = MibScalar
snmpTrapCommunity = _SnmpTrapCommunity_Object(
    (1, 3, 6, 1, 6, 3, 18, 1, 4),
    _SnmpTrapCommunity_Type()
)
snmpTrapCommunity.setMaxAccess("accessible-for-notify")
if mibBuilder.loadTexts:
    snmpTrapCommunity.setStatus("current")
if mibBuilder.loadTexts:
    snmpTrapCommunity.setDescription("The value of the community string field of an SNMPv1 message containing a Trap PDU which is forwarded by a a proxy forwarder application using an SNMP version other than SNMPv1. The value of this object SHOULD contain the value of the community string field from the original SNMPv1 message containing a Trap PDU as generated by an SNMPv1 agent. There is no SIZE constraint specified for this object because RFC 1157 does not impose any explicit limitation on the length of community strings (their size is constrained indirectly by the SNMP message size).")
_SnmpCommunityMIBConformance_ObjectIdentity = ObjectIdentity
snmpCommunityMIBConformance = _SnmpCommunityMIBConformance_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 18, 2)
)
_SnmpCommunityMIBCompliances_ObjectIdentity = ObjectIdentity
snmpCommunityMIBCompliances = _SnmpCommunityMIBCompliances_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 18, 2, 1)
)
_SnmpCommunityMIBGroups_ObjectIdentity = ObjectIdentity
snmpCommunityMIBGroups = _SnmpCommunityMIBGroups_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 18, 2, 2)
)
snmpTargetAddrEntry.registerAugmentions(
    ("SNMP-COMMUNITY-MIB",
     "snmpTargetAddrExtEntry")
)
snmpTargetAddrExtEntry.setIndexNames(*snmpTargetAddrEntry.getIndexNames())

# Managed Objects groups

snmpCommunityTableGroup = ObjectGroup(
    (1, 3, 6, 1, 6, 3, 18, 2, 2, 1)
)
snmpCommunityTableGroup.setObjects(
      *(("SNMP-COMMUNITY-MIB", "snmpCommunityName"),
        ("SNMP-COMMUNITY-MIB", "snmpCommunitySecurityName"),
        ("SNMP-COMMUNITY-MIB", "snmpCommunityContextEngineID"),
        ("SNMP-COMMUNITY-MIB", "snmpCommunityContextName"),
        ("SNMP-COMMUNITY-MIB", "snmpCommunityTransportTag"),
        ("SNMP-COMMUNITY-MIB", "snmpCommunityStorageType"),
        ("SNMP-COMMUNITY-MIB", "snmpCommunityStatus"),
        ("SNMP-COMMUNITY-MIB", "snmpTargetAddrTMask"),
        ("SNMP-COMMUNITY-MIB", "snmpTargetAddrMMS"))
)
if mibBuilder.loadTexts:
    snmpCommunityTableGroup.setStatus("current")
if mibBuilder.loadTexts:
    snmpCommunityTableGroup.setDescription("A collection of objects providing for configuration of community strings for SNMPv1 (and SNMPv2c) usage.")

snmpProxyTrapForwardGroup = ObjectGroup(
    (1, 3, 6, 1, 6, 3, 18, 2, 2, 3)
)
snmpProxyTrapForwardGroup.setObjects(
      *(("SNMP-COMMUNITY-MIB", "snmpTrapAddress"),
        ("SNMP-COMMUNITY-MIB", "snmpTrapCommunity"))
)
if mibBuilder.loadTexts:
    snmpProxyTrapForwardGroup.setStatus("current")
if mibBuilder.loadTexts:
    snmpProxyTrapForwardGroup.setDescription("Objects which are used by proxy forwarding applications when translating traps between SNMP versions. These are used to preserve SNMPv1-specific information when translating to SNMPv2c or SNMPv3.")


# Notification objects


# Notifications groups


# Agent capabilities


# Module compliance

snmpCommunityMIBCompliance = ModuleCompliance(
    (1, 3, 6, 1, 6, 3, 18, 2, 1, 1)
)
snmpCommunityMIBCompliance.setObjects(
    ("SNMP-COMMUNITY-MIB", "snmpCommunityTableGroup")
)
if mibBuilder.loadTexts:
    snmpCommunityMIBCompliance.setStatus(
        "current"
    )
if mibBuilder.loadTexts:
    snmpCommunityMIBCompliance.setDescription("The compliance statement for SNMP engines which implement the SNMP-COMMUNITY-MIB.")

snmpProxyTrapForwardCompliance = ModuleCompliance(
    (1, 3, 6, 1, 6, 3, 18, 2, 1, 2)
)
snmpProxyTrapForwardCompliance.setObjects(
    ("SNMP-COMMUNITY-MIB", "snmpProxyTrapForwardGroup")
)
if mibBuilder.loadTexts:
    snmpProxyTrapForwardCompliance.setStatus(
        "current"
    )
if mibBuilder.loadTexts:
    snmpProxyTrapForwardCompliance.setDescription("The compliance statement for SNMP engines which contain a proxy forwarding application which is capable of forwarding SNMPv1 traps using SNMPv2c or SNMPv3.")

snmpCommunityMIBFullCompliance = ModuleCompliance(
    (1, 3, 6, 1, 6, 3, 18, 2, 1, 3)
)
snmpCommunityMIBFullCompliance.setObjects(
    ("SNMP-COMMUNITY-MIB", "snmpCommunityTableGroup")
)
if mibBuilder.loadTexts:
    snmpCommunityMIBFullCompliance.setStatus(
        "current"
    )
if mibBuilder.loadTexts:
    snmpCommunityMIBFullCompliance.setDescription("The compliance statement for SNMP engines which implement the SNMP-COMMUNITY-MIB with full read-create access.")


# Export all MIB objects to the MIB builder

mibBuilder.export_symbols(
    "SNMP-COMMUNITY-MIB",
    **{"snmpCommunityMIB": snmpCommunityMIB,
       "snmpCommunityMIBObjects": snmpCommunityMIBObjects,
       "snmpCommunityTable": snmpCommunityTable,
       "snmpCommunityEntry": snmpCommunityEntry,
       "snmpCommunityIndex": snmpCommunityIndex,
       "snmpCommunityName": snmpCommunityName,
       "snmpCommunitySecurityName": snmpCommunitySecurityName,
       "snmpCommunityContextEngineID": snmpCommunityContextEngineID,
       "snmpCommunityContextName": snmpCommunityContextName,
       "snmpCommunityTransportTag": snmpCommunityTransportTag,
       "snmpCommunityStorageType": snmpCommunityStorageType,
       "snmpCommunityStatus": snmpCommunityStatus,
       "snmpTargetAddrExtTable": snmpTargetAddrExtTable,
       "snmpTargetAddrExtEntry": snmpTargetAddrExtEntry,
       "snmpTargetAddrTMask": snmpTargetAddrTMask,
       "snmpTargetAddrMMS": snmpTargetAddrMMS,
       "snmpTrapAddress": snmpTrapAddress,
       "snmpTrapCommunity": snmpTrapCommunity,
       "snmpCommunityMIBConformance": snmpCommunityMIBConformance,
       "snmpCommunityMIBCompliances": snmpCommunityMIBCompliances,
       "snmpCommunityMIBCompliance": snmpCommunityMIBCompliance,
       "snmpProxyTrapForwardCompliance": snmpProxyTrapForwardCompliance,
       "snmpCommunityMIBFullCompliance": snmpCommunityMIBFullCompliance,
       "snmpCommunityMIBGroups": snmpCommunityMIBGroups,
       "snmpCommunityTableGroup": snmpCommunityTableGroup,
       "snmpProxyTrapForwardGroup": snmpProxyTrapForwardGroup,
       "PYSNMP_MODULE_ID": snmpCommunityMIB}
)
