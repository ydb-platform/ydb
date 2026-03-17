#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
# PySNMP MIB module SNMP-PROXY-MIB (https://www.pysnmp.com/pysnmp)
# ASN.1 source file://asn1/SNMP-PROXY-MIB
# Produced by pysmi-1.5.8 at Sat Nov  2 15:25:34 2024
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

(SnmpTagValue,) = mibBuilder.import_symbols(
    "SNMP-TARGET-MIB",
    "SnmpTagValue")

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

snmpProxyMIB = ModuleIdentity(
    (1, 3, 6, 1, 6, 3, 14)
)
if mibBuilder.loadTexts:
    snmpProxyMIB.setRevisions(
        ("2002-10-14 00:00",
         "1998-08-04 00:00",
         "1997-07-14 00:00")
    )
if mibBuilder.loadTexts:
    snmpProxyMIB.setLastUpdated("200210140000Z")
if mibBuilder.loadTexts:
    snmpProxyMIB.setOrganization("IETF SNMPv3 Working Group")
if mibBuilder.loadTexts:
    snmpProxyMIB.setContactInfo("WG-email: snmpv3@lists.tislabs.com Subscribe: majordomo@lists.tislabs.com In message body: subscribe snmpv3 Co-Chair: Russ Mundy Network Associates Laboratories Postal: 15204 Omega Drive, Suite 300 Rockville, MD 20850-4601 USA EMail: mundy@tislabs.com Phone: +1 301-947-7107 Co-Chair: David Harrington Enterasys Networks Postal: 35 Industrial Way P. O. Box 5004 Rochester, New Hampshire 03866-5005 USA EMail: dbh@enterasys.com Phone: +1 603-337-2614 Co-editor: David B. Levi Nortel Networks Postal: 3505 Kesterwood Drive Knoxville, Tennessee 37918 EMail: dlevi@nortelnetworks.com Phone: +1 865 686 0432 Co-editor: Paul Meyer Secure Computing Corporation Postal: 2675 Long Lake Road Roseville, Minnesota 55113 EMail: paul_meyer@securecomputing.com Phone: +1 651 628 1592 Co-editor: Bob Stewart Retired")
if mibBuilder.loadTexts:
    snmpProxyMIB.setDescription("This MIB module defines MIB objects which provide mechanisms to remotely configure the parameters used by a proxy forwarding application. Copyright (C) The Internet Society (2002). This version of this MIB module is part of RFC 3413; see the RFC itself for full legal notices. ")


# Types definitions


# TEXTUAL-CONVENTIONS



# MIB Managed Objects in the order of their OIDs

_SnmpProxyObjects_ObjectIdentity = ObjectIdentity
snmpProxyObjects = _SnmpProxyObjects_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 14, 1)
)
_SnmpProxyTable_Object = MibTable
snmpProxyTable = _SnmpProxyTable_Object(
    (1, 3, 6, 1, 6, 3, 14, 1, 2)
)
if mibBuilder.loadTexts:
    snmpProxyTable.setStatus("current")
if mibBuilder.loadTexts:
    snmpProxyTable.setDescription("The table of translation parameters used by proxy forwarder applications for forwarding SNMP messages.")
_SnmpProxyEntry_Object = MibTableRow
snmpProxyEntry = _SnmpProxyEntry_Object(
    (1, 3, 6, 1, 6, 3, 14, 1, 2, 1)
)
snmpProxyEntry.setIndexNames(
    (1, "SNMP-PROXY-MIB", "snmpProxyName"),
)
if mibBuilder.loadTexts:
    snmpProxyEntry.setStatus("current")
if mibBuilder.loadTexts:
    snmpProxyEntry.setDescription("A set of translation parameters used by a proxy forwarder application for forwarding SNMP messages. Entries in the snmpProxyTable are created and deleted using the snmpProxyRowStatus object.")


class _SnmpProxyName_Type(SnmpAdminString):
    """Custom type snmpProxyName based on SnmpAdminString"""
    subtypeSpec = SnmpAdminString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(1, 32),
    )


_SnmpProxyName_Type.__name__ = "SnmpAdminString"
_SnmpProxyName_Object = MibTableColumn
snmpProxyName = _SnmpProxyName_Object(
    (1, 3, 6, 1, 6, 3, 14, 1, 2, 1, 1),
    _SnmpProxyName_Type()
)
snmpProxyName.setMaxAccess("not-accessible")
if mibBuilder.loadTexts:
    snmpProxyName.setStatus("current")
if mibBuilder.loadTexts:
    snmpProxyName.setDescription("The locally arbitrary, but unique identifier associated with this snmpProxyEntry.")


class _SnmpProxyType_Type(Integer32):
    """Custom type snmpProxyType based on Integer32"""
    subtypeSpec = Integer32.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        SingleValueConstraint(
            *(1,
              2,
              3,
              4)
        )
    )
    namedValues = NamedValues(
        *(("read", 1),
          ("write", 2),
          ("trap", 3),
          ("inform", 4))
    )


_SnmpProxyType_Type.__name__ = "Integer32"
_SnmpProxyType_Object = MibTableColumn
snmpProxyType = _SnmpProxyType_Object(
    (1, 3, 6, 1, 6, 3, 14, 1, 2, 1, 2),
    _SnmpProxyType_Type()
)
snmpProxyType.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpProxyType.setStatus("current")
if mibBuilder.loadTexts:
    snmpProxyType.setDescription("The type of message that may be forwarded using the translation parameters defined by this entry.")
_SnmpProxyContextEngineID_Type = SnmpEngineID
_SnmpProxyContextEngineID_Object = MibTableColumn
snmpProxyContextEngineID = _SnmpProxyContextEngineID_Object(
    (1, 3, 6, 1, 6, 3, 14, 1, 2, 1, 3),
    _SnmpProxyContextEngineID_Type()
)
snmpProxyContextEngineID.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpProxyContextEngineID.setStatus("current")
if mibBuilder.loadTexts:
    snmpProxyContextEngineID.setDescription("The contextEngineID contained in messages that may be forwarded using the translation parameters defined by this entry.")
_SnmpProxyContextName_Type = SnmpAdminString
_SnmpProxyContextName_Object = MibTableColumn
snmpProxyContextName = _SnmpProxyContextName_Object(
    (1, 3, 6, 1, 6, 3, 14, 1, 2, 1, 4),
    _SnmpProxyContextName_Type()
)
snmpProxyContextName.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpProxyContextName.setStatus("current")
if mibBuilder.loadTexts:
    snmpProxyContextName.setDescription("The contextName contained in messages that may be forwarded using the translation parameters defined by this entry. This object is optional, and if not supported, the contextName contained in a message is ignored when selecting an entry in the snmpProxyTable.")
_SnmpProxyTargetParamsIn_Type = SnmpAdminString
_SnmpProxyTargetParamsIn_Object = MibTableColumn
snmpProxyTargetParamsIn = _SnmpProxyTargetParamsIn_Object(
    (1, 3, 6, 1, 6, 3, 14, 1, 2, 1, 5),
    _SnmpProxyTargetParamsIn_Type()
)
snmpProxyTargetParamsIn.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpProxyTargetParamsIn.setStatus("current")
if mibBuilder.loadTexts:
    snmpProxyTargetParamsIn.setDescription("This object selects an entry in the snmpTargetParamsTable. The selected entry is used to determine which row of the snmpProxyTable to use for forwarding received messages.")
_SnmpProxySingleTargetOut_Type = SnmpAdminString
_SnmpProxySingleTargetOut_Object = MibTableColumn
snmpProxySingleTargetOut = _SnmpProxySingleTargetOut_Object(
    (1, 3, 6, 1, 6, 3, 14, 1, 2, 1, 6),
    _SnmpProxySingleTargetOut_Type()
)
snmpProxySingleTargetOut.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpProxySingleTargetOut.setStatus("current")
if mibBuilder.loadTexts:
    snmpProxySingleTargetOut.setDescription("This object selects a management target defined in the snmpTargetAddrTable (in the SNMP-TARGET-MIB). The selected target is defined by an entry in the snmpTargetAddrTable whose index value (snmpTargetAddrName) is equal to this object. This object is only used when selection of a single target is required (i.e. when forwarding an incoming read or write request).")
_SnmpProxyMultipleTargetOut_Type = SnmpTagValue
_SnmpProxyMultipleTargetOut_Object = MibTableColumn
snmpProxyMultipleTargetOut = _SnmpProxyMultipleTargetOut_Object(
    (1, 3, 6, 1, 6, 3, 14, 1, 2, 1, 7),
    _SnmpProxyMultipleTargetOut_Type()
)
snmpProxyMultipleTargetOut.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpProxyMultipleTargetOut.setStatus("current")
if mibBuilder.loadTexts:
    snmpProxyMultipleTargetOut.setDescription("This object selects a set of management targets defined in the snmpTargetAddrTable (in the SNMP-TARGET-MIB). This object is only used when selection of multiple targets is required (i.e. when forwarding an incoming notification).")


class _SnmpProxyStorageType_Type(StorageType):
    """Custom type snmpProxyStorageType based on StorageType"""
    defaultValue = 3


_SnmpProxyStorageType_Type.__name__ = "StorageType"
_SnmpProxyStorageType_Object = MibTableColumn
snmpProxyStorageType = _SnmpProxyStorageType_Object(
    (1, 3, 6, 1, 6, 3, 14, 1, 2, 1, 8),
    _SnmpProxyStorageType_Type()
)
snmpProxyStorageType.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpProxyStorageType.setStatus("current")
if mibBuilder.loadTexts:
    snmpProxyStorageType.setDescription("The storage type of this conceptual row. Conceptual rows having the value 'permanent' need not allow write-access to any columnar objects in the row.")
_SnmpProxyRowStatus_Type = RowStatus
_SnmpProxyRowStatus_Object = MibTableColumn
snmpProxyRowStatus = _SnmpProxyRowStatus_Object(
    (1, 3, 6, 1, 6, 3, 14, 1, 2, 1, 9),
    _SnmpProxyRowStatus_Type()
)
snmpProxyRowStatus.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpProxyRowStatus.setStatus("current")
if mibBuilder.loadTexts:
    snmpProxyRowStatus.setDescription("The status of this conceptual row. To create a row in this table, a manager must set this object to either createAndGo(4) or createAndWait(5). The following objects may not be modified while the value of this object is active(1): - snmpProxyType - snmpProxyContextEngineID - snmpProxyContextName - snmpProxyTargetParamsIn - snmpProxySingleTargetOut - snmpProxyMultipleTargetOut")
_SnmpProxyConformance_ObjectIdentity = ObjectIdentity
snmpProxyConformance = _SnmpProxyConformance_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 14, 3)
)
_SnmpProxyCompliances_ObjectIdentity = ObjectIdentity
snmpProxyCompliances = _SnmpProxyCompliances_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 14, 3, 1)
)
_SnmpProxyGroups_ObjectIdentity = ObjectIdentity
snmpProxyGroups = _SnmpProxyGroups_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 14, 3, 2)
)

# Managed Objects groups

snmpProxyGroup = ObjectGroup(
    (1, 3, 6, 1, 6, 3, 14, 3, 2, 3)
)
snmpProxyGroup.setObjects(
      *(("SNMP-PROXY-MIB", "snmpProxyType"),
        ("SNMP-PROXY-MIB", "snmpProxyContextEngineID"),
        ("SNMP-PROXY-MIB", "snmpProxyContextName"),
        ("SNMP-PROXY-MIB", "snmpProxyTargetParamsIn"),
        ("SNMP-PROXY-MIB", "snmpProxySingleTargetOut"),
        ("SNMP-PROXY-MIB", "snmpProxyMultipleTargetOut"),
        ("SNMP-PROXY-MIB", "snmpProxyStorageType"),
        ("SNMP-PROXY-MIB", "snmpProxyRowStatus"))
)
if mibBuilder.loadTexts:
    snmpProxyGroup.setStatus("current")
if mibBuilder.loadTexts:
    snmpProxyGroup.setDescription("A collection of objects providing remote configuration of management target translation parameters for use by proxy forwarder applications.")


# Notification objects


# Notifications groups


# Agent capabilities


# Module compliance

snmpProxyCompliance = ModuleCompliance(
    (1, 3, 6, 1, 6, 3, 14, 3, 1, 1)
)
snmpProxyCompliance.setObjects(
      *(("SNMP-TARGET-MIB", "snmpTargetBasicGroup"),
        ("SNMP-TARGET-MIB", "snmpTargetResponseGroup"),
        ("SNMP-PROXY-MIB", "snmpProxyGroup"))
)
if mibBuilder.loadTexts:
    snmpProxyCompliance.setStatus(
        "current"
    )
if mibBuilder.loadTexts:
    snmpProxyCompliance.setDescription("The compliance statement for SNMP entities which include a proxy forwarding application.")


# Export all MIB objects to the MIB builder

mibBuilder.export_symbols(
    "SNMP-PROXY-MIB",
    **{"snmpProxyMIB": snmpProxyMIB,
       "snmpProxyObjects": snmpProxyObjects,
       "snmpProxyTable": snmpProxyTable,
       "snmpProxyEntry": snmpProxyEntry,
       "snmpProxyName": snmpProxyName,
       "snmpProxyType": snmpProxyType,
       "snmpProxyContextEngineID": snmpProxyContextEngineID,
       "snmpProxyContextName": snmpProxyContextName,
       "snmpProxyTargetParamsIn": snmpProxyTargetParamsIn,
       "snmpProxySingleTargetOut": snmpProxySingleTargetOut,
       "snmpProxyMultipleTargetOut": snmpProxyMultipleTargetOut,
       "snmpProxyStorageType": snmpProxyStorageType,
       "snmpProxyRowStatus": snmpProxyRowStatus,
       "snmpProxyConformance": snmpProxyConformance,
       "snmpProxyCompliances": snmpProxyCompliances,
       "snmpProxyCompliance": snmpProxyCompliance,
       "snmpProxyGroups": snmpProxyGroups,
       "snmpProxyGroup": snmpProxyGroup,
       "PYSNMP_MODULE_ID": snmpProxyMIB}
)
