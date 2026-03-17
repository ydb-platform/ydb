#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
# PySNMP MIB module SNMP-NOTIFICATION-MIB (https://www.pysnmp.com/pysnmp)
# ASN.1 source file://asn1/SNMP-NOTIFICATION-MIB
# Produced by pysmi-1.5.8 at Sat Nov  2 15:25:33 2024
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

(SnmpAdminString,) = mibBuilder.import_symbols(
    "SNMP-FRAMEWORK-MIB",
    "SnmpAdminString")

(SnmpTagValue,
 snmpTargetParamsName) = mibBuilder.import_symbols(
    "SNMP-TARGET-MIB",
    "SnmpTagValue",
    "snmpTargetParamsName")

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

snmpNotificationMIB = ModuleIdentity(
    (1, 3, 6, 1, 6, 3, 13)
)
if mibBuilder.loadTexts:
    snmpNotificationMIB.setRevisions(
        ("2002-10-14 00:00",
         "1998-08-04 00:00",
         "1997-07-14 00:00")
    )
if mibBuilder.loadTexts:
    snmpNotificationMIB.setLastUpdated("200210140000Z")
if mibBuilder.loadTexts:
    snmpNotificationMIB.setOrganization("IETF SNMPv3 Working Group")
if mibBuilder.loadTexts:
    snmpNotificationMIB.setContactInfo("WG-email: snmpv3@lists.tislabs.com Subscribe: majordomo@lists.tislabs.com In message body: subscribe snmpv3 Co-Chair: Russ Mundy Network Associates Laboratories Postal: 15204 Omega Drive, Suite 300 Rockville, MD 20850-4601 USA EMail: mundy@tislabs.com Phone: +1 301-947-7107 Co-Chair: David Harrington Enterasys Networks Postal: 35 Industrial Way P. O. Box 5004 Rochester, New Hampshire 03866-5005 USA EMail: dbh@enterasys.com Phone: +1 603-337-2614 Co-editor: David B. Levi Nortel Networks Postal: 3505 Kesterwood Drive Knoxville, Tennessee 37918 EMail: dlevi@nortelnetworks.com Phone: +1 865 686 0432 Co-editor: Paul Meyer Secure Computing Corporation Postal: 2675 Long Lake Road Roseville, Minnesota 55113 EMail: paul_meyer@securecomputing.com Phone: +1 651 628 1592 Co-editor: Bob Stewart Retired")
if mibBuilder.loadTexts:
    snmpNotificationMIB.setDescription("This MIB module defines MIB objects which provide mechanisms to remotely configure the parameters used by an SNMP entity for the generation of notifications. Copyright (C) The Internet Society (2002). This version of this MIB module is part of RFC 3413; see the RFC itself for full legal notices. ")


# Types definitions


# TEXTUAL-CONVENTIONS



# MIB Managed Objects in the order of their OIDs

_SnmpNotifyObjects_ObjectIdentity = ObjectIdentity
snmpNotifyObjects = _SnmpNotifyObjects_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 13, 1)
)
_SnmpNotifyTable_Object = MibTable
snmpNotifyTable = _SnmpNotifyTable_Object(
    (1, 3, 6, 1, 6, 3, 13, 1, 1)
)
if mibBuilder.loadTexts:
    snmpNotifyTable.setStatus("current")
if mibBuilder.loadTexts:
    snmpNotifyTable.setDescription("This table is used to select management targets which should receive notifications, as well as the type of notification which should be sent to each selected management target.")
_SnmpNotifyEntry_Object = MibTableRow
snmpNotifyEntry = _SnmpNotifyEntry_Object(
    (1, 3, 6, 1, 6, 3, 13, 1, 1, 1)
)
snmpNotifyEntry.setIndexNames(
    (1, "SNMP-NOTIFICATION-MIB", "snmpNotifyName"),
)
if mibBuilder.loadTexts:
    snmpNotifyEntry.setStatus("current")
if mibBuilder.loadTexts:
    snmpNotifyEntry.setDescription("An entry in this table selects a set of management targets which should receive notifications, as well as the type of notification which should be sent to each selected management target. Entries in the snmpNotifyTable are created and deleted using the snmpNotifyRowStatus object.")


class _SnmpNotifyName_Type(SnmpAdminString):
    """Custom type snmpNotifyName based on SnmpAdminString"""
    subtypeSpec = SnmpAdminString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(1, 32),
    )


_SnmpNotifyName_Type.__name__ = "SnmpAdminString"
_SnmpNotifyName_Object = MibTableColumn
snmpNotifyName = _SnmpNotifyName_Object(
    (1, 3, 6, 1, 6, 3, 13, 1, 1, 1, 1),
    _SnmpNotifyName_Type()
)
snmpNotifyName.setMaxAccess("not-accessible")
if mibBuilder.loadTexts:
    snmpNotifyName.setStatus("current")
if mibBuilder.loadTexts:
    snmpNotifyName.setDescription("The locally arbitrary, but unique identifier associated with this snmpNotifyEntry.")
_SnmpNotifyTag_Type = SnmpTagValue
_SnmpNotifyTag_Object = MibTableColumn
snmpNotifyTag = _SnmpNotifyTag_Object(
    (1, 3, 6, 1, 6, 3, 13, 1, 1, 1, 2),
    _SnmpNotifyTag_Type()
)
snmpNotifyTag.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpNotifyTag.setStatus("current")
if mibBuilder.loadTexts:
    snmpNotifyTag.setDescription("This object contains a single tag value which is used to select entries in the snmpTargetAddrTable. Any entry in the snmpTargetAddrTable which contains a tag value which is equal to the value of an instance of this object is selected. If this object contains a value of zero length, no entries are selected.")


class _SnmpNotifyType_Type(Integer32):
    """Custom type snmpNotifyType based on Integer32"""
    defaultValue = 1

    subtypeSpec = Integer32.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        SingleValueConstraint(
            *(1,
              2)
        )
    )
    namedValues = NamedValues(
        *(("trap", 1),
          ("inform", 2))
    )


_SnmpNotifyType_Type.__name__ = "Integer32"
_SnmpNotifyType_Object = MibTableColumn
snmpNotifyType = _SnmpNotifyType_Object(
    (1, 3, 6, 1, 6, 3, 13, 1, 1, 1, 3),
    _SnmpNotifyType_Type()
)
snmpNotifyType.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpNotifyType.setStatus("current")
if mibBuilder.loadTexts:
    snmpNotifyType.setDescription("This object determines the type of notification to be generated for entries in the snmpTargetAddrTable selected by the corresponding instance of snmpNotifyTag. This value is only used when generating notifications, and is ignored when using the snmpTargetAddrTable for other purposes. If the value of this object is trap(1), then any messages generated for selected rows will contain Unconfirmed-Class PDUs. If the value of this object is inform(2), then any messages generated for selected rows will contain Confirmed-Class PDUs. Note that if an SNMP entity only supports generation of Unconfirmed-Class PDUs (and not Confirmed-Class PDUs), then this object may be read-only.")


class _SnmpNotifyStorageType_Type(StorageType):
    """Custom type snmpNotifyStorageType based on StorageType"""
    defaultValue = 3


_SnmpNotifyStorageType_Type.__name__ = "StorageType"
_SnmpNotifyStorageType_Object = MibTableColumn
snmpNotifyStorageType = _SnmpNotifyStorageType_Object(
    (1, 3, 6, 1, 6, 3, 13, 1, 1, 1, 4),
    _SnmpNotifyStorageType_Type()
)
snmpNotifyStorageType.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpNotifyStorageType.setStatus("current")
if mibBuilder.loadTexts:
    snmpNotifyStorageType.setDescription("The storage type for this conceptual row. Conceptual rows having the value 'permanent' need not allow write-access to any columnar objects in the row.")
_SnmpNotifyRowStatus_Type = RowStatus
_SnmpNotifyRowStatus_Object = MibTableColumn
snmpNotifyRowStatus = _SnmpNotifyRowStatus_Object(
    (1, 3, 6, 1, 6, 3, 13, 1, 1, 1, 5),
    _SnmpNotifyRowStatus_Type()
)
snmpNotifyRowStatus.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpNotifyRowStatus.setStatus("current")
if mibBuilder.loadTexts:
    snmpNotifyRowStatus.setDescription("The status of this conceptual row. To create a row in this table, a manager must set this object to either createAndGo(4) or createAndWait(5).")
_SnmpNotifyFilterProfileTable_Object = MibTable
snmpNotifyFilterProfileTable = _SnmpNotifyFilterProfileTable_Object(
    (1, 3, 6, 1, 6, 3, 13, 1, 2)
)
if mibBuilder.loadTexts:
    snmpNotifyFilterProfileTable.setStatus("current")
if mibBuilder.loadTexts:
    snmpNotifyFilterProfileTable.setDescription("This table is used to associate a notification filter profile with a particular set of target parameters.")
_SnmpNotifyFilterProfileEntry_Object = MibTableRow
snmpNotifyFilterProfileEntry = _SnmpNotifyFilterProfileEntry_Object(
    (1, 3, 6, 1, 6, 3, 13, 1, 2, 1)
)
snmpNotifyFilterProfileEntry.setIndexNames(
    (1, "SNMP-TARGET-MIB", "snmpTargetParamsName"),
)
if mibBuilder.loadTexts:
    snmpNotifyFilterProfileEntry.setStatus("current")
if mibBuilder.loadTexts:
    snmpNotifyFilterProfileEntry.setDescription("An entry in this table indicates the name of the filter profile to be used when generating notifications using the corresponding entry in the snmpTargetParamsTable. Entries in the snmpNotifyFilterProfileTable are created and deleted using the snmpNotifyFilterProfileRowStatus object.")


class _SnmpNotifyFilterProfileName_Type(SnmpAdminString):
    """Custom type snmpNotifyFilterProfileName based on SnmpAdminString"""
    subtypeSpec = SnmpAdminString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(1, 32),
    )


_SnmpNotifyFilterProfileName_Type.__name__ = "SnmpAdminString"
_SnmpNotifyFilterProfileName_Object = MibTableColumn
snmpNotifyFilterProfileName = _SnmpNotifyFilterProfileName_Object(
    (1, 3, 6, 1, 6, 3, 13, 1, 2, 1, 1),
    _SnmpNotifyFilterProfileName_Type()
)
snmpNotifyFilterProfileName.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpNotifyFilterProfileName.setStatus("current")
if mibBuilder.loadTexts:
    snmpNotifyFilterProfileName.setDescription("The name of the filter profile to be used when generating notifications using the corresponding entry in the snmpTargetAddrTable.")


class _SnmpNotifyFilterProfileStorType_Type(StorageType):
    """Custom type snmpNotifyFilterProfileStorType based on StorageType"""
    defaultValue = 3


_SnmpNotifyFilterProfileStorType_Type.__name__ = "StorageType"
_SnmpNotifyFilterProfileStorType_Object = MibTableColumn
snmpNotifyFilterProfileStorType = _SnmpNotifyFilterProfileStorType_Object(
    (1, 3, 6, 1, 6, 3, 13, 1, 2, 1, 2),
    _SnmpNotifyFilterProfileStorType_Type()
)
snmpNotifyFilterProfileStorType.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpNotifyFilterProfileStorType.setStatus("current")
if mibBuilder.loadTexts:
    snmpNotifyFilterProfileStorType.setDescription("The storage type for this conceptual row. Conceptual rows having the value 'permanent' need not allow write-access to any columnar objects in the row.")
_SnmpNotifyFilterProfileRowStatus_Type = RowStatus
_SnmpNotifyFilterProfileRowStatus_Object = MibTableColumn
snmpNotifyFilterProfileRowStatus = _SnmpNotifyFilterProfileRowStatus_Object(
    (1, 3, 6, 1, 6, 3, 13, 1, 2, 1, 3),
    _SnmpNotifyFilterProfileRowStatus_Type()
)
snmpNotifyFilterProfileRowStatus.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpNotifyFilterProfileRowStatus.setStatus("current")
if mibBuilder.loadTexts:
    snmpNotifyFilterProfileRowStatus.setDescription("The status of this conceptual row. To create a row in this table, a manager must set this object to either createAndGo(4) or createAndWait(5). Until instances of all corresponding columns are appropriately configured, the value of the corresponding instance of the snmpNotifyFilterProfileRowStatus column is 'notReady'. In particular, a newly created row cannot be made active until the corresponding instance of snmpNotifyFilterProfileName has been set.")
_SnmpNotifyFilterTable_Object = MibTable
snmpNotifyFilterTable = _SnmpNotifyFilterTable_Object(
    (1, 3, 6, 1, 6, 3, 13, 1, 3)
)
if mibBuilder.loadTexts:
    snmpNotifyFilterTable.setStatus("current")
if mibBuilder.loadTexts:
    snmpNotifyFilterTable.setDescription("The table of filter profiles. Filter profiles are used to determine whether particular management targets should receive particular notifications. When a notification is generated, it must be compared with the filters associated with each management target which is configured to receive notifications, in order to determine whether it may be sent to each such management target. A more complete discussion of notification filtering can be found in section 6. of [SNMP-APPL].")
_SnmpNotifyFilterEntry_Object = MibTableRow
snmpNotifyFilterEntry = _SnmpNotifyFilterEntry_Object(
    (1, 3, 6, 1, 6, 3, 13, 1, 3, 1)
)
snmpNotifyFilterEntry.setIndexNames(
    (0, "SNMP-NOTIFICATION-MIB", "snmpNotifyFilterProfileName"),
    (1, "SNMP-NOTIFICATION-MIB", "snmpNotifyFilterSubtree"),
)
if mibBuilder.loadTexts:
    snmpNotifyFilterEntry.setStatus("current")
if mibBuilder.loadTexts:
    snmpNotifyFilterEntry.setDescription("An element of a filter profile. Entries in the snmpNotifyFilterTable are created and deleted using the snmpNotifyFilterRowStatus object.")
_SnmpNotifyFilterSubtree_Type = ObjectIdentifier
_SnmpNotifyFilterSubtree_Object = MibTableColumn
snmpNotifyFilterSubtree = _SnmpNotifyFilterSubtree_Object(
    (1, 3, 6, 1, 6, 3, 13, 1, 3, 1, 1),
    _SnmpNotifyFilterSubtree_Type()
)
snmpNotifyFilterSubtree.setMaxAccess("not-accessible")
if mibBuilder.loadTexts:
    snmpNotifyFilterSubtree.setStatus("current")
if mibBuilder.loadTexts:
    snmpNotifyFilterSubtree.setDescription("The MIB subtree which, when combined with the corresponding instance of snmpNotifyFilterMask, defines a family of subtrees which are included in or excluded from the filter profile.")


class _SnmpNotifyFilterMask_Type(OctetString):
    """Custom type snmpNotifyFilterMask based on OctetString"""
    defaultHexValue = ""

    subtypeSpec = OctetString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(0, 16),
    )


_SnmpNotifyFilterMask_Type.__name__ = "OctetString"
_SnmpNotifyFilterMask_Object = MibTableColumn
snmpNotifyFilterMask = _SnmpNotifyFilterMask_Object(
    (1, 3, 6, 1, 6, 3, 13, 1, 3, 1, 2),
    _SnmpNotifyFilterMask_Type()
)
snmpNotifyFilterMask.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpNotifyFilterMask.setStatus("current")
if mibBuilder.loadTexts:
    snmpNotifyFilterMask.setDescription("The bit mask which, in combination with the corresponding instance of snmpNotifyFilterSubtree, defines a family of subtrees which are included in or excluded from the filter profile. Each bit of this bit mask corresponds to a sub-identifier of snmpNotifyFilterSubtree, with the most significant bit of the i-th octet of this octet string value (extended if necessary, see below) corresponding to the (8*i - 7)-th sub-identifier, and the least significant bit of the i-th octet of this octet string corresponding to the (8*i)-th sub-identifier, where i is in the range 1 through 16. Each bit of this bit mask specifies whether or not the corresponding sub-identifiers must match when determining if an OBJECT IDENTIFIER matches this family of filter subtrees; a '1' indicates that an exact match must occur; a '0' indicates 'wild card', i.e., any sub-identifier value matches. Thus, the OBJECT IDENTIFIER X of an object instance is contained in a family of filter subtrees if, for each sub-identifier of the value of snmpNotifyFilterSubtree, either: the i-th bit of snmpNotifyFilterMask is 0, or the i-th sub-identifier of X is equal to the i-th sub-identifier of the value of snmpNotifyFilterSubtree. If the value of this bit mask is M bits long and there are more than M sub-identifiers in the corresponding instance of snmpNotifyFilterSubtree, then the bit mask is extended with 1's to be the required length. Note that when the value of this object is the zero-length string, this extension rule results in a mask of all-1's being used (i.e., no 'wild card'), and the family of filter subtrees is the one subtree uniquely identified by the corresponding instance of snmpNotifyFilterSubtree.")


class _SnmpNotifyFilterType_Type(Integer32):
    """Custom type snmpNotifyFilterType based on Integer32"""
    defaultValue = 1

    subtypeSpec = Integer32.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        SingleValueConstraint(
            *(1,
              2)
        )
    )
    namedValues = NamedValues(
        *(("included", 1),
          ("excluded", 2))
    )


_SnmpNotifyFilterType_Type.__name__ = "Integer32"
_SnmpNotifyFilterType_Object = MibTableColumn
snmpNotifyFilterType = _SnmpNotifyFilterType_Object(
    (1, 3, 6, 1, 6, 3, 13, 1, 3, 1, 3),
    _SnmpNotifyFilterType_Type()
)
snmpNotifyFilterType.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpNotifyFilterType.setStatus("current")
if mibBuilder.loadTexts:
    snmpNotifyFilterType.setDescription("This object indicates whether the family of filter subtrees defined by this entry are included in or excluded from a filter. A more detailed discussion of the use of this object can be found in section 6. of [SNMP-APPL].")


class _SnmpNotifyFilterStorageType_Type(StorageType):
    """Custom type snmpNotifyFilterStorageType based on StorageType"""
    defaultValue = 3


_SnmpNotifyFilterStorageType_Type.__name__ = "StorageType"
_SnmpNotifyFilterStorageType_Object = MibTableColumn
snmpNotifyFilterStorageType = _SnmpNotifyFilterStorageType_Object(
    (1, 3, 6, 1, 6, 3, 13, 1, 3, 1, 4),
    _SnmpNotifyFilterStorageType_Type()
)
snmpNotifyFilterStorageType.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpNotifyFilterStorageType.setStatus("current")
if mibBuilder.loadTexts:
    snmpNotifyFilterStorageType.setDescription("The storage type for this conceptual row. Conceptual rows having the value 'permanent' need not allow write-access to any columnar objects in the row.")
_SnmpNotifyFilterRowStatus_Type = RowStatus
_SnmpNotifyFilterRowStatus_Object = MibTableColumn
snmpNotifyFilterRowStatus = _SnmpNotifyFilterRowStatus_Object(
    (1, 3, 6, 1, 6, 3, 13, 1, 3, 1, 5),
    _SnmpNotifyFilterRowStatus_Type()
)
snmpNotifyFilterRowStatus.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpNotifyFilterRowStatus.setStatus("current")
if mibBuilder.loadTexts:
    snmpNotifyFilterRowStatus.setDescription("The status of this conceptual row. To create a row in this table, a manager must set this object to either createAndGo(4) or createAndWait(5).")
_SnmpNotifyConformance_ObjectIdentity = ObjectIdentity
snmpNotifyConformance = _SnmpNotifyConformance_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 13, 3)
)
_SnmpNotifyCompliances_ObjectIdentity = ObjectIdentity
snmpNotifyCompliances = _SnmpNotifyCompliances_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 13, 3, 1)
)
_SnmpNotifyGroups_ObjectIdentity = ObjectIdentity
snmpNotifyGroups = _SnmpNotifyGroups_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 13, 3, 2)
)

# Managed Objects groups

snmpNotifyGroup = ObjectGroup(
    (1, 3, 6, 1, 6, 3, 13, 3, 2, 1)
)
snmpNotifyGroup.setObjects(
      *(("SNMP-NOTIFICATION-MIB", "snmpNotifyTag"),
        ("SNMP-NOTIFICATION-MIB", "snmpNotifyType"),
        ("SNMP-NOTIFICATION-MIB", "snmpNotifyStorageType"),
        ("SNMP-NOTIFICATION-MIB", "snmpNotifyRowStatus"))
)
if mibBuilder.loadTexts:
    snmpNotifyGroup.setStatus("current")
if mibBuilder.loadTexts:
    snmpNotifyGroup.setDescription("A collection of objects for selecting which management targets are used for generating notifications, and the type of notification to be generated for each selected management target.")

snmpNotifyFilterGroup = ObjectGroup(
    (1, 3, 6, 1, 6, 3, 13, 3, 2, 2)
)
snmpNotifyFilterGroup.setObjects(
      *(("SNMP-NOTIFICATION-MIB", "snmpNotifyFilterProfileName"),
        ("SNMP-NOTIFICATION-MIB", "snmpNotifyFilterProfileStorType"),
        ("SNMP-NOTIFICATION-MIB", "snmpNotifyFilterProfileRowStatus"),
        ("SNMP-NOTIFICATION-MIB", "snmpNotifyFilterMask"),
        ("SNMP-NOTIFICATION-MIB", "snmpNotifyFilterType"),
        ("SNMP-NOTIFICATION-MIB", "snmpNotifyFilterStorageType"),
        ("SNMP-NOTIFICATION-MIB", "snmpNotifyFilterRowStatus"))
)
if mibBuilder.loadTexts:
    snmpNotifyFilterGroup.setStatus("current")
if mibBuilder.loadTexts:
    snmpNotifyFilterGroup.setDescription("A collection of objects providing remote configuration of notification filters.")


# Notification objects


# Notifications groups


# Agent capabilities


# Module compliance

snmpNotifyBasicCompliance = ModuleCompliance(
    (1, 3, 6, 1, 6, 3, 13, 3, 1, 1)
)
snmpNotifyBasicCompliance.setObjects(
      *(("SNMP-TARGET-MIB", "snmpTargetBasicGroup"),
        ("SNMP-NOTIFICATION-MIB", "snmpNotifyGroup"))
)
if mibBuilder.loadTexts:
    snmpNotifyBasicCompliance.setStatus(
        "current"
    )
if mibBuilder.loadTexts:
    snmpNotifyBasicCompliance.setDescription("The compliance statement for minimal SNMP entities which implement only SNMP Unconfirmed-Class notifications and read-create operations on only the snmpTargetAddrTable.")

snmpNotifyBasicFiltersCompliance = ModuleCompliance(
    (1, 3, 6, 1, 6, 3, 13, 3, 1, 2)
)
snmpNotifyBasicFiltersCompliance.setObjects(
      *(("SNMP-TARGET-MIB", "snmpTargetBasicGroup"),
        ("SNMP-NOTIFICATION-MIB", "snmpNotifyGroup"),
        ("SNMP-NOTIFICATION-MIB", "snmpNotifyFilterGroup"))
)
if mibBuilder.loadTexts:
    snmpNotifyBasicFiltersCompliance.setStatus(
        "current"
    )
if mibBuilder.loadTexts:
    snmpNotifyBasicFiltersCompliance.setDescription("The compliance statement for SNMP entities which implement SNMP Unconfirmed-Class notifications with filtering, and read-create operations on all related tables.")

snmpNotifyFullCompliance = ModuleCompliance(
    (1, 3, 6, 1, 6, 3, 13, 3, 1, 3)
)
snmpNotifyFullCompliance.setObjects(
      *(("SNMP-TARGET-MIB", "snmpTargetBasicGroup"),
        ("SNMP-TARGET-MIB", "snmpTargetResponseGroup"),
        ("SNMP-NOTIFICATION-MIB", "snmpNotifyGroup"),
        ("SNMP-NOTIFICATION-MIB", "snmpNotifyFilterGroup"))
)
if mibBuilder.loadTexts:
    snmpNotifyFullCompliance.setStatus(
        "current"
    )
if mibBuilder.loadTexts:
    snmpNotifyFullCompliance.setDescription("The compliance statement for SNMP entities which either implement only SNMP Confirmed-Class notifications, or both SNMP Unconfirmed-Class and Confirmed-Class notifications, plus filtering and read-create operations on all related tables.")


# Export all MIB objects to the MIB builder

mibBuilder.export_symbols(
    "SNMP-NOTIFICATION-MIB",
    **{"snmpNotificationMIB": snmpNotificationMIB,
       "snmpNotifyObjects": snmpNotifyObjects,
       "snmpNotifyTable": snmpNotifyTable,
       "snmpNotifyEntry": snmpNotifyEntry,
       "snmpNotifyName": snmpNotifyName,
       "snmpNotifyTag": snmpNotifyTag,
       "snmpNotifyType": snmpNotifyType,
       "snmpNotifyStorageType": snmpNotifyStorageType,
       "snmpNotifyRowStatus": snmpNotifyRowStatus,
       "snmpNotifyFilterProfileTable": snmpNotifyFilterProfileTable,
       "snmpNotifyFilterProfileEntry": snmpNotifyFilterProfileEntry,
       "snmpNotifyFilterProfileName": snmpNotifyFilterProfileName,
       "snmpNotifyFilterProfileStorType": snmpNotifyFilterProfileStorType,
       "snmpNotifyFilterProfileRowStatus": snmpNotifyFilterProfileRowStatus,
       "snmpNotifyFilterTable": snmpNotifyFilterTable,
       "snmpNotifyFilterEntry": snmpNotifyFilterEntry,
       "snmpNotifyFilterSubtree": snmpNotifyFilterSubtree,
       "snmpNotifyFilterMask": snmpNotifyFilterMask,
       "snmpNotifyFilterType": snmpNotifyFilterType,
       "snmpNotifyFilterStorageType": snmpNotifyFilterStorageType,
       "snmpNotifyFilterRowStatus": snmpNotifyFilterRowStatus,
       "snmpNotifyConformance": snmpNotifyConformance,
       "snmpNotifyCompliances": snmpNotifyCompliances,
       "snmpNotifyBasicCompliance": snmpNotifyBasicCompliance,
       "snmpNotifyBasicFiltersCompliance": snmpNotifyBasicFiltersCompliance,
       "snmpNotifyFullCompliance": snmpNotifyFullCompliance,
       "snmpNotifyGroups": snmpNotifyGroups,
       "snmpNotifyGroup": snmpNotifyGroup,
       "snmpNotifyFilterGroup": snmpNotifyFilterGroup,
       "PYSNMP_MODULE_ID": snmpNotificationMIB}
)
