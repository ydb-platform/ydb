#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
# PySNMP MIB module SNMP-VIEW-BASED-ACM-MIB (https://www.pysnmp.com/pysnmp)
# ASN.1 source file://asn1/SNMP-VIEW-BASED-ACM-MIB
# Produced by pysmi-1.5.8 at Sat Nov  2 15:25:41 2024
# On host MacBook-Pro.local platform Darwin version 24.1.0 by user lextm
# Using Python version 3.12.0 (main, Nov 14 2023, 23:52:11) [Clang 15.0.0 (clang-1500.0.40.1)]
#
# IMPORTANT: customization

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
 SnmpSecurityLevel,
 SnmpSecurityModel) = mibBuilder.import_symbols(
    "SNMP-FRAMEWORK-MIB",
    "SnmpAdminString",
    "SnmpSecurityLevel",
    "SnmpSecurityModel")

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
 TextualConvention,
 TestAndIncr) = mibBuilder.import_symbols(
    "SNMPv2-TC",
    "DisplayString",
    "RowStatus",
    "StorageType",
    "TextualConvention",
    "TestAndIncr")


# MODULE-IDENTITY

snmpVacmMIB = ModuleIdentity(
    (1, 3, 6, 1, 6, 3, 16)
)
if mibBuilder.loadTexts:
    snmpVacmMIB.setRevisions(
        ("2002-10-16 00:00",
         "1999-01-20 00:00",
         "1997-11-20 00:00")
    )
if mibBuilder.loadTexts:
    snmpVacmMIB.setLastUpdated("200210160000Z")
if mibBuilder.loadTexts:
    snmpVacmMIB.setOrganization("SNMPv3 Working Group")
if mibBuilder.loadTexts:
    snmpVacmMIB.setContactInfo("WG-email: snmpv3@lists.tislabs.com Subscribe: majordomo@lists.tislabs.com In message body: subscribe snmpv3 Co-Chair: Russ Mundy Network Associates Laboratories postal: 15204 Omega Drive, Suite 300 Rockville, MD 20850-4601 USA email: mundy@tislabs.com phone: +1 301-947-7107 Co-Chair: David Harrington Enterasys Networks Postal: 35 Industrial Way P. O. Box 5004 Rochester, New Hampshire 03866-5005 USA EMail: dbh@enterasys.com Phone: +1 603-337-2614 Co-editor: Bert Wijnen Lucent Technologies postal: Schagen 33 3461 GL Linschoten Netherlands email: bwijnen@lucent.com phone: +31-348-480-685 Co-editor: Randy Presuhn BMC Software, Inc. postal: 2141 North First Street San Jose, CA 95131 USA email: randy_presuhn@bmc.com phone: +1 408-546-1006 Co-editor: Keith McCloghrie Cisco Systems, Inc. postal: 170 West Tasman Drive San Jose, CA 95134-1706 USA email: kzm@cisco.com phone: +1-408-526-5260 ")
if mibBuilder.loadTexts:
    snmpVacmMIB.setDescription("The management information definitions for the View-based Access Control Model for SNMP. Copyright (C) The Internet Society (2002). This version of this MIB module is part of RFC 3415; see the RFC itself for full legal notices. ")


# Types definitions


# TEXTUAL-CONVENTIONS



# MIB Managed Objects in the order of their OIDs

_VacmMIBObjects_ObjectIdentity = ObjectIdentity
vacmMIBObjects = _VacmMIBObjects_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 16, 1)
)
_VacmContextTable_Object = MibTable
vacmContextTable = _VacmContextTable_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 1)
)
if mibBuilder.loadTexts:
    vacmContextTable.setStatus("current")
if mibBuilder.loadTexts:
    vacmContextTable.setDescription("The table of locally available contexts. This table provides information to SNMP Command Generator applications so that they can properly configure the vacmAccessTable to control access to all contexts at the SNMP entity. This table may change dynamically if the SNMP entity allows that contexts are added/deleted dynamically (for instance when its configuration changes). Such changes would happen only if the management instrumentation at that SNMP entity recognizes more (or fewer) contexts. The presence of entries in this table and of entries in the vacmAccessTable are independent. That is, a context identified by an entry in this table is not necessarily referenced by any entries in the vacmAccessTable; and the context(s) referenced by an entry in the vacmAccessTable does not necessarily currently exist and thus need not be identified by an entry in this table. This table must be made accessible via the default context so that Command Responder applications have a standard way of retrieving the information. This table is read-only. It cannot be configured via SNMP. ")
_VacmContextEntry_Object = MibTableRow
vacmContextEntry = _VacmContextEntry_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 1, 1)
)
vacmContextEntry.setIndexNames(
    (0, "SNMP-VIEW-BASED-ACM-MIB", "vacmContextName"),
)
if mibBuilder.loadTexts:
    vacmContextEntry.setStatus("current")
if mibBuilder.loadTexts:
    vacmContextEntry.setDescription("Information about a particular context.")


class _VacmContextName_Type(SnmpAdminString):
    """Custom type vacmContextName based on SnmpAdminString"""
    subtypeSpec = SnmpAdminString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(0, 32),
    )


_VacmContextName_Type.__name__ = "SnmpAdminString"
_VacmContextName_Object = MibTableColumn
vacmContextName = _VacmContextName_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 1, 1, 1),
    _VacmContextName_Type()
)
vacmContextName.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    vacmContextName.setStatus("current")
if mibBuilder.loadTexts:
    vacmContextName.setDescription("A human readable name identifying a particular context at a particular SNMP entity. The empty contextName (zero length) represents the default context. ")

# IMPORTANT: customization
# The RowStatus column is not present in the MIB
vacmContextStatus = MibTableColumn(
    (1, 3, 6, 1, 6, 3, 16, 1, 1, 1, 2), RowStatus()
).setMaxAccess("read-create")
if mibBuilder.loadTexts:
    vacmContextStatus.setStatus("current")
if mibBuilder.loadTexts:
    vacmContextStatus.setDescription(
        "The status of this conceptual row. Until instances of all corresponding columns are appropriately configured, the value of the corresponding instance of the vacmContextTableStatus column is 'notReady'. In particular, a newly created row cannot be made active until a value has been set for vacmContextName. The RowStatus TC [RFC2579] requires that this DESCRIPTION clause states under which circumstances other objects in this row can be modified: The value of this object has no effect on whether other objects in this conceptual row can be modified. "
    )

_VacmSecurityToGroupTable_Object = MibTable
vacmSecurityToGroupTable = _VacmSecurityToGroupTable_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 2)
)
if mibBuilder.loadTexts:
    vacmSecurityToGroupTable.setStatus("current")
if mibBuilder.loadTexts:
    vacmSecurityToGroupTable.setDescription("This table maps a combination of securityModel and securityName into a groupName which is used to define an access control policy for a group of principals. ")
_VacmSecurityToGroupEntry_Object = MibTableRow
vacmSecurityToGroupEntry = _VacmSecurityToGroupEntry_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 2, 1)
)
vacmSecurityToGroupEntry.setIndexNames(
    (0, "SNMP-VIEW-BASED-ACM-MIB", "vacmSecurityModel"),
    (0, "SNMP-VIEW-BASED-ACM-MIB", "vacmSecurityName"),
)
if mibBuilder.loadTexts:
    vacmSecurityToGroupEntry.setStatus("current")
if mibBuilder.loadTexts:
    vacmSecurityToGroupEntry.setDescription("An entry in this table maps the combination of a securityModel and securityName into a groupName. ")


class _VacmSecurityModel_Type(SnmpSecurityModel):
    """Custom type vacmSecurityModel based on SnmpSecurityModel"""
    subtypeSpec = SnmpSecurityModel.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueRangeConstraint(1, 2147483647),
    )


_VacmSecurityModel_Type.__name__ = "SnmpSecurityModel"
_VacmSecurityModel_Object = MibTableColumn
vacmSecurityModel = _VacmSecurityModel_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 2, 1, 1),
    _VacmSecurityModel_Type()
)
vacmSecurityModel.setMaxAccess("not-accessible")
if mibBuilder.loadTexts:
    vacmSecurityModel.setStatus("current")
if mibBuilder.loadTexts:
    vacmSecurityModel.setDescription("The Security Model, by which the vacmSecurityName referenced by this entry is provided. Note, this object may not take the 'any' (0) value. ")


class _VacmSecurityName_Type(SnmpAdminString):
    """Custom type vacmSecurityName based on SnmpAdminString"""
    subtypeSpec = SnmpAdminString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(1, 32),
    )


_VacmSecurityName_Type.__name__ = "SnmpAdminString"
_VacmSecurityName_Object = MibTableColumn
vacmSecurityName = _VacmSecurityName_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 2, 1, 2),
    _VacmSecurityName_Type()
)
vacmSecurityName.setMaxAccess("not-accessible")
if mibBuilder.loadTexts:
    vacmSecurityName.setStatus("current")
if mibBuilder.loadTexts:
    vacmSecurityName.setDescription("The securityName for the principal, represented in a Security Model independent format, which is mapped by this entry to a groupName. ")


class _VacmGroupName_Type(SnmpAdminString):
    """Custom type vacmGroupName based on SnmpAdminString"""
    subtypeSpec = SnmpAdminString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(1, 32),
    )


_VacmGroupName_Type.__name__ = "SnmpAdminString"
_VacmGroupName_Object = MibTableColumn
vacmGroupName = _VacmGroupName_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 2, 1, 3),
    _VacmGroupName_Type()
)
vacmGroupName.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    vacmGroupName.setStatus("current")
if mibBuilder.loadTexts:
    vacmGroupName.setDescription("The name of the group to which this entry (e.g., the combination of securityModel and securityName) belongs. This groupName is used as index into the vacmAccessTable to select an access control policy. However, a value in this table does not imply that an instance with the value exists in table vacmAccesTable. ")


class _VacmSecurityToGroupStorageType_Type(StorageType):
    """Custom type vacmSecurityToGroupStorageType based on StorageType"""
    defaultValue = 3


_VacmSecurityToGroupStorageType_Type.__name__ = "StorageType"
_VacmSecurityToGroupStorageType_Object = MibTableColumn
vacmSecurityToGroupStorageType = _VacmSecurityToGroupStorageType_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 2, 1, 4),
    _VacmSecurityToGroupStorageType_Type()
)
vacmSecurityToGroupStorageType.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    vacmSecurityToGroupStorageType.setStatus("current")
if mibBuilder.loadTexts:
    vacmSecurityToGroupStorageType.setDescription("The storage type for this conceptual row. Conceptual rows having the value 'permanent' need not allow write-access to any columnar objects in the row. ")
_VacmSecurityToGroupStatus_Type = RowStatus
_VacmSecurityToGroupStatus_Object = MibTableColumn
vacmSecurityToGroupStatus = _VacmSecurityToGroupStatus_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 2, 1, 5),
    _VacmSecurityToGroupStatus_Type()
)
vacmSecurityToGroupStatus.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    vacmSecurityToGroupStatus.setStatus("current")
if mibBuilder.loadTexts:
    vacmSecurityToGroupStatus.setDescription("The status of this conceptual row. Until instances of all corresponding columns are appropriately configured, the value of the corresponding instance of the vacmSecurityToGroupStatus column is 'notReady'. In particular, a newly created row cannot be made active until a value has been set for vacmGroupName. The RowStatus TC [RFC2579] requires that this DESCRIPTION clause states under which circumstances other objects in this row can be modified: The value of this object has no effect on whether other objects in this conceptual row can be modified. ")
_VacmAccessTable_Object = MibTable
vacmAccessTable = _VacmAccessTable_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 4)
)
if mibBuilder.loadTexts:
    vacmAccessTable.setStatus("current")
if mibBuilder.loadTexts:
    vacmAccessTable.setDescription("The table of access rights for groups. Each entry is indexed by a groupName, a contextPrefix, a securityModel and a securityLevel. To determine whether access is allowed, one entry from this table needs to be selected and the proper viewName from that entry must be used for access control checking. To select the proper entry, follow these steps: 1) the set of possible matches is formed by the intersection of the following sets of entries: the set of entries with identical vacmGroupName the union of these two sets: - the set with identical vacmAccessContextPrefix - the set of entries with vacmAccessContextMatch value of 'prefix' and matching vacmAccessContextPrefix intersected with the union of these two sets: - the set of entries with identical vacmSecurityModel - the set of entries with vacmSecurityModel value of 'any' intersected with the set of entries with vacmAccessSecurityLevel value less than or equal to the requested securityLevel 2) if this set has only one member, we're done otherwise, it comes down to deciding how to weight the preferences between ContextPrefixes, SecurityModels, and SecurityLevels as follows: a) if the subset of entries with securityModel matching the securityModel in the message is not empty, then discard the rest. b) if the subset of entries with vacmAccessContextPrefix matching the contextName in the message is not empty, then discard the rest c) discard all entries with ContextPrefixes shorter than the longest one remaining in the set d) select the entry with the highest securityLevel Please note that for securityLevel noAuthNoPriv, all groups are really equivalent since the assumption that the securityName has been authenticated does not hold. ")
_VacmAccessEntry_Object = MibTableRow
vacmAccessEntry = _VacmAccessEntry_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 4, 1)
)
vacmAccessEntry.setIndexNames(
    (0, "SNMP-VIEW-BASED-ACM-MIB", "vacmGroupName"),
    (0, "SNMP-VIEW-BASED-ACM-MIB", "vacmAccessContextPrefix"),
    (0, "SNMP-VIEW-BASED-ACM-MIB", "vacmAccessSecurityModel"),
    (0, "SNMP-VIEW-BASED-ACM-MIB", "vacmAccessSecurityLevel"),
)
if mibBuilder.loadTexts:
    vacmAccessEntry.setStatus("current")
if mibBuilder.loadTexts:
    vacmAccessEntry.setDescription("An access right configured in the Local Configuration Datastore (LCD) authorizing access to an SNMP context. Entries in this table can use an instance value for object vacmGroupName even if no entry in table vacmAccessSecurityToGroupTable has a corresponding value for object vacmGroupName. ")


class _VacmAccessContextPrefix_Type(SnmpAdminString):
    """Custom type vacmAccessContextPrefix based on SnmpAdminString"""
    subtypeSpec = SnmpAdminString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(0, 32),
    )


_VacmAccessContextPrefix_Type.__name__ = "SnmpAdminString"
_VacmAccessContextPrefix_Object = MibTableColumn
vacmAccessContextPrefix = _VacmAccessContextPrefix_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 4, 1, 1),
    _VacmAccessContextPrefix_Type()
)
vacmAccessContextPrefix.setMaxAccess("not-accessible")
if mibBuilder.loadTexts:
    vacmAccessContextPrefix.setStatus("current")
if mibBuilder.loadTexts:
    vacmAccessContextPrefix.setDescription("In order to gain the access rights allowed by this conceptual row, a contextName must match exactly (if the value of vacmAccessContextMatch is 'exact') or partially (if the value of vacmAccessContextMatch is 'prefix') to the value of the instance of this object. ")
_VacmAccessSecurityModel_Type = SnmpSecurityModel
_VacmAccessSecurityModel_Object = MibTableColumn
vacmAccessSecurityModel = _VacmAccessSecurityModel_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 4, 1, 2),
    _VacmAccessSecurityModel_Type()
)
vacmAccessSecurityModel.setMaxAccess("not-accessible")
if mibBuilder.loadTexts:
    vacmAccessSecurityModel.setStatus("current")
if mibBuilder.loadTexts:
    vacmAccessSecurityModel.setDescription("In order to gain the access rights allowed by this conceptual row, this securityModel must be in use. ")
_VacmAccessSecurityLevel_Type = SnmpSecurityLevel
_VacmAccessSecurityLevel_Object = MibTableColumn
vacmAccessSecurityLevel = _VacmAccessSecurityLevel_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 4, 1, 3),
    _VacmAccessSecurityLevel_Type()
)
vacmAccessSecurityLevel.setMaxAccess("not-accessible")
if mibBuilder.loadTexts:
    vacmAccessSecurityLevel.setStatus("current")
if mibBuilder.loadTexts:
    vacmAccessSecurityLevel.setDescription("The minimum level of security required in order to gain the access rights allowed by this conceptual row. A securityLevel of noAuthNoPriv is less than authNoPriv which in turn is less than authPriv. If multiple entries are equally indexed except for this vacmAccessSecurityLevel index, then the entry which has the highest value for vacmAccessSecurityLevel is selected. ")


class _VacmAccessContextMatch_Type(Integer32):
    """Custom type vacmAccessContextMatch based on Integer32"""
    defaultValue = 1

    subtypeSpec = Integer32.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        SingleValueConstraint(
            *(1,
              2)
        )
    )
    namedValues = NamedValues(
        *(("exact", 1),
          ("prefix", 2))
    )


_VacmAccessContextMatch_Type.__name__ = "Integer32"
_VacmAccessContextMatch_Object = MibTableColumn
vacmAccessContextMatch = _VacmAccessContextMatch_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 4, 1, 4),
    _VacmAccessContextMatch_Type()
)
vacmAccessContextMatch.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    vacmAccessContextMatch.setStatus("current")
if mibBuilder.loadTexts:
    vacmAccessContextMatch.setDescription("If the value of this object is exact(1), then all rows where the contextName exactly matches vacmAccessContextPrefix are selected. If the value of this object is prefix(2), then all rows where the contextName whose starting octets exactly match vacmAccessContextPrefix are selected. This allows for a simple form of wildcarding. ")


class _VacmAccessReadViewName_Type(SnmpAdminString):
    """Custom type vacmAccessReadViewName based on SnmpAdminString"""
    defaultHexValue = ""

    subtypeSpec = SnmpAdminString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(0, 32),
    )


_VacmAccessReadViewName_Type.__name__ = "SnmpAdminString"
_VacmAccessReadViewName_Object = MibTableColumn
vacmAccessReadViewName = _VacmAccessReadViewName_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 4, 1, 5),
    _VacmAccessReadViewName_Type()
)
vacmAccessReadViewName.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    vacmAccessReadViewName.setStatus("current")
if mibBuilder.loadTexts:
    vacmAccessReadViewName.setDescription("The value of an instance of this object identifies the MIB view of the SNMP context to which this conceptual row authorizes read access. The identified MIB view is that one for which the vacmViewTreeFamilyViewName has the same value as the instance of this object; if the value is the empty string or if there is no active MIB view having this value of vacmViewTreeFamilyViewName, then no access is granted. ")


class _VacmAccessWriteViewName_Type(SnmpAdminString):
    """Custom type vacmAccessWriteViewName based on SnmpAdminString"""
    defaultHexValue = ""

    subtypeSpec = SnmpAdminString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(0, 32),
    )


_VacmAccessWriteViewName_Type.__name__ = "SnmpAdminString"
_VacmAccessWriteViewName_Object = MibTableColumn
vacmAccessWriteViewName = _VacmAccessWriteViewName_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 4, 1, 6),
    _VacmAccessWriteViewName_Type()
)
vacmAccessWriteViewName.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    vacmAccessWriteViewName.setStatus("current")
if mibBuilder.loadTexts:
    vacmAccessWriteViewName.setDescription("The value of an instance of this object identifies the MIB view of the SNMP context to which this conceptual row authorizes write access. The identified MIB view is that one for which the vacmViewTreeFamilyViewName has the same value as the instance of this object; if the value is the empty string or if there is no active MIB view having this value of vacmViewTreeFamilyViewName, then no access is granted. ")


class _VacmAccessNotifyViewName_Type(SnmpAdminString):
    """Custom type vacmAccessNotifyViewName based on SnmpAdminString"""
    defaultHexValue = ""

    subtypeSpec = SnmpAdminString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(0, 32),
    )


_VacmAccessNotifyViewName_Type.__name__ = "SnmpAdminString"
_VacmAccessNotifyViewName_Object = MibTableColumn
vacmAccessNotifyViewName = _VacmAccessNotifyViewName_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 4, 1, 7),
    _VacmAccessNotifyViewName_Type()
)
vacmAccessNotifyViewName.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    vacmAccessNotifyViewName.setStatus("current")
if mibBuilder.loadTexts:
    vacmAccessNotifyViewName.setDescription("The value of an instance of this object identifies the MIB view of the SNMP context to which this conceptual row authorizes access for notifications. The identified MIB view is that one for which the vacmViewTreeFamilyViewName has the same value as the instance of this object; if the value is the empty string or if there is no active MIB view having this value of vacmViewTreeFamilyViewName, then no access is granted. ")


class _VacmAccessStorageType_Type(StorageType):
    """Custom type vacmAccessStorageType based on StorageType"""
    defaultValue = 3


_VacmAccessStorageType_Type.__name__ = "StorageType"
_VacmAccessStorageType_Object = MibTableColumn
vacmAccessStorageType = _VacmAccessStorageType_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 4, 1, 8),
    _VacmAccessStorageType_Type()
)
vacmAccessStorageType.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    vacmAccessStorageType.setStatus("current")
if mibBuilder.loadTexts:
    vacmAccessStorageType.setDescription("The storage type for this conceptual row. Conceptual rows having the value 'permanent' need not allow write-access to any columnar objects in the row. ")
_VacmAccessStatus_Type = RowStatus
_VacmAccessStatus_Object = MibTableColumn
vacmAccessStatus = _VacmAccessStatus_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 4, 1, 9),
    _VacmAccessStatus_Type()
)
vacmAccessStatus.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    vacmAccessStatus.setStatus("current")
if mibBuilder.loadTexts:
    vacmAccessStatus.setDescription("The status of this conceptual row. The RowStatus TC [RFC2579] requires that this DESCRIPTION clause states under which circumstances other objects in this row can be modified: The value of this object has no effect on whether other objects in this conceptual row can be modified. ")
_VacmMIBViews_ObjectIdentity = ObjectIdentity
vacmMIBViews = _VacmMIBViews_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 16, 1, 5)
)
_VacmViewSpinLock_Type = TestAndIncr
_VacmViewSpinLock_Object = MibScalar
vacmViewSpinLock = _VacmViewSpinLock_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 5, 1),
    _VacmViewSpinLock_Type()
)
vacmViewSpinLock.setMaxAccess("read-write")
if mibBuilder.loadTexts:
    vacmViewSpinLock.setStatus("current")
if mibBuilder.loadTexts:
    vacmViewSpinLock.setDescription("An advisory lock used to allow cooperating SNMP Command Generator applications to coordinate their use of the Set operation in creating or modifying views. When creating a new view or altering an existing view, it is important to understand the potential interactions with other uses of the view. The vacmViewSpinLock should be retrieved. The name of the view to be created should be determined to be unique by the SNMP Command Generator application by consulting the vacmViewTreeFamilyTable. Finally, the named view may be created (Set), including the advisory lock. If another SNMP Command Generator application has altered the views in the meantime, then the spin lock's value will have changed, and so this creation will fail because it will specify the wrong value for the spin lock. Since this is an advisory lock, the use of this lock is not enforced. ")
_VacmViewTreeFamilyTable_Object = MibTable
vacmViewTreeFamilyTable = _VacmViewTreeFamilyTable_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 5, 2)
)
if mibBuilder.loadTexts:
    vacmViewTreeFamilyTable.setStatus("current")
if mibBuilder.loadTexts:
    vacmViewTreeFamilyTable.setDescription("Locally held information about families of subtrees within MIB views. Each MIB view is defined by two sets of view subtrees: - the included view subtrees, and - the excluded view subtrees. Every such view subtree, both the included and the excluded ones, is defined in this table. To determine if a particular object instance is in a particular MIB view, compare the object instance's OBJECT IDENTIFIER with each of the MIB view's active entries in this table. If none match, then the object instance is not in the MIB view. If one or more match, then the object instance is included in, or excluded from, the MIB view according to the value of vacmViewTreeFamilyType in the entry whose value of vacmViewTreeFamilySubtree has the most sub-identifiers. If multiple entries match and have the same number of sub-identifiers (when wildcarding is specified with the value of vacmViewTreeFamilyMask), then the lexicographically greatest instance of vacmViewTreeFamilyType determines the inclusion or exclusion. An object instance's OBJECT IDENTIFIER X matches an active entry in this table when the number of sub-identifiers in X is at least as many as in the value of vacmViewTreeFamilySubtree for the entry, and each sub-identifier in the value of vacmViewTreeFamilySubtree matches its corresponding sub-identifier in X. Two sub-identifiers match either if the corresponding bit of the value of vacmViewTreeFamilyMask for the entry is zero (the 'wild card' value), or if they are equal. A 'family' of subtrees is the set of subtrees defined by a particular combination of values of vacmViewTreeFamilySubtree and vacmViewTreeFamilyMask. In the case where no 'wild card' is defined in the vacmViewTreeFamilyMask, the family of subtrees reduces to a single subtree. When creating or changing MIB views, an SNMP Command Generator application should utilize the vacmViewSpinLock to try to avoid collisions. See DESCRIPTION clause of vacmViewSpinLock. When creating MIB views, it is strongly advised that first the 'excluded' vacmViewTreeFamilyEntries are created and then the 'included' entries. When deleting MIB views, it is strongly advised that first the 'included' vacmViewTreeFamilyEntries are deleted and then the 'excluded' entries. If a create for an entry for instance-level access control is received and the implementation does not support instance-level granularity, then an inconsistentName error must be returned. ")
_VacmViewTreeFamilyEntry_Object = MibTableRow
vacmViewTreeFamilyEntry = _VacmViewTreeFamilyEntry_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 5, 2, 1)
)
vacmViewTreeFamilyEntry.setIndexNames(
    (0, "SNMP-VIEW-BASED-ACM-MIB", "vacmViewTreeFamilyViewName"),
    (0, "SNMP-VIEW-BASED-ACM-MIB", "vacmViewTreeFamilySubtree"),
)
if mibBuilder.loadTexts:
    vacmViewTreeFamilyEntry.setStatus("current")
if mibBuilder.loadTexts:
    vacmViewTreeFamilyEntry.setDescription("Information on a particular family of view subtrees included in or excluded from a particular SNMP context's MIB view. Implementations must not restrict the number of families of view subtrees for a given MIB view, except as dictated by resource constraints on the overall number of entries in the vacmViewTreeFamilyTable. If no conceptual rows exist in this table for a given MIB view (viewName), that view may be thought of as consisting of the empty set of view subtrees. ")


class _VacmViewTreeFamilyViewName_Type(SnmpAdminString):
    """Custom type vacmViewTreeFamilyViewName based on SnmpAdminString"""
    subtypeSpec = SnmpAdminString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(1, 32),
    )


_VacmViewTreeFamilyViewName_Type.__name__ = "SnmpAdminString"
_VacmViewTreeFamilyViewName_Object = MibTableColumn
vacmViewTreeFamilyViewName = _VacmViewTreeFamilyViewName_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 5, 2, 1, 1),
    _VacmViewTreeFamilyViewName_Type()
)
vacmViewTreeFamilyViewName.setMaxAccess("not-accessible")
if mibBuilder.loadTexts:
    vacmViewTreeFamilyViewName.setStatus("current")
if mibBuilder.loadTexts:
    vacmViewTreeFamilyViewName.setDescription("The human readable name for a family of view subtrees. ")
_VacmViewTreeFamilySubtree_Type = ObjectIdentifier
_VacmViewTreeFamilySubtree_Object = MibTableColumn
vacmViewTreeFamilySubtree = _VacmViewTreeFamilySubtree_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 5, 2, 1, 2),
    _VacmViewTreeFamilySubtree_Type()
)
vacmViewTreeFamilySubtree.setMaxAccess("not-accessible")
if mibBuilder.loadTexts:
    vacmViewTreeFamilySubtree.setStatus("current")
if mibBuilder.loadTexts:
    vacmViewTreeFamilySubtree.setDescription("The MIB subtree which when combined with the corresponding instance of vacmViewTreeFamilyMask defines a family of view subtrees. ")


class _VacmViewTreeFamilyMask_Type(OctetString):
    """Custom type vacmViewTreeFamilyMask based on OctetString"""
    defaultHexValue = ""

    subtypeSpec = OctetString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(0, 16),
    )


_VacmViewTreeFamilyMask_Type.__name__ = "OctetString"
_VacmViewTreeFamilyMask_Object = MibTableColumn
vacmViewTreeFamilyMask = _VacmViewTreeFamilyMask_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 5, 2, 1, 3),
    _VacmViewTreeFamilyMask_Type()
)
vacmViewTreeFamilyMask.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    vacmViewTreeFamilyMask.setStatus("current")
if mibBuilder.loadTexts:
    vacmViewTreeFamilyMask.setDescription("The bit mask which, in combination with the corresponding instance of vacmViewTreeFamilySubtree, defines a family of view subtrees. Each bit of this bit mask corresponds to a sub-identifier of vacmViewTreeFamilySubtree, with the most significant bit of the i-th octet of this octet string value (extended if necessary, see below) corresponding to the (8*i - 7)-th sub-identifier, and the least significant bit of the i-th octet of this octet string corresponding to the (8*i)-th sub-identifier, where i is in the range 1 through 16. Each bit of this bit mask specifies whether or not the corresponding sub-identifiers must match when determining if an OBJECT IDENTIFIER is in this family of view subtrees; a '1' indicates that an exact match must occur; a '0' indicates 'wild card', i.e., any sub-identifier value matches. Thus, the OBJECT IDENTIFIER X of an object instance is contained in a family of view subtrees if, for each sub-identifier of the value of vacmViewTreeFamilySubtree, either: the i-th bit of vacmViewTreeFamilyMask is 0, or the i-th sub-identifier of X is equal to the i-th sub-identifier of the value of vacmViewTreeFamilySubtree. If the value of this bit mask is M bits long and there are more than M sub-identifiers in the corresponding instance of vacmViewTreeFamilySubtree, then the bit mask is extended with 1's to be the required length. Note that when the value of this object is the zero-length string, this extension rule results in a mask of all-1's being used (i.e., no 'wild card'), and the family of view subtrees is the one view subtree uniquely identified by the corresponding instance of vacmViewTreeFamilySubtree. Note that masks of length greater than zero length do not need to be supported. In this case this object is made read-only. ")


class _VacmViewTreeFamilyType_Type(Integer32):
    """Custom type vacmViewTreeFamilyType based on Integer32"""
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


_VacmViewTreeFamilyType_Type.__name__ = "Integer32"
_VacmViewTreeFamilyType_Object = MibTableColumn
vacmViewTreeFamilyType = _VacmViewTreeFamilyType_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 5, 2, 1, 4),
    _VacmViewTreeFamilyType_Type()
)
vacmViewTreeFamilyType.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    vacmViewTreeFamilyType.setStatus("current")
if mibBuilder.loadTexts:
    vacmViewTreeFamilyType.setDescription("Indicates whether the corresponding instances of vacmViewTreeFamilySubtree and vacmViewTreeFamilyMask define a family of view subtrees which is included in or excluded from the MIB view. ")


class _VacmViewTreeFamilyStorageType_Type(StorageType):
    """Custom type vacmViewTreeFamilyStorageType based on StorageType"""
    defaultValue = 3


_VacmViewTreeFamilyStorageType_Type.__name__ = "StorageType"
_VacmViewTreeFamilyStorageType_Object = MibTableColumn
vacmViewTreeFamilyStorageType = _VacmViewTreeFamilyStorageType_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 5, 2, 1, 5),
    _VacmViewTreeFamilyStorageType_Type()
)
vacmViewTreeFamilyStorageType.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    vacmViewTreeFamilyStorageType.setStatus("current")
if mibBuilder.loadTexts:
    vacmViewTreeFamilyStorageType.setDescription("The storage type for this conceptual row. Conceptual rows having the value 'permanent' need not allow write-access to any columnar objects in the row. ")
_VacmViewTreeFamilyStatus_Type = RowStatus
_VacmViewTreeFamilyStatus_Object = MibTableColumn
vacmViewTreeFamilyStatus = _VacmViewTreeFamilyStatus_Object(
    (1, 3, 6, 1, 6, 3, 16, 1, 5, 2, 1, 6),
    _VacmViewTreeFamilyStatus_Type()
)
vacmViewTreeFamilyStatus.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    vacmViewTreeFamilyStatus.setStatus("current")
if mibBuilder.loadTexts:
    vacmViewTreeFamilyStatus.setDescription("The status of this conceptual row. The RowStatus TC [RFC2579] requires that this DESCRIPTION clause states under which circumstances other objects in this row can be modified: The value of this object has no effect on whether other objects in this conceptual row can be modified. ")
_VacmMIBConformance_ObjectIdentity = ObjectIdentity
vacmMIBConformance = _VacmMIBConformance_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 16, 2)
)
_VacmMIBCompliances_ObjectIdentity = ObjectIdentity
vacmMIBCompliances = _VacmMIBCompliances_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 16, 2, 1)
)
_VacmMIBGroups_ObjectIdentity = ObjectIdentity
vacmMIBGroups = _VacmMIBGroups_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 16, 2, 2)
)

# Managed Objects groups

vacmBasicGroup = ObjectGroup(
    (1, 3, 6, 1, 6, 3, 16, 2, 2, 1)
)
vacmBasicGroup.setObjects(
      *(("SNMP-VIEW-BASED-ACM-MIB", "vacmContextName"),
        ("SNMP-VIEW-BASED-ACM-MIB", "vacmGroupName"),
        ("SNMP-VIEW-BASED-ACM-MIB", "vacmSecurityToGroupStorageType"),
        ("SNMP-VIEW-BASED-ACM-MIB", "vacmSecurityToGroupStatus"),
        ("SNMP-VIEW-BASED-ACM-MIB", "vacmAccessContextMatch"),
        ("SNMP-VIEW-BASED-ACM-MIB", "vacmAccessReadViewName"),
        ("SNMP-VIEW-BASED-ACM-MIB", "vacmAccessWriteViewName"),
        ("SNMP-VIEW-BASED-ACM-MIB", "vacmAccessNotifyViewName"),
        ("SNMP-VIEW-BASED-ACM-MIB", "vacmAccessStorageType"),
        ("SNMP-VIEW-BASED-ACM-MIB", "vacmAccessStatus"),
        ("SNMP-VIEW-BASED-ACM-MIB", "vacmViewSpinLock"),
        ("SNMP-VIEW-BASED-ACM-MIB", "vacmViewTreeFamilyMask"),
        ("SNMP-VIEW-BASED-ACM-MIB", "vacmViewTreeFamilyType"),
        ("SNMP-VIEW-BASED-ACM-MIB", "vacmViewTreeFamilyStorageType"),
        ("SNMP-VIEW-BASED-ACM-MIB", "vacmViewTreeFamilyStatus"))
)
if mibBuilder.loadTexts:
    vacmBasicGroup.setStatus("current")
if mibBuilder.loadTexts:
    vacmBasicGroup.setDescription("A collection of objects providing for remote configuration of an SNMP engine which implements the SNMP View-based Access Control Model. ")


# Notification objects


# Notifications groups


# Agent capabilities


# Module compliance

vacmMIBCompliance = ModuleCompliance(
    (1, 3, 6, 1, 6, 3, 16, 2, 1, 1)
)
vacmMIBCompliance.setObjects(
    ("SNMP-VIEW-BASED-ACM-MIB", "vacmBasicGroup")
)
if mibBuilder.loadTexts:
    vacmMIBCompliance.setStatus(
        "current"
    )
if mibBuilder.loadTexts:
    vacmMIBCompliance.setDescription("The compliance statement for SNMP engines which implement the SNMP View-based Access Control Model configuration MIB. ")


# Export all MIB objects to the MIB builder

mibBuilder.export_symbols(
    "SNMP-VIEW-BASED-ACM-MIB",
    **{"snmpVacmMIB": snmpVacmMIB,
       "vacmMIBObjects": vacmMIBObjects,
       "vacmContextTable": vacmContextTable,
       "vacmContextEntry": vacmContextEntry,
       "vacmContextName": vacmContextName,
       "vacmContextStatus": vacmContextStatus,
       "vacmSecurityToGroupTable": vacmSecurityToGroupTable,
       "vacmSecurityToGroupEntry": vacmSecurityToGroupEntry,
       "vacmSecurityModel": vacmSecurityModel,
       "vacmSecurityName": vacmSecurityName,
       "vacmGroupName": vacmGroupName,
       "vacmSecurityToGroupStorageType": vacmSecurityToGroupStorageType,
       "vacmSecurityToGroupStatus": vacmSecurityToGroupStatus,
       "vacmAccessTable": vacmAccessTable,
       "vacmAccessEntry": vacmAccessEntry,
       "vacmAccessContextPrefix": vacmAccessContextPrefix,
       "vacmAccessSecurityModel": vacmAccessSecurityModel,
       "vacmAccessSecurityLevel": vacmAccessSecurityLevel,
       "vacmAccessContextMatch": vacmAccessContextMatch,
       "vacmAccessReadViewName": vacmAccessReadViewName,
       "vacmAccessWriteViewName": vacmAccessWriteViewName,
       "vacmAccessNotifyViewName": vacmAccessNotifyViewName,
       "vacmAccessStorageType": vacmAccessStorageType,
       "vacmAccessStatus": vacmAccessStatus,
       "vacmMIBViews": vacmMIBViews,
       "vacmViewSpinLock": vacmViewSpinLock,
       "vacmViewTreeFamilyTable": vacmViewTreeFamilyTable,
       "vacmViewTreeFamilyEntry": vacmViewTreeFamilyEntry,
       "vacmViewTreeFamilyViewName": vacmViewTreeFamilyViewName,
       "vacmViewTreeFamilySubtree": vacmViewTreeFamilySubtree,
       "vacmViewTreeFamilyMask": vacmViewTreeFamilyMask,
       "vacmViewTreeFamilyType": vacmViewTreeFamilyType,
       "vacmViewTreeFamilyStorageType": vacmViewTreeFamilyStorageType,
       "vacmViewTreeFamilyStatus": vacmViewTreeFamilyStatus,
       "vacmMIBConformance": vacmMIBConformance,
       "vacmMIBCompliances": vacmMIBCompliances,
       "vacmMIBCompliance": vacmMIBCompliance,
       "vacmMIBGroups": vacmMIBGroups,
       "vacmBasicGroup": vacmBasicGroup,
       "PYSNMP_MODULE_ID": snmpVacmMIB}
)
