#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
# PySNMP MIB module SNMP-TARGET-MIB (https://www.pysnmp.com/pysnmp)
# ASN.1 source file://asn1/SNMP-TARGET-MIB
# Produced by pysmi-1.5.8 at Sat Nov  2 15:25:25 2024
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
 SnmpMessageProcessingModel,
 SnmpSecurityLevel,
 SnmpSecurityModel) = mibBuilder.import_symbols(
    "SNMP-FRAMEWORK-MIB",
    "SnmpAdminString",
    "SnmpMessageProcessingModel",
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
 TAddress,
 TDomain,
 TextualConvention,
 TestAndIncr,
 TimeInterval) = mibBuilder.import_symbols(
    "SNMPv2-TC",
    "DisplayString",
    "RowStatus",
    "StorageType",
    "TAddress",
    "TDomain",
    "TextualConvention",
    "TestAndIncr",
    "TimeInterval")


# MODULE-IDENTITY

snmpTargetMIB = ModuleIdentity(
    (1, 3, 6, 1, 6, 3, 12)
)
if mibBuilder.loadTexts:
    snmpTargetMIB.setRevisions(
        ("2012-11-27 00:00",
         "2002-10-14 00:00",
         "1998-08-04 00:00",
         "1997-07-14 00:00")
    )
if mibBuilder.loadTexts:
    snmpTargetMIB.setLastUpdated("201211270000Z")
if mibBuilder.loadTexts:
    snmpTargetMIB.setOrganization("IETF SNMPv3 Working Group")
if mibBuilder.loadTexts:
    snmpTargetMIB.setContactInfo("WG-email: snmpv3@lists.tislabs.com Subscribe: majordomo@lists.tislabs.com In message body: subscribe snmpv3 Co-Chair: Russ Mundy Network Associates Laboratories Postal: 15204 Omega Drive, Suite 300 Rockville, MD 20850-4601 USA EMail: mundy@tislabs.com Phone: +1 301-947-7107 Co-Chair: David Harrington Enterasys Networks Postal: 35 Industrial Way P. O. Box 5004 Rochester, New Hampshire 03866-5005 USA EMail: dbh@enterasys.com Phone: +1 603-337-2614 Co-editor: David B. Levi Nortel Networks Postal: 3505 Kesterwood Drive Knoxville, Tennessee 37918 EMail: dlevi@nortelnetworks.com Phone: +1 865 686 0432 Co-editor: Paul Meyer Secure Computing Corporation Postal: 2675 Long Lake Road Roseville, Minnesota 55113 EMail: paul_meyer@securecomputing.com Phone: +1 651 628 1592 Co-editor: Bob Stewart Retired")
if mibBuilder.loadTexts:
    snmpTargetMIB.setDescription("This MIB module defines MIB objects which provide mechanisms to remotely configure the parameters used by an SNMP entity for the generation of SNMP messages. Copyright (C) The Internet Society (2002). This version of this MIB module is part of RFC 3413; see the RFC itself for full legal notices. ")


# Types definitions


# TEXTUAL-CONVENTIONS



class SnmpTagValue(TextualConvention, OctetString):
    status = "current"
    displayHint = "255a"
    subtypeSpec = OctetString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(0, 255),
    )

    if mibBuilder.loadTexts:
        description = "An octet string containing a tag value. Tag values are preferably in human-readable form. To facilitate internationalization, this information is represented using the ISO/IEC IS 10646-1 character set, encoded as an octet string using the UTF-8 character encoding scheme described in RFC 2279. Since additional code points are added by amendments to the 10646 standard from time to time, implementations must be prepared to encounter any code point from 0x00000000 to 0x7fffffff. The use of control codes should be avoided, and certain control codes are not allowed as described below. For code points not directly supported by user interface hardware or software, an alternative means of entry and display, such as hexadecimal, may be provided. For information encoded in 7-bit US-ASCII, the UTF-8 representation is identical to the US-ASCII encoding. Note that when this TC is used for an object that is used or envisioned to be used as an index, then a SIZE restriction must be specified so that the number of sub-identifiers for any object instance does not exceed the limit of 128, as defined by [RFC1905]. An object of this type contains a single tag value which is used to select a set of entries in a table. A tag value is an arbitrary string of octets, but may not contain a delimiter character. Delimiter characters are defined to be one of the following: - An ASCII space character (0x20). - An ASCII TAB character (0x09). - An ASCII carriage return (CR) character (0x0D). - An ASCII line feed (LF) character (0x0A). Delimiter characters are used to separate tag values in a tag list. An object of this type may only contain a single tag value, and so delimiter characters are not allowed in a value of this type. Some examples of valid tag values are: - 'acme' - 'router' - 'host' The use of a tag value to select table entries is application and MIB specific."

    # IMPORTANT: customization
    encoding = "utf-8"
    _delimiters = (" ", "\n", "\t", "\t")

    def prettyIn(self, value):
        for v in str(value):
            if v in self._delimiters:
                raise error.SmiError("Delimiters not allowed in tag value")
        return OctetString.prettyIn(self, value)

class SnmpTagList(TextualConvention, OctetString):
    status = "current"
    displayHint = "255a"
    subtypeSpec = OctetString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(0, 255),
    )

    if mibBuilder.loadTexts:
        description = "An octet string containing a list of tag values. Tag values are preferably in human-readable form. To facilitate internationalization, this information is represented using the ISO/IEC IS 10646-1 character set, encoded as an octet string using the UTF-8 character encoding scheme described in RFC 2279. Since additional code points are added by amendments to the 10646 standard from time to time, implementations must be prepared to encounter any code point from 0x00000000 to 0x7fffffff. The use of control codes should be avoided, except as described below. For code points not directly supported by user interface hardware or software, an alternative means of entry and display, such as hexadecimal, may be provided. For information encoded in 7-bit US-ASCII, the UTF-8 representation is identical to the US-ASCII encoding. An object of this type contains a list of tag values which are used to select a set of entries in a table. A tag value is an arbitrary string of octets, but may not contain a delimiter character. Delimiter characters are defined to be one of the following: - An ASCII space character (0x20). - An ASCII TAB character (0x09). - An ASCII carriage return (CR) character (0x0D). - An ASCII line feed (LF) character (0x0A). Delimiter characters are used to separate tag values in a tag list. Only a single delimiter character may occur between two tag values. A tag value may not have a zero length. These constraints imply certain restrictions on the contents of this object: - There cannot be a leading or trailing delimiter character. - There cannot be multiple adjacent delimiter characters. Some examples of valid tag lists are: - An empty string - 'acme router' - 'host managerStation' Note that although a tag value may not have a length of zero, an empty string is still valid. This indicates an empty list (i.e. there are no tag values in the list). The use of the tag list to select table entries is application and MIB specific. Typically, an application will provide one or more tag values, and any entry which contains some combination of these tag values will be selected."

    # IMPORTANT: customization
    encoding = "utf-8"
    _delimiters = (" ", "\n", "\t", "\t")

    def prettyIn(self, value):
        inDelim = True
        for v in str(value):
            if v in self._delimiters:
                if inDelim:
                    raise error.SmiError(
                        "Leading or multiple delimiters not allowed in tag list %r"
                        % value
                    )
                inDelim = True
            else:
                inDelim = False
        if value and inDelim:
            raise error.SmiError(
                "Dangling delimiter not allowed in tag list %r" % value
            )
        return OctetString.prettyIn(self, value)


# MIB Managed Objects in the order of their OIDs

_SnmpTargetObjects_ObjectIdentity = ObjectIdentity
snmpTargetObjects = _SnmpTargetObjects_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 12, 1)
)
_SnmpTargetSpinLock_Type = TestAndIncr
_SnmpTargetSpinLock_Object = MibScalar
snmpTargetSpinLock = _SnmpTargetSpinLock_Object(
    (1, 3, 6, 1, 6, 3, 12, 1, 1),
    _SnmpTargetSpinLock_Type()
)
snmpTargetSpinLock.setMaxAccess("read-write")
if mibBuilder.loadTexts:
    snmpTargetSpinLock.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetSpinLock.setDescription("This object is used to facilitate modification of table entries in the SNMP-TARGET-MIB module by multiple managers. In particular, it is useful when modifying the value of the snmpTargetAddrTagList object. The procedure for modifying the snmpTargetAddrTagList object is as follows: 1. Retrieve the value of snmpTargetSpinLock and of snmpTargetAddrTagList. 2. Generate a new value for snmpTargetAddrTagList. 3. Set the value of snmpTargetSpinLock to the retrieved value, and the value of snmpTargetAddrTagList to the new value. If the set fails for the snmpTargetSpinLock object, go back to step 1.")
_SnmpTargetAddrTable_Object = MibTable
snmpTargetAddrTable = _SnmpTargetAddrTable_Object(
    (1, 3, 6, 1, 6, 3, 12, 1, 2)
)
if mibBuilder.loadTexts:
    snmpTargetAddrTable.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetAddrTable.setDescription("A table of transport addresses to be used in the generation of SNMP messages.")
_SnmpTargetAddrEntry_Object = MibTableRow
snmpTargetAddrEntry = _SnmpTargetAddrEntry_Object(
    (1, 3, 6, 1, 6, 3, 12, 1, 2, 1)
)
snmpTargetAddrEntry.setIndexNames(
    (1, "SNMP-TARGET-MIB", "snmpTargetAddrName"),
)
if mibBuilder.loadTexts:
    snmpTargetAddrEntry.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetAddrEntry.setDescription("A transport address to be used in the generation of SNMP operations. Entries in the snmpTargetAddrTable are created and deleted using the snmpTargetAddrRowStatus object.")


class _SnmpTargetAddrName_Type(SnmpAdminString):
    """Custom type snmpTargetAddrName based on SnmpAdminString"""
    subtypeSpec = SnmpAdminString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(1, 32),
    )


_SnmpTargetAddrName_Type.__name__ = "SnmpAdminString"
_SnmpTargetAddrName_Object = MibTableColumn
snmpTargetAddrName = _SnmpTargetAddrName_Object(
    (1, 3, 6, 1, 6, 3, 12, 1, 2, 1, 1),
    _SnmpTargetAddrName_Type()
)
snmpTargetAddrName.setMaxAccess("not-accessible")
if mibBuilder.loadTexts:
    snmpTargetAddrName.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetAddrName.setDescription("The locally arbitrary, but unique identifier associated with this snmpTargetAddrEntry.")
_SnmpTargetAddrTDomain_Type = TDomain
_SnmpTargetAddrTDomain_Object = MibTableColumn
snmpTargetAddrTDomain = _SnmpTargetAddrTDomain_Object(
    (1, 3, 6, 1, 6, 3, 12, 1, 2, 1, 2),
    _SnmpTargetAddrTDomain_Type()
)
snmpTargetAddrTDomain.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpTargetAddrTDomain.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetAddrTDomain.setDescription("This object indicates the transport type of the address contained in the snmpTargetAddrTAddress object.")
_SnmpTargetAddrTAddress_Type = TAddress
_SnmpTargetAddrTAddress_Object = MibTableColumn
snmpTargetAddrTAddress = _SnmpTargetAddrTAddress_Object(
    (1, 3, 6, 1, 6, 3, 12, 1, 2, 1, 3),
    _SnmpTargetAddrTAddress_Type()
)
snmpTargetAddrTAddress.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpTargetAddrTAddress.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetAddrTAddress.setDescription("This object contains a transport address. The format of this address depends on the value of the snmpTargetAddrTDomain object.")


class _SnmpTargetAddrTimeout_Type(TimeInterval):
    """Custom type snmpTargetAddrTimeout based on TimeInterval"""
    defaultValue = 1500


_SnmpTargetAddrTimeout_Type.__name__ = "TimeInterval"
_SnmpTargetAddrTimeout_Object = MibTableColumn
snmpTargetAddrTimeout = _SnmpTargetAddrTimeout_Object(
    (1, 3, 6, 1, 6, 3, 12, 1, 2, 1, 4),
    _SnmpTargetAddrTimeout_Type()
)
snmpTargetAddrTimeout.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpTargetAddrTimeout.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetAddrTimeout.setDescription("This object should reflect the expected maximum round trip time for communicating with the transport address defined by this row. When a message is sent to this address, and a response (if one is expected) is not received within this time period, an implementation may assume that the response will not be delivered. Note that the time interval that an application waits for a response may actually be derived from the value of this object. The method for deriving the actual time interval is implementation dependent. One such method is to derive the expected round trip time based on a particular retransmission algorithm and on the number of timeouts which have occurred. The type of message may also be considered when deriving expected round trip times for retransmissions. For example, if a message is being sent with a securityLevel that indicates both authentication and privacy, the derived value may be increased to compensate for extra processing time spent during authentication and encryption processing.")


class _SnmpTargetAddrRetryCount_Type(Integer32):
    """Custom type snmpTargetAddrRetryCount based on Integer32"""
    defaultValue = 3

    subtypeSpec = Integer32.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueRangeConstraint(0, 255),
    )


_SnmpTargetAddrRetryCount_Type.__name__ = "Integer32"
_SnmpTargetAddrRetryCount_Object = MibTableColumn
snmpTargetAddrRetryCount = _SnmpTargetAddrRetryCount_Object(
    (1, 3, 6, 1, 6, 3, 12, 1, 2, 1, 5),
    _SnmpTargetAddrRetryCount_Type()
)
snmpTargetAddrRetryCount.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpTargetAddrRetryCount.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetAddrRetryCount.setDescription("This object specifies a default number of retries to be attempted when a response is not received for a generated message. An application may provide its own retry count, in which case the value of this object is ignored.")
_SnmpTargetAddrTagList_Type = SnmpTagList
_SnmpTargetAddrTagList_Object = MibTableColumn
snmpTargetAddrTagList = _SnmpTargetAddrTagList_Object(
    (1, 3, 6, 1, 6, 3, 12, 1, 2, 1, 6),
    _SnmpTargetAddrTagList_Type()
)
snmpTargetAddrTagList.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpTargetAddrTagList.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetAddrTagList.setDescription("This object contains a list of tag values which are used to select target addresses for a particular operation.")


class _SnmpTargetAddrParams_Type(SnmpAdminString):
    """Custom type snmpTargetAddrParams based on SnmpAdminString"""
    subtypeSpec = SnmpAdminString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(1, 32),
    )


_SnmpTargetAddrParams_Type.__name__ = "SnmpAdminString"
_SnmpTargetAddrParams_Object = MibTableColumn
snmpTargetAddrParams = _SnmpTargetAddrParams_Object(
    (1, 3, 6, 1, 6, 3, 12, 1, 2, 1, 7),
    _SnmpTargetAddrParams_Type()
)
snmpTargetAddrParams.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpTargetAddrParams.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetAddrParams.setDescription("The value of this object identifies an entry in the snmpTargetParamsTable. The identified entry contains SNMP parameters to be used when generating messages to be sent to this transport address.")


class _SnmpTargetAddrStorageType_Type(StorageType):
    """Custom type snmpTargetAddrStorageType based on StorageType"""
    defaultValue = 3


_SnmpTargetAddrStorageType_Type.__name__ = "StorageType"
_SnmpTargetAddrStorageType_Object = MibTableColumn
snmpTargetAddrStorageType = _SnmpTargetAddrStorageType_Object(
    (1, 3, 6, 1, 6, 3, 12, 1, 2, 1, 8),
    _SnmpTargetAddrStorageType_Type()
)
snmpTargetAddrStorageType.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpTargetAddrStorageType.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetAddrStorageType.setDescription("The storage type for this conceptual row. Conceptual rows having the value 'permanent' need not allow write-access to any columnar objects in the row.")
_SnmpTargetAddrRowStatus_Type = RowStatus
_SnmpTargetAddrRowStatus_Object = MibTableColumn
snmpTargetAddrRowStatus = _SnmpTargetAddrRowStatus_Object(
    (1, 3, 6, 1, 6, 3, 12, 1, 2, 1, 9),
    _SnmpTargetAddrRowStatus_Type()
)
snmpTargetAddrRowStatus.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpTargetAddrRowStatus.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetAddrRowStatus.setDescription("The status of this conceptual row. To create a row in this table, a manager must set this object to either createAndGo(4) or createAndWait(5). Until instances of all corresponding columns are appropriately configured, the value of the corresponding instance of the snmpTargetAddrRowStatus column is 'notReady'. In particular, a newly created row cannot be made active until the corresponding instances of snmpTargetAddrTDomain, snmpTargetAddrTAddress, and snmpTargetAddrParams have all been set. The following objects may not be modified while the value of this object is active(1): - snmpTargetAddrTDomain - snmpTargetAddrTAddress An attempt to set these objects while the value of snmpTargetAddrRowStatus is active(1) will result in an inconsistentValue error.")
_SnmpTargetParamsTable_Object = MibTable
snmpTargetParamsTable = _SnmpTargetParamsTable_Object(
    (1, 3, 6, 1, 6, 3, 12, 1, 3)
)
if mibBuilder.loadTexts:
    snmpTargetParamsTable.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetParamsTable.setDescription("A table of SNMP target information to be used in the generation of SNMP messages.")
_SnmpTargetParamsEntry_Object = MibTableRow
snmpTargetParamsEntry = _SnmpTargetParamsEntry_Object(
    (1, 3, 6, 1, 6, 3, 12, 1, 3, 1)
)
snmpTargetParamsEntry.setIndexNames(
    (1, "SNMP-TARGET-MIB", "snmpTargetParamsName"),
)
if mibBuilder.loadTexts:
    snmpTargetParamsEntry.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetParamsEntry.setDescription("A set of SNMP target information. Entries in the snmpTargetParamsTable are created and deleted using the snmpTargetParamsRowStatus object.")


class _SnmpTargetParamsName_Type(SnmpAdminString):
    """Custom type snmpTargetParamsName based on SnmpAdminString"""
    subtypeSpec = SnmpAdminString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(1, 32),
    )


_SnmpTargetParamsName_Type.__name__ = "SnmpAdminString"
_SnmpTargetParamsName_Object = MibTableColumn
snmpTargetParamsName = _SnmpTargetParamsName_Object(
    (1, 3, 6, 1, 6, 3, 12, 1, 3, 1, 1),
    _SnmpTargetParamsName_Type()
)
snmpTargetParamsName.setMaxAccess("not-accessible")
if mibBuilder.loadTexts:
    snmpTargetParamsName.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetParamsName.setDescription("The locally arbitrary, but unique identifier associated with this snmpTargetParamsEntry.")
_SnmpTargetParamsMPModel_Type = SnmpMessageProcessingModel
_SnmpTargetParamsMPModel_Object = MibTableColumn
snmpTargetParamsMPModel = _SnmpTargetParamsMPModel_Object(
    (1, 3, 6, 1, 6, 3, 12, 1, 3, 1, 2),
    _SnmpTargetParamsMPModel_Type()
)
snmpTargetParamsMPModel.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpTargetParamsMPModel.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetParamsMPModel.setDescription("The Message Processing Model to be used when generating SNMP messages using this entry.")


class _SnmpTargetParamsSecurityModel_Type(SnmpSecurityModel):
    """Custom type snmpTargetParamsSecurityModel based on SnmpSecurityModel"""
    subtypeSpec = SnmpSecurityModel.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueRangeConstraint(1, 2147483647),
    )


_SnmpTargetParamsSecurityModel_Type.__name__ = "SnmpSecurityModel"
_SnmpTargetParamsSecurityModel_Object = MibTableColumn
snmpTargetParamsSecurityModel = _SnmpTargetParamsSecurityModel_Object(
    (1, 3, 6, 1, 6, 3, 12, 1, 3, 1, 3),
    _SnmpTargetParamsSecurityModel_Type()
)
snmpTargetParamsSecurityModel.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpTargetParamsSecurityModel.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetParamsSecurityModel.setDescription("The Security Model to be used when generating SNMP messages using this entry. An implementation may choose to return an inconsistentValue error if an attempt is made to set this variable to a value for a security model which the implementation does not support.")
_SnmpTargetParamsSecurityName_Type = SnmpAdminString
_SnmpTargetParamsSecurityName_Object = MibTableColumn
snmpTargetParamsSecurityName = _SnmpTargetParamsSecurityName_Object(
    (1, 3, 6, 1, 6, 3, 12, 1, 3, 1, 4),
    _SnmpTargetParamsSecurityName_Type()
)
snmpTargetParamsSecurityName.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpTargetParamsSecurityName.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetParamsSecurityName.setDescription("The securityName which identifies the Principal on whose behalf SNMP messages will be generated using this entry.")
_SnmpTargetParamsSecurityLevel_Type = SnmpSecurityLevel
_SnmpTargetParamsSecurityLevel_Object = MibTableColumn
snmpTargetParamsSecurityLevel = _SnmpTargetParamsSecurityLevel_Object(
    (1, 3, 6, 1, 6, 3, 12, 1, 3, 1, 5),
    _SnmpTargetParamsSecurityLevel_Type()
)
snmpTargetParamsSecurityLevel.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpTargetParamsSecurityLevel.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetParamsSecurityLevel.setDescription("The Level of Security to be used when generating SNMP messages using this entry.")


class _SnmpTargetParamsStorageType_Type(StorageType):
    """Custom type snmpTargetParamsStorageType based on StorageType"""
    defaultValue = 3


_SnmpTargetParamsStorageType_Type.__name__ = "StorageType"
_SnmpTargetParamsStorageType_Object = MibTableColumn
snmpTargetParamsStorageType = _SnmpTargetParamsStorageType_Object(
    (1, 3, 6, 1, 6, 3, 12, 1, 3, 1, 6),
    _SnmpTargetParamsStorageType_Type()
)
snmpTargetParamsStorageType.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpTargetParamsStorageType.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetParamsStorageType.setDescription("The storage type for this conceptual row. Conceptual rows having the value 'permanent' need not allow write-access to any columnar objects in the row.")
_SnmpTargetParamsRowStatus_Type = RowStatus
_SnmpTargetParamsRowStatus_Object = MibTableColumn
snmpTargetParamsRowStatus = _SnmpTargetParamsRowStatus_Object(
    (1, 3, 6, 1, 6, 3, 12, 1, 3, 1, 7),
    _SnmpTargetParamsRowStatus_Type()
)
snmpTargetParamsRowStatus.setMaxAccess("read-create")
if mibBuilder.loadTexts:
    snmpTargetParamsRowStatus.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetParamsRowStatus.setDescription("The status of this conceptual row. To create a row in this table, a manager must set this object to either createAndGo(4) or createAndWait(5). Until instances of all corresponding columns are appropriately configured, the value of the corresponding instance of the snmpTargetParamsRowStatus column is 'notReady'. In particular, a newly created row cannot be made active until the corresponding snmpTargetParamsMPModel, snmpTargetParamsSecurityModel, snmpTargetParamsSecurityName, and snmpTargetParamsSecurityLevel have all been set. The following objects may not be modified while the value of this object is active(1): - snmpTargetParamsMPModel - snmpTargetParamsSecurityModel - snmpTargetParamsSecurityName - snmpTargetParamsSecurityLevel An attempt to set these objects while the value of snmpTargetParamsRowStatus is active(1) will result in an inconsistentValue error.")
_SnmpUnavailableContexts_Type = Counter32
_SnmpUnavailableContexts_Object = MibScalar
snmpUnavailableContexts = _SnmpUnavailableContexts_Object(
    (1, 3, 6, 1, 6, 3, 12, 1, 4),
    _SnmpUnavailableContexts_Type()
)
snmpUnavailableContexts.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpUnavailableContexts.setStatus("current")
if mibBuilder.loadTexts:
    snmpUnavailableContexts.setDescription("The total number of packets received by the SNMP engine which were dropped because the context contained in the message was unavailable.")
_SnmpUnknownContexts_Type = Counter32
_SnmpUnknownContexts_Object = MibScalar
snmpUnknownContexts = _SnmpUnknownContexts_Object(
    (1, 3, 6, 1, 6, 3, 12, 1, 5),
    _SnmpUnknownContexts_Type()
)
snmpUnknownContexts.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpUnknownContexts.setStatus("current")
if mibBuilder.loadTexts:
    snmpUnknownContexts.setDescription("The total number of packets received by the SNMP engine which were dropped because the context contained in the message was unknown.")
_SnmpTargetConformance_ObjectIdentity = ObjectIdentity
snmpTargetConformance = _SnmpTargetConformance_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 12, 3)
)
_SnmpTargetCompliances_ObjectIdentity = ObjectIdentity
snmpTargetCompliances = _SnmpTargetCompliances_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 12, 3, 1)
)
_SnmpTargetGroups_ObjectIdentity = ObjectIdentity
snmpTargetGroups = _SnmpTargetGroups_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 12, 3, 2)
)

# Managed Objects groups

snmpTargetBasicGroup = ObjectGroup(
    (1, 3, 6, 1, 6, 3, 12, 3, 2, 1)
)
snmpTargetBasicGroup.setObjects(
      *(("SNMP-TARGET-MIB", "snmpTargetSpinLock"),
        ("SNMP-TARGET-MIB", "snmpTargetAddrTDomain"),
        ("SNMP-TARGET-MIB", "snmpTargetAddrTAddress"),
        ("SNMP-TARGET-MIB", "snmpTargetAddrTagList"),
        ("SNMP-TARGET-MIB", "snmpTargetAddrParams"),
        ("SNMP-TARGET-MIB", "snmpTargetAddrStorageType"),
        ("SNMP-TARGET-MIB", "snmpTargetAddrRowStatus"),
        ("SNMP-TARGET-MIB", "snmpTargetParamsMPModel"),
        ("SNMP-TARGET-MIB", "snmpTargetParamsSecurityModel"),
        ("SNMP-TARGET-MIB", "snmpTargetParamsSecurityName"),
        ("SNMP-TARGET-MIB", "snmpTargetParamsSecurityLevel"),
        ("SNMP-TARGET-MIB", "snmpTargetParamsStorageType"),
        ("SNMP-TARGET-MIB", "snmpTargetParamsRowStatus"))
)
if mibBuilder.loadTexts:
    snmpTargetBasicGroup.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetBasicGroup.setDescription("A collection of objects providing basic remote configuration of management targets.")

snmpTargetResponseGroup = ObjectGroup(
    (1, 3, 6, 1, 6, 3, 12, 3, 2, 2)
)
snmpTargetResponseGroup.setObjects(
      *(("SNMP-TARGET-MIB", "snmpTargetAddrTimeout"),
        ("SNMP-TARGET-MIB", "snmpTargetAddrRetryCount"))
)
if mibBuilder.loadTexts:
    snmpTargetResponseGroup.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetResponseGroup.setDescription("A collection of objects providing remote configuration of management targets for applications which generate SNMP messages for which a response message would be expected.")

snmpTargetCommandResponderGroup = ObjectGroup(
    (1, 3, 6, 1, 6, 3, 12, 3, 2, 3)
)
snmpTargetCommandResponderGroup.setObjects(
      *(("SNMP-TARGET-MIB", "snmpUnavailableContexts"),
        ("SNMP-TARGET-MIB", "snmpUnknownContexts"))
)
if mibBuilder.loadTexts:
    snmpTargetCommandResponderGroup.setStatus("current")
if mibBuilder.loadTexts:
    snmpTargetCommandResponderGroup.setDescription("A collection of objects required for command responder applications, used for counting error conditions.")


# Notification objects


# Notifications groups


# Agent capabilities


# Module compliance

snmpTargetCommandResponderCompliance = ModuleCompliance(
    (1, 3, 6, 1, 6, 3, 12, 3, 1, 1)
)
snmpTargetCommandResponderCompliance.setObjects(
    ("SNMP-TARGET-MIB", "snmpTargetCommandResponderGroup")
)
if mibBuilder.loadTexts:
    snmpTargetCommandResponderCompliance.setStatus(
        "current"
    )
if mibBuilder.loadTexts:
    snmpTargetCommandResponderCompliance.setDescription("The compliance statement for SNMP entities which include a command responder application.")


# Export all MIB objects to the MIB builder

mibBuilder.export_symbols(
    "SNMP-TARGET-MIB",
    **{"SnmpTagValue": SnmpTagValue,
       "SnmpTagList": SnmpTagList,
       "snmpTargetMIB": snmpTargetMIB,
       "snmpTargetObjects": snmpTargetObjects,
       "snmpTargetSpinLock": snmpTargetSpinLock,
       "snmpTargetAddrTable": snmpTargetAddrTable,
       "snmpTargetAddrEntry": snmpTargetAddrEntry,
       "snmpTargetAddrName": snmpTargetAddrName,
       "snmpTargetAddrTDomain": snmpTargetAddrTDomain,
       "snmpTargetAddrTAddress": snmpTargetAddrTAddress,
       "snmpTargetAddrTimeout": snmpTargetAddrTimeout,
       "snmpTargetAddrRetryCount": snmpTargetAddrRetryCount,
       "snmpTargetAddrTagList": snmpTargetAddrTagList,
       "snmpTargetAddrParams": snmpTargetAddrParams,
       "snmpTargetAddrStorageType": snmpTargetAddrStorageType,
       "snmpTargetAddrRowStatus": snmpTargetAddrRowStatus,
       "snmpTargetParamsTable": snmpTargetParamsTable,
       "snmpTargetParamsEntry": snmpTargetParamsEntry,
       "snmpTargetParamsName": snmpTargetParamsName,
       "snmpTargetParamsMPModel": snmpTargetParamsMPModel,
       "snmpTargetParamsSecurityModel": snmpTargetParamsSecurityModel,
       "snmpTargetParamsSecurityName": snmpTargetParamsSecurityName,
       "snmpTargetParamsSecurityLevel": snmpTargetParamsSecurityLevel,
       "snmpTargetParamsStorageType": snmpTargetParamsStorageType,
       "snmpTargetParamsRowStatus": snmpTargetParamsRowStatus,
       "snmpUnavailableContexts": snmpUnavailableContexts,
       "snmpUnknownContexts": snmpUnknownContexts,
       "snmpTargetConformance": snmpTargetConformance,
       "snmpTargetCompliances": snmpTargetCompliances,
       "snmpTargetCommandResponderCompliance": snmpTargetCommandResponderCompliance,
       "snmpTargetGroups": snmpTargetGroups,
       "snmpTargetBasicGroup": snmpTargetBasicGroup,
       "snmpTargetResponseGroup": snmpTargetResponseGroup,
       "snmpTargetCommandResponderGroup": snmpTargetCommandResponderGroup,
       "PYSNMP_MODULE_ID": snmpTargetMIB}
)
