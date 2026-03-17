#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
# PySNMP MIB module SNMPv2-MIB (https://www.pysnmp.com/pysnmp)
# ASN.1 source file://asn1/SNMPv2-MIB
# Produced by pysmi-1.5.8 at Sat Nov  2 15:25:43 2024
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
 mib_2,
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
    "mib-2",
    "snmpModules")

(DisplayString,
 TextualConvention,
 TestAndIncr,
 TimeStamp) = mibBuilder.import_symbols(
    "SNMPv2-TC",
    "DisplayString",
    "TextualConvention",
    "TestAndIncr",
    "TimeStamp")


# MODULE-IDENTITY

snmpMIB = ModuleIdentity(
    (1, 3, 6, 1, 6, 3, 1)
)
if mibBuilder.loadTexts:
    snmpMIB.setRevisions(
        ("2002-10-16 00:00",
         "1995-11-09 00:00",
         "1993-04-01 00:00")
    )
if mibBuilder.loadTexts:
    snmpMIB.setLastUpdated("200210160000Z")
if mibBuilder.loadTexts:
    snmpMIB.setOrganization("IETF SNMPv3 Working Group")
if mibBuilder.loadTexts:
    snmpMIB.setContactInfo("WG-EMail: snmpv3@lists.tislabs.com Subscribe: snmpv3-request@lists.tislabs.com Co-Chair: Russ Mundy Network Associates Laboratories postal: 15204 Omega Drive, Suite 300 Rockville, MD 20850-4601 USA EMail: mundy@tislabs.com phone: +1 301 947-7107 Co-Chair: David Harrington Enterasys Networks postal: 35 Industrial Way P. O. Box 5005 Rochester, NH 03866-5005 USA EMail: dbh@enterasys.com phone: +1 603 337-2614 Editor: Randy Presuhn BMC Software, Inc. postal: 2141 North First Street San Jose, CA 95131 USA EMail: randy_presuhn@bmc.com phone: +1 408 546-1006")
if mibBuilder.loadTexts:
    snmpMIB.setDescription("The MIB module for SNMP entities. Copyright (C) The Internet Society (2002). This version of this MIB module is part of RFC 3418; see the RFC itself for full legal notices. ")


# Types definitions


# TEXTUAL-CONVENTIONS



# MIB Managed Objects in the order of their OIDs

_System_ObjectIdentity = ObjectIdentity
system = _System_ObjectIdentity(
    (1, 3, 6, 1, 2, 1, 1)
)


class _SysDescr_Type(DisplayString):
    """Custom type sysDescr based on DisplayString"""
    subtypeSpec = DisplayString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(0, 255),
    )


_SysDescr_Type.__name__ = "DisplayString"
_SysDescr_Object = MibScalar
sysDescr = _SysDescr_Object(
    (1, 3, 6, 1, 2, 1, 1, 1),
    _SysDescr_Type()
)
sysDescr.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    sysDescr.setStatus("current")
if mibBuilder.loadTexts:
    sysDescr.setDescription("A textual description of the entity. This value should include the full name and version identification of the system's hardware type, software operating-system, and networking software.")
_SysObjectID_Type = ObjectIdentifier
_SysObjectID_Object = MibScalar
sysObjectID = _SysObjectID_Object(
    (1, 3, 6, 1, 2, 1, 1, 2),
    _SysObjectID_Type()
)
sysObjectID.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    sysObjectID.setStatus("current")
if mibBuilder.loadTexts:
    sysObjectID.setDescription("The vendor's authoritative identification of the network management subsystem contained in the entity. This value is allocated within the SMI enterprises subtree (1.3.6.1.4.1) and provides an easy and unambiguous means for determining `what kind of box' is being managed. For example, if vendor `Flintstones, Inc.' was assigned the subtree 1.3.6.1.4.1.424242, it could assign the identifier 1.3.6.1.4.1.424242.1.1 to its `Fred Router'.")
_SysUpTime_Type = TimeTicks
_SysUpTime_Object = MibScalar
sysUpTime = _SysUpTime_Object(
    (1, 3, 6, 1, 2, 1, 1, 3),
    _SysUpTime_Type()
)
sysUpTime.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    sysUpTime.setStatus("current")
if mibBuilder.loadTexts:
    sysUpTime.setDescription("The time (in hundredths of a second) since the network management portion of the system was last re-initialized.")


class _SysContact_Type(DisplayString):
    """Custom type sysContact based on DisplayString"""
    subtypeSpec = DisplayString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(0, 255),
    )


_SysContact_Type.__name__ = "DisplayString"
_SysContact_Object = MibScalar
sysContact = _SysContact_Object(
    (1, 3, 6, 1, 2, 1, 1, 4),
    _SysContact_Type()
)
sysContact.setMaxAccess("read-write")
if mibBuilder.loadTexts:
    sysContact.setStatus("current")
if mibBuilder.loadTexts:
    sysContact.setDescription("The textual identification of the contact person for this managed node, together with information on how to contact this person. If no contact information is known, the value is the zero-length string.")


class _SysName_Type(DisplayString):
    """Custom type sysName based on DisplayString"""
    subtypeSpec = DisplayString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(0, 255),
    )


_SysName_Type.__name__ = "DisplayString"
_SysName_Object = MibScalar
sysName = _SysName_Object(
    (1, 3, 6, 1, 2, 1, 1, 5),
    _SysName_Type()
)
sysName.setMaxAccess("read-write")
if mibBuilder.loadTexts:
    sysName.setStatus("current")
if mibBuilder.loadTexts:
    sysName.setDescription("An administratively-assigned name for this managed node. By convention, this is the node's fully-qualified domain name. If the name is unknown, the value is the zero-length string.")


class _SysLocation_Type(DisplayString):
    """Custom type sysLocation based on DisplayString"""
    subtypeSpec = DisplayString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(0, 255),
    )


_SysLocation_Type.__name__ = "DisplayString"
_SysLocation_Object = MibScalar
sysLocation = _SysLocation_Object(
    (1, 3, 6, 1, 2, 1, 1, 6),
    _SysLocation_Type()
)
sysLocation.setMaxAccess("read-write")
if mibBuilder.loadTexts:
    sysLocation.setStatus("current")
if mibBuilder.loadTexts:
    sysLocation.setDescription("The physical location of this node (e.g., 'telephone closet, 3rd floor'). If the location is unknown, the value is the zero-length string.")


class _SysServices_Type(Integer32):
    """Custom type sysServices based on Integer32"""
    subtypeSpec = Integer32.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueRangeConstraint(0, 127),
    )


_SysServices_Type.__name__ = "Integer32"
_SysServices_Object = MibScalar
sysServices = _SysServices_Object(
    (1, 3, 6, 1, 2, 1, 1, 7),
    _SysServices_Type()
)
sysServices.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    sysServices.setStatus("current")
if mibBuilder.loadTexts:
    sysServices.setDescription("A value which indicates the set of services that this entity may potentially offer. The value is a sum. This sum initially takes the value zero. Then, for each layer, L, in the range 1 through 7, that this node performs transactions for, 2 raised to (L - 1) is added to the sum. For example, a node which performs only routing functions would have a value of 4 (2^(3-1)). In contrast, a node which is a host offering application services would have a value of 72 (2^(4-1) + 2^(7-1)). Note that in the context of the Internet suite of protocols, values should be calculated accordingly: layer functionality 1 physical (e.g., repeaters) 2 datalink/subnetwork (e.g., bridges) 3 internet (e.g., supports the IP) 4 end-to-end (e.g., supports the TCP) 7 applications (e.g., supports the SMTP) For systems including OSI protocols, layers 5 and 6 may also be counted.")
_SysORLastChange_Type = TimeStamp
_SysORLastChange_Object = MibScalar
sysORLastChange = _SysORLastChange_Object(
    (1, 3, 6, 1, 2, 1, 1, 8),
    _SysORLastChange_Type()
)
sysORLastChange.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    sysORLastChange.setStatus("current")
if mibBuilder.loadTexts:
    sysORLastChange.setDescription("The value of sysUpTime at the time of the most recent change in state or value of any instance of sysORID.")
_SysORTable_Object = MibTable
sysORTable = _SysORTable_Object(
    (1, 3, 6, 1, 2, 1, 1, 9)
)
if mibBuilder.loadTexts:
    sysORTable.setStatus("current")
if mibBuilder.loadTexts:
    sysORTable.setDescription("The (conceptual) table listing the capabilities of the local SNMP application acting as a command responder with respect to various MIB modules. SNMP entities having dynamically-configurable support of MIB modules will have a dynamically-varying number of conceptual rows.")
_SysOREntry_Object = MibTableRow
sysOREntry = _SysOREntry_Object(
    (1, 3, 6, 1, 2, 1, 1, 9, 1)
)
sysOREntry.setIndexNames(
    (0, "SNMPv2-MIB", "sysORIndex"),
)
if mibBuilder.loadTexts:
    sysOREntry.setStatus("current")
if mibBuilder.loadTexts:
    sysOREntry.setDescription("An entry (conceptual row) in the sysORTable.")


class _SysORIndex_Type(Integer32):
    """Custom type sysORIndex based on Integer32"""
    subtypeSpec = Integer32.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueRangeConstraint(1, 2147483647),
    )


_SysORIndex_Type.__name__ = "Integer32"
_SysORIndex_Object = MibTableColumn
sysORIndex = _SysORIndex_Object(
    (1, 3, 6, 1, 2, 1, 1, 9, 1, 1),
    _SysORIndex_Type()
)
sysORIndex.setMaxAccess("not-accessible")
if mibBuilder.loadTexts:
    sysORIndex.setStatus("current")
if mibBuilder.loadTexts:
    sysORIndex.setDescription("The auxiliary variable used for identifying instances of the columnar objects in the sysORTable.")
_SysORID_Type = ObjectIdentifier
_SysORID_Object = MibTableColumn
sysORID = _SysORID_Object(
    (1, 3, 6, 1, 2, 1, 1, 9, 1, 2),
    _SysORID_Type()
)
sysORID.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    sysORID.setStatus("current")
if mibBuilder.loadTexts:
    sysORID.setDescription("An authoritative identification of a capabilities statement with respect to various MIB modules supported by the local SNMP application acting as a command responder.")
_SysORDescr_Type = DisplayString
_SysORDescr_Object = MibTableColumn
sysORDescr = _SysORDescr_Object(
    (1, 3, 6, 1, 2, 1, 1, 9, 1, 3),
    _SysORDescr_Type()
)
sysORDescr.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    sysORDescr.setStatus("current")
if mibBuilder.loadTexts:
    sysORDescr.setDescription("A textual description of the capabilities identified by the corresponding instance of sysORID.")
_SysORUpTime_Type = TimeStamp
_SysORUpTime_Object = MibTableColumn
sysORUpTime = _SysORUpTime_Object(
    (1, 3, 6, 1, 2, 1, 1, 9, 1, 4),
    _SysORUpTime_Type()
)
sysORUpTime.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    sysORUpTime.setStatus("current")
if mibBuilder.loadTexts:
    sysORUpTime.setDescription("The value of sysUpTime at the time this conceptual row was last instantiated.")
_Snmp_ObjectIdentity = ObjectIdentity
snmp = _Snmp_ObjectIdentity(
    (1, 3, 6, 1, 2, 1, 11)
)
_SnmpInPkts_Type = Counter32
_SnmpInPkts_Object = MibScalar
snmpInPkts = _SnmpInPkts_Object(
    (1, 3, 6, 1, 2, 1, 11, 1),
    _SnmpInPkts_Type()
)
snmpInPkts.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpInPkts.setStatus("current")
if mibBuilder.loadTexts:
    snmpInPkts.setDescription("The total number of messages delivered to the SNMP entity from the transport service.")
_SnmpOutPkts_Type = Counter32
_SnmpOutPkts_Object = MibScalar
snmpOutPkts = _SnmpOutPkts_Object(
    (1, 3, 6, 1, 2, 1, 11, 2),
    _SnmpOutPkts_Type()
)
snmpOutPkts.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpOutPkts.setStatus("obsolete")
if mibBuilder.loadTexts:
    snmpOutPkts.setDescription("The total number of SNMP Messages which were passed from the SNMP protocol entity to the transport service.")
_SnmpInBadVersions_Type = Counter32
_SnmpInBadVersions_Object = MibScalar
snmpInBadVersions = _SnmpInBadVersions_Object(
    (1, 3, 6, 1, 2, 1, 11, 3),
    _SnmpInBadVersions_Type()
)
snmpInBadVersions.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpInBadVersions.setStatus("current")
if mibBuilder.loadTexts:
    snmpInBadVersions.setDescription("The total number of SNMP messages which were delivered to the SNMP entity and were for an unsupported SNMP version.")
_SnmpInBadCommunityNames_Type = Counter32
_SnmpInBadCommunityNames_Object = MibScalar
snmpInBadCommunityNames = _SnmpInBadCommunityNames_Object(
    (1, 3, 6, 1, 2, 1, 11, 4),
    _SnmpInBadCommunityNames_Type()
)
snmpInBadCommunityNames.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpInBadCommunityNames.setStatus("current")
if mibBuilder.loadTexts:
    snmpInBadCommunityNames.setDescription("The total number of community-based SNMP messages (for example, SNMPv1) delivered to the SNMP entity which used an SNMP community name not known to said entity. Also, implementations which authenticate community-based SNMP messages using check(s) in addition to matching the community name (for example, by also checking whether the message originated from a transport address allowed to use a specified community name) MAY include in this value the number of messages which failed the additional check(s). It is strongly RECOMMENDED that the documentation for any security model which is used to authenticate community-based SNMP messages specify the precise conditions that contribute to this value.")
_SnmpInBadCommunityUses_Type = Counter32
_SnmpInBadCommunityUses_Object = MibScalar
snmpInBadCommunityUses = _SnmpInBadCommunityUses_Object(
    (1, 3, 6, 1, 2, 1, 11, 5),
    _SnmpInBadCommunityUses_Type()
)
snmpInBadCommunityUses.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpInBadCommunityUses.setStatus("current")
if mibBuilder.loadTexts:
    snmpInBadCommunityUses.setDescription("The total number of community-based SNMP messages (for example, SNMPv1) delivered to the SNMP entity which represented an SNMP operation that was not allowed for the SNMP community named in the message. The precise conditions under which this counter is incremented (if at all) depend on how the SNMP entity implements its access control mechanism and how its applications interact with that access control mechanism. It is strongly RECOMMENDED that the documentation for any access control mechanism which is used to control access to and visibility of MIB instrumentation specify the precise conditions that contribute to this value.")
_SnmpInASNParseErrs_Type = Counter32
_SnmpInASNParseErrs_Object = MibScalar
snmpInASNParseErrs = _SnmpInASNParseErrs_Object(
    (1, 3, 6, 1, 2, 1, 11, 6),
    _SnmpInASNParseErrs_Type()
)
snmpInASNParseErrs.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpInASNParseErrs.setStatus("current")
if mibBuilder.loadTexts:
    snmpInASNParseErrs.setDescription("The total number of ASN.1 or BER errors encountered by the SNMP entity when decoding received SNMP messages.")
_SnmpInTooBigs_Type = Counter32
_SnmpInTooBigs_Object = MibScalar
snmpInTooBigs = _SnmpInTooBigs_Object(
    (1, 3, 6, 1, 2, 1, 11, 8),
    _SnmpInTooBigs_Type()
)
snmpInTooBigs.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpInTooBigs.setStatus("obsolete")
if mibBuilder.loadTexts:
    snmpInTooBigs.setDescription("The total number of SNMP PDUs which were delivered to the SNMP protocol entity and for which the value of the error-status field was `tooBig'.")
_SnmpInNoSuchNames_Type = Counter32
_SnmpInNoSuchNames_Object = MibScalar
snmpInNoSuchNames = _SnmpInNoSuchNames_Object(
    (1, 3, 6, 1, 2, 1, 11, 9),
    _SnmpInNoSuchNames_Type()
)
snmpInNoSuchNames.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpInNoSuchNames.setStatus("obsolete")
if mibBuilder.loadTexts:
    snmpInNoSuchNames.setDescription("The total number of SNMP PDUs which were delivered to the SNMP protocol entity and for which the value of the error-status field was `noSuchName'.")
_SnmpInBadValues_Type = Counter32
_SnmpInBadValues_Object = MibScalar
snmpInBadValues = _SnmpInBadValues_Object(
    (1, 3, 6, 1, 2, 1, 11, 10),
    _SnmpInBadValues_Type()
)
snmpInBadValues.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpInBadValues.setStatus("obsolete")
if mibBuilder.loadTexts:
    snmpInBadValues.setDescription("The total number of SNMP PDUs which were delivered to the SNMP protocol entity and for which the value of the error-status field was `badValue'.")
_SnmpInReadOnlys_Type = Counter32
_SnmpInReadOnlys_Object = MibScalar
snmpInReadOnlys = _SnmpInReadOnlys_Object(
    (1, 3, 6, 1, 2, 1, 11, 11),
    _SnmpInReadOnlys_Type()
)
snmpInReadOnlys.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpInReadOnlys.setStatus("obsolete")
if mibBuilder.loadTexts:
    snmpInReadOnlys.setDescription("The total number valid SNMP PDUs which were delivered to the SNMP protocol entity and for which the value of the error-status field was `readOnly'. It should be noted that it is a protocol error to generate an SNMP PDU which contains the value `readOnly' in the error-status field, as such this object is provided as a means of detecting incorrect implementations of the SNMP.")
_SnmpInGenErrs_Type = Counter32
_SnmpInGenErrs_Object = MibScalar
snmpInGenErrs = _SnmpInGenErrs_Object(
    (1, 3, 6, 1, 2, 1, 11, 12),
    _SnmpInGenErrs_Type()
)
snmpInGenErrs.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpInGenErrs.setStatus("obsolete")
if mibBuilder.loadTexts:
    snmpInGenErrs.setDescription("The total number of SNMP PDUs which were delivered to the SNMP protocol entity and for which the value of the error-status field was `genErr'.")
_SnmpInTotalReqVars_Type = Counter32
_SnmpInTotalReqVars_Object = MibScalar
snmpInTotalReqVars = _SnmpInTotalReqVars_Object(
    (1, 3, 6, 1, 2, 1, 11, 13),
    _SnmpInTotalReqVars_Type()
)
snmpInTotalReqVars.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpInTotalReqVars.setStatus("obsolete")
if mibBuilder.loadTexts:
    snmpInTotalReqVars.setDescription("The total number of MIB objects which have been retrieved successfully by the SNMP protocol entity as the result of receiving valid SNMP Get-Request and Get-Next PDUs.")
_SnmpInTotalSetVars_Type = Counter32
_SnmpInTotalSetVars_Object = MibScalar
snmpInTotalSetVars = _SnmpInTotalSetVars_Object(
    (1, 3, 6, 1, 2, 1, 11, 14),
    _SnmpInTotalSetVars_Type()
)
snmpInTotalSetVars.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpInTotalSetVars.setStatus("obsolete")
if mibBuilder.loadTexts:
    snmpInTotalSetVars.setDescription("The total number of MIB objects which have been altered successfully by the SNMP protocol entity as the result of receiving valid SNMP Set-Request PDUs.")
_SnmpInGetRequests_Type = Counter32
_SnmpInGetRequests_Object = MibScalar
snmpInGetRequests = _SnmpInGetRequests_Object(
    (1, 3, 6, 1, 2, 1, 11, 15),
    _SnmpInGetRequests_Type()
)
snmpInGetRequests.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpInGetRequests.setStatus("obsolete")
if mibBuilder.loadTexts:
    snmpInGetRequests.setDescription("The total number of SNMP Get-Request PDUs which have been accepted and processed by the SNMP protocol entity.")
_SnmpInGetNexts_Type = Counter32
_SnmpInGetNexts_Object = MibScalar
snmpInGetNexts = _SnmpInGetNexts_Object(
    (1, 3, 6, 1, 2, 1, 11, 16),
    _SnmpInGetNexts_Type()
)
snmpInGetNexts.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpInGetNexts.setStatus("obsolete")
if mibBuilder.loadTexts:
    snmpInGetNexts.setDescription("The total number of SNMP Get-Next PDUs which have been accepted and processed by the SNMP protocol entity.")
_SnmpInSetRequests_Type = Counter32
_SnmpInSetRequests_Object = MibScalar
snmpInSetRequests = _SnmpInSetRequests_Object(
    (1, 3, 6, 1, 2, 1, 11, 17),
    _SnmpInSetRequests_Type()
)
snmpInSetRequests.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpInSetRequests.setStatus("obsolete")
if mibBuilder.loadTexts:
    snmpInSetRequests.setDescription("The total number of SNMP Set-Request PDUs which have been accepted and processed by the SNMP protocol entity.")
_SnmpInGetResponses_Type = Counter32
_SnmpInGetResponses_Object = MibScalar
snmpInGetResponses = _SnmpInGetResponses_Object(
    (1, 3, 6, 1, 2, 1, 11, 18),
    _SnmpInGetResponses_Type()
)
snmpInGetResponses.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpInGetResponses.setStatus("obsolete")
if mibBuilder.loadTexts:
    snmpInGetResponses.setDescription("The total number of SNMP Get-Response PDUs which have been accepted and processed by the SNMP protocol entity.")
_SnmpInTraps_Type = Counter32
_SnmpInTraps_Object = MibScalar
snmpInTraps = _SnmpInTraps_Object(
    (1, 3, 6, 1, 2, 1, 11, 19),
    _SnmpInTraps_Type()
)
snmpInTraps.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpInTraps.setStatus("obsolete")
if mibBuilder.loadTexts:
    snmpInTraps.setDescription("The total number of SNMP Trap PDUs which have been accepted and processed by the SNMP protocol entity.")
_SnmpOutTooBigs_Type = Counter32
_SnmpOutTooBigs_Object = MibScalar
snmpOutTooBigs = _SnmpOutTooBigs_Object(
    (1, 3, 6, 1, 2, 1, 11, 20),
    _SnmpOutTooBigs_Type()
)
snmpOutTooBigs.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpOutTooBigs.setStatus("obsolete")
if mibBuilder.loadTexts:
    snmpOutTooBigs.setDescription("The total number of SNMP PDUs which were generated by the SNMP protocol entity and for which the value of the error-status field was `tooBig.'")
_SnmpOutNoSuchNames_Type = Counter32
_SnmpOutNoSuchNames_Object = MibScalar
snmpOutNoSuchNames = _SnmpOutNoSuchNames_Object(
    (1, 3, 6, 1, 2, 1, 11, 21),
    _SnmpOutNoSuchNames_Type()
)
snmpOutNoSuchNames.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpOutNoSuchNames.setStatus("obsolete")
if mibBuilder.loadTexts:
    snmpOutNoSuchNames.setDescription("The total number of SNMP PDUs which were generated by the SNMP protocol entity and for which the value of the error-status was `noSuchName'.")
_SnmpOutBadValues_Type = Counter32
_SnmpOutBadValues_Object = MibScalar
snmpOutBadValues = _SnmpOutBadValues_Object(
    (1, 3, 6, 1, 2, 1, 11, 22),
    _SnmpOutBadValues_Type()
)
snmpOutBadValues.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpOutBadValues.setStatus("obsolete")
if mibBuilder.loadTexts:
    snmpOutBadValues.setDescription("The total number of SNMP PDUs which were generated by the SNMP protocol entity and for which the value of the error-status field was `badValue'.")
_SnmpOutGenErrs_Type = Counter32
_SnmpOutGenErrs_Object = MibScalar
snmpOutGenErrs = _SnmpOutGenErrs_Object(
    (1, 3, 6, 1, 2, 1, 11, 24),
    _SnmpOutGenErrs_Type()
)
snmpOutGenErrs.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpOutGenErrs.setStatus("obsolete")
if mibBuilder.loadTexts:
    snmpOutGenErrs.setDescription("The total number of SNMP PDUs which were generated by the SNMP protocol entity and for which the value of the error-status field was `genErr'.")
_SnmpOutGetRequests_Type = Counter32
_SnmpOutGetRequests_Object = MibScalar
snmpOutGetRequests = _SnmpOutGetRequests_Object(
    (1, 3, 6, 1, 2, 1, 11, 25),
    _SnmpOutGetRequests_Type()
)
snmpOutGetRequests.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpOutGetRequests.setStatus("obsolete")
if mibBuilder.loadTexts:
    snmpOutGetRequests.setDescription("The total number of SNMP Get-Request PDUs which have been generated by the SNMP protocol entity.")
_SnmpOutGetNexts_Type = Counter32
_SnmpOutGetNexts_Object = MibScalar
snmpOutGetNexts = _SnmpOutGetNexts_Object(
    (1, 3, 6, 1, 2, 1, 11, 26),
    _SnmpOutGetNexts_Type()
)
snmpOutGetNexts.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpOutGetNexts.setStatus("obsolete")
if mibBuilder.loadTexts:
    snmpOutGetNexts.setDescription("The total number of SNMP Get-Next PDUs which have been generated by the SNMP protocol entity.")
_SnmpOutSetRequests_Type = Counter32
_SnmpOutSetRequests_Object = MibScalar
snmpOutSetRequests = _SnmpOutSetRequests_Object(
    (1, 3, 6, 1, 2, 1, 11, 27),
    _SnmpOutSetRequests_Type()
)
snmpOutSetRequests.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpOutSetRequests.setStatus("obsolete")
if mibBuilder.loadTexts:
    snmpOutSetRequests.setDescription("The total number of SNMP Set-Request PDUs which have been generated by the SNMP protocol entity.")
_SnmpOutGetResponses_Type = Counter32
_SnmpOutGetResponses_Object = MibScalar
snmpOutGetResponses = _SnmpOutGetResponses_Object(
    (1, 3, 6, 1, 2, 1, 11, 28),
    _SnmpOutGetResponses_Type()
)
snmpOutGetResponses.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpOutGetResponses.setStatus("obsolete")
if mibBuilder.loadTexts:
    snmpOutGetResponses.setDescription("The total number of SNMP Get-Response PDUs which have been generated by the SNMP protocol entity.")
_SnmpOutTraps_Type = Counter32
_SnmpOutTraps_Object = MibScalar
snmpOutTraps = _SnmpOutTraps_Object(
    (1, 3, 6, 1, 2, 1, 11, 29),
    _SnmpOutTraps_Type()
)
snmpOutTraps.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpOutTraps.setStatus("obsolete")
if mibBuilder.loadTexts:
    snmpOutTraps.setDescription("The total number of SNMP Trap PDUs which have been generated by the SNMP protocol entity.")


class _SnmpEnableAuthenTraps_Type(Integer32):
    """Custom type snmpEnableAuthenTraps based on Integer32"""
    subtypeSpec = Integer32.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        SingleValueConstraint(
            *(1,
              2)
        )
    )
    namedValues = NamedValues(
        *(("enabled", 1),
          ("disabled", 2))
    )


_SnmpEnableAuthenTraps_Type.__name__ = "Integer32"
_SnmpEnableAuthenTraps_Object = MibScalar
snmpEnableAuthenTraps = _SnmpEnableAuthenTraps_Object(
    (1, 3, 6, 1, 2, 1, 11, 30),
    _SnmpEnableAuthenTraps_Type()
)
snmpEnableAuthenTraps.setMaxAccess("read-write")
if mibBuilder.loadTexts:
    snmpEnableAuthenTraps.setStatus("current")
if mibBuilder.loadTexts:
    snmpEnableAuthenTraps.setDescription("Indicates whether the SNMP entity is permitted to generate authenticationFailure traps. The value of this object overrides any configuration information; as such, it provides a means whereby all authenticationFailure traps may be disabled. Note that it is strongly recommended that this object be stored in non-volatile memory so that it remains constant across re-initializations of the network management system.")
_SnmpSilentDrops_Type = Counter32
_SnmpSilentDrops_Object = MibScalar
snmpSilentDrops = _SnmpSilentDrops_Object(
    (1, 3, 6, 1, 2, 1, 11, 31),
    _SnmpSilentDrops_Type()
)
snmpSilentDrops.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpSilentDrops.setStatus("current")
if mibBuilder.loadTexts:
    snmpSilentDrops.setDescription("The total number of Confirmed Class PDUs (such as GetRequest-PDUs, GetNextRequest-PDUs, GetBulkRequest-PDUs, SetRequest-PDUs, and InformRequest-PDUs) delivered to the SNMP entity which were silently dropped because the size of a reply containing an alternate Response Class PDU (such as a Response-PDU) with an empty variable-bindings field was greater than either a local constraint or the maximum message size associated with the originator of the request.")
_SnmpProxyDrops_Type = Counter32
_SnmpProxyDrops_Object = MibScalar
snmpProxyDrops = _SnmpProxyDrops_Object(
    (1, 3, 6, 1, 2, 1, 11, 32),
    _SnmpProxyDrops_Type()
)
snmpProxyDrops.setMaxAccess("read-only")
if mibBuilder.loadTexts:
    snmpProxyDrops.setStatus("current")
if mibBuilder.loadTexts:
    snmpProxyDrops.setDescription("The total number of Confirmed Class PDUs (such as GetRequest-PDUs, GetNextRequest-PDUs, GetBulkRequest-PDUs, SetRequest-PDUs, and InformRequest-PDUs) delivered to the SNMP entity which were silently dropped because the transmission of the (possibly translated) message to a proxy target failed in a manner (other than a time-out) such that no Response Class PDU (such as a Response-PDU) could be returned.")
_SnmpMIBObjects_ObjectIdentity = ObjectIdentity
snmpMIBObjects = _SnmpMIBObjects_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 1, 1)
)
_SnmpTrap_ObjectIdentity = ObjectIdentity
snmpTrap = _SnmpTrap_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 1, 1, 4)
)
_SnmpTrapOID_Type = ObjectIdentifier
_SnmpTrapOID_Object = MibScalar
snmpTrapOID = _SnmpTrapOID_Object(
    (1, 3, 6, 1, 6, 3, 1, 1, 4, 1),
    _SnmpTrapOID_Type()
)
snmpTrapOID.setMaxAccess("accessible-for-notify")
if mibBuilder.loadTexts:
    snmpTrapOID.setStatus("current")
if mibBuilder.loadTexts:
    snmpTrapOID.setDescription("The authoritative identification of the notification currently being sent. This variable occurs as the second varbind in every SNMPv2-Trap-PDU and InformRequest-PDU.")
_SnmpTrapEnterprise_Type = ObjectIdentifier
_SnmpTrapEnterprise_Object = MibScalar
snmpTrapEnterprise = _SnmpTrapEnterprise_Object(
    (1, 3, 6, 1, 6, 3, 1, 1, 4, 3),
    _SnmpTrapEnterprise_Type()
)
snmpTrapEnterprise.setMaxAccess("accessible-for-notify")
if mibBuilder.loadTexts:
    snmpTrapEnterprise.setStatus("current")
if mibBuilder.loadTexts:
    snmpTrapEnterprise.setDescription("The authoritative identification of the enterprise associated with the trap currently being sent. When an SNMP proxy agent is mapping an RFC1157 Trap-PDU into a SNMPv2-Trap-PDU, this variable occurs as the last varbind.")
_SnmpTraps_ObjectIdentity = ObjectIdentity
snmpTraps = _SnmpTraps_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 1, 1, 5)
)
_SnmpSet_ObjectIdentity = ObjectIdentity
snmpSet = _SnmpSet_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 1, 1, 6)
)
_SnmpSetSerialNo_Type = TestAndIncr
_SnmpSetSerialNo_Object = MibScalar
snmpSetSerialNo = _SnmpSetSerialNo_Object(
    (1, 3, 6, 1, 6, 3, 1, 1, 6, 1),
    _SnmpSetSerialNo_Type()
)
snmpSetSerialNo.setMaxAccess("read-write")
if mibBuilder.loadTexts:
    snmpSetSerialNo.setStatus("current")
if mibBuilder.loadTexts:
    snmpSetSerialNo.setDescription("An advisory lock used to allow several cooperating command generator applications to coordinate their use of the SNMP set operation. This object is used for coarse-grain coordination. To achieve fine-grain coordination, one or more similar objects might be defined within each MIB group, as appropriate.")
_SnmpMIBConformance_ObjectIdentity = ObjectIdentity
snmpMIBConformance = _SnmpMIBConformance_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 1, 2)
)
_SnmpMIBCompliances_ObjectIdentity = ObjectIdentity
snmpMIBCompliances = _SnmpMIBCompliances_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 1, 2, 1)
)
_SnmpMIBGroups_ObjectIdentity = ObjectIdentity
snmpMIBGroups = _SnmpMIBGroups_ObjectIdentity(
    (1, 3, 6, 1, 6, 3, 1, 2, 2)
)

# Managed Objects groups

snmpSetGroup = ObjectGroup(
    (1, 3, 6, 1, 6, 3, 1, 2, 2, 5)
)
snmpSetGroup.setObjects(
    ("SNMPv2-MIB", "snmpSetSerialNo")
)
if mibBuilder.loadTexts:
    snmpSetGroup.setStatus("current")
if mibBuilder.loadTexts:
    snmpSetGroup.setDescription("A collection of objects which allow several cooperating command generator applications to coordinate their use of the set operation.")

systemGroup = ObjectGroup(
    (1, 3, 6, 1, 6, 3, 1, 2, 2, 6)
)
systemGroup.setObjects(
      *(("SNMPv2-MIB", "sysDescr"),
        ("SNMPv2-MIB", "sysObjectID"),
        ("SNMPv2-MIB", "sysUpTime"),
        ("SNMPv2-MIB", "sysContact"),
        ("SNMPv2-MIB", "sysName"),
        ("SNMPv2-MIB", "sysLocation"),
        ("SNMPv2-MIB", "sysServices"),
        ("SNMPv2-MIB", "sysORLastChange"),
        ("SNMPv2-MIB", "sysORID"),
        ("SNMPv2-MIB", "sysORUpTime"),
        ("SNMPv2-MIB", "sysORDescr"))
)
if mibBuilder.loadTexts:
    systemGroup.setStatus("current")
if mibBuilder.loadTexts:
    systemGroup.setDescription("The system group defines objects which are common to all managed systems.")

snmpGroup = ObjectGroup(
    (1, 3, 6, 1, 6, 3, 1, 2, 2, 8)
)
snmpGroup.setObjects(
      *(("SNMPv2-MIB", "snmpInPkts"),
        ("SNMPv2-MIB", "snmpInBadVersions"),
        ("SNMPv2-MIB", "snmpInASNParseErrs"),
        ("SNMPv2-MIB", "snmpSilentDrops"),
        ("SNMPv2-MIB", "snmpProxyDrops"),
        ("SNMPv2-MIB", "snmpEnableAuthenTraps"))
)
if mibBuilder.loadTexts:
    snmpGroup.setStatus("current")
if mibBuilder.loadTexts:
    snmpGroup.setDescription("A collection of objects providing basic instrumentation and control of an SNMP entity.")

snmpCommunityGroup = ObjectGroup(
    (1, 3, 6, 1, 6, 3, 1, 2, 2, 9)
)
snmpCommunityGroup.setObjects(
      *(("SNMPv2-MIB", "snmpInBadCommunityNames"),
        ("SNMPv2-MIB", "snmpInBadCommunityUses"))
)
if mibBuilder.loadTexts:
    snmpCommunityGroup.setStatus("current")
if mibBuilder.loadTexts:
    snmpCommunityGroup.setDescription("A collection of objects providing basic instrumentation of a SNMP entity which supports community-based authentication.")

snmpObsoleteGroup = ObjectGroup(
    (1, 3, 6, 1, 6, 3, 1, 2, 2, 10)
)
snmpObsoleteGroup.setObjects(
      *(("SNMPv2-MIB", "snmpOutPkts"),
        ("SNMPv2-MIB", "snmpInTooBigs"),
        ("SNMPv2-MIB", "snmpInNoSuchNames"),
        ("SNMPv2-MIB", "snmpInBadValues"),
        ("SNMPv2-MIB", "snmpInReadOnlys"),
        ("SNMPv2-MIB", "snmpInGenErrs"),
        ("SNMPv2-MIB", "snmpInTotalReqVars"),
        ("SNMPv2-MIB", "snmpInTotalSetVars"),
        ("SNMPv2-MIB", "snmpInGetRequests"),
        ("SNMPv2-MIB", "snmpInGetNexts"),
        ("SNMPv2-MIB", "snmpInSetRequests"),
        ("SNMPv2-MIB", "snmpInGetResponses"),
        ("SNMPv2-MIB", "snmpInTraps"),
        ("SNMPv2-MIB", "snmpOutTooBigs"),
        ("SNMPv2-MIB", "snmpOutNoSuchNames"),
        ("SNMPv2-MIB", "snmpOutBadValues"),
        ("SNMPv2-MIB", "snmpOutGenErrs"),
        ("SNMPv2-MIB", "snmpOutGetRequests"),
        ("SNMPv2-MIB", "snmpOutGetNexts"),
        ("SNMPv2-MIB", "snmpOutSetRequests"),
        ("SNMPv2-MIB", "snmpOutGetResponses"),
        ("SNMPv2-MIB", "snmpOutTraps"))
)
if mibBuilder.loadTexts:
    snmpObsoleteGroup.setStatus("obsolete")
if mibBuilder.loadTexts:
    snmpObsoleteGroup.setDescription("A collection of objects from RFC 1213 made obsolete by this MIB module.")

snmpNotificationGroup = ObjectGroup(
    (1, 3, 6, 1, 6, 3, 1, 2, 2, 12)
)
snmpNotificationGroup.setObjects(
      *(("SNMPv2-MIB", "snmpTrapOID"),
        ("SNMPv2-MIB", "snmpTrapEnterprise"))
)
if mibBuilder.loadTexts:
    snmpNotificationGroup.setStatus("current")
if mibBuilder.loadTexts:
    snmpNotificationGroup.setDescription("These objects are required for entities which support notification originator applications.")


# Notification objects

coldStart = NotificationType(
    (1, 3, 6, 1, 6, 3, 1, 1, 5, 1)
)
if mibBuilder.loadTexts:
    coldStart.setStatus(
        "current"
    )
if mibBuilder.loadTexts:
    coldStart.setDescription("A coldStart trap signifies that the SNMP entity, supporting a notification originator application, is reinitializing itself and that its configuration may have been altered.")

warmStart = NotificationType(
    (1, 3, 6, 1, 6, 3, 1, 1, 5, 2)
)
if mibBuilder.loadTexts:
    warmStart.setStatus(
        "current"
    )
if mibBuilder.loadTexts:
    warmStart.setDescription("A warmStart trap signifies that the SNMP entity, supporting a notification originator application, is reinitializing itself such that its configuration is unaltered.")

authenticationFailure = NotificationType(
    (1, 3, 6, 1, 6, 3, 1, 1, 5, 5)
)
if mibBuilder.loadTexts:
    authenticationFailure.setStatus(
        "current"
    )
if mibBuilder.loadTexts:
    authenticationFailure.setDescription("An authenticationFailure trap signifies that the SNMP entity has received a protocol message that is not properly authenticated. While all implementations of SNMP entities MAY be capable of generating this trap, the snmpEnableAuthenTraps object indicates whether this trap will be generated.")


# Notifications groups

snmpBasicNotificationsGroup = NotificationGroup(
    (1, 3, 6, 1, 6, 3, 1, 2, 2, 7)
)
snmpBasicNotificationsGroup.setObjects(
      *(("SNMPv2-MIB", "coldStart"),
        ("SNMPv2-MIB", "authenticationFailure"))
)
if mibBuilder.loadTexts:
    snmpBasicNotificationsGroup.setStatus(
        "current"
    )
if mibBuilder.loadTexts:
    snmpBasicNotificationsGroup.setDescription("The basic notifications implemented by an SNMP entity supporting command responder applications.")

snmpWarmStartNotificationGroup = NotificationGroup(
    (1, 3, 6, 1, 6, 3, 1, 2, 2, 11)
)
snmpWarmStartNotificationGroup.setObjects(
    ("SNMPv2-MIB", "warmStart")
)
if mibBuilder.loadTexts:
    snmpWarmStartNotificationGroup.setStatus(
        "current"
    )
if mibBuilder.loadTexts:
    snmpWarmStartNotificationGroup.setDescription("An additional notification for an SNMP entity supporting command responder applications, if it is able to reinitialize itself such that its configuration is unaltered.")


# Agent capabilities


# Module compliance

snmpBasicCompliance = ModuleCompliance(
    (1, 3, 6, 1, 6, 3, 1, 2, 1, 2)
)
snmpBasicCompliance.setObjects(
      *(("SNMPv2-MIB", "snmpGroup"),
        ("SNMPv2-MIB", "snmpSetGroup"),
        ("SNMPv2-MIB", "systemGroup"),
        ("SNMPv2-MIB", "snmpBasicNotificationsGroup"),
        ("SNMPv2-MIB", "snmpCommunityGroup"))
)
if mibBuilder.loadTexts:
    snmpBasicCompliance.setStatus(
        "deprecated"
    )
if mibBuilder.loadTexts:
    snmpBasicCompliance.setDescription("The compliance statement for SNMPv2 entities which implement the SNMPv2 MIB. This compliance statement is replaced by snmpBasicComplianceRev2.")

snmpBasicComplianceRev2 = ModuleCompliance(
    (1, 3, 6, 1, 6, 3, 1, 2, 1, 3)
)
snmpBasicComplianceRev2.setObjects(
      *(("SNMPv2-MIB", "snmpGroup"),
        ("SNMPv2-MIB", "snmpSetGroup"),
        ("SNMPv2-MIB", "systemGroup"),
        ("SNMPv2-MIB", "snmpBasicNotificationsGroup"),
        ("SNMPv2-MIB", "snmpCommunityGroup"),
        ("SNMPv2-MIB", "snmpWarmStartNotificationGroup"))
)
if mibBuilder.loadTexts:
    snmpBasicComplianceRev2.setStatus(
        "current"
    )
if mibBuilder.loadTexts:
    snmpBasicComplianceRev2.setDescription("The compliance statement for SNMP entities which implement this MIB module.")


# Export all MIB objects to the MIB builder

mibBuilder.export_symbols(
    "SNMPv2-MIB",
    **{"system": system,
       "sysDescr": sysDescr,
       "sysObjectID": sysObjectID,
       "sysUpTime": sysUpTime,
       "sysContact": sysContact,
       "sysName": sysName,
       "sysLocation": sysLocation,
       "sysServices": sysServices,
       "sysORLastChange": sysORLastChange,
       "sysORTable": sysORTable,
       "sysOREntry": sysOREntry,
       "sysORIndex": sysORIndex,
       "sysORID": sysORID,
       "sysORDescr": sysORDescr,
       "sysORUpTime": sysORUpTime,
       "snmp": snmp,
       "snmpInPkts": snmpInPkts,
       "snmpOutPkts": snmpOutPkts,
       "snmpInBadVersions": snmpInBadVersions,
       "snmpInBadCommunityNames": snmpInBadCommunityNames,
       "snmpInBadCommunityUses": snmpInBadCommunityUses,
       "snmpInASNParseErrs": snmpInASNParseErrs,
       "snmpInTooBigs": snmpInTooBigs,
       "snmpInNoSuchNames": snmpInNoSuchNames,
       "snmpInBadValues": snmpInBadValues,
       "snmpInReadOnlys": snmpInReadOnlys,
       "snmpInGenErrs": snmpInGenErrs,
       "snmpInTotalReqVars": snmpInTotalReqVars,
       "snmpInTotalSetVars": snmpInTotalSetVars,
       "snmpInGetRequests": snmpInGetRequests,
       "snmpInGetNexts": snmpInGetNexts,
       "snmpInSetRequests": snmpInSetRequests,
       "snmpInGetResponses": snmpInGetResponses,
       "snmpInTraps": snmpInTraps,
       "snmpOutTooBigs": snmpOutTooBigs,
       "snmpOutNoSuchNames": snmpOutNoSuchNames,
       "snmpOutBadValues": snmpOutBadValues,
       "snmpOutGenErrs": snmpOutGenErrs,
       "snmpOutGetRequests": snmpOutGetRequests,
       "snmpOutGetNexts": snmpOutGetNexts,
       "snmpOutSetRequests": snmpOutSetRequests,
       "snmpOutGetResponses": snmpOutGetResponses,
       "snmpOutTraps": snmpOutTraps,
       "snmpEnableAuthenTraps": snmpEnableAuthenTraps,
       "snmpSilentDrops": snmpSilentDrops,
       "snmpProxyDrops": snmpProxyDrops,
       "snmpMIB": snmpMIB,
       "snmpMIBObjects": snmpMIBObjects,
       "snmpTrap": snmpTrap,
       "snmpTrapOID": snmpTrapOID,
       "snmpTrapEnterprise": snmpTrapEnterprise,
       "snmpTraps": snmpTraps,
       "coldStart": coldStart,
       "warmStart": warmStart,
       "authenticationFailure": authenticationFailure,
       "snmpSet": snmpSet,
       "snmpSetSerialNo": snmpSetSerialNo,
       "snmpMIBConformance": snmpMIBConformance,
       "snmpMIBCompliances": snmpMIBCompliances,
       "snmpBasicCompliance": snmpBasicCompliance,
       "snmpBasicComplianceRev2": snmpBasicComplianceRev2,
       "snmpMIBGroups": snmpMIBGroups,
       "snmpSetGroup": snmpSetGroup,
       "systemGroup": systemGroup,
       "snmpBasicNotificationsGroup": snmpBasicNotificationsGroup,
       "snmpGroup": snmpGroup,
       "snmpCommunityGroup": snmpCommunityGroup,
       "snmpObsoleteGroup": snmpObsoleteGroup,
       "snmpWarmStartNotificationGroup": snmpWarmStartNotificationGroup,
       "snmpNotificationGroup": snmpNotificationGroup,
       "PYSNMP_MODULE_ID": snmpMIB}
)
