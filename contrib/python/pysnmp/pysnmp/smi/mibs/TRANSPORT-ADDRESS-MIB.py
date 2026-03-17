#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
# PySNMP MIB module TRANSPORT-ADDRESS-MIB (https://www.pysnmp.com/pysnmp)
# ASN.1 source file://asn1/TRANSPORT-ADDRESS-MIB
# Produced by pysmi-1.5.8 at Sat Nov  2 17:55:00 2024
# On host MacBook-Pro.local platform Darwin version 24.1.0 by user lextm
# Using Python version 3.12.0 (main, Nov 14 2023, 23:52:11) [Clang 15.0.0 (clang-1500.0.40.1)]

# IMPORTANT: this file contains customizations

import socket


from pysnmp import error


has_ipv6 = socket.has_ipv6

if hasattr(socket, "inet_ntop") and hasattr(socket, "inet_pton"):
    inet_ntop = socket.inet_ntop
    inet_pton = socket.inet_pton
else:
    import sys

    if sys.platform != "win32":
        from socket import inet_ntoa, inet_aton

        inet_ntop = lambda x, y: inet_ntoa(y)
        inet_pton = lambda x, y: inet_aton(y)
        has_ipv6 = False
    elif has_ipv6:
        import struct  # The case of old Python at old Windows

        def inet_pton(address_family, ip_string):
            if address_family == socket.AF_INET:
                return socket.inet_aton(ip_string)
            elif address_family != socket.AF_INET6:
                raise OSError(f"Unknown address family {address_family}")

            groups = ip_string.split(":")
            spaces = groups.count("")

            if "." in groups[-1]:
                groups[-1:] = [
                    "%x" % x for x in struct.unpack("!HH", socket.inet_aton(groups[-1]))
                ]

            if spaces == 1:
                idx = groups.index("")
                groups[idx : idx + 1] = ["0"] * (8 - len(groups) + 1)
            elif spaces == 2:
                zeros = ["0"] * (8 - len(groups) + 2)
                if ip_string.startswith("::"):
                    groups[:2] = zeros
                elif ip_string.endswith("::"):
                    groups[-2:] = zeros
                else:
                    raise OSError(f'Invalid IPv6 address: "{ip_string}"')
            elif spaces == 3:
                if ip_string != "::":
                    raise OSError(f'Invalid IPv6 address: "{ip_string}"')
                return "\x00" * 16
            elif spaces > 3:
                raise OSError(f'Invalid IPv6 address: "{ip_string}"')

            groups = [t for t in [int(t, 16) for t in groups] if t & 0xFFFF == t]

            if len(groups) != 8:
                raise OSError(f'Invalid IPv6 address: "{ip_string}"')

            return struct.pack("!8H", *groups)

        def inet_ntop(address_family, packed_ip):
            if address_family == socket.AF_INET:
                return socket.inet_ntop(packed_ip)
            elif address_family != socket.AF_INET6:
                raise OSError(f"Unknown address family {address_family}")

            if len(packed_ip) != 16:
                raise OSError("incorrect address length: %s" % len(packed_ip))

            groups = list(struct.unpack("!8H", packed_ip))

            cur_base = best_base = cur_len = best_len = -1

            for idx in range(8):
                if groups[idx]:
                    if cur_base != -1:
                        if best_base == -1 or cur_len > best_len:
                            best_base, best_len = cur_base, cur_len
                        cur_base = -1
                else:
                    if cur_base == -1:
                        cur_base, cur_len = idx, 1
                    else:
                        cur_len += 1

            if cur_base != -1:
                if best_base == -1 or cur_len > best_len:
                    best_base, best_len = cur_base, cur_len

            if best_base != -1 and best_len > 1:
                groups[best_base : best_base + best_len] = [":"]

            if groups[0] == ":":
                groups.insert(0, ":")
            if groups[-1] == ":":
                groups.append(":")

            f = lambda x: x != ":" and "%x" % x or ""

            return ":".join([f(x) for x in groups])


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

transportAddressMIB = ModuleIdentity(
    (1, 3, 6, 1, 2, 1, 100)
)
if mibBuilder.loadTexts:
    transportAddressMIB.setRevisions(
        ("2002-11-01 00:00",)
    )
if mibBuilder.loadTexts:
    transportAddressMIB.setLastUpdated("200211010000Z")
if mibBuilder.loadTexts:
    transportAddressMIB.setOrganization("IETF Operations and Management Area")
if mibBuilder.loadTexts:
    transportAddressMIB.setContactInfo("Juergen Schoenwaelder (Editor) TU Braunschweig Bueltenweg 74/75 38106 Braunschweig, Germany Phone: +49 531 391-3289 EMail: schoenw@ibr.cs.tu-bs.de Send comments to <mibs@ops.ietf.org>.")
if mibBuilder.loadTexts:
    transportAddressMIB.setDescription("This MIB module provides commonly used transport address definitions. Copyright (C) The Internet Society (2002). This version of this MIB module is part of RFC 3419; see the RFC itself for full legal notices.")


# Types definitions


# TEXTUAL-CONVENTIONS



class TransportDomain(TextualConvention, ObjectIdentifier):
    status = "current"
    if mibBuilder.loadTexts:
        description = "A value that represents a transport domain. Some possible values, such as transportDomainUdpIpv4, are defined in this module. Other possible values can be defined in other MIB modules."


class TransportAddressType(TextualConvention, Integer32):
    status = "current"
    subtypeSpec = Integer32.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        SingleValueConstraint(
            *(0,
              1,
              2,
              3,
              4,
              5,
              6,
              7,
              8,
              9,
              10,
              11,
              12,
              13,
              14,
              15,
              16)
        )
    )
    namedValues = NamedValues(
        *(("unknown", 0),
          ("udpIpv4", 1),
          ("udpIpv6", 2),
          ("udpIpv4z", 3),
          ("udpIpv6z", 4),
          ("tcpIpv4", 5),
          ("tcpIpv6", 6),
          ("tcpIpv4z", 7),
          ("tcpIpv6z", 8),
          ("sctpIpv4", 9),
          ("sctpIpv6", 10),
          ("sctpIpv4z", 11),
          ("sctpIpv6z", 12),
          ("local", 13),
          ("udpDns", 14),
          ("tcpDns", 15),
          ("sctpDns", 16))
    )

    if mibBuilder.loadTexts:
        description = "A value that represents a transport domain. This is the enumerated version of the transport domain registrations in this MIB module. The enumerated values have the following meaning: unknown(0) unknown transport address type udpIpv4(1) transportDomainUdpIpv4 udpIpv6(2) transportDomainUdpIpv6 udpIpv4z(3) transportDomainUdpIpv4z udpIpv6z(4) transportDomainUdpIpv6z tcpIpv4(5) transportDomainTcpIpv4 tcpIpv6(6) transportDomainTcpIpv6 tcpIpv4z(7) transportDomainTcpIpv4z tcpIpv6z(8) transportDomainTcpIpv6z sctpIpv4(9) transportDomainSctpIpv4 sctpIpv6(10) transportDomainSctpIpv6 sctpIpv4z(11) transportDomainSctpIpv4z sctpIpv6z(12) transportDomainSctpIpv6z local(13) transportDomainLocal udpDns(14) transportDomainUdpDns tcpDns(15) transportDomainTcpDns sctpDns(16) transportDomainSctpDns This textual convention can be used to represent transport domains in situations where a syntax of TransportDomain is unwieldy (for example, when used as an index). The usage of this textual convention implies that additional transport domains can only be supported by updating this MIB module. This extensibility restriction does not apply for the TransportDomain textual convention which allows MIB authors to define additional transport domains independently in other MIB modules."


class TransportAddress(TextualConvention, OctetString):
    status = "current"
    subtypeSpec = OctetString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(0, 255),
    )

    if mibBuilder.loadTexts:
        description = "Denotes a generic transport address. A TransportAddress value is always interpreted within the context of a TransportAddressType or TransportDomain value. Every usage of the TransportAddress textual convention MUST specify the TransportAddressType or TransportDomain object which provides the context. Furthermore, MIB authors SHOULD define a separate TransportAddressType or TransportDomain object for each TransportAddress object. It is suggested that the TransportAddressType or TransportDomain is logically registered before the object(s) which use the TransportAddress textual convention if they appear in the same logical row. The value of a TransportAddress object must always be consistent with the value of the associated TransportAddressType or TransportDomain object. Attempts to set a TransportAddress object to a value which is inconsistent with the associated TransportAddressType or TransportDomain must fail with an inconsistentValue error. When this textual convention is used as a syntax of an index object, there may be issues with the limit of 128 sub-identifiers specified in SMIv2, STD 58. In this case, the OBJECT-TYPE declaration MUST include a 'SIZE' clause to limit the number of potential instance sub-identifiers."


class TransportAddressIPv4(TextualConvention, OctetString):
    status = "current"
    displayHint = "1d.1d.1d.1d:2d"
    subtypeSpec = OctetString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(6, 6),
    )
    fixed_length = 6

    if mibBuilder.loadTexts:
        description = "Represents a transport address consisting of an IPv4 address and a port number (as used for example by UDP, TCP and SCTP): octets contents encoding 1-4 IPv4 address network-byte order 5-6 port number network-byte order This textual convention SHOULD NOT be used directly in object definitions since it restricts addresses to a specific format. However, if it is used, it MAY be used either on its own or in conjunction with TransportAddressType or TransportDomain as a pair."

    def prettyIn(self, value):
        if isinstance(value, tuple):
            # Wild hack -- need to implement TextualConvention.prettyIn
            value = (
                inet_pton(socket.AF_INET, value[0])
                + bytes((((value[1] >> 8) & 0xFF),))
                + bytes(((value[1] & 0xFF),))
            )
        return OctetString.prettyIn(self, value)

    # Socket address syntax coercion
    def __asSocketAddress(self):
        if not hasattr(self, "__tuple_value"):
            v = self.asOctets()
            self.__tuple_value = (
                inet_ntop(socket.AF_INET, v[:4]),
                v[4] << 8 | v[5],
            )
        return self.__tuple_value

    def __iter__(self):
        return iter(self.__asSocketAddress())

    def __getitem__(self, item):
        return self.__asSocketAddress()[item]


class TransportAddressIPv6(TextualConvention, OctetString):
    status = "current"
    displayHint = "0a[2x:2x:2x:2x:2x:2x:2x:2x]0a:2d"
    subtypeSpec = OctetString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(18, 18),
    )
    fixed_length = 18

    if mibBuilder.loadTexts:
        description = "Represents a transport address consisting of an IPv6 address and a port number (as used for example by UDP, TCP and SCTP): octets contents encoding 1-16 IPv6 address network-byte order 17-18 port number network-byte order This textual convention SHOULD NOT be used directly in object definitions since it restricts addresses to a specific format. However, if it is used, it MAY be used either on its own or in conjunction with TransportAddressType or TransportDomain as a pair."

    def prettyIn(self, value):
        if not has_ipv6:
            raise error.PySnmpError("IPv6 not supported by platform")
        if isinstance(value, tuple):
            value = (
                inet_pton(socket.AF_INET6, value[0])
                + bytes((((value[1] >> 8) & 0xFF),))
                + bytes(((value[1] & 0xFF),))
            )
        return OctetString.prettyIn(self, value)

    # Socket address syntax coercion
    def __asSocketAddress(self):
        if not hasattr(self, "__tuple_value"):
            if not has_ipv6:
                raise error.PySnmpError("IPv6 not supported by platform")
            v = self.asOctets()
            self.__tuple_value = (
                inet_ntop(socket.AF_INET6, v[:16]),
                v[16] << 8 | v[17],
                0,  # flowinfo
                0,
            )  # scopeid
        return self.__tuple_value

    def __iter__(self):
        return iter(self.__asSocketAddress())

    def __getitem__(self, item):
        return self.__asSocketAddress()[item]


class TransportAddressIPv4z(TextualConvention, OctetString):
    status = "current"
    displayHint = "1d.1d.1d.1d%4d:2d"
    subtypeSpec = OctetString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(10, 10),
    )
    fixed_length = 10

    if mibBuilder.loadTexts:
        description = "Represents a transport address consisting of an IPv4 address, a zone index and a port number (as used for example by UDP, TCP and SCTP): octets contents encoding 1-4 IPv4 address network-byte order 5-8 zone index network-byte order 9-10 port number network-byte order This textual convention SHOULD NOT be used directly in object definitions since it restricts addresses to a specific format. However, if it is used, it MAY be used either on its own or in conjunction with TransportAddressType or TransportDomain as a pair."


class TransportAddressIPv6z(TextualConvention, OctetString):
    status = "current"
    displayHint = "0a[2x:2x:2x:2x:2x:2x:2x:2x%4d]0a:2d"
    subtypeSpec = OctetString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(22, 22),
    )
    fixed_length = 22

    if mibBuilder.loadTexts:
        description = "Represents a transport address consisting of an IPv6 address, a zone index and a port number (as used for example by UDP, TCP and SCTP): octets contents encoding 1-16 IPv6 address network-byte order 17-20 zone index network-byte order 21-22 port number network-byte order This textual convention SHOULD NOT be used directly in object definitions since it restricts addresses to a specific format. However, if it is used, it MAY be used either on its own or in conjunction with TransportAddressType or TransportDomain as a pair."


class TransportAddressLocal(TextualConvention, OctetString):
    status = "current"
    displayHint = "1a"
    subtypeSpec = OctetString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(1, 255),
    )

    if mibBuilder.loadTexts:
        description = "Represents a POSIX Local IPC transport address: octets contents encoding all POSIX Local IPC address string The Posix Local IPC transport domain subsumes UNIX domain sockets. This textual convention SHOULD NOT be used directly in object definitions since it restricts addresses to a specific format. However, if it is used, it MAY be used either on its own or in conjunction with TransportAddressType or TransportDomain as a pair. When this textual convention is used as a syntax of an index object, there may be issues with the limit of 128 sub-identifiers specified in SMIv2, STD 58. In this case, the OBJECT-TYPE declaration MUST include a 'SIZE' clause to limit the number of potential instance sub-identifiers."
    if mibBuilder.loadTexts:
        reference = "Protocol Independent Interfaces (IEEE POSIX 1003.1g)"


class TransportAddressDns(TextualConvention, OctetString):
    status = "current"
    displayHint = "1a"
    subtypeSpec = OctetString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(1, 255),
    )

    if mibBuilder.loadTexts:
        description = "Represents a DNS domain name followed by a colon ':' (ASCII character 0x3A) and a port number in ASCII. The name SHOULD be fully qualified whenever possible. Values of this textual convention are not directly useable as transport-layer addressing information, and require runtime resolution. As such, applications that write them must be prepared for handling errors if such values are not supported, or cannot be resolved (if resolution occurs at the time of the management operation). The DESCRIPTION clause of TransportAddress objects that may have TransportAddressDns values must fully describe how (and when) such names are to be resolved to IP addresses and vice versa. This textual convention SHOULD NOT be used directly in object definitions since it restricts addresses to a specific format. However, if it is used, it MAY be used either on its own or in conjunction with TransportAddressType or TransportDomain as a pair. When this textual convention is used as a syntax of an index object, there may be issues with the limit of 128 sub-identifiers specified in SMIv2, STD 58. In this case, the OBJECT-TYPE declaration MUST include a 'SIZE' clause to limit the number of potential instance sub-identifiers."


# MIB Managed Objects in the order of their OIDs

_TransportDomains_ObjectIdentity = ObjectIdentity
transportDomains = _TransportDomains_ObjectIdentity(
    (1, 3, 6, 1, 2, 1, 100, 1)
)
_TransportDomainUdpIpv4_ObjectIdentity = ObjectIdentity
transportDomainUdpIpv4 = _TransportDomainUdpIpv4_ObjectIdentity(
    (1, 3, 6, 1, 2, 1, 100, 1, 1)
)
if mibBuilder.loadTexts:
    transportDomainUdpIpv4.setStatus("current")
if mibBuilder.loadTexts:
    transportDomainUdpIpv4.setDescription("The UDP over IPv4 transport domain. The corresponding transport address is of type TransportAddressIPv4 for global IPv4 addresses.")
_TransportDomainUdpIpv6_ObjectIdentity = ObjectIdentity
transportDomainUdpIpv6 = _TransportDomainUdpIpv6_ObjectIdentity(
    (1, 3, 6, 1, 2, 1, 100, 1, 2)
)
if mibBuilder.loadTexts:
    transportDomainUdpIpv6.setStatus("current")
if mibBuilder.loadTexts:
    transportDomainUdpIpv6.setDescription("The UDP over IPv6 transport domain. The corresponding transport address is of type TransportAddressIPv6 for global IPv6 addresses.")
_TransportDomainUdpIpv4z_ObjectIdentity = ObjectIdentity
transportDomainUdpIpv4z = _TransportDomainUdpIpv4z_ObjectIdentity(
    (1, 3, 6, 1, 2, 1, 100, 1, 3)
)
if mibBuilder.loadTexts:
    transportDomainUdpIpv4z.setStatus("current")
if mibBuilder.loadTexts:
    transportDomainUdpIpv4z.setDescription("The UDP over IPv4 transport domain. The corresponding transport address is of type TransportAddressIPv4z for scoped IPv4 addresses with a zone index.")
_TransportDomainUdpIpv6z_ObjectIdentity = ObjectIdentity
transportDomainUdpIpv6z = _TransportDomainUdpIpv6z_ObjectIdentity(
    (1, 3, 6, 1, 2, 1, 100, 1, 4)
)
if mibBuilder.loadTexts:
    transportDomainUdpIpv6z.setStatus("current")
if mibBuilder.loadTexts:
    transportDomainUdpIpv6z.setDescription("The UDP over IPv6 transport domain. The corresponding transport address is of type TransportAddressIPv6z for scoped IPv6 addresses with a zone index.")
_TransportDomainTcpIpv4_ObjectIdentity = ObjectIdentity
transportDomainTcpIpv4 = _TransportDomainTcpIpv4_ObjectIdentity(
    (1, 3, 6, 1, 2, 1, 100, 1, 5)
)
if mibBuilder.loadTexts:
    transportDomainTcpIpv4.setStatus("current")
if mibBuilder.loadTexts:
    transportDomainTcpIpv4.setDescription("The TCP over IPv4 transport domain. The corresponding transport address is of type TransportAddressIPv4 for global IPv4 addresses.")
_TransportDomainTcpIpv6_ObjectIdentity = ObjectIdentity
transportDomainTcpIpv6 = _TransportDomainTcpIpv6_ObjectIdentity(
    (1, 3, 6, 1, 2, 1, 100, 1, 6)
)
if mibBuilder.loadTexts:
    transportDomainTcpIpv6.setStatus("current")
if mibBuilder.loadTexts:
    transportDomainTcpIpv6.setDescription("The TCP over IPv6 transport domain. The corresponding transport address is of type TransportAddressIPv6 for global IPv6 addresses.")
_TransportDomainTcpIpv4z_ObjectIdentity = ObjectIdentity
transportDomainTcpIpv4z = _TransportDomainTcpIpv4z_ObjectIdentity(
    (1, 3, 6, 1, 2, 1, 100, 1, 7)
)
if mibBuilder.loadTexts:
    transportDomainTcpIpv4z.setStatus("current")
if mibBuilder.loadTexts:
    transportDomainTcpIpv4z.setDescription("The TCP over IPv4 transport domain. The corresponding transport address is of type TransportAddressIPv4z for scoped IPv4 addresses with a zone index.")
_TransportDomainTcpIpv6z_ObjectIdentity = ObjectIdentity
transportDomainTcpIpv6z = _TransportDomainTcpIpv6z_ObjectIdentity(
    (1, 3, 6, 1, 2, 1, 100, 1, 8)
)
if mibBuilder.loadTexts:
    transportDomainTcpIpv6z.setStatus("current")
if mibBuilder.loadTexts:
    transportDomainTcpIpv6z.setDescription("The TCP over IPv6 transport domain. The corresponding transport address is of type TransportAddressIPv6z for scoped IPv6 addresses with a zone index.")
_TransportDomainSctpIpv4_ObjectIdentity = ObjectIdentity
transportDomainSctpIpv4 = _TransportDomainSctpIpv4_ObjectIdentity(
    (1, 3, 6, 1, 2, 1, 100, 1, 9)
)
if mibBuilder.loadTexts:
    transportDomainSctpIpv4.setStatus("current")
if mibBuilder.loadTexts:
    transportDomainSctpIpv4.setDescription("The SCTP over IPv4 transport domain. The corresponding transport address is of type TransportAddressIPv4 for global IPv4 addresses. This transport domain usually represents the primary address on multihomed SCTP endpoints.")
_TransportDomainSctpIpv6_ObjectIdentity = ObjectIdentity
transportDomainSctpIpv6 = _TransportDomainSctpIpv6_ObjectIdentity(
    (1, 3, 6, 1, 2, 1, 100, 1, 10)
)
if mibBuilder.loadTexts:
    transportDomainSctpIpv6.setStatus("current")
if mibBuilder.loadTexts:
    transportDomainSctpIpv6.setDescription("The SCTP over IPv6 transport domain. The corresponding transport address is of type TransportAddressIPv6 for global IPv6 addresses. This transport domain usually represents the primary address on multihomed SCTP endpoints.")
_TransportDomainSctpIpv4z_ObjectIdentity = ObjectIdentity
transportDomainSctpIpv4z = _TransportDomainSctpIpv4z_ObjectIdentity(
    (1, 3, 6, 1, 2, 1, 100, 1, 11)
)
if mibBuilder.loadTexts:
    transportDomainSctpIpv4z.setStatus("current")
if mibBuilder.loadTexts:
    transportDomainSctpIpv4z.setDescription("The SCTP over IPv4 transport domain. The corresponding transport address is of type TransportAddressIPv4z for scoped IPv4 addresses with a zone index. This transport domain usually represents the primary address on multihomed SCTP endpoints.")
_TransportDomainSctpIpv6z_ObjectIdentity = ObjectIdentity
transportDomainSctpIpv6z = _TransportDomainSctpIpv6z_ObjectIdentity(
    (1, 3, 6, 1, 2, 1, 100, 1, 12)
)
if mibBuilder.loadTexts:
    transportDomainSctpIpv6z.setStatus("current")
if mibBuilder.loadTexts:
    transportDomainSctpIpv6z.setDescription("The SCTP over IPv6 transport domain. The corresponding transport address is of type TransportAddressIPv6z for scoped IPv6 addresses with a zone index. This transport domain usually represents the primary address on multihomed SCTP endpoints.")
_TransportDomainLocal_ObjectIdentity = ObjectIdentity
transportDomainLocal = _TransportDomainLocal_ObjectIdentity(
    (1, 3, 6, 1, 2, 1, 100, 1, 13)
)
if mibBuilder.loadTexts:
    transportDomainLocal.setStatus("current")
if mibBuilder.loadTexts:
    transportDomainLocal.setDescription("The Posix Local IPC transport domain. The corresponding transport address is of type TransportAddressLocal. The Posix Local IPC transport domain incorporates the well-known UNIX domain sockets.")
_TransportDomainUdpDns_ObjectIdentity = ObjectIdentity
transportDomainUdpDns = _TransportDomainUdpDns_ObjectIdentity(
    (1, 3, 6, 1, 2, 1, 100, 1, 14)
)
if mibBuilder.loadTexts:
    transportDomainUdpDns.setStatus("current")
if mibBuilder.loadTexts:
    transportDomainUdpDns.setDescription("The UDP transport domain using fully qualified domain names. The corresponding transport address is of type TransportAddressDns.")
_TransportDomainTcpDns_ObjectIdentity = ObjectIdentity
transportDomainTcpDns = _TransportDomainTcpDns_ObjectIdentity(
    (1, 3, 6, 1, 2, 1, 100, 1, 15)
)
if mibBuilder.loadTexts:
    transportDomainTcpDns.setStatus("current")
if mibBuilder.loadTexts:
    transportDomainTcpDns.setDescription("The TCP transport domain using fully qualified domain names. The corresponding transport address is of type TransportAddressDns.")
_TransportDomainSctpDns_ObjectIdentity = ObjectIdentity
transportDomainSctpDns = _TransportDomainSctpDns_ObjectIdentity(
    (1, 3, 6, 1, 2, 1, 100, 1, 16)
)
if mibBuilder.loadTexts:
    transportDomainSctpDns.setStatus("current")
if mibBuilder.loadTexts:
    transportDomainSctpDns.setDescription("The SCTP transport domain using fully qualified domain names. The corresponding transport address is of type TransportAddressDns.")

# Managed Objects groups


# Notification objects


# Notifications groups


# Agent capabilities


# Module compliance


# Export all MIB objects to the MIB builder

mibBuilder.export_symbols(
    "TRANSPORT-ADDRESS-MIB",
    **{"TransportDomain": TransportDomain,
       "TransportAddressType": TransportAddressType,
       "TransportAddress": TransportAddress,
       "TransportAddressIPv4": TransportAddressIPv4,
       "TransportAddressIPv6": TransportAddressIPv6,
       "TransportAddressIPv4z": TransportAddressIPv4z,
       "TransportAddressIPv6z": TransportAddressIPv6z,
       "TransportAddressLocal": TransportAddressLocal,
       "TransportAddressDns": TransportAddressDns,
       "transportAddressMIB": transportAddressMIB,
       "transportDomains": transportDomains,
       "transportDomainUdpIpv4": transportDomainUdpIpv4,
       "transportDomainUdpIpv6": transportDomainUdpIpv6,
       "transportDomainUdpIpv4z": transportDomainUdpIpv4z,
       "transportDomainUdpIpv6z": transportDomainUdpIpv6z,
       "transportDomainTcpIpv4": transportDomainTcpIpv4,
       "transportDomainTcpIpv6": transportDomainTcpIpv6,
       "transportDomainTcpIpv4z": transportDomainTcpIpv4z,
       "transportDomainTcpIpv6z": transportDomainTcpIpv6z,
       "transportDomainSctpIpv4": transportDomainSctpIpv4,
       "transportDomainSctpIpv6": transportDomainSctpIpv6,
       "transportDomainSctpIpv4z": transportDomainSctpIpv4z,
       "transportDomainSctpIpv6z": transportDomainSctpIpv6z,
       "transportDomainLocal": transportDomainLocal,
       "transportDomainUdpDns": transportDomainUdpDns,
       "transportDomainTcpDns": transportDomainTcpDns,
       "transportDomainSctpDns": transportDomainSctpDns,
       "PYSNMP_MODULE_ID": transportAddressMIB}
)
