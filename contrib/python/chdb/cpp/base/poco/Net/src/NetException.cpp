//
// NetException.cpp
//
// Library: Net
// Package: NetCore
// Module:  NetException
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Net/NetException.h"
#include <typeinfo>


using CHDBPoco::IOException;


namespace CHDBPoco {
namespace Net {


CHDB_POCO_IMPLEMENT_EXCEPTION(NetException, IOException, "Net Exception")
CHDB_POCO_IMPLEMENT_EXCEPTION(InvalidAddressException, NetException, "Invalid address")
CHDB_POCO_IMPLEMENT_EXCEPTION(InvalidSocketException, NetException, "Invalid socket")
CHDB_POCO_IMPLEMENT_EXCEPTION(ServiceNotFoundException, NetException, "Service not found")
CHDB_POCO_IMPLEMENT_EXCEPTION(ConnectionAbortedException, NetException, "Software caused connection abort")
CHDB_POCO_IMPLEMENT_EXCEPTION(ConnectionResetException, NetException, "Connection reset by peer")
CHDB_POCO_IMPLEMENT_EXCEPTION(ConnectionRefusedException, NetException, "Connection refused")
CHDB_POCO_IMPLEMENT_EXCEPTION(DNSException, NetException, "DNS error")
CHDB_POCO_IMPLEMENT_EXCEPTION(HostNotFoundException, DNSException, "Host not found")
CHDB_POCO_IMPLEMENT_EXCEPTION(NoAddressFoundException, DNSException, "No address found")
CHDB_POCO_IMPLEMENT_EXCEPTION(InterfaceNotFoundException, NetException, "Interface not found")
CHDB_POCO_IMPLEMENT_EXCEPTION(NoMessageException, NetException, "No message received")
CHDB_POCO_IMPLEMENT_EXCEPTION(MessageException, NetException, "Malformed message")
CHDB_POCO_IMPLEMENT_EXCEPTION(MultipartException, MessageException, "Malformed multipart message")
CHDB_POCO_IMPLEMENT_EXCEPTION(HTTPException, NetException, "HTTP Exception")
CHDB_POCO_IMPLEMENT_EXCEPTION(NotAuthenticatedException, HTTPException, "No authentication information found")
CHDB_POCO_IMPLEMENT_EXCEPTION(UnsupportedRedirectException, HTTPException, "Unsupported HTTP redirect (protocol change)")
CHDB_POCO_IMPLEMENT_EXCEPTION(FTPException, NetException, "FTP Exception")
CHDB_POCO_IMPLEMENT_EXCEPTION(SMTPException, NetException, "SMTP Exception")
CHDB_POCO_IMPLEMENT_EXCEPTION(POP3Exception, NetException, "POP3 Exception")
CHDB_POCO_IMPLEMENT_EXCEPTION(ICMPException, NetException, "ICMP Exception")
CHDB_POCO_IMPLEMENT_EXCEPTION(NTPException, NetException, "NTP Exception")
CHDB_POCO_IMPLEMENT_EXCEPTION(HTMLFormException, NetException, "HTML Form Exception")
CHDB_POCO_IMPLEMENT_EXCEPTION(WebSocketException, NetException, "WebSocket Exception")
CHDB_POCO_IMPLEMENT_EXCEPTION(UnsupportedFamilyException, NetException, "Unknown or unsupported socket family")
CHDB_POCO_IMPLEMENT_EXCEPTION(AddressFamilyMismatchException, NetException, "Address family mismatch")


} } // namespace CHDBPoco::Net
