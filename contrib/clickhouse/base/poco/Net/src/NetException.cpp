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


#include "DBPoco/Net/NetException.h"
#include <typeinfo>


using DBPoco::IOException;


namespace DBPoco {
namespace Net {


DB_POCO_IMPLEMENT_EXCEPTION(NetException, IOException, "Net Exception")
DB_POCO_IMPLEMENT_EXCEPTION(InvalidAddressException, NetException, "Invalid address")
DB_POCO_IMPLEMENT_EXCEPTION(InvalidSocketException, NetException, "Invalid socket")
DB_POCO_IMPLEMENT_EXCEPTION(ServiceNotFoundException, NetException, "Service not found")
DB_POCO_IMPLEMENT_EXCEPTION(ConnectionAbortedException, NetException, "Software caused connection abort")
DB_POCO_IMPLEMENT_EXCEPTION(ConnectionResetException, NetException, "Connection reset by peer")
DB_POCO_IMPLEMENT_EXCEPTION(ConnectionRefusedException, NetException, "Connection refused")
DB_POCO_IMPLEMENT_EXCEPTION(DNSException, NetException, "DNS error")
DB_POCO_IMPLEMENT_EXCEPTION(HostNotFoundException, DNSException, "Host not found")
DB_POCO_IMPLEMENT_EXCEPTION(NoAddressFoundException, DNSException, "No address found")
DB_POCO_IMPLEMENT_EXCEPTION(InterfaceNotFoundException, NetException, "Interface not found")
DB_POCO_IMPLEMENT_EXCEPTION(NoMessageException, NetException, "No message received")
DB_POCO_IMPLEMENT_EXCEPTION(MessageException, NetException, "Malformed message")
DB_POCO_IMPLEMENT_EXCEPTION(MultipartException, MessageException, "Malformed multipart message")
DB_POCO_IMPLEMENT_EXCEPTION(HTTPException, NetException, "HTTP Exception")
DB_POCO_IMPLEMENT_EXCEPTION(NotAuthenticatedException, HTTPException, "No authentication information found")
DB_POCO_IMPLEMENT_EXCEPTION(UnsupportedRedirectException, HTTPException, "Unsupported HTTP redirect (protocol change)")
DB_POCO_IMPLEMENT_EXCEPTION(FTPException, NetException, "FTP Exception")
DB_POCO_IMPLEMENT_EXCEPTION(SMTPException, NetException, "SMTP Exception")
DB_POCO_IMPLEMENT_EXCEPTION(POP3Exception, NetException, "POP3 Exception")
DB_POCO_IMPLEMENT_EXCEPTION(ICMPException, NetException, "ICMP Exception")
DB_POCO_IMPLEMENT_EXCEPTION(NTPException, NetException, "NTP Exception")
DB_POCO_IMPLEMENT_EXCEPTION(HTMLFormException, NetException, "HTML Form Exception")
DB_POCO_IMPLEMENT_EXCEPTION(WebSocketException, NetException, "WebSocket Exception")
DB_POCO_IMPLEMENT_EXCEPTION(UnsupportedFamilyException, NetException, "Unknown or unsupported socket family")
DB_POCO_IMPLEMENT_EXCEPTION(AddressFamilyMismatchException, NetException, "Address family mismatch")


} } // namespace DBPoco::Net
