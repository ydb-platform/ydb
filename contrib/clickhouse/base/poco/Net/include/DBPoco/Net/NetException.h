//
// NetException.h
//
// Library: Net
// Package: NetCore
// Module:  NetException
//
// Definition of the NetException class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Net_NetException_INCLUDED
#define DB_Net_NetException_INCLUDED


#include "DBPoco/Exception.h"
#include "DBPoco/Net/Net.h"


namespace DBPoco
{
namespace Net
{


    DB_POCO_DECLARE_EXCEPTION(Net_API, NetException, DBPoco::IOException)
    DB_POCO_DECLARE_EXCEPTION(Net_API, InvalidAddressException, NetException)
    DB_POCO_DECLARE_EXCEPTION(Net_API, InvalidSocketException, NetException)
    DB_POCO_DECLARE_EXCEPTION(Net_API, ServiceNotFoundException, NetException)
    DB_POCO_DECLARE_EXCEPTION(Net_API, ConnectionAbortedException, NetException)
    DB_POCO_DECLARE_EXCEPTION(Net_API, ConnectionResetException, NetException)
    DB_POCO_DECLARE_EXCEPTION(Net_API, ConnectionRefusedException, NetException)
    DB_POCO_DECLARE_EXCEPTION(Net_API, DNSException, NetException)
    DB_POCO_DECLARE_EXCEPTION(Net_API, HostNotFoundException, DNSException)
    DB_POCO_DECLARE_EXCEPTION(Net_API, NoAddressFoundException, DNSException)
    DB_POCO_DECLARE_EXCEPTION(Net_API, InterfaceNotFoundException, NetException)
    DB_POCO_DECLARE_EXCEPTION(Net_API, NoMessageException, NetException)
    DB_POCO_DECLARE_EXCEPTION(Net_API, MessageException, NetException)
    DB_POCO_DECLARE_EXCEPTION(Net_API, MultipartException, MessageException)
    DB_POCO_DECLARE_EXCEPTION(Net_API, HTTPException, NetException)
    DB_POCO_DECLARE_EXCEPTION(Net_API, NotAuthenticatedException, HTTPException)
    DB_POCO_DECLARE_EXCEPTION(Net_API, UnsupportedRedirectException, HTTPException)
    DB_POCO_DECLARE_EXCEPTION(Net_API, FTPException, NetException)
    DB_POCO_DECLARE_EXCEPTION(Net_API, SMTPException, NetException)
    DB_POCO_DECLARE_EXCEPTION(Net_API, POP3Exception, NetException)
    DB_POCO_DECLARE_EXCEPTION(Net_API, ICMPException, NetException)
    DB_POCO_DECLARE_EXCEPTION(Net_API, NTPException, NetException)
    DB_POCO_DECLARE_EXCEPTION(Net_API, HTMLFormException, NetException)
    DB_POCO_DECLARE_EXCEPTION(Net_API, WebSocketException, NetException)
    DB_POCO_DECLARE_EXCEPTION(Net_API, UnsupportedFamilyException, NetException)
    DB_POCO_DECLARE_EXCEPTION(Net_API, AddressFamilyMismatchException, NetException)


}
} // namespace DBPoco::Net


#endif // DB_Net_NetException_INCLUDED
