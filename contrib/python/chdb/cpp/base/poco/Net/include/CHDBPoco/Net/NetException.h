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


#ifndef CHDB_Net_NetException_INCLUDED
#define CHDB_Net_NetException_INCLUDED


#include "CHDBPoco/Exception.h"
#include "CHDBPoco/Net/Net.h"


namespace CHDBPoco
{
namespace Net
{


    CHDB_POCO_DECLARE_EXCEPTION(Net_API, NetException, CHDBPoco::IOException)
    CHDB_POCO_DECLARE_EXCEPTION(Net_API, InvalidAddressException, NetException)
    CHDB_POCO_DECLARE_EXCEPTION(Net_API, InvalidSocketException, NetException)
    CHDB_POCO_DECLARE_EXCEPTION(Net_API, ServiceNotFoundException, NetException)
    CHDB_POCO_DECLARE_EXCEPTION(Net_API, ConnectionAbortedException, NetException)
    CHDB_POCO_DECLARE_EXCEPTION(Net_API, ConnectionResetException, NetException)
    CHDB_POCO_DECLARE_EXCEPTION(Net_API, ConnectionRefusedException, NetException)
    CHDB_POCO_DECLARE_EXCEPTION(Net_API, DNSException, NetException)
    CHDB_POCO_DECLARE_EXCEPTION(Net_API, HostNotFoundException, DNSException)
    CHDB_POCO_DECLARE_EXCEPTION(Net_API, NoAddressFoundException, DNSException)
    CHDB_POCO_DECLARE_EXCEPTION(Net_API, InterfaceNotFoundException, NetException)
    CHDB_POCO_DECLARE_EXCEPTION(Net_API, NoMessageException, NetException)
    CHDB_POCO_DECLARE_EXCEPTION(Net_API, MessageException, NetException)
    CHDB_POCO_DECLARE_EXCEPTION(Net_API, MultipartException, MessageException)
    CHDB_POCO_DECLARE_EXCEPTION(Net_API, HTTPException, NetException)
    CHDB_POCO_DECLARE_EXCEPTION(Net_API, NotAuthenticatedException, HTTPException)
    CHDB_POCO_DECLARE_EXCEPTION(Net_API, UnsupportedRedirectException, HTTPException)
    CHDB_POCO_DECLARE_EXCEPTION(Net_API, FTPException, NetException)
    CHDB_POCO_DECLARE_EXCEPTION(Net_API, SMTPException, NetException)
    CHDB_POCO_DECLARE_EXCEPTION(Net_API, POP3Exception, NetException)
    CHDB_POCO_DECLARE_EXCEPTION(Net_API, ICMPException, NetException)
    CHDB_POCO_DECLARE_EXCEPTION(Net_API, NTPException, NetException)
    CHDB_POCO_DECLARE_EXCEPTION(Net_API, HTMLFormException, NetException)
    CHDB_POCO_DECLARE_EXCEPTION(Net_API, WebSocketException, NetException)
    CHDB_POCO_DECLARE_EXCEPTION(Net_API, UnsupportedFamilyException, NetException)
    CHDB_POCO_DECLARE_EXCEPTION(Net_API, AddressFamilyMismatchException, NetException)


}
} // namespace CHDBPoco::Net


#endif // CHDB_Net_NetException_INCLUDED
