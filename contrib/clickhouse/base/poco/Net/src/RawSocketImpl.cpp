//
// RawSocketImpl.cpp
//
// Library: Net
// Package: Sockets
// Module:  RawSocketImpl
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Net/RawSocketImpl.h"
#include "DBPoco/Net/NetException.h"


using DBPoco::InvalidArgumentException;


namespace DBPoco {
namespace Net {


RawSocketImpl::RawSocketImpl()
{
	init(AF_INET);
}


RawSocketImpl::RawSocketImpl(SocketAddress::Family family, int proto)
{
	if (family == SocketAddress::IPv4)
		init2(AF_INET, proto);
#if defined(DB_POCO_HAVE_IPv6)
	else if (family == SocketAddress::IPv6)
		init2(AF_INET6, proto);
#endif
	else throw InvalidArgumentException("Invalid or unsupported address family passed to RawSocketImpl");

}

	
RawSocketImpl::RawSocketImpl(DB_poco_socket_t sockfd): 
	SocketImpl(sockfd)
{
}


RawSocketImpl::~RawSocketImpl()
{
}


void RawSocketImpl::init(int af)
{
	init2(af, IPPROTO_RAW);
}


void RawSocketImpl::init2(int af, int proto)
{
	initSocket(af, SOCK_RAW, proto);
	setOption(IPPROTO_IP, IP_HDRINCL, 0);
}


} } // namespace DBPoco::Net
