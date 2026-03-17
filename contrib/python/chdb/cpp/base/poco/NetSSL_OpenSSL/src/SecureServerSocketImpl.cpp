//
// SecureServerSocketImpl.cpp
//
// Library: NetSSL_OpenSSL
// Package: SSLSockets
// Module:  SecureServerSocketImpl
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Net/SecureServerSocketImpl.h"


namespace CHDBPoco {
namespace Net {


SecureServerSocketImpl::SecureServerSocketImpl(Context::Ptr pContext):
	_impl(new ServerSocketImpl, pContext)
{
}


SecureServerSocketImpl::~SecureServerSocketImpl()
{
	try
	{
		reset();
	}
	catch (...)
	{
		CHDB_poco_unexpected();
	}
}


SocketImpl* SecureServerSocketImpl::acceptConnection(SocketAddress& clientAddr)
{
	return _impl.acceptConnection(clientAddr);
}


void SecureServerSocketImpl::connect(const SocketAddress& address)
{
	throw CHDBPoco::InvalidAccessException("Cannot connect() a SecureServerSocket");
}


void SecureServerSocketImpl::connect(const SocketAddress& address, const CHDBPoco::Timespan& timeout)
{
	throw CHDBPoco::InvalidAccessException("Cannot connect() a SecureServerSocket");
}
	

void SecureServerSocketImpl::connectNB(const SocketAddress& address)
{
	throw CHDBPoco::InvalidAccessException("Cannot connect() a SecureServerSocket");
}
	

void SecureServerSocketImpl::bind(const SocketAddress& address, bool reuseAddress, bool reusePort)
{
	_impl.bind(address, reuseAddress, reusePort);
	reset(_impl.sockfd());
}

	
void SecureServerSocketImpl::listen(int backlog)
{
	_impl.listen(backlog);
	reset(_impl.sockfd());
}
	

void SecureServerSocketImpl::close()
{
	reset();
	_impl.close();
}
	

int SecureServerSocketImpl::sendBytes(const void* buffer, int length, int flags)
{
	throw CHDBPoco::InvalidAccessException("Cannot sendBytes() on a SecureServerSocket");
}


int SecureServerSocketImpl::receiveBytes(void* buffer, int length, int flags)
{
	throw CHDBPoco::InvalidAccessException("Cannot receiveBytes() on a SecureServerSocket");
}


int SecureServerSocketImpl::sendTo(const void* buffer, int length, const SocketAddress& address, int flags)
{
	throw CHDBPoco::InvalidAccessException("Cannot sendTo() on a SecureServerSocket");
}


int SecureServerSocketImpl::receiveFrom(void* buffer, int length, SocketAddress& address, int flags)
{
	throw CHDBPoco::InvalidAccessException("Cannot receiveFrom() on a SecureServerSocket");
}


void SecureServerSocketImpl::sendUrgent(unsigned char data)
{
	throw CHDBPoco::InvalidAccessException("Cannot sendUrgent() on a SecureServerSocket");
}


bool SecureServerSocketImpl::secure() const
{
	return true;
}


} } // namespace CHDBPoco::Net
