//
// StreamSocket.cpp
//
// Library: Net
// Package: Sockets
// Module:  StreamSocket
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Net/StreamSocket.h"
#include "CHDBPoco/Net/StreamSocketImpl.h"
#include "CHDBPoco/FIFOBuffer.h"
#include "CHDBPoco/Mutex.h"
#include "CHDBPoco/Exception.h"


using CHDBPoco::InvalidArgumentException;
using CHDBPoco::Mutex;
using CHDBPoco::ScopedLock;


namespace CHDBPoco {
namespace Net {


StreamSocket::StreamSocket(): Socket(new StreamSocketImpl)
{
}


StreamSocket::StreamSocket(const SocketAddress& address): Socket(new StreamSocketImpl(address.family()))
{
	connect(address);
}


StreamSocket::StreamSocket(SocketAddress::Family family): Socket(new StreamSocketImpl(family))
{
}


StreamSocket::StreamSocket(const Socket& socket): Socket(socket)
{
	if (!dynamic_cast<StreamSocketImpl*>(impl()))
		throw InvalidArgumentException("Cannot assign incompatible socket");
}


StreamSocket::StreamSocket(SocketImpl* pImpl): Socket(pImpl)
{
	if (!dynamic_cast<StreamSocketImpl*>(impl()))
		throw InvalidArgumentException("Cannot assign incompatible socket");
}


StreamSocket::~StreamSocket()
{
}


StreamSocket& StreamSocket::operator = (const Socket& socket)
{
	if (dynamic_cast<StreamSocketImpl*>(socket.impl()))
		Socket::operator = (socket);
	else
		throw InvalidArgumentException("Cannot assign incompatible socket");
	return *this;
}


void StreamSocket::connect(const SocketAddress& address)
{
	impl()->connect(address);
}


void StreamSocket::connect(const SocketAddress& address, const CHDBPoco::Timespan& timeout)
{
	impl()->connect(address, timeout);
}


void StreamSocket::connectNB(const SocketAddress& address)
{
	impl()->connectNB(address);
}


void StreamSocket::shutdownReceive()
{
	impl()->shutdownReceive();
}

	
void StreamSocket::shutdownSend()
{
	impl()->shutdownSend();
}

	
void StreamSocket::shutdown()
{
	impl()->shutdown();
}


int StreamSocket::sendBytes(const void* buffer, int length, int flags)
{
	return impl()->sendBytes(buffer, length, flags);
}


int StreamSocket::sendBytes(FIFOBuffer& fifoBuf)
{
	ScopedLock<Mutex> l(fifoBuf.mutex());

	int ret = impl()->sendBytes(fifoBuf.begin(), (int) fifoBuf.used());
	if (ret > 0) fifoBuf.drain(ret);
	return ret;
}


int StreamSocket::receiveBytes(void* buffer, int length, int flags)
{
	return impl()->receiveBytes(buffer, length, flags);
}


int StreamSocket::receiveBytes(FIFOBuffer& fifoBuf)
{
	ScopedLock<Mutex> l(fifoBuf.mutex());

	int ret = impl()->receiveBytes(fifoBuf.next(), (int)fifoBuf.available());
	if (ret > 0) fifoBuf.advance(ret);
	return ret;
}


void StreamSocket::sendUrgent(unsigned char data)
{
	impl()->sendUrgent(data);
}


} } // namespace CHDBPoco::Net
