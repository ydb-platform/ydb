//
// SocketStream.cpp
//
// Library: Net
// Package: Sockets
// Module:  SocketStream
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Net/SocketStream.h"
#include "CHDBPoco/Net/StreamSocketImpl.h"
#include "CHDBPoco/Exception.h"


using CHDBPoco::BufferedBidirectionalStreamBuf;
using CHDBPoco::InvalidArgumentException;


namespace CHDBPoco {
namespace Net {


//
// SocketStreamBuf
//


SocketStreamBuf::SocketStreamBuf(const Socket& socket): 
	BufferedBidirectionalStreamBuf(STREAM_BUFFER_SIZE, std::ios::in | std::ios::out),
	_pImpl(dynamic_cast<StreamSocketImpl*>(socket.impl()))
{
	if (_pImpl)
		_pImpl->duplicate(); 
	else
		throw InvalidArgumentException("Invalid or null SocketImpl passed to SocketStreamBuf");
}


SocketStreamBuf::~SocketStreamBuf()
{
	_pImpl->release();
}


int SocketStreamBuf::readFromDevice(char* buffer, std::streamsize length)
{
	return _pImpl->receiveBytes(buffer, (int) length);
}


int SocketStreamBuf::writeToDevice(const char* buffer, std::streamsize length)
{
	return _pImpl->sendBytes(buffer, (int) length);
}


//
// SocketIOS
//


SocketIOS::SocketIOS(const Socket& socket):
	_buf(socket)
{
	CHDB_poco_ios_init(&_buf);
}


SocketIOS::~SocketIOS()
{
	try
	{
		_buf.sync();
	}
	catch (...)
	{
	}
}


SocketStreamBuf* SocketIOS::rdbuf()
{
	return &_buf;
}


void SocketIOS::close()
{
	_buf.sync();
	_buf.socketImpl()->close();
}


StreamSocket SocketIOS::socket() const
{
	return StreamSocket(_buf.socketImpl());
}


//
// SocketOutputStream
//


SocketOutputStream::SocketOutputStream(const Socket& socket):
	SocketIOS(socket),
	std::ostream(&_buf)
{
}


SocketOutputStream::~SocketOutputStream()
{
}


//
// SocketInputStream
//


SocketInputStream::SocketInputStream(const Socket& socket):
	SocketIOS(socket),
	std::istream(&_buf)
{
}


SocketInputStream::~SocketInputStream()
{
}


//
// SocketStream
//


SocketStream::SocketStream(const Socket& socket):
	SocketIOS(socket),
	std::iostream(&_buf)
{
}


SocketStream::~SocketStream()
{
}


} } // namespace CHDBPoco::Net
