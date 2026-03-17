//
// TCPServerConnection.cpp
//
// Library: Net
// Package: TCPServer
// Module:  TCPServerConnection
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Net/TCPServerConnection.h"
#include "CHDBPoco/Exception.h"
#include "CHDBPoco/ErrorHandler.h"


using CHDBPoco::Exception;
using CHDBPoco::ErrorHandler;


namespace CHDBPoco {
namespace Net {


TCPServerConnection::TCPServerConnection(const StreamSocket& socket):
	_socket(socket)
{
}


TCPServerConnection::~TCPServerConnection()
{
}


void TCPServerConnection::start()
{
	try
	{
		run();
	}
	catch (Exception& exc)
	{
		ErrorHandler::handle(exc);
	}
	catch (std::exception& exc)
	{
		ErrorHandler::handle(exc);
	}
	catch (...)
	{
		ErrorHandler::handle();
	}
}


} } // namespace CHDBPoco::Net
