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


#include "DBPoco/Net/TCPServerConnection.h"
#include "DBPoco/Exception.h"
#include "DBPoco/ErrorHandler.h"


using DBPoco::Exception;


namespace DBPoco {
namespace Net {


TCPServerConnection::TCPServerConnection(const StreamSocket& socket):
	_socket(socket)
{
}


TCPServerConnection::~TCPServerConnection() = default;


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


} } // namespace DBPoco::Net
