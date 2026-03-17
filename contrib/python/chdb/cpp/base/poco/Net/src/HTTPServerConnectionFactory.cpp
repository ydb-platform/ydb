//
// HTTPServerConnectionFactory.cpp
//
// Library: Net
// Package: HTTPServer
// Module:  HTTPServerConnectionFactory
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Net/HTTPServerConnectionFactory.h"
#include "CHDBPoco/Net/HTTPServerConnection.h"
#include "CHDBPoco/Net/HTTPRequestHandlerFactory.h"


namespace CHDBPoco {
namespace Net {


HTTPServerConnectionFactory::HTTPServerConnectionFactory(HTTPServerParams::Ptr pParams, HTTPRequestHandlerFactory::Ptr pFactory):
	_pParams(pParams),
	_pFactory(pFactory)
{
	CHDB_poco_check_ptr (pFactory);
}


HTTPServerConnectionFactory::~HTTPServerConnectionFactory()
{
}


TCPServerConnection* HTTPServerConnectionFactory::createConnection(const StreamSocket& socket)
{
	return new HTTPServerConnection(socket, _pParams, _pFactory);
}


} } // namespace CHDBPoco::Net
