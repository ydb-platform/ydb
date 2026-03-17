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


#include "DBPoco/Net/HTTPServerConnectionFactory.h"
#include "DBPoco/Net/HTTPServerConnection.h"
#include "DBPoco/Net/HTTPRequestHandlerFactory.h"


namespace DBPoco {
namespace Net {


HTTPServerConnectionFactory::HTTPServerConnectionFactory(HTTPServerParams::Ptr pParams, HTTPRequestHandlerFactory::Ptr pFactory):
	_pParams(pParams),
	_pFactory(pFactory)
{
	DB_poco_check_ptr (pFactory);
}


HTTPServerConnectionFactory::~HTTPServerConnectionFactory()
{
}


TCPServerConnection* HTTPServerConnectionFactory::createConnection(const StreamSocket& socket)
{
	return new HTTPServerConnection(socket, _pParams, _pFactory);
}


} } // namespace DBPoco::Net
