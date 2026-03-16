//
// HTTPServer.cpp
//
// Library: Net
// Package: HTTPServer
// Module:  HTTPServer
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Net/HTTPServer.h"
#include "DBPoco/Net/HTTPServerConnectionFactory.h"


namespace DBPoco {
namespace Net {


HTTPServer::HTTPServer(HTTPRequestHandlerFactory::Ptr pFactory, DBPoco::UInt16 portNumber, HTTPServerParams::Ptr pParams):
	TCPServer(new HTTPServerConnectionFactory(pParams, pFactory), portNumber, pParams),
	_pFactory(pFactory)
{
}


HTTPServer::HTTPServer(HTTPRequestHandlerFactory::Ptr pFactory, const ServerSocket& socket, HTTPServerParams::Ptr pParams):
	TCPServer(new HTTPServerConnectionFactory(pParams, pFactory), socket, pParams),
	_pFactory(pFactory)
{
}


HTTPServer::HTTPServer(HTTPRequestHandlerFactory::Ptr pFactory, DBPoco::ThreadPool& threadPool, const ServerSocket& socket, HTTPServerParams::Ptr pParams):
	TCPServer(new HTTPServerConnectionFactory(pParams, pFactory), threadPool, socket, pParams),
	_pFactory(pFactory)
{
}


HTTPServer::~HTTPServer()
{
	/// We should call stop and join thread here instead of destructor of parent TCPHandler,
	/// because there's possible race on 'vptr' between this virtual destructor and 'run' method.
	stop();
}


void HTTPServer::stopAll(bool abortCurrent)
{
	stop();
	_pFactory->serverStopped(this, abortCurrent);
}


} } // namespace DBPoco::Net
