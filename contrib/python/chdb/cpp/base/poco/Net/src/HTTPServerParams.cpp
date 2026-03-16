//
// HTTPServerParams.cpp
//
// Library: Net
// Package: HTTPServer
// Module:  HTTPServerParams
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Net/HTTPServerParams.h"


namespace CHDBPoco {
namespace Net {


HTTPServerParams::HTTPServerParams():
	_timeout(60000000),
	_keepAlive(true),
	_maxKeepAliveRequests(0),
	_keepAliveTimeout(15000000)
{
}


HTTPServerParams::~HTTPServerParams()
{
}

	
void HTTPServerParams::setServerName(const std::string& serverName)
{
	_serverName = serverName;
}
	

void HTTPServerParams::setSoftwareVersion(const std::string& softwareVersion)
{
	_softwareVersion = softwareVersion;
}


void HTTPServerParams::setTimeout(const CHDBPoco::Timespan& timeout)
{
	_timeout = timeout;
}

	
void HTTPServerParams::setKeepAlive(bool keepAlive)
{
	_keepAlive = keepAlive;
}

	
void HTTPServerParams::setKeepAliveTimeout(const CHDBPoco::Timespan& timeout)
{
	_keepAliveTimeout = timeout;
}

	
void HTTPServerParams::setMaxKeepAliveRequests(int maxKeepAliveRequests)
{
	CHDB_poco_assert (maxKeepAliveRequests >= 0);
	_maxKeepAliveRequests = maxKeepAliveRequests;
}
	

} } // namespace CHDBPoco::Net
