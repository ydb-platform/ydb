//
// HTTPSessionInstantiator.cpp
//
// Library: Net
// Package: HTTPClient
// Module:  HTTPSessionInstantiator
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Net/HTTPSessionInstantiator.h"
#include "CHDBPoco/Net/HTTPSessionFactory.h"
#include "CHDBPoco/Net/HTTPClientSession.h"


using CHDBPoco::URI;


namespace CHDBPoco {
namespace Net {


HTTPSessionInstantiator::HTTPSessionInstantiator():
	_proxyPort(0)
{
}


HTTPSessionInstantiator::~HTTPSessionInstantiator()
{
}


HTTPClientSession* HTTPSessionInstantiator::createClientSession(const CHDBPoco::URI& uri)
{
	CHDB_poco_assert (uri.getScheme() == "http");
	HTTPClientSession* pSession = new HTTPClientSession(uri.getHost(), uri.getPort());
	if (!proxyHost().empty())
	{
		pSession->setProxy(proxyHost(), proxyPort());
		pSession->setProxyCredentials(proxyUsername(), proxyPassword());
	}
	return pSession;
}


void HTTPSessionInstantiator::registerInstantiator()
{
	HTTPSessionFactory::defaultFactory().registerProtocol("http", new HTTPSessionInstantiator);
}


void HTTPSessionInstantiator::unregisterInstantiator()
{
	HTTPSessionFactory::defaultFactory().unregisterProtocol("http");
}


void HTTPSessionInstantiator::setProxy(const std::string& host, CHDBPoco::UInt16 port)
{
	_proxyHost = host;
	_proxyPort = port;
}


void HTTPSessionInstantiator::setProxyCredentials(const std::string& username, const std::string& password)
{
	_proxyUsername = username;
	_proxyPassword = password;
}


} } // namespace CHDBPoco::Net
