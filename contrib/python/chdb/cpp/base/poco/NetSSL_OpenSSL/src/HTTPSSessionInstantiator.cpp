//
// HTTPSSessionInstantiator.cpp
//
// Library: NetSSL_OpenSSL
// Package: HTTPSClient
// Module:  HTTPSSessionInstantiator
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Net/HTTPSSessionInstantiator.h"
#include "CHDBPoco/Net/HTTPSessionFactory.h"
#include "CHDBPoco/Net/HTTPSClientSession.h"


namespace CHDBPoco {
namespace Net {


HTTPSSessionInstantiator::HTTPSSessionInstantiator()
{
}


HTTPSSessionInstantiator::HTTPSSessionInstantiator(Context::Ptr pContext) :
	_pContext(pContext)
{
}


HTTPSSessionInstantiator::~HTTPSSessionInstantiator()
{
}


HTTPClientSession* HTTPSSessionInstantiator::createClientSession(const CHDBPoco::URI& uri)
{
	CHDB_poco_assert (uri.getScheme() == "https");
	HTTPSClientSession* pSession = _pContext.isNull() ? new HTTPSClientSession(uri.getHost(), uri.getPort()) : new HTTPSClientSession(uri.getHost(), uri.getPort(), _pContext);
	if (!proxyHost().empty())
	{
		pSession->setProxy(proxyHost(), proxyPort());
		pSession->setProxyCredentials(proxyUsername(), proxyPassword());
	}
	return pSession;
}


void HTTPSSessionInstantiator::registerInstantiator()
{
	HTTPSessionFactory::defaultFactory().registerProtocol("https", new HTTPSSessionInstantiator);
}


void HTTPSSessionInstantiator::registerInstantiator(Context::Ptr context)
{
	HTTPSessionFactory::defaultFactory().registerProtocol("https", new HTTPSSessionInstantiator(context));
}


void HTTPSSessionInstantiator::unregisterInstantiator()
{
	HTTPSessionFactory::defaultFactory().unregisterProtocol("https");
}


} } // namespace CHDBPoco::Net
