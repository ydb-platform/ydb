//
// HTTPSessionFactory.cpp
//
// Library: Net
// Package: HTTPClient
// Module:  HTTPSessionFactory
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Net/HTTPSessionFactory.h"
#include "DBPoco/Net/HTTPSessionInstantiator.h"
#include "DBPoco/Exception.h"


using DBPoco::SingletonHolder;
using DBPoco::FastMutex;
using DBPoco::NotFoundException;
using DBPoco::ExistsException;


namespace DBPoco {
namespace Net {


HTTPSessionFactory::HTTPSessionFactory():
	_proxyPort(0)
{
}


HTTPSessionFactory::HTTPSessionFactory(const std::string& proxyHost, DBPoco::UInt16 proxyPort):
	_proxyHost(proxyHost),
	_proxyPort(proxyPort)
{
}


HTTPSessionFactory::~HTTPSessionFactory()
{
	for (Instantiators::iterator it = _instantiators.begin(); it != _instantiators.end(); ++it)
	{
		delete it->second.pIn;
	}
}


void HTTPSessionFactory::registerProtocol(const std::string& protocol, HTTPSessionInstantiator* pSessionInstantiator)
{
	DB_poco_assert_dbg(pSessionInstantiator);

	FastMutex::ScopedLock lock(_mutex);
	std::pair<Instantiators::iterator, bool> tmp = _instantiators.insert(make_pair(protocol, InstantiatorInfo(pSessionInstantiator)));
	if (!tmp.second) 
	{
		++tmp.first->second.cnt;
		delete pSessionInstantiator;
	}
}


void HTTPSessionFactory::unregisterProtocol(const std::string& protocol)
{
	FastMutex::ScopedLock lock(_mutex);
	
	Instantiators::iterator it = _instantiators.find(protocol);
	if (it != _instantiators.end())
	{
		if (it->second.cnt == 1)
		{
			delete it->second.pIn;
			_instantiators.erase(it);
		}
		else --it->second.cnt;
	}
	else throw NotFoundException("No HTTPSessionInstantiator registered for", protocol);
}


bool HTTPSessionFactory::supportsProtocol(const std::string& protocol)
{
	FastMutex::ScopedLock lock(_mutex);
	
	Instantiators::iterator it = _instantiators.find(protocol);
	return it != _instantiators.end();
}


HTTPClientSession* HTTPSessionFactory::createClientSession(const DBPoco::URI& uri)
{
	FastMutex::ScopedLock lock(_mutex);
	
	if (uri.isRelative()) throw DBPoco::UnknownURISchemeException("Relative URIs are not supported by HTTPSessionFactory.");

	Instantiators::iterator it = _instantiators.find(uri.getScheme());
	if (it != _instantiators.end())
	{
		it->second.pIn->setProxy(_proxyHost, _proxyPort);
		it->second.pIn->setProxyCredentials(_proxyUsername, _proxyPassword);
		return it->second.pIn->createClientSession(uri);
	}
	else throw DBPoco::UnknownURISchemeException(uri.getScheme());
}


void HTTPSessionFactory::setProxy(const std::string& host, DBPoco::UInt16 port)
{
	FastMutex::ScopedLock lock(_mutex);

	_proxyHost = host;
	_proxyPort = port;
}


void HTTPSessionFactory::setProxyCredentials(const std::string& username, const std::string& password)
{
	FastMutex::ScopedLock lock(_mutex);

	_proxyUsername = username;
	_proxyPassword = password;
}


namespace
{
	static SingletonHolder<HTTPSessionFactory> singleton;
}


HTTPSessionFactory& HTTPSessionFactory::defaultFactory()
{
	return *singleton.get();
}


HTTPSessionFactory::InstantiatorInfo::InstantiatorInfo(HTTPSessionInstantiator* pInst): pIn(pInst), cnt(1)
{
	DB_poco_check_ptr (pIn);
}


} } // namespace DBPoco::Net
