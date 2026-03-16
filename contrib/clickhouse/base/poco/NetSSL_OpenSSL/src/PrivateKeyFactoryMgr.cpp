//
// PrivateKeyFactoryMgr.cpp
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  PrivateKeyFactoryMgr
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Net/PrivateKeyFactoryMgr.h"
#include "DBPoco/Net/KeyFileHandler.h"
#include "DBPoco/Net/KeyConsoleHandler.h"


namespace DBPoco {
namespace Net {


PrivateKeyFactoryMgr::PrivateKeyFactoryMgr()
{
	setFactory("KeyFileHandler", new PrivateKeyFactoryImpl<KeyFileHandler>());
	setFactory("KeyConsoleHandler", new PrivateKeyFactoryImpl<KeyConsoleHandler>());
}


PrivateKeyFactoryMgr::~PrivateKeyFactoryMgr()
{
}


void PrivateKeyFactoryMgr::setFactory(const std::string& name, PrivateKeyFactory* pFactory)
{
	bool success = _factories.insert(make_pair(name, DBPoco::SharedPtr<PrivateKeyFactory>(pFactory))).second;
	if (!success)
		delete pFactory;
	DB_poco_assert(success);
}
		

bool PrivateKeyFactoryMgr::hasFactory(const std::string& name) const
{
	return _factories.find(name) != _factories.end();
}
		
	
const PrivateKeyFactory* PrivateKeyFactoryMgr::getFactory(const std::string& name) const
{
	FactoriesMap::const_iterator it = _factories.find(name);
	if (it != _factories.end())
		return it->second;
	else
		return 0;
}


void PrivateKeyFactoryMgr::removeFactory(const std::string& name)
{
	_factories.erase(name);
}


} } // namespace DBPoco::Net
