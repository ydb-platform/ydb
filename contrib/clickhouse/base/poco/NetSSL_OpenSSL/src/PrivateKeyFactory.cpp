//
// PrivateKeyFactory.cpp
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  PrivateKeyFactory
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Net/PrivateKeyFactory.h"
#include "DBPoco/Net/SSLManager.h"


namespace DBPoco {
namespace Net {


PrivateKeyFactory::PrivateKeyFactory()
{
}


PrivateKeyFactory::~PrivateKeyFactory()
{
}


PrivateKeyFactoryRegistrar::PrivateKeyFactoryRegistrar(const std::string& name, PrivateKeyFactory* pFactory)
{
	SSLManager::instance().privateKeyFactoryMgr().setFactory(name, pFactory);
}


PrivateKeyFactoryRegistrar::~PrivateKeyFactoryRegistrar()
{
}


} } // namespace DBPoco::Net
