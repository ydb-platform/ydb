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


#include "CHDBPoco/Net/PrivateKeyFactory.h"
#include "CHDBPoco/Net/SSLManager.h"


namespace CHDBPoco {
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


} } // namespace CHDBPoco::Net
