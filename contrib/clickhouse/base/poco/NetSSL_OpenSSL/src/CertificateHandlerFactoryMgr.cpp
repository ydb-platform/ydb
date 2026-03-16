//
// CertificateHandlerFactoryMgr.cpp
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  CertificateHandlerFactoryMgr
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Net/CertificateHandlerFactoryMgr.h"
#include "DBPoco/Net/AcceptCertificateHandler.h"
#include "DBPoco/Net/RejectCertificateHandler.h"


namespace DBPoco {
namespace Net {


CertificateHandlerFactoryMgr::CertificateHandlerFactoryMgr()
{
	setFactory("AcceptCertificateHandler", new CertificateHandlerFactoryImpl<AcceptCertificateHandler>());
	setFactory("RejectCertificateHandler", new CertificateHandlerFactoryImpl<RejectCertificateHandler>());
}


CertificateHandlerFactoryMgr::~CertificateHandlerFactoryMgr()
{
}


void CertificateHandlerFactoryMgr::setFactory(const std::string& name, CertificateHandlerFactory* pFactory)
{
	bool success = _factories.insert(make_pair(name, DBPoco::SharedPtr<CertificateHandlerFactory>(pFactory))).second;
	if (!success)
		delete pFactory;
	DB_poco_assert(success);
}
		

bool CertificateHandlerFactoryMgr::hasFactory(const std::string& name) const
{
	return _factories.find(name) != _factories.end();
}
		
	
const CertificateHandlerFactory* CertificateHandlerFactoryMgr::getFactory(const std::string& name) const
{
	FactoriesMap::const_iterator it = _factories.find(name);
	if (it != _factories.end())
		return it->second;
	else
		return 0;
}


void CertificateHandlerFactoryMgr::removeFactory(const std::string& name)
{
	_factories.erase(name);
}


} } // namespace DBPoco::Net
