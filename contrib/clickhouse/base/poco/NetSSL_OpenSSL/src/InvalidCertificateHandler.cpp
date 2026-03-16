//
// InvalidCertificateHandler.cpp
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  InvalidCertificateHandler
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Net/InvalidCertificateHandler.h"
#include "DBPoco/Net/SSLManager.h"
#include "DBPoco/Delegate.h"


using DBPoco::Delegate;


namespace DBPoco {
namespace Net {


InvalidCertificateHandler::InvalidCertificateHandler(bool handleErrorsOnServerSide): _handleErrorsOnServerSide(handleErrorsOnServerSide)
{
	if (_handleErrorsOnServerSide)
		SSLManager::instance().ServerVerificationError += Delegate<InvalidCertificateHandler, VerificationErrorArgs>(this, &InvalidCertificateHandler::onInvalidCertificate);
	else
		SSLManager::instance().ClientVerificationError += Delegate<InvalidCertificateHandler, VerificationErrorArgs>(this, &InvalidCertificateHandler::onInvalidCertificate);
}


InvalidCertificateHandler::~InvalidCertificateHandler()
{
	try
	{
		if (_handleErrorsOnServerSide)
			SSLManager::instance().ServerVerificationError -= Delegate<InvalidCertificateHandler, VerificationErrorArgs>(this, &InvalidCertificateHandler::onInvalidCertificate);
		else
			SSLManager::instance().ClientVerificationError -= Delegate<InvalidCertificateHandler, VerificationErrorArgs>(this, &InvalidCertificateHandler::onInvalidCertificate);
	}
	catch (...)
	{
		DB_poco_unexpected();
	}
}


} } // namespace DBPoco::Net
