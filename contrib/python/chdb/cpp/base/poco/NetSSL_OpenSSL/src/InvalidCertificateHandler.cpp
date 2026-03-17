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


#include "CHDBPoco/Net/InvalidCertificateHandler.h"
#include "CHDBPoco/Net/SSLManager.h"
#include "CHDBPoco/Delegate.h"


using CHDBPoco::Delegate;


namespace CHDBPoco {
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
		CHDB_poco_unexpected();
	}
}


} } // namespace CHDBPoco::Net
