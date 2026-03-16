//
// RejectCertificateHandler.cpp
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  RejectCertificateHandler
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Net/RejectCertificateHandler.h"


namespace DBPoco {
namespace Net {


RejectCertificateHandler::RejectCertificateHandler(bool server): InvalidCertificateHandler(server)
{
}


RejectCertificateHandler::~RejectCertificateHandler()
{
}


void RejectCertificateHandler::onInvalidCertificate(const void*, VerificationErrorArgs& errorCert)
{
	errorCert.setIgnoreError(false);
}


} } // namespace DBPoco::Net
