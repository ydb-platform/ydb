//
// SSLException.cpp
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  SSLException
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Net/SSLException.h"
#include <typeinfo>


namespace DBPoco {
namespace Net {


DB_POCO_IMPLEMENT_EXCEPTION(SSLException, NetException, "SSL Exception")
DB_POCO_IMPLEMENT_EXCEPTION(SSLContextException, SSLException, "SSL context exception")
DB_POCO_IMPLEMENT_EXCEPTION(InvalidCertificateException, SSLException, "Invalid certificate")
DB_POCO_IMPLEMENT_EXCEPTION(CertificateValidationException, SSLException, "Certificate validation error")
DB_POCO_IMPLEMENT_EXCEPTION(SSLConnectionUnexpectedlyClosedException, SSLException, "SSL connection unexpectedly closed")


} } // namespace DBPoco::Net
