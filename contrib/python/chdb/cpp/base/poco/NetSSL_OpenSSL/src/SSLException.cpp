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


#include "CHDBPoco/Net/SSLException.h"
#include <typeinfo>


namespace CHDBPoco {
namespace Net {


CHDB_POCO_IMPLEMENT_EXCEPTION(SSLException, NetException, "SSL Exception")
CHDB_POCO_IMPLEMENT_EXCEPTION(SSLContextException, SSLException, "SSL context exception")
CHDB_POCO_IMPLEMENT_EXCEPTION(InvalidCertificateException, SSLException, "Invalid certificate")
CHDB_POCO_IMPLEMENT_EXCEPTION(CertificateValidationException, SSLException, "Certificate validation error")
CHDB_POCO_IMPLEMENT_EXCEPTION(SSLConnectionUnexpectedlyClosedException, SSLException, "SSL connection unexpectedly closed")


} } // namespace CHDBPoco::Net
