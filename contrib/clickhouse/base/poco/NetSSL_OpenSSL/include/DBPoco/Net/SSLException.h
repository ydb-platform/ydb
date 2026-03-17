//
// SSLException.h
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  SSLException
//
// Definition of the SSLException class.
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_NetSSL_SSLException_INCLUDED
#define DB_NetSSL_SSLException_INCLUDED


#include "DBPoco/Net/NetException.h"
#include "DBPoco/Net/NetSSL.h"


namespace DBPoco
{
namespace Net
{


    DB_POCO_DECLARE_EXCEPTION(NetSSL_API, SSLException, NetException)
    DB_POCO_DECLARE_EXCEPTION(NetSSL_API, SSLContextException, SSLException)
    DB_POCO_DECLARE_EXCEPTION(NetSSL_API, InvalidCertificateException, SSLException)
    DB_POCO_DECLARE_EXCEPTION(NetSSL_API, CertificateValidationException, SSLException)
    DB_POCO_DECLARE_EXCEPTION(NetSSL_API, SSLConnectionUnexpectedlyClosedException, SSLException)


}
} // namespace DBPoco::Net


#endif // DB_NetSSL_SSLException_INCLUDED
