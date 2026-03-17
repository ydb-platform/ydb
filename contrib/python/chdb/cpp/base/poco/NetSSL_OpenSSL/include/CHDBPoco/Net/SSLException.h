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


#ifndef CHDB_NetSSL_SSLException_INCLUDED
#define CHDB_NetSSL_SSLException_INCLUDED


#include "CHDBPoco/Net/NetException.h"
#include "CHDBPoco/Net/NetSSL.h"


namespace CHDBPoco
{
namespace Net
{


    CHDB_POCO_DECLARE_EXCEPTION(NetSSL_API, SSLException, NetException)
    CHDB_POCO_DECLARE_EXCEPTION(NetSSL_API, SSLContextException, SSLException)
    CHDB_POCO_DECLARE_EXCEPTION(NetSSL_API, InvalidCertificateException, SSLException)
    CHDB_POCO_DECLARE_EXCEPTION(NetSSL_API, CertificateValidationException, SSLException)
    CHDB_POCO_DECLARE_EXCEPTION(NetSSL_API, SSLConnectionUnexpectedlyClosedException, SSLException)


}
} // namespace CHDBPoco::Net


#endif // CHDB_NetSSL_SSLException_INCLUDED
