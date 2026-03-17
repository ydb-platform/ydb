//
// NetSSL.h
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  OpenSSL
//
// Basic definitions for the Poco OpenSSL library.
// This file must be the first file included by every other OpenSSL
// header file.
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_NetSSL_NetSSL_INCLUDED
#define DB_NetSSL_NetSSL_INCLUDED


#include "DBPoco/Crypto/Crypto.h"
#include "DBPoco/Net/Net.h"


//
// The following block is the standard way of creating macros which make exporting
// from a DLL simpler. All files within this DLL are compiled with the NetSSL_EXPORTS
// symbol defined on the command line. this symbol should not be defined on any project
// that uses this DLL. This way any other project whose source files include this file see
// NetSSL_API functions as being imported from a DLL, whereas this DLL sees symbols
// defined with this macro as being exported.
//


#if !defined(NetSSL_API)
#    if !defined(POCO_NO_GCC_API_ATTRIBUTE) && defined(__GNUC__) && (__GNUC__ >= 4)
#        define NetSSL_API __attribute__((visibility("default")))
#    else
#        define NetSSL_API
#    endif
#endif


//
// Automatically link NetSSL and OpenSSL libraries.
//


namespace DBPoco
{
namespace Net
{


    void NetSSL_API initializeSSL();
    /// Initialize the NetSSL library, as well as the underlying OpenSSL
    /// libraries, by calling DBPoco::Crypto::OpenSSLInitializer::initialize().
    ///
    /// Should be called before using any class from the NetSSL library.
    /// The NetSSL will be initialized automatically, through
    /// DBPoco::Crypto::OpenSSLInitializer instances or similar mechanisms
    /// when creating Context or SSLManager instances.
    /// However, it is recommended to call initializeSSL()
    /// in any case at application startup.
    ///
    /// Can be called multiple times; however, for every call to
    /// initializeSSL(), a matching call to uninitializeSSL()
    /// must be performed.


    void NetSSL_API uninitializeSSL();
    /// Uninitializes the NetSSL library by calling
    /// DBPoco::Crypto::OpenSSLInitializer::uninitialize() and
    /// shutting down the SSLManager.


}
} // namespace DBPoco::Net


#endif // DB_NetSSL_NetSSL_INCLUDED
