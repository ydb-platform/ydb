//
// Net.h
//
// Library: Net
// Package: NetCore
// Module:  Net
//
// Basic definitions for the Poco Net library.
// This file must be the first file included by every other Net
// header file.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Net_Net_INCLUDED
#define DB_Net_Net_INCLUDED


#include "DBPoco/Foundation.h"


//
// The following block is the standard way of creating macros which make exporting
// from a DLL simpler. All files within this DLL are compiled with the Net_EXPORTS
// symbol defined on the command line. this symbol should not be defined on any project
// that uses this DLL. This way any other project whose source files include this file see
// Net_API functions as being imported from a DLL, whereas this DLL sees symbols
// defined with this macro as being exported.
//


#if !defined(Net_API)
#    if !defined(POCO_NO_GCC_API_ATTRIBUTE) && defined(__GNUC__) && (__GNUC__ >= 4)
#        define Net_API __attribute__((visibility("default")))
#    else
#        define Net_API
#    endif
#endif


//
// Automatically link Net library.
//


// Default to enabled IPv6 support if not explicitly disabled
#if !defined(DB_POCO_NET_NO_IPv6) && !defined(DB_POCO_HAVE_IPv6)
#    define DB_POCO_HAVE_IPv6
#elif defined(DB_POCO_NET_NO_IPv6) && defined(DB_POCO_HAVE_IPv6)
#    undef DB_POCO_HAVE_IPv6
#endif // DB_POCO_NET_NO_IPv6, DB_POCO_HAVE_IPv6


namespace DBPoco
{
namespace Net
{


    void Net_API initializeNetwork();
    /// Initialize the network subsystem.
    /// (Windows only, no-op elsewhere)


    void Net_API uninitializeNetwork();
    /// Uninitialize the network subsystem.
    /// (Windows only, no-op elsewhere)


}
} // namespace DBPoco::Net


//
// Automate network initialization (only relevant on Windows).
//



//
// Define DB_POCO_NET_HAS_INTERFACE for platforms that have network interface detection implemented.
//
#if defined(DB_POCO_OS_FAMILY_WINDOWS) || (DB_POCO_OS == DB_POCO_OS_LINUX) || (DB_POCO_OS == DB_POCO_OS_ANDROID) || defined(DB_POCO_OS_FAMILY_BSD) \
    || (DB_POCO_OS == DB_POCO_OS_SOLARIS) || (DB_POCO_OS == DB_POCO_OS_QNX)
#    define DB_POCO_NET_HAS_INTERFACE
#endif


#endif // DB_Net_Net_INCLUDED
