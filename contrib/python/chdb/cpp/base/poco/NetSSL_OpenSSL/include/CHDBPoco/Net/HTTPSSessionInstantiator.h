//
// HTTPSSessionInstantiator.h
//
// Library: NetSSL_OpenSSL
// Package: HTTPSClient
// Module:  HTTPSSessionInstantiator
//
// Definition of the HTTPSSessionInstantiator class.
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Net_HTTPSSessionInstantiator_INCLUDED
#define CHDB_Net_HTTPSSessionInstantiator_INCLUDED


#include "CHDBPoco/Net/Context.h"
#include "CHDBPoco/Net/HTTPSessionInstantiator.h"
#include "CHDBPoco/Net/NetSSL.h"
#include "CHDBPoco/Net/Utility.h"
#include "CHDBPoco/URI.h"


namespace CHDBPoco
{
namespace Net
{


    class NetSSL_API HTTPSSessionInstantiator : public HTTPSessionInstantiator
    /// The HTTPSessionInstantiator for HTTPSClientSession.
    {
    public:
        HTTPSSessionInstantiator();
        /// Creates the HTTPSSessionInstantiator.

        HTTPSSessionInstantiator(Context::Ptr pContext);
        /// Creates the HTTPSSessionInstantiator using the given SSL context.

        ~HTTPSSessionInstantiator();
        /// Destroys the HTTPSSessionInstantiator.

        HTTPClientSession * createClientSession(const CHDBPoco::URI & uri);
        /// Creates a HTTPSClientSession for the given URI.

        static void registerInstantiator();
        /// Registers the instantiator with the global HTTPSessionFactory.

        static void registerInstantiator(Context::Ptr pContext);
        /// Registers the instantiator with the global HTTPSessionFactory using the given SSL context.

        static void unregisterInstantiator();
        /// Unregisters the factory with the global HTTPSessionFactory.

    private:
        Context::Ptr _pContext;
    };


}
} // namespace CHDBPoco::Net


#endif // CHDB_Net_HTTPSSessionInstantiator_INCLUDED
