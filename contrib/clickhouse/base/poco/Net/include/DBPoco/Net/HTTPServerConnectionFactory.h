//
// HTTPServerConnectionFactory.h
//
// Library: Net
// Package: HTTPServer
// Module:  HTTPServerConnectionFactory
//
// Definition of the HTTPServerConnectionFactory class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Net_HTTPServerConnectionFactory_INCLUDED
#define DB_Net_HTTPServerConnectionFactory_INCLUDED


#include "DBPoco/Net/HTTPRequestHandlerFactory.h"
#include "DBPoco/Net/HTTPServerParams.h"
#include "DBPoco/Net/Net.h"
#include "DBPoco/Net/TCPServerConnectionFactory.h"


namespace DBPoco
{
namespace Net
{


    class Net_API HTTPServerConnectionFactory : public TCPServerConnectionFactory
    /// This implementation of a TCPServerConnectionFactory
    /// is used by HTTPServer to create HTTPServerConnection objects.
    {
    public:
        HTTPServerConnectionFactory(HTTPServerParams::Ptr pParams, HTTPRequestHandlerFactory::Ptr pFactory);
        /// Creates the HTTPServerConnectionFactory.

        ~HTTPServerConnectionFactory();
        /// Destroys the HTTPServerConnectionFactory.

        TCPServerConnection * createConnection(const StreamSocket & socket);
        /// Creates an instance of HTTPServerConnection
        /// using the given StreamSocket.

    private:
        HTTPServerParams::Ptr _pParams;
        HTTPRequestHandlerFactory::Ptr _pFactory;
    };


}
} // namespace DBPoco::Net


#endif // DB_Net_HTTPServerConnectionFactory_INCLUDED
