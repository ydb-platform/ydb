//
// HTTPRequestHandler.h
//
// Library: Net
// Package: HTTPServer
// Module:  HTTPRequestHandler
//
// Definition of the HTTPRequestHandler class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Net_HTTPRequestHandler_INCLUDED
#define DB_Net_HTTPRequestHandler_INCLUDED


#include "DBPoco/Net/Net.h"


namespace DBPoco
{
namespace Net
{


    class HTTPServerRequest;
    class HTTPServerResponse;


    class Net_API HTTPRequestHandler
    /// The abstract base class for HTTPRequestHandlers
    /// created by HTTPServer.
    ///
    /// Derived classes must override the handleRequest() method.
    /// Furthermore, a HTTPRequestHandlerFactory must be provided.
    ///
    /// The handleRequest() method must perform the complete handling
    /// of the HTTP request connection. As soon as the handleRequest()
    /// method returns, the request handler object is destroyed.
    ///
    /// A new HTTPRequestHandler object will be created for
    /// each new HTTP request that is received by the HTTPServer.
    {
    public:
        HTTPRequestHandler();
        /// Creates the HTTPRequestHandler.

        virtual ~HTTPRequestHandler();
        /// Destroys the HTTPRequestHandler.

        virtual void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) = 0;
        /// Must be overridden by subclasses.
        ///
        /// Handles the given request.

    private:
        HTTPRequestHandler(const HTTPRequestHandler &);
        HTTPRequestHandler & operator=(const HTTPRequestHandler &);
    };


}
} // namespace DBPoco::Net


#endif // DB_Net_HTTPRequestHandler_INCLUDED
