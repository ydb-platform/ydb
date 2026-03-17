//
// HTTPServerConnection.h
//
// Library: Net
// Package: HTTPServer
// Module:  HTTPServerConnection
//
// Definition of the HTTPServerConnection class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Net_HTTPServerConnection_INCLUDED
#define CHDB_Net_HTTPServerConnection_INCLUDED


#include "CHDBPoco/Mutex.h"
#include "CHDBPoco/Net/HTTPRequestHandlerFactory.h"
#include "CHDBPoco/Net/HTTPResponse.h"
#include "CHDBPoco/Net/HTTPServerParams.h"
#include "CHDBPoco/Net/Net.h"
#include "CHDBPoco/Net/TCPServerConnection.h"


namespace CHDBPoco
{
namespace Net
{


    class HTTPServerSession;


    class Net_API HTTPServerConnection : public TCPServerConnection
    /// This subclass of TCPServerConnection handles HTTP
    /// connections.
    {
    public:
        HTTPServerConnection(const StreamSocket & socket, HTTPServerParams::Ptr pParams, HTTPRequestHandlerFactory::Ptr pFactory);
        /// Creates the HTTPServerConnection.

        virtual ~HTTPServerConnection();
        /// Destroys the HTTPServerConnection.

        void run();
        /// Handles all HTTP requests coming in.

    protected:
        void sendErrorResponse(HTTPServerSession & session, HTTPResponse::HTTPStatus status);
        void onServerStopped(const bool & abortCurrent);

    private:
        HTTPServerParams::Ptr _pParams;
        HTTPRequestHandlerFactory::Ptr _pFactory;
        bool _stopped;
        CHDBPoco::FastMutex _mutex;
    };


}
} // namespace CHDBPoco::Net


#endif // CHDB_Net_HTTPServerConnection_INCLUDED
