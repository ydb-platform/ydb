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


#ifndef DB_Net_HTTPServerConnection_INCLUDED
#define DB_Net_HTTPServerConnection_INCLUDED


#include "DBPoco/Mutex.h"
#include "DBPoco/Net/HTTPRequestHandlerFactory.h"
#include "DBPoco/Net/HTTPResponse.h"
#include "DBPoco/Net/HTTPServerParams.h"
#include "DBPoco/Net/Net.h"
#include "DBPoco/Net/TCPServerConnection.h"


namespace DBPoco
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
        DBPoco::FastMutex _mutex;
    };


}
} // namespace DBPoco::Net


#endif // DB_Net_HTTPServerConnection_INCLUDED
