//
// HTTPServerSession.h
//
// Library: Net
// Package: HTTPServer
// Module:  HTTPServerSession
//
// Definition of the HTTPServerSession class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Net_HTTPServerSession_INCLUDED
#define CHDB_Net_HTTPServerSession_INCLUDED


#include "CHDBPoco/Net/HTTPServerParams.h"
#include "CHDBPoco/Net/HTTPServerSession.h"
#include "CHDBPoco/Net/HTTPSession.h"
#include "CHDBPoco/Net/Net.h"
#include "CHDBPoco/Net/SocketAddress.h"
#include "CHDBPoco/Timespan.h"


namespace CHDBPoco
{
namespace Net
{


    class Net_API HTTPServerSession : public HTTPSession
    /// This class handles the server side of a
    /// HTTP session. It is used internally by
    /// HTTPServer.
    {
    public:
        HTTPServerSession(const StreamSocket & socket, HTTPServerParams::Ptr pParams);
        /// Creates the HTTPServerSession.

        virtual ~HTTPServerSession();
        /// Destroys the HTTPServerSession.

        bool hasMoreRequests();
        /// Returns true if there are requests available.

        bool canKeepAlive() const;
        /// Returns true if the session can be kept alive.

        SocketAddress clientAddress();
        /// Returns the client's address.

        SocketAddress serverAddress();
        /// Returns the server's address.

        void setKeepAliveTimeout(CHDBPoco::Timespan keepAliveTimeout);

        size_t getKeepAliveTimeout() const { return _keepAliveTimeout.totalSeconds(); }

        size_t getMaxKeepAliveRequests() const { return _maxKeepAliveRequests; }

    private:
        bool _firstRequest;
        CHDBPoco::Timespan _keepAliveTimeout;
        int _maxKeepAliveRequests;
    };


    //
    // inlines
    //
    inline bool HTTPServerSession::canKeepAlive() const
    {
        return _maxKeepAliveRequests != 0;
    }


}
} // namespace CHDBPoco::Net


#endif // CHDB_Net_HTTPServerSession_INCLUDED
