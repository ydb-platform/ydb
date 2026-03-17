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


#ifndef DB_Net_HTTPServerSession_INCLUDED
#define DB_Net_HTTPServerSession_INCLUDED


#include "DBPoco/Net/HTTPServerParams.h"
#include "DBPoco/Net/HTTPServerSession.h"
#include "DBPoco/Net/HTTPSession.h"
#include "DBPoco/Net/Net.h"
#include "DBPoco/Net/SocketAddress.h"
#include "DBPoco/Timespan.h"


namespace DBPoco
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

        void setKeepAliveTimeout(DBPoco::Timespan keepAliveTimeout);

        size_t getKeepAliveTimeout() const { return _keepAliveTimeout.totalSeconds(); }

        size_t getMaxKeepAliveRequests() const { return _maxKeepAliveRequests; }

    private:
        bool _firstRequest;
        DBPoco::Timespan _keepAliveTimeout;
        size_t _maxKeepAliveRequests;
    };


    //
    // inlines
    //
    inline bool HTTPServerSession::canKeepAlive() const
    {
        return getKeepAlive() && _maxKeepAliveRequests > 0;
    }


}
} // namespace DBPoco::Net


#endif // DB_Net_HTTPServerSession_INCLUDED
