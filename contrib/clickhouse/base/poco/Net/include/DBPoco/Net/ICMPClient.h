//
// ICMPClient.h
//
// Library: Net
// Package: ICMP
// Module:  ICMPClient
//
// Definition of the ICMPClient class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Net_ICMPClient_INCLUDED
#define DB_Net_ICMPClient_INCLUDED


#include "DBPoco/BasicEvent.h"
#include "DBPoco/Net/ICMPEventArgs.h"
#include "DBPoco/Net/ICMPSocket.h"
#include "DBPoco/Net/Net.h"
#include "DBPoco/Net/SocketAddress.h"


namespace DBPoco
{
namespace Net
{


    class Net_API ICMPClient
    /// This class provides ICMP Ping functionality.
    ///
    /// The events are available when class is instantiated
    ///	and non-static member functions are called.
    ///
    ///	A "lightweight" alternative is direct (without instantiation)
    ///	use of static member functions.
    {
    public:
        mutable DBPoco::BasicEvent<ICMPEventArgs> pingBegin;
        mutable DBPoco::BasicEvent<ICMPEventArgs> pingReply;
        mutable DBPoco::BasicEvent<ICMPEventArgs> pingError;
        mutable DBPoco::BasicEvent<ICMPEventArgs> pingEnd;

        explicit ICMPClient(SocketAddress::Family family, int dataSize = 48, int ttl = 128, int timeout = 50000);
        /// Creates an ICMP client.

        ~ICMPClient();
        /// Destroys the ICMP client.

        int ping(SocketAddress & address, int repeat = 1) const;
        /// Pings the specified address [repeat] times.
        /// Notifications are posted for events.
        ///
        /// Returns the number of valid replies.

        int ping(const std::string & address, int repeat = 1) const;
        /// Calls ICMPClient::ping(SocketAddress&, int) and
        /// returns the result.
        ///
        /// Returns the number of valid replies.

        static int
        ping(SocketAddress & address, SocketAddress::Family family, int repeat = 1, int dataSize = 48, int ttl = 128, int timeout = 50000);
        /// Pings the specified address [repeat] times.
        /// Notifications are not posted for events.
        ///
        /// Returns the number of valid replies.

        static int pingIPv4(const std::string & address, int repeat = 1, int dataSize = 48, int ttl = 128, int timeout = 50000);
        /// Calls ICMPClient::ping(SocketAddress&, int) and
        /// returns the result.
        ///
        /// Returns the number of valid replies.

    private:
        mutable SocketAddress::Family _family;
        int _dataSize;
        int _ttl;
        int _timeout;
    };


}
} // namespace DBPoco::Net


#endif // DB_Net_ICMPClient_INCLUDED
