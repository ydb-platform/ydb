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


#ifndef CHDB_Net_ICMPClient_INCLUDED
#define CHDB_Net_ICMPClient_INCLUDED


#include "CHDBPoco/BasicEvent.h"
#include "CHDBPoco/Net/ICMPEventArgs.h"
#include "CHDBPoco/Net/ICMPSocket.h"
#include "CHDBPoco/Net/Net.h"
#include "CHDBPoco/Net/SocketAddress.h"


namespace CHDBPoco
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
        mutable CHDBPoco::BasicEvent<ICMPEventArgs> pingBegin;
        mutable CHDBPoco::BasicEvent<ICMPEventArgs> pingReply;
        mutable CHDBPoco::BasicEvent<ICMPEventArgs> pingError;
        mutable CHDBPoco::BasicEvent<ICMPEventArgs> pingEnd;

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
} // namespace CHDBPoco::Net


#endif // CHDB_Net_ICMPClient_INCLUDED
