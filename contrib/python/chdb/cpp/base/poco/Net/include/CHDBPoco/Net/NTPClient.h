//
// NTPClient.h
//
// Library: Net
// Package: NTP
// Module:  NTPClient
//
// Definition of the NTPClient class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Net_NTPClient_INCLUDED
#define CHDB_Net_NTPClient_INCLUDED


#include "CHDBPoco/BasicEvent.h"
#include "CHDBPoco/Net/NTPEventArgs.h"
#include "CHDBPoco/Net/Net.h"
#include "CHDBPoco/Net/SocketAddress.h"


namespace CHDBPoco
{
namespace Net
{


    class Net_API NTPClient
    /// This class provides NTP (Network Time Protocol) client functionality.
    {
    public:
        mutable CHDBPoco::BasicEvent<NTPEventArgs> response;

        explicit NTPClient(SocketAddress::Family family, int timeout = 3000000);
        /// Creates an NTP client.

        ~NTPClient();
        /// Destroys the NTP client.

        int request(SocketAddress & address) const;
        /// Request the time from the server at address.
        /// Notifications are posted for events.
        ///
        /// Returns the number of valid replies.

        int request(const std::string & address) const;
        /// Request the time from the server at address.
        /// Notifications are posted for events.
        ///
        /// Returns the number of valid replies.

    private:
        mutable SocketAddress::Family _family;
        int _timeout;
    };


}
} // namespace CHDBPoco::Net


#endif // CHDB_Net_NTPClient_INCLUDED
