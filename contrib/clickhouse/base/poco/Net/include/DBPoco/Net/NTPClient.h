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


#ifndef DB_Net_NTPClient_INCLUDED
#define DB_Net_NTPClient_INCLUDED


#include "DBPoco/BasicEvent.h"
#include "DBPoco/Net/NTPEventArgs.h"
#include "DBPoco/Net/Net.h"
#include "DBPoco/Net/SocketAddress.h"


namespace DBPoco
{
namespace Net
{


    class Net_API NTPClient
    /// This class provides NTP (Network Time Protocol) client functionality.
    {
    public:
        mutable DBPoco::BasicEvent<NTPEventArgs> response;

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
} // namespace DBPoco::Net


#endif // DB_Net_NTPClient_INCLUDED
