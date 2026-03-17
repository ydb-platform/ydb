//
// NTPEventArgs.h
//
// Library: Net
// Package: NTP
// Module:  NTPEventArgs
//
// Definition of NTPEventArgs.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Net_NTPEventArgs_INCLUDED
#define CHDB_Net_NTPEventArgs_INCLUDED


#include "CHDBPoco/Net/NTPPacket.h"
#include "CHDBPoco/Net/Net.h"
#include "CHDBPoco/Net/SocketAddress.h"


namespace CHDBPoco
{
namespace Net
{


    class Net_API NTPEventArgs
    /// The purpose of the NTPEventArgs class is to be used as template parameter
    /// to instantiate event members in NTPClient class.
    /// When clients register for an event notification, the reference to the class is
    ///	passed to the handler function to provide information about the event.
    {
    public:
        NTPEventArgs(const SocketAddress & address);
        /// Creates NTPEventArgs.

        virtual ~NTPEventArgs();
        /// Destroys NTPEventArgs.

        std::string hostName() const;
        /// Tries to resolve the target IP address into host name.
        /// If unsuccessful, all exceptions are silently ignored and
        ///	the IP address is returned.

        std::string hostAddress() const;
        /// Returns the target IP address.

        const NTPPacket & packet();
        /// Returns the NTP packet.

    private:
        NTPEventArgs();

        void setPacket(NTPPacket & packet);

        SocketAddress _address;
        NTPPacket _packet;

        friend class NTPClient;
    };


    //
    // inlines
    //
    inline const NTPPacket & NTPEventArgs::packet()
    {
        return _packet;
    }


    inline void NTPEventArgs::setPacket(NTPPacket & packet)
    {
        _packet = packet;
    }


}
} // namespace CHDBPoco::Net


#endif
