//
// NTPPacket.h
//
// Library: Net
// Package: NTP
// Module:  NTPPacket
//
// Definition of the NTPPacket class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Net_NTPPacket_INCLUDED
#define CHDB_Net_NTPPacket_INCLUDED


#include "CHDBPoco/Foundation.h"
#include "CHDBPoco/Net/Net.h"
#include "CHDBPoco/Timestamp.h"

namespace CHDBPoco
{
namespace Net
{


    class Net_API NTPPacket
    /// This class is the NTP packet abstraction.
    {
    public:
        NTPPacket();
        /// Creates an NTPPacket.

        NTPPacket(CHDBPoco::UInt8 * packet);
        /// Creates an NTPPacket.
        ///
        /// Assumed to have at least 48 bytes.

        ~NTPPacket();
        /// Destroys the NTPPacket.

        void packet(CHDBPoco::UInt8 * packet) const;
        /// Returns the NTP packet.
        ///
        /// Assumed to have at least 48 bytes.

        void setPacket(CHDBPoco::UInt8 * packet);
        /// Returns the NTP packet.
        ///
        /// Assumed to have exactly 48 bytes.

        CHDBPoco::Int8 leapIndicator() const;
        /// Returns the leap indicator.

        CHDBPoco::Int8 version() const;
        /// Returns the version.

        CHDBPoco::Int8 mode() const;
        /// Returns the mode.

        CHDBPoco::Int8 stratum() const;
        /// Returns the stratum.

        CHDBPoco::Int8 pool() const;
        /// Returns the pool.

        CHDBPoco::Int8 precision() const;
        /// Returns the precision

        CHDBPoco::Int32 rootDelay() const;
        /// Returns the root delay

        CHDBPoco::Int32 rootDispersion() const;
        /// Returns the root dispersion

        CHDBPoco::Int32 referenceId() const;
        /// Returns the reference id

        CHDBPoco::Int64 referenceTimestamp() const;
        /// Returns the reference timestamp

        CHDBPoco::Int64 originateTimestamp() const;
        /// Returns the originate timestamp

        CHDBPoco::Int64 receiveTimestamp() const;
        /// Returns the receive timestamp

        CHDBPoco::Int64 transmitTimestamp() const;
        /// Returns the transmit timestamp

        CHDBPoco::Timestamp referenceTime() const;
        /// Returns the reference time

        CHDBPoco::Timestamp originateTime() const;
        /// Returns the originate time

        CHDBPoco::Timestamp receiveTime() const;
        /// Returns the receive time

        CHDBPoco::Timestamp transmitTime() const;
        /// Returns the transmit time
    private:
        CHDBPoco::Timestamp convertTime(CHDBPoco::Int64 tm) const;

        CHDBPoco::Int8 _leapIndicator;
        CHDBPoco::Int8 _version;
        CHDBPoco::Int8 _mode;
        CHDBPoco::Int8 _stratum;
        CHDBPoco::Int8 _pool;
        CHDBPoco::Int8 _precision;
        CHDBPoco::Int32 _rootDelay;
        CHDBPoco::Int32 _rootDispersion;
        CHDBPoco::Int32 _referenceId;
        CHDBPoco::Int64 _referenceTimestamp;
        CHDBPoco::Int64 _originateTimestamp;
        CHDBPoco::Int64 _receiveTimestamp;
        CHDBPoco::Int64 _transmitTimestamp;
    };


    //
    // inlines
    //
    inline CHDBPoco::Int8 NTPPacket::leapIndicator() const
    {
        return _leapIndicator;
    }


    inline CHDBPoco::Int8 NTPPacket::version() const
    {
        return _version;
    }


    inline CHDBPoco::Int8 NTPPacket::mode() const
    {
        return _mode;
    }


    inline CHDBPoco::Int8 NTPPacket::stratum() const
    {
        return _stratum;
    }


    inline CHDBPoco::Int8 NTPPacket::pool() const
    {
        return _pool;
    }


    inline CHDBPoco::Int8 NTPPacket::precision() const
    {
        return _precision;
    }


    inline CHDBPoco::Int32 NTPPacket::rootDelay() const
    {
        return _rootDelay;
    }


    inline CHDBPoco::Int32 NTPPacket::rootDispersion() const
    {
        return _rootDispersion;
    }


    inline CHDBPoco::Int32 NTPPacket::referenceId() const
    {
        return _referenceId;
    }


    inline CHDBPoco::Int64 NTPPacket::referenceTimestamp() const
    {
        return _referenceTimestamp;
    }


    inline CHDBPoco::Int64 NTPPacket::originateTimestamp() const
    {
        return _originateTimestamp;
    }


    inline CHDBPoco::Int64 NTPPacket::receiveTimestamp() const
    {
        return _receiveTimestamp;
    }


    inline CHDBPoco::Int64 NTPPacket::transmitTimestamp() const
    {
        return _transmitTimestamp;
    }


}
} // namespace CHDBPoco::Net


#endif // CHDB_Net_NTPPacket_INCLUDED
