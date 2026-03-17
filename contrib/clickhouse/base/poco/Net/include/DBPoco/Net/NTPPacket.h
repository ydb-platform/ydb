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


#ifndef DB_Net_NTPPacket_INCLUDED
#define DB_Net_NTPPacket_INCLUDED


#include "DBPoco/Foundation.h"
#include "DBPoco/Net/Net.h"
#include "DBPoco/Timestamp.h"

namespace DBPoco
{
namespace Net
{


    class Net_API NTPPacket
    /// This class is the NTP packet abstraction.
    {
    public:
        NTPPacket();
        /// Creates an NTPPacket.

        NTPPacket(DBPoco::UInt8 * packet);
        /// Creates an NTPPacket.
        ///
        /// Assumed to have at least 48 bytes.

        ~NTPPacket();
        /// Destroys the NTPPacket.

        void packet(DBPoco::UInt8 * packet) const;
        /// Returns the NTP packet.
        ///
        /// Assumed to have at least 48 bytes.

        void setPacket(DBPoco::UInt8 * packet);
        /// Returns the NTP packet.
        ///
        /// Assumed to have exactly 48 bytes.

        DBPoco::Int8 leapIndicator() const;
        /// Returns the leap indicator.

        DBPoco::Int8 version() const;
        /// Returns the version.

        DBPoco::Int8 mode() const;
        /// Returns the mode.

        DBPoco::Int8 stratum() const;
        /// Returns the stratum.

        DBPoco::Int8 pool() const;
        /// Returns the pool.

        DBPoco::Int8 precision() const;
        /// Returns the precision

        DBPoco::Int32 rootDelay() const;
        /// Returns the root delay

        DBPoco::Int32 rootDispersion() const;
        /// Returns the root dispersion

        DBPoco::Int32 referenceId() const;
        /// Returns the reference id

        DBPoco::Int64 referenceTimestamp() const;
        /// Returns the reference timestamp

        DBPoco::Int64 originateTimestamp() const;
        /// Returns the originate timestamp

        DBPoco::Int64 receiveTimestamp() const;
        /// Returns the receive timestamp

        DBPoco::Int64 transmitTimestamp() const;
        /// Returns the transmit timestamp

        DBPoco::Timestamp referenceTime() const;
        /// Returns the reference time

        DBPoco::Timestamp originateTime() const;
        /// Returns the originate time

        DBPoco::Timestamp receiveTime() const;
        /// Returns the receive time

        DBPoco::Timestamp transmitTime() const;
        /// Returns the transmit time
    private:
        DBPoco::Timestamp convertTime(DBPoco::Int64 tm) const;

        DBPoco::Int8 _leapIndicator;
        DBPoco::Int8 _version;
        DBPoco::Int8 _mode;
        DBPoco::Int8 _stratum;
        DBPoco::Int8 _pool;
        DBPoco::Int8 _precision;
        DBPoco::Int32 _rootDelay;
        DBPoco::Int32 _rootDispersion;
        DBPoco::Int32 _referenceId;
        DBPoco::Int64 _referenceTimestamp;
        DBPoco::Int64 _originateTimestamp;
        DBPoco::Int64 _receiveTimestamp;
        DBPoco::Int64 _transmitTimestamp;
    };


    //
    // inlines
    //
    inline DBPoco::Int8 NTPPacket::leapIndicator() const
    {
        return _leapIndicator;
    }


    inline DBPoco::Int8 NTPPacket::version() const
    {
        return _version;
    }


    inline DBPoco::Int8 NTPPacket::mode() const
    {
        return _mode;
    }


    inline DBPoco::Int8 NTPPacket::stratum() const
    {
        return _stratum;
    }


    inline DBPoco::Int8 NTPPacket::pool() const
    {
        return _pool;
    }


    inline DBPoco::Int8 NTPPacket::precision() const
    {
        return _precision;
    }


    inline DBPoco::Int32 NTPPacket::rootDelay() const
    {
        return _rootDelay;
    }


    inline DBPoco::Int32 NTPPacket::rootDispersion() const
    {
        return _rootDispersion;
    }


    inline DBPoco::Int32 NTPPacket::referenceId() const
    {
        return _referenceId;
    }


    inline DBPoco::Int64 NTPPacket::referenceTimestamp() const
    {
        return _referenceTimestamp;
    }


    inline DBPoco::Int64 NTPPacket::originateTimestamp() const
    {
        return _originateTimestamp;
    }


    inline DBPoco::Int64 NTPPacket::receiveTimestamp() const
    {
        return _receiveTimestamp;
    }


    inline DBPoco::Int64 NTPPacket::transmitTimestamp() const
    {
        return _transmitTimestamp;
    }


}
} // namespace DBPoco::Net


#endif // DB_Net_NTPPacket_INCLUDED
