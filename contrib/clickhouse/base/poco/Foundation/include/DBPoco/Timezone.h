//
// Timezone.h
//
// Library: Foundation
// Package: DateTime
// Module:  Timezone
//
// Definition of the Timezone class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_Timezone_INCLUDED
#define DB_Foundation_Timezone_INCLUDED


#include "DBPoco/Foundation.h"
#include "DBPoco/Timestamp.h"


namespace DBPoco
{


class Foundation_API Timezone
/// This class provides information about the current timezone.
{
public:
    static int utcOffset();
    /// Returns the offset of local time to UTC, in seconds.
    ///     local time = UTC + utcOffset() + dst().

    static int dst();
    /// Returns the daylight saving time offset in seconds if
    /// daylight saving time is in use.
    ///     local time = UTC + utcOffset() + dst().

    static bool isDst(const Timestamp & timestamp);
    /// Returns true if daylight saving time is in effect
    /// for the given time. Depending on the operating system
    /// platform this might only work reliably for certain
    /// date ranges, as the C library's localtime() function
    /// is used.

    static int tzd();
    /// Returns the time zone differential for the current timezone.
    /// The timezone differential is computed as utcOffset() + dst()
    /// and is expressed in seconds.

    static std::string name();
    /// Returns the timezone name currently in effect.

    static std::string standardName();
    /// Returns the timezone name if not daylight saving time is in effect.

    static std::string dstName();
    /// Returns the timezone name if daylight saving time is in effect.
};


} // namespace DBPoco


#endif // DB_Foundation_Timezone_INCLUDED
