//
// Timezone_UNIX.cpp
//
// Library: Foundation
// Package: DateTime
// Module:  Timezone
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Timezone.h"
#include "CHDBPoco/Exception.h"
#include "CHDBPoco/Mutex.h"
#include <ctime>


namespace CHDBPoco {


class TZInfo
{
public:
	TZInfo()
	{
		tzset();
	}
	
	int timeZone()
	{
		CHDBPoco::FastMutex::ScopedLock lock(_mutex);

	#if defined(__APPLE__)  || defined(__FreeBSD__) || defined (__OpenBSD__) || CHDB_POCO_OS == CHDB_POCO_OS_ANDROID // no timezone global var
		std::time_t now = std::time(NULL);
		struct std::tm t;
		gmtime_r(&now, &t);
		std::time_t utc = std::mktime(&t);
		return now - utc;
	#else
		tzset();
		return -timezone;
	#endif
	}
	
	const char* name(bool dst)
	{
		CHDBPoco::FastMutex::ScopedLock lock(_mutex);

		tzset();		
		return tzname[dst ? 1 : 0];
	}
		
private:
	CHDBPoco::FastMutex _mutex;
};


static TZInfo tzInfo;


int Timezone::utcOffset()
{
	return tzInfo.timeZone();
}

	
int Timezone::dst()
{
	std::time_t now = std::time(NULL);
	struct std::tm t;
	if (!localtime_r(&now, &t))
		throw CHDBPoco::SystemException("cannot get local time DST offset");
	return t.tm_isdst == 1 ? 3600 : 0;
}


bool Timezone::isDst(const Timestamp& timestamp)
{
	std::time_t time = timestamp.epochTime();
	struct std::tm* tms = std::localtime(&time);
	if (!tms) throw CHDBPoco::SystemException("cannot get local time DST flag");
	return tms->tm_isdst > 0;
}

	
std::string Timezone::name()
{
	return std::string(tzInfo.name(dst() != 0));
}

	
std::string Timezone::standardName()
{
	return std::string(tzInfo.name(false));
}

	
std::string Timezone::dstName()
{
	return std::string(tzInfo.name(true));
}


} // namespace CHDBPoco
