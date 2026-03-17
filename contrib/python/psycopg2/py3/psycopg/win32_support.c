/* win32_support.c - emulate some functions missing on Win32
 *
 * Copyright (C) 2003-2019 Federico Di Gregorio <fog@debian.org>
 * Copyright (C) 2020-2021 The Psycopg Team
 *
 * This file is part of psycopg.
 *
 * psycopg2 is free software: you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * In addition, as a special exception, the copyright holders give
 * permission to link this program with the OpenSSL library (or with
 * modified versions of OpenSSL that use the same license as OpenSSL),
 * and distribute linked combinations including the two.
 *
 * You must obey the GNU Lesser General Public License in all respects for
 * all of the code used other than OpenSSL.
 *
 * psycopg2 is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 */

#define PSYCOPG_MODULE
#include "psycopg/psycopg.h"

#include "psycopg/win32_support.h"

#ifdef _WIN32

#ifndef __MINGW32__
/* millisecond-precision port of gettimeofday for Win32, taken from
   src/port/gettimeofday.c in PostgreSQL core */

/* FILETIME of Jan 1 1970 00:00:00. */
static const unsigned __int64 epoch = ((unsigned __int64) 116444736000000000ULL);

/*
 * timezone information is stored outside the kernel so tzp isn't used anymore.
 *
 * Note: this function is not for Win32 high precision timing purpose. See
 * elapsed_time().
 */
int
gettimeofday(struct timeval * tp, void * tzp)
{
    FILETIME	file_time;
    SYSTEMTIME	system_time;
    ULARGE_INTEGER ularge;

    GetSystemTime(&system_time);
    SystemTimeToFileTime(&system_time, &file_time);
    ularge.LowPart = file_time.dwLowDateTime;
    ularge.HighPart = file_time.dwHighDateTime;

    tp->tv_sec = (long) ((ularge.QuadPart - epoch) / 10000000L);
    tp->tv_usec = (long) (system_time.wMilliseconds * 1000);

    return 0;
}

/* timeradd missing on MS VC */
void
timeradd(struct timeval *a, struct timeval *b, struct timeval *c)
{
  c->tv_sec = a->tv_sec + b->tv_sec;
  c->tv_usec = a->tv_usec + b->tv_usec;
  if(c->tv_usec >= 1000000L) {
    c->tv_usec -= 1000000L;
    c->tv_sec += 1;
  }
}
#endif /* !defined(__MINGW32__) */

/* timersub is missing on mingw & MS VC */
void
timersub(struct timeval *a, struct timeval *b, struct timeval *c)
{
    c->tv_sec  = a->tv_sec  - b->tv_sec;
    c->tv_usec = a->tv_usec - b->tv_usec;
    if (c->tv_usec < 0) {
        c->tv_usec += 1000000;
        c->tv_sec  -= 1;
    }
}

#endif /* defined(_WIN32) */
