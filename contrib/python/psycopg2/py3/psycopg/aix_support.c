/* aix_support.c - emulate functions missing on AIX
 *
 * Copyright (C) 2017 My Karlsson <mk@acc.umu.se>
 * Copyright (c) 2018, Joyent, Inc.
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
#include "psycopg/aix_support.h"

#if defined(_AIX)
/* timeradd is missing on AIX */
#ifndef timeradd
void
timeradd(struct timeval *a, struct timeval *b, struct timeval *c)
{
    c->tv_sec = a->tv_sec + b->tv_sec;
    c->tv_usec = a->tv_usec + b->tv_usec;
    if (c->tv_usec >= 1000000) {
        c->tv_usec -= 1000000;
        c->tv_sec += 1;
    }
}

/* timersub is missing on AIX */
void
timersub(struct timeval *a, struct timeval *b, struct timeval *c)
{
    c->tv_sec = a->tv_sec - b->tv_sec;
    c->tv_usec = a->tv_usec - b->tv_usec;
    if (c->tv_usec < 0) {
        c->tv_usec += 1000000;
        c->tv_sec -= 1;
    }
}
#endif /* timeradd */
#endif /* defined(_AIX)*/
