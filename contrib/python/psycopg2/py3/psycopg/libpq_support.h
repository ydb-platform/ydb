/* libpq_support.h - definitions for libpq_support.c
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
#ifndef PSYCOPG_LIBPQ_SUPPORT_H
#define PSYCOPG_LIBPQ_SUPPORT_H 1

#include "psycopg/config.h"

/* type and constant definitions from internal postgres include */
typedef uint64_t XLogRecPtr;

/* have to use lowercase %x, as PyString_FromFormat can't do %X */
#define XLOGFMTSTR "%x/%x"
#define XLOGFMTARGS(x) ((uint32_t)((x) >> 32)), ((uint32_t)((x) & 0xFFFFFFFF))

/* Julian-date equivalents of Day 0 in Unix and Postgres reckoning */
#define UNIX_EPOCH_JDATE	2440588 /* == date2j(1970, 1, 1) */
#define POSTGRES_EPOCH_JDATE	2451545 /* == date2j(2000, 1, 1) */

#define SECS_PER_DAY	86400
#define USECS_PER_SEC	1000000LL

HIDDEN int64_t feGetCurrentTimestamp(void);
HIDDEN void fe_sendint64(int64_t i, char *buf);
HIDDEN int64_t fe_recvint64(char *buf);

#endif /* !defined(PSYCOPG_LIBPQ_SUPPORT_H) */
