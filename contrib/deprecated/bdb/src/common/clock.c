/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"
/*
 * __clock_set_expires --
 *	Set the expire time given the time to live.
 *
 * PUBLIC: void __clock_set_expires __P((ENV *, db_timespec *, db_timeout_t));
 */
void
__clock_set_expires(env, timespecp, timeout)
	ENV *env;
	db_timespec *timespecp;
	db_timeout_t timeout;
{
	db_timespec v;

	/*
	 * If timespecp is set then it contains "now".  This avoids repeated
	 * system calls to get the time.
	 */
	if (!timespecisset(timespecp))
		__os_gettime(env, timespecp, 1);

	/* Convert the microsecond timeout argument to a timespec. */
	DB_TIMEOUT_TO_TIMESPEC(timeout, &v);

	/* Add the timeout to "now". */
	timespecadd(timespecp, &v);
}

/*
 * __clock_expired -- determine if a timeout has expired.
 *
 * PUBLIC: int __clock_expired __P((ENV *, db_timespec *, db_timespec *));
 */
int
__clock_expired(env, now, timespecp)
	ENV *env;
	db_timespec *now, *timespecp;
{
	if (!timespecisset(timespecp))
		return (0);

	if (!timespecisset(now))
		__os_gettime(env, now, 1);

	return (timespeccmp(now, timespecp, >=));
}
