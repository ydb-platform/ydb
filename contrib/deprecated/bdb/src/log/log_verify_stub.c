/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#ifndef HAVE_VERIFY

#include "db_config.h"
#include "db_int.h"

static int __db_log_novrfy __P((ENV *));
int __log_verify_pp __P((DB_ENV *, const DB_LOG_VERIFY_CONFIG *));
int __log_verify __P((DB_ENV *, const DB_LOG_VERIFY_CONFIG *));
int __log_verify_wrap __P((ENV *env, const char *, u_int32_t, const char *,
    const char *, time_t, time_t, u_int32_t, u_int32_t, u_int32_t, u_int32_t,
    int, int));

/*
 * __db_log_novrfy --
 *	Error when a Berkeley DB build doesn't include the access method.
 */
static int
__db_log_novrfy(env)
	ENV *env;
{
	__db_errx(env, DB_STR("2523",
	    "library build did not include support for log verification"));
	return (DB_OPNOTSUP);
}

int
__log_verify_pp(dbenv, lvconfig)
	DB_ENV *dbenv;
	const DB_LOG_VERIFY_CONFIG *lvconfig;
{
	COMPQUIET(lvconfig, NULL);

	/* The dbenv is intact, callers should properly take care of it. */
	return (__db_log_novrfy(dbenv->env));
}

int
__log_verify(dbenv, lvconfig)
	DB_ENV *dbenv;
	const DB_LOG_VERIFY_CONFIG *lvconfig;
{
	COMPQUIET(lvconfig, NULL);

	return (__db_log_novrfy(dbenv->env));
}

int
__log_verify_wrap(env, envhome, cachesize, dbfile, dbname,
    stime, etime, stfile, stoffset, efile, eoffset, caf, verbose)
	ENV *env;
	const char *envhome, *dbfile, *dbname;
	time_t stime, etime;
	u_int32_t cachesize, stfile, stoffset, efile, eoffset;
	int caf, verbose;
{
	COMPQUIET(envhome, NULL);
	COMPQUIET(dbfile, NULL);
	COMPQUIET(dbname, NULL);
	COMPQUIET(stime, 0);
	COMPQUIET(etime, 0);
	COMPQUIET(cachesize, 0);
	COMPQUIET(stfile, 0);
	COMPQUIET(stoffset, 0);
	COMPQUIET(efile, 0);
	COMPQUIET(eoffset, 0);
	COMPQUIET(caf, 0);
	COMPQUIET(verbose, 0);
	return (__db_log_novrfy(env));
}

#endif /* !HAVE_VERIFY */
