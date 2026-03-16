/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#ifndef HAVE_REPLICATION_THREADS
#include "db_config.h"

#include "db_int.h"

/*
 * If the library wasn't compiled with replication support, various routines
 * aren't available.  Stub them here, returning an appropriate error.
 */
static int __db_norepmgr __P((DB_ENV *));

/*
 * __db_norepmgr --
 *	Error when a Berkeley DB build doesn't include replication mgr support.
 */
static int
__db_norepmgr(dbenv)
	DB_ENV *dbenv;
{
	__db_errx(dbenv->env, DB_STR("3628",
    "library build did not include support for the Replication Manager"));
	return (DB_OPNOTSUP);
}

/*
 * PUBLIC: #ifndef HAVE_REPLICATION_THREADS
 * PUBLIC: int __repmgr_close __P((ENV *));
 * PUBLIC: #endif
 */
int
__repmgr_close(env)
	ENV *env;
{
	COMPQUIET(env, NULL);
	return (0);
}

/*
 * PUBLIC: #ifndef HAVE_REPLICATION_THREADS
 * PUBLIC: int __repmgr_get_ack_policy __P((DB_ENV *, int *));
 * PUBLIC: #endif
 */
int
__repmgr_get_ack_policy(dbenv, policy)
	DB_ENV *dbenv;
	int *policy;
{
	COMPQUIET(policy, NULL);
	return (__db_norepmgr(dbenv));
}

/*
 * PUBLIC: #ifndef HAVE_REPLICATION_THREADS
 * PUBLIC: int __repmgr_set_ack_policy __P((DB_ENV *, int));
 * PUBLIC: #endif
 */
int
__repmgr_set_ack_policy(dbenv, policy)
	DB_ENV *dbenv;
	int policy;
{
	COMPQUIET(policy, 0);
	return (__db_norepmgr(dbenv));
}

/*
 * PUBLIC: #ifndef HAVE_REPLICATION_THREADS
 * PUBLIC: int __repmgr_site
 * PUBLIC:     __P((DB_ENV *, const char *, u_int, DB_SITE **, u_int32_t));
 * PUBLIC: #endif
 */
int
__repmgr_site(dbenv, host, port, dbsitep, flags)
	DB_ENV *dbenv;
	const char *host;
	u_int port;
	DB_SITE **dbsitep;
	u_int32_t flags;
{
	COMPQUIET(host, NULL);
	COMPQUIET(port, 0);
	COMPQUIET(dbsitep, NULL);
	COMPQUIET(flags, 0);
	return (__db_norepmgr(dbenv));
}

/*
 * PUBLIC: #ifndef HAVE_REPLICATION_THREADS
 * PUBLIC: int __repmgr_site_by_eid __P((DB_ENV *, int, DB_SITE **));
 * PUBLIC: #endif
 */
int
__repmgr_site_by_eid(dbenv, eid, dbsitep)
	DB_ENV *dbenv;
	int eid;
	DB_SITE **dbsitep;
{
	COMPQUIET(eid, 0);
	COMPQUIET(dbsitep, NULL);
	return (__db_norepmgr(dbenv));
}

/*
 * PUBLIC: #ifndef HAVE_REPLICATION_THREADS
 * PUBLIC: int __repmgr_local_site
 * PUBLIC:     __P((DB_ENV *, DB_SITE **));
 * PUBLIC: #endif
 */
int
__repmgr_local_site(dbenv, dbsitep)
	DB_ENV *dbenv;
	DB_SITE **dbsitep;
{
	COMPQUIET(dbsitep, NULL);
	return (__db_norepmgr(dbenv));
}

/*
 * PUBLIC: #ifndef HAVE_REPLICATION_THREADS
 * PUBLIC: int __repmgr_site_list __P((DB_ENV *, u_int *, DB_REPMGR_SITE **));
 * PUBLIC: #endif
 */
int
__repmgr_site_list(dbenv, countp, listp)
	DB_ENV *dbenv;
	u_int *countp;
	DB_REPMGR_SITE **listp;
{
	COMPQUIET(countp, NULL);
	COMPQUIET(listp, NULL);
	return (__db_norepmgr(dbenv));
}

/*
 * PUBLIC: #ifndef HAVE_REPLICATION_THREADS
 * PUBLIC: int __repmgr_start __P((DB_ENV *, int, u_int32_t));
 * PUBLIC: #endif
 */
int
__repmgr_start(dbenv, nthreads, flags)
	DB_ENV *dbenv;
	int nthreads;
	u_int32_t flags;
{
	COMPQUIET(nthreads, 0);
	COMPQUIET(flags, 0);
	return (__db_norepmgr(dbenv));
}

/*
 * PUBLIC: #ifndef HAVE_REPLICATION_THREADS
 * PUBLIC: int __repmgr_stat_pp __P((DB_ENV *, DB_REPMGR_STAT **, u_int32_t));
 * PUBLIC: #endif
 */
int
__repmgr_stat_pp(dbenv, statp, flags)
	DB_ENV *dbenv;
	DB_REPMGR_STAT **statp;
	u_int32_t flags;
{
	COMPQUIET(statp, NULL);
	COMPQUIET(flags, 0);
	return (__db_norepmgr(dbenv));
}

/*
 * PUBLIC: #ifndef HAVE_REPLICATION_THREADS
 * PUBLIC: int __repmgr_stat_print_pp __P((DB_ENV *, u_int32_t));
 * PUBLIC: #endif
 */
int
__repmgr_stat_print_pp(dbenv, flags)
	DB_ENV *dbenv;
	u_int32_t flags;
{
	COMPQUIET(flags, 0);
	return (__db_norepmgr(dbenv));
}

/*
 * PUBLIC: #ifndef HAVE_REPLICATION_THREADS
 * PUBLIC: int __repmgr_handle_event __P((ENV *, u_int32_t, void *));
 * PUBLIC: #endif
 */
int
__repmgr_handle_event(env, event, info)
	ENV *env;
	u_int32_t event;
	void *info;
{
	COMPQUIET(env, NULL);
	COMPQUIET(event, 0);
	COMPQUIET(info, NULL);

	/*
	 * It's not an error for this function to be called.  Replication calls
	 * this to let repmgr handle events.  If repmgr isn't part of the build,
	 * all replication events should be forwarded to the application.
	 */
	return (DB_EVENT_NOT_HANDLED);
}

/*
 * PUBLIC: #ifndef HAVE_REPLICATION_THREADS
 * PUBLIC: int __repmgr_channel __P((DB_ENV *, int, DB_CHANNEL **, u_int32_t));
 * PUBLIC: #endif
 */
int
__repmgr_channel(dbenv, eid, dbchannelp, flags)
	DB_ENV *dbenv;
	int eid;
	DB_CHANNEL **dbchannelp;
	u_int32_t flags;
{
	COMPQUIET(eid, 0);
	COMPQUIET(dbchannelp, NULL);
	COMPQUIET(flags, 0);
	return (__db_norepmgr(dbenv));
}

/*
 * PUBLIC: #ifndef HAVE_REPLICATION_THREADS
 * PUBLIC: int __repmgr_set_msg_dispatch __P((DB_ENV *,
 * PUBLIC:     void (*)(DB_ENV *, DB_CHANNEL *, DBT *, u_int32_t, u_int32_t),
 * PUBLIC:     u_int32_t));
 * PUBLIC: #endif
 */
int
__repmgr_set_msg_dispatch(dbenv, dispatch, flags)
	DB_ENV *dbenv;
	void (*dispatch) __P((DB_ENV *,
		DB_CHANNEL *, DBT *, u_int32_t, u_int32_t));
	u_int32_t flags;
{
	COMPQUIET(dispatch, NULL);
	COMPQUIET(flags, 0);
	return (__db_norepmgr(dbenv));
}

/*
 * PUBLIC: #ifndef HAVE_REPLICATION_THREADS
 * PUBLIC: int __repmgr_init_recover __P((ENV *, DB_DISTAB *));
 * PUBLIC: #endif
 */
int
__repmgr_init_recover(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	COMPQUIET(env, NULL);
	COMPQUIET(dtabp, NULL);
	return (0);
}
#endif /* !HAVE_REPLICATION_THREADS */
