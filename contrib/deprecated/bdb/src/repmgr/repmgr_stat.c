/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2005, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"

#ifdef HAVE_STATISTICS
static int __repmgr_print_all __P((ENV *, u_int32_t));
static int __repmgr_print_sites __P((ENV *));
static int __repmgr_print_stats __P((ENV *, u_int32_t));
static int __repmgr_stat __P((ENV *, DB_REPMGR_STAT **, u_int32_t));

/*
 * __repmgr_stat_pp --
 *	DB_ENV->repmgr_stat pre/post processing.
 *
 * PUBLIC: int __repmgr_stat_pp __P((DB_ENV *, DB_REPMGR_STAT **, u_int32_t));
 */
int
__repmgr_stat_pp(dbenv, statp, flags)
	DB_ENV *dbenv;
	DB_REPMGR_STAT **statp;
	u_int32_t flags;
{
	ENV *env;
	int ret;

	env = dbenv->env;

	ENV_REQUIRES_CONFIG_XX(
	    env, rep_handle, "DB_ENV->repmgr_stat", DB_INIT_REP);

	if ((ret = __db_fchk(env,
	    "DB_ENV->repmgr_stat", flags, DB_STAT_CLEAR)) != 0)
		return (ret);

	return (__repmgr_stat(env, statp, flags));
}

/*
 * __repmgr_stat --
 *	ENV->repmgr_stat.
 */
static int
__repmgr_stat(env, statp, flags)
	ENV *env;
	DB_REPMGR_STAT **statp;
	u_int32_t flags;
{
	DB_REP *db_rep;
	DB_REPMGR_STAT *copy, *stats;
	uintmax_t tmp;
	int ret;

	db_rep = env->rep_handle;
	stats = &db_rep->region->mstat;

	*statp = NULL;

	/* Allocate a stat struct to return to the user. */
	if ((ret = __os_umalloc(env, sizeof(DB_REPMGR_STAT), &copy)) != 0)
		return (ret);

	memcpy(copy, stats, sizeof(*stats));
	if (LF_ISSET(DB_STAT_CLEAR)) {
		tmp = stats->st_max_elect_threads;
		memset(stats, 0, sizeof(DB_REPMGR_STAT));
		stats->st_max_elect_threads = tmp;
	}

	*statp = copy;
	return (0);
}

/*
 * __repmgr_stat_print_pp --
 *	DB_ENV->repmgr_stat_print pre/post processing.
 *
 * PUBLIC: int __repmgr_stat_print_pp __P((DB_ENV *, u_int32_t));
 */
int
__repmgr_stat_print_pp(dbenv, flags)
	DB_ENV *dbenv;
	u_int32_t flags;
{
	ENV *env;
	int ret;

	env = dbenv->env;

	ENV_REQUIRES_CONFIG_XX(
	    env, rep_handle, "DB_ENV->repmgr_stat_print", DB_INIT_REP);

	if ((ret = __db_fchk(env, "DB_ENV->repmgr_stat_print",
	    flags, DB_STAT_ALL | DB_STAT_CLEAR)) != 0)
		return (ret);

	return (__repmgr_stat_print(env, flags));
}

/*
 * PUBLIC: int __repmgr_stat_print __P((ENV *, u_int32_t));
 */
int
__repmgr_stat_print(env, flags)
	ENV *env;
	u_int32_t flags;
{
	u_int32_t orig_flags;
	int ret;

	orig_flags = flags;
	LF_CLR(DB_STAT_CLEAR | DB_STAT_SUBSYSTEM);
	if (flags == 0 || LF_ISSET(DB_STAT_ALL)) {
		if ((ret = __repmgr_print_stats(env, orig_flags)) == 0)
			ret = __repmgr_print_sites(env);
		if (flags == 0 || ret != 0)
			return (ret);
	}

	if (LF_ISSET(DB_STAT_ALL) &&
	    (ret = __repmgr_print_all(env, orig_flags)) != 0)
		return (ret);

	return (0);
}

static int
__repmgr_print_stats(env, flags)
	ENV *env;
	u_int32_t flags;
{
	DB_REPMGR_STAT *sp;
	int ret;

	if ((ret = __repmgr_stat(env, &sp, flags)) != 0)
		return (ret);

	__db_dl(env, "Number of PERM messages not acknowledged",
	    (u_long)sp->st_perm_failed);
	__db_dl(env, "Number of messages queued due to network delay",
	    (u_long)sp->st_msgs_queued);
	__db_dl(env, "Number of messages discarded due to queue length",
	    (u_long)sp->st_msgs_dropped);
	__db_dl(env, "Number of existing connections dropped",
	    (u_long)sp->st_connection_drop);
	__db_dl(env, "Number of failed new connection attempts",
	    (u_long)sp->st_connect_fail);
	__db_dl(env, "Number of currently active election threads",
	    (u_long)sp->st_elect_threads);
	__db_dl(env, "Election threads for which space is reserved",
	    (u_long)sp->st_max_elect_threads);

	__os_ufree(env, sp);

	return (0);
}

static int
__repmgr_print_sites(env)
	ENV *env;
{
	DB_REPMGR_SITE *list;
	DB_MSGBUF mb;
	u_int count, i;
	int ret;

	if ((ret = __repmgr_site_list(env->dbenv, &count, &list)) != 0)
		return (ret);

	if (count == 0)
		return (0);

	__db_msg(env, "%s", DB_GLOBAL(db_line));
	__db_msg(env, "DB_REPMGR site information:");

	DB_MSGBUF_INIT(&mb);
	for (i = 0; i < count; ++i) {
		__db_msgadd(env, &mb, "%s (eid: %d, port: %u",
		    list[i].host, list[i].eid, list[i].port);
		if (list[i].status != 0)
			__db_msgadd(env, &mb, ", %sconnected",
			    list[i].status == DB_REPMGR_CONNECTED ? "" : "dis");
		__db_msgadd(env, &mb, ", %speer",
		    F_ISSET(&list[i], DB_REPMGR_ISPEER) ? "" : "non-");
		__db_msgadd(env, &mb, ")");
		DB_MSGBUF_FLUSH(env, &mb);
	}

	__os_ufree(env, list);

	return (0);
}

/*
 * __repmgr_print_all --
 *	Display debugging replication manager statistics.
 */
static int
__repmgr_print_all(env, flags)
	ENV *env;
	u_int32_t flags;
{
	COMPQUIET(env, NULL);
	COMPQUIET(flags, 0);
	return (0);
}

#else /* !HAVE_STATISTICS */

int
__repmgr_stat_pp(dbenv, statp, flags)
	DB_ENV *dbenv;
	DB_REPMGR_STAT **statp;
	u_int32_t flags;
{
	COMPQUIET(statp, NULL);
	COMPQUIET(flags, 0);

	return (__db_stat_not_built(dbenv->env));
}

int
__repmgr_stat_print_pp(dbenv, flags)
	DB_ENV *dbenv;
	u_int32_t flags;
{
	COMPQUIET(flags, 0);

	return (__db_stat_not_built(dbenv->env));
}
#endif

/*
 * PUBLIC: int __repmgr_site_list __P((DB_ENV *, u_int *, DB_REPMGR_SITE **));
 */
int
__repmgr_site_list(dbenv, countp, listp)
	DB_ENV *dbenv;
	u_int *countp;
	DB_REPMGR_SITE **listp;
{
	DB_REP *db_rep;
	REP *rep;
	DB_REPMGR_SITE *status;
	ENV *env;
	DB_THREAD_INFO *ip;
	REPMGR_SITE *site;
	size_t array_size, total_size;
	int eid, locked, ret;
	u_int count, i;
	char *name;

	env = dbenv->env;
	db_rep = env->rep_handle;
	ret = 0;

	ENV_NOT_CONFIGURED(
	    env, db_rep->region, "DB_ENV->repmgr_site_list", DB_INIT_REP);

	if (REP_ON(env)) {
		rep = db_rep->region;
		LOCK_MUTEX(db_rep->mutex);
		locked = TRUE;

		ENV_ENTER(env, ip);
		if (rep->siteinfo_seq > db_rep->siteinfo_seq)
			ret = __repmgr_sync_siteaddr(env);
		ENV_LEAVE(env, ip);
		if (ret != 0)
			goto err;
	} else {
		rep = NULL;
		locked = FALSE;
	}

	/* Initialize for empty list or error return. */
	*countp = 0;
	*listp = NULL;

	/*
	 * First, add up how much memory we need for the host names, excluding
	 * the local site.
	 */
	for (i = 0, count = 0, total_size = 0; i < db_rep->site_cnt; i++) {
		site = &db_rep->sites[i];

		if ((int)i == db_rep->self_eid || site->membership == 0)
			continue;

		/* Make room for the NUL terminating byte. */
		total_size += strlen(site->net_addr.host) + 1;
		count++;
	}
	if (count == 0)
		goto err;
	array_size = sizeof(DB_REPMGR_SITE) * count;
	total_size += array_size;

	if ((ret = __os_umalloc(env, total_size, &status)) != 0)
		goto err;

	/*
	 * Put the storage for the host names after the array of structs.  This
	 * way, the caller can free the whole thing in one single operation.
	 */
	name = (char *)((u_int8_t *)status + array_size);
	for (eid = 0, i = 0; eid < (int)db_rep->site_cnt; eid++) {
		site = &db_rep->sites[eid];
		if (eid == db_rep->self_eid || site->membership == 0)
			continue;

		/* If we don't have rep, we can't really know EID yet. */
		status[i].eid = rep ? eid : DB_EID_INVALID;

		status[i].host = name;
		(void)strcpy(name, site->net_addr.host);
		name += strlen(name) + 1;

		status[i].port = site->net_addr.port;

		status[i].flags = 0;

		if (FLD_ISSET(site->config, DB_REPMGR_PEER))
			F_SET(&status[i], DB_REPMGR_ISPEER);

		/*
		 * If we haven't started a communications thread, connection
		 * status is kind of meaningless.  This distinction is useful
		 * for calls from the db_stat utility: it could be useful for
		 * db_stat to display known sites with EID; but would be
		 * confusing for it to display "disconnected" if another process
		 * does indeed have a connection established (db_stat can't know
		 * that).
		 */
		if (db_rep->selector == NULL)
			status[i].status = 0;
		else if (site->state != SITE_CONNECTED)
			status[i].status = DB_REPMGR_DISCONNECTED;
		else if ((site->ref.conn.in != NULL &&
		    IS_READY_STATE(site->ref.conn.in->state)) ||
		    (site->ref.conn.out != NULL &&
		    IS_READY_STATE(site->ref.conn.out->state)))
			status[i].status = DB_REPMGR_CONNECTED;
		else
			status[i].status = DB_REPMGR_DISCONNECTED;

		i++;
	}

	*countp = count;
	*listp = status;

err:	if (locked)
		UNLOCK_MUTEX(db_rep->mutex);
	return (ret);
}
