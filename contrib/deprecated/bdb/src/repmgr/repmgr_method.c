/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2005, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/btree.h"
#include "dbinc/txn.h"

/* Context for an API thread waiting for response to a synchronous request. */
struct response_wait {
	REPMGR_CONNECTION *conn;
	u_int32_t index;
};

static int addr_chk __P((const ENV *, const char *, u_int));
static void adjust_bulk_response __P((ENV *, DBT *));
static int bad_callback_method __P((DB_CHANNEL *, const char *));
static void copy_body __P((u_int8_t *, REPMGR_IOVECS *));
static int get_shared_netaddr __P((ENV *, int, repmgr_netaddr_t *));
static int establish_connection __P((ENV *, int, REPMGR_CONNECTION **));
static int get_channel_connection __P((CHANNEL *, REPMGR_CONNECTION **));
static int init_dbsite __P((ENV *, int, const char *, u_int, DB_SITE **));
static int join_group_at_site __P((ENV *, repmgr_netaddr_t *));
static int kick_blockers __P((ENV *, REPMGR_CONNECTION *, void *));
static int make_request_conn __P((ENV *,
    repmgr_netaddr_t *, REPMGR_CONNECTION **));
static int set_local_site __P((DB_SITE *, u_int32_t));
static int read_own_msg __P((ENV *,
    REPMGR_CONNECTION *, u_int32_t *, u_int8_t **, size_t *));
static int refresh_site __P((DB_SITE *));
static int __repmgr_await_threads __P((ENV *));
static int __repmgr_build_data_out __P((ENV *,
    DBT *, u_int32_t, __repmgr_msg_metadata_args *, REPMGR_IOVECS **iovecsp));
static int __repmgr_build_msg_out __P((ENV *,
    DBT *, u_int32_t, __repmgr_msg_metadata_args *, REPMGR_IOVECS **iovecsp));
static int repmgr_only __P((ENV *, const char *));
static int __repmgr_restart __P((ENV *, int, u_int32_t));
static int __repmgr_remove_site __P((DB_SITE *));
static int __repmgr_remove_site_pp __P((DB_SITE *));
static int __repmgr_start_msg_threads __P((ENV *, u_int));
static int request_self __P((ENV *, DBT *, u_int32_t, DBT *, u_int32_t));
static int response_complete __P((ENV *, void *));
static int send_msg_conn __P((ENV *, REPMGR_CONNECTION *, DBT *, u_int32_t));
static int send_msg_self __P((ENV *, REPMGR_IOVECS *, u_int32_t));
static int site_by_addr __P((ENV *, const char *, u_int, DB_SITE **));

/*
 * PUBLIC: int __repmgr_start __P((DB_ENV *, int, u_int32_t));
 */
int
__repmgr_start(dbenv, nthreads, flags)
	DB_ENV *dbenv;
	int nthreads;
	u_int32_t flags;
{
	DB_REP *db_rep;
	REP *rep;
	REPMGR_SITE *me, *site;
	DB_THREAD_INFO *ip;
	ENV *env;
	int first, is_listener, locked, min, need_masterseek, ret, start_master;
	u_int i, n;

	env = dbenv->env;
	db_rep = env->rep_handle;
	rep = db_rep->region;

	switch (flags) {
	case 0:
	case DB_REP_CLIENT:
	case DB_REP_ELECTION:
	case DB_REP_MASTER:
		break;
	default:
		__db_errx(env, DB_STR("3635",
		    "repmgr_start: unrecognized flags parameter value"));
		return (EINVAL);
	}

	ENV_REQUIRES_CONFIG_XX(
	    env, rep_handle, "DB_ENV->repmgr_start", DB_INIT_REP);
	if (!F_ISSET(env, ENV_THREAD)) {
		__db_errx(env, DB_STR("3636",
	    "Replication Manager needs an environment with DB_THREAD"));
		return (EINVAL);
	}

	if (APP_IS_BASEAPI(env))
		return (repmgr_only(env, "repmgr_start"));

	/* Check that the required initialization has been done. */
	if (!IS_VALID_EID(db_rep->self_eid)) {
		__db_errx(env, DB_STR("3637",
		    "A local site must be named before calling repmgr_start"));
		return (EINVAL);
	}

	/* Check if it is a shut-down site, if so, clean the resources. */
	if (db_rep->repmgr_status == stopped) {
		if ((ret = __repmgr_stop(env)) != 0) {
			__db_errx(env, DB_STR("3638",
			    "Could not clean up repmgr"));
			return (ret);
		}
		db_rep->repmgr_status = ready;
	}

	db_rep->init_policy = flags;
	if ((ret = __rep_set_transport_int(env,
	    db_rep->self_eid, __repmgr_send)) != 0)
		return (ret);
	if (!REPMGR_INITED(db_rep) && (ret = __repmgr_init(env)) != 0)
		return (ret);
	/*
	 * As a prerequisite to starting replication, get our list of remote
	 * sites properly set up.  Mainly this involves reading the group
	 * membership database; but alternatively, deciding what to do when it's
	 * not present (which depends on various conditions).
	 */
	start_master = (flags == DB_REP_MASTER);

	if (db_rep->restored_list != NULL) {
		ret = __repmgr_refresh_membership(env,
		    db_rep->restored_list, db_rep->restored_list_length);
		__os_free(env, db_rep->restored_list);
		db_rep->restored_list = NULL;
	} else {
		ret = __repmgr_reload_gmdb(env);
		me = SITE_FROM_EID(db_rep->self_eid);
		if (ret == 0) {
			if (me->membership != SITE_PRESENT)
				/*
				 * We have a database but the local site is not
				 * shown as "present" in the group.  We must
				 * have been removed from the group, or perhaps
				 * we're being created via hot backup.  In
				 * either case the thing to do is to try to
				 * join.
				 */
				ret = __repmgr_join_group(env);
		} else if (ret == ENOENT) {
			ENV_ENTER(env, ip);
			if (FLD_ISSET(me->config, DB_GROUP_CREATOR))
				start_master = TRUE;
			/*
			 * LEGACY is inconsistent with CREATOR, but start_master
			 * could still be true due to "flags" being passed as
			 * DB_REP_MASTER.  In that case, being started as master
			 * is irrelevant to establishing initial membership
			 * list: LEGACY always takes precedence if set.
			 */
			if (FLD_ISSET(me->config, DB_LEGACY)) {
				LOCK_MUTEX(db_rep->mutex);
				db_rep->membership_version = 1;
				db_rep->member_version_gen = 1;
				for (n = i = 0; i < db_rep->site_cnt; i++) {
					site = SITE_FROM_EID(i);
					if (!FLD_ISSET(site->config, DB_LEGACY))
						continue;
					if ((ret = __repmgr_set_membership(env,
					    site->net_addr.host,
					    site->net_addr.port,
					    SITE_PRESENT)) != 0)
						break;
					n++;
				}
				ret = __rep_set_nsites_int(env, n);
				DB_ASSERT(env, ret == 0);
				UNLOCK_MUTEX(db_rep->mutex);
			} else if (start_master) {
				LOCK_MUTEX(db_rep->mutex);
				db_rep->membership_version = 1;
				db_rep->member_version_gen = 1;
				if ((ret = __repmgr_set_membership(env,
				    me->net_addr.host, me->net_addr.port,
				    SITE_PRESENT)) == 0) {
					ret = __rep_set_nsites_int(env, 1);
					DB_ASSERT(env, ret == 0);
				}
				UNLOCK_MUTEX(db_rep->mutex);
			} else
				ret = __repmgr_join_group(env);
			ENV_LEAVE(env, ip);
		} else if (ret == DB_DELETED)
			ret = DB_REP_UNAVAIL;
	}
	if (ret != 0)
		return (ret);

	DB_ASSERT(env, start_master ||
	    SITE_FROM_EID(db_rep->self_eid)->membership == SITE_PRESENT);

	/*
	 * If we're the first repmgr_start() call, we will have to start threads.
	 * Therefore, we require a flags value (to tell us how).
	 */
	if (db_rep->repmgr_status != running && flags == 0) {
		__db_errx(env, DB_STR("3639",
	"a non-zero flags value is required for initial repmgr_start() call"));
		return (EINVAL);
	}

	/*
	 * Figure out the current situation.  The current invocation of
	 * repmgr_start() is either the first one (on the given env handle), or
	 * a subsequent one.
	 *
	 * Then, in case there could be multiple processes, we're either the
	 * main listener process or a subordinate process.  On a "subsequent"
	 * repmgr_start() call we already have enough information to know which
	 * it is.  Otherwise, negotiate with information in the shared region to
	 * claim the listener role if possible.
	 *
	 * To avoid a race, once we decide we're in the first call, mark the
	 * handle as started, so that no other thread thinks the same thing.
	 */
	LOCK_MUTEX(db_rep->mutex);
	locked = TRUE;
	if (db_rep->repmgr_status == running) {
		first = FALSE;
		is_listener = !IS_SUBORDINATE(db_rep);
	} else {
		first = TRUE;
		db_rep->repmgr_status = running;

		ENV_ENTER(env, ip);
		MUTEX_LOCK(env, rep->mtx_repmgr);
		if (rep->listener == 0) {
			is_listener = TRUE;
			__os_id(dbenv, &rep->listener, NULL);
		} else {
			is_listener = FALSE;
			nthreads = 0;
		}
		MUTEX_UNLOCK(env, rep->mtx_repmgr);
		ENV_LEAVE(env, ip);
	}
	UNLOCK_MUTEX(db_rep->mutex);
	locked = FALSE;

	if (!first) {
		/*
		 * Subsequent call is allowed when ELECTIONS are turned off, so
		 * that the application can make its own dynamic role changes.
		 * It's also allowed in any case, if not trying to change roles
		 * (flags == 0), in order to change number of message processing
		 * threads.  The __repmgr_restart() function will take care of
		 * these cases entirely.
		 */
		if (!is_listener || (flags != 0 &&
		    FLD_ISSET(db_rep->region->config, REP_C_ELECTIONS))) {
			__db_errx(env, DB_STR("3640",
			    "repmgr is already started"));
			ret = EINVAL;
		} else
			ret = __repmgr_restart(env, nthreads, flags);
		return (ret);
	}

	/*
	 * The minimum legal number of threads is either 1 or 0, depending upon
	 * whether we're the main process or a subordinate.
	 */
	min = is_listener ? 1 : 0;
	if (nthreads < min) {
		__db_errx(env, DB_STR_A("3641",
		    "repmgr_start: nthreads parameter must be >= %d",
		    "%d"), min);
		ret = EINVAL;
		goto err;
	}

	/*
	 * Ensure at least one more thread (for channel messages and GMDB
	 * requests) beyond those set aside to avoid starvation of rep
	 * messages.
	 *
	 * Note that it's OK to silently fudge the number here, because the
	 * documentation says that "[i]n addition to these message processing
	 * threads, the Replication Manager creates and manages a few of its own
	 * threads of control."
	 */
	min = RESERVED_MSG_TH(env) + 1;
	if (nthreads < min && is_listener)
		nthreads = min;

	if (is_listener) {
		if ((ret = __repmgr_listen(env)) != 0)
			goto err;
		/*
		 * Make some sort of call to rep_start before starting message
		 * processing threads, to ensure that incoming messages being
		 * processed always have a rep context properly configured.
		 * Note that even if we're starting without recovery, we need a
		 * rep_start call in case we're using leases.  Leases keep track
		 * of rep_start calls even within an env region lifetime.
		 */
		if (start_master) {
			ret = __repmgr_become_master(env);
			/* No other repmgr threads running yet. */
			DB_ASSERT(env, ret != DB_REP_UNAVAIL);
			if (ret != 0)
				goto err;
			need_masterseek = FALSE;
		} else {
			if ((ret = __repmgr_repstart(env, DB_REP_CLIENT)) != 0)
				goto err;
			/*
			 * The repmgr election code starts elections only if
			 * the DB_REP_ELECTION start flag was specified, but
			 * it performs other actions to help find a master for
			 * DB_REP_CLIENT, which is why we need_masterseek for
			 * both cases.
			 */
			need_masterseek = TRUE;
		}

		LOCK_MUTEX(db_rep->mutex);
		locked = TRUE;

		/*
		 * Since these allocated memory blocks are used by other
		 * threads, we have to be a bit careful about freeing them in
		 * case of any errors.  __repmgr_await_threads (which we call in
		 * the err: coda below) takes care of that.
		 *
		 * Start by allocating enough space for 2 election threads.  We
		 * occasionally need that many; more are possible, but would be
		 * extremely rare.
		 */
#define	ELECT_THREADS_ALLOC	2

		if ((ret = __os_calloc(env, ELECT_THREADS_ALLOC,
		    sizeof(REPMGR_RUNNABLE *), &db_rep->elect_threads)) != 0)
			goto err;
		db_rep->aelect_threads = ELECT_THREADS_ALLOC;
		STAT(rep->mstat.st_max_elect_threads = ELECT_THREADS_ALLOC);

		if ((ret = __os_calloc(env, (u_int)nthreads,
		    sizeof(REPMGR_RUNNABLE *), &db_rep->messengers)) != 0)
			goto err;
		db_rep->athreads = (u_int)nthreads;

		db_rep->nthreads = 0;
		if ((ret =
		    __repmgr_start_msg_threads(env, (u_int)nthreads)) != 0)
			goto err;

		if (need_masterseek) {
			/*
			 * The repstart_time field records that time when we
			 * last issued a rep_start(CLIENT) that sent out a
			 * NEWCLIENT message.  We use it to avoid doing so
			 * twice in quick succession (to give the master a
			 * reasonable chance to respond).  The rep_start()
			 * that we just issued above doesn't count, because we
			 * haven't established any connections yet, and so no
			 * message could have been sent out.  The instant we
			 * get our first connection set up we want to send out
			 * our first real NEWCLIENT.
			 */
			timespecclear(&db_rep->repstart_time);

			if ((ret = __repmgr_init_election(env,
			    ELECT_F_STARTUP)) != 0)
				goto err;
		}
		UNLOCK_MUTEX(db_rep->mutex);
		locked = FALSE;
	}
	/* All processes (even non-listeners) need a select() thread. */
	if ((ret = __repmgr_start_selector(env)) == 0)
		return (is_listener ? 0 : DB_REP_IGNORE);

err:
	/* If we couldn't succeed at everything, undo the parts we did do. */
	if (db_rep->selector != NULL) {
		if (!locked)
			LOCK_MUTEX(db_rep->mutex);
		(void)__repmgr_stop_threads(env);
		UNLOCK_MUTEX(db_rep->mutex);
		locked = FALSE;
	}
	(void)__repmgr_await_threads(env);
	if (!locked)
		LOCK_MUTEX(db_rep->mutex);
	(void)__repmgr_net_close(env);
	UNLOCK_MUTEX(db_rep->mutex);
	return (ret);
}

/*
 * PUBLIC: int __repmgr_valid_config __P((ENV *, u_int32_t));
 */
int
__repmgr_valid_config(env, flags)
	ENV *env;
	u_int32_t flags;
{
	DB_REP *db_rep;
	int ret;

	db_rep = env->rep_handle;
	ret = 0;

	DB_ASSERT(env, REP_ON(env));
	LOCK_MUTEX(db_rep->mutex);

	/* (Can't check IS_SUBORDINATE if select thread isn't running yet.) */
	if (LF_ISSET(REP_C_ELECTIONS) &&
	    db_rep->selector != NULL && IS_SUBORDINATE(db_rep)) {
		__db_errx(env, DB_STR("3642",
	    "can't configure repmgr elections from subordinate process"));
		ret = EINVAL;
	}
	UNLOCK_MUTEX(db_rep->mutex);
	return (ret);
}

/*
 * Starts message processing threads.  On entry, the actual number of threads
 * already active is db_rep->nthreads; the desired number of threads is passed
 * as "n".
 *
 * Caller must hold mutex.
 */
static int
__repmgr_start_msg_threads(env, n)
	ENV *env;
	u_int n;
{
	DB_REP *db_rep;
	REPMGR_RUNNABLE *messenger;
	int ret;

	db_rep = env->rep_handle;
	DB_ASSERT(env, db_rep->athreads >= n);
	while (db_rep->nthreads < n) {
		if ((ret = __os_calloc(env,
		    1, sizeof(REPMGR_RUNNABLE), &messenger)) != 0)
			return (ret);

		messenger->run = __repmgr_msg_thread;
		if ((ret = __repmgr_thread_start(env, messenger)) != 0) {
			__os_free(env, messenger);
			return (ret);
		}
		db_rep->messengers[db_rep->nthreads++] = messenger;
	}
	return (0);
}

/*
 * Handles a repmgr_start() call that occurs when repmgr is already running.
 * This is allowed (when elections are not in use), to dynamically change
 * master/client role.  It is also allowed (regardless of the ELECTIONS setting)
 * to change the number of msg processing threads.
 */
static int
__repmgr_restart(env, nthreads, flags)
	ENV *env;
	int nthreads;
	u_int32_t flags;
{
	DB_REP *db_rep;
	REP *rep;
	REPMGR_RUNNABLE **th;
	u_int32_t cur_repflags;
	int locked, ret, t_ret;
	u_int delta, i, min, nth;

	th = NULL;
	locked = FALSE;

	if (flags == DB_REP_ELECTION) {
		__db_errx(env, DB_STR("3643",
	    "subsequent repmgr_start() call may not specify DB_REP_ELECTION"));
		return (EINVAL);
	}
	if (nthreads < 0) {
		__db_errx(env, DB_STR("3644",
		    "repmgr_start: nthreads parameter must be >= 0"));
		return (EINVAL);
	}

	ret = 0;
	db_rep = env->rep_handle;
	DB_ASSERT(env, REP_ON(env));
	rep = db_rep->region;

	cur_repflags = F_ISSET(rep, REP_F_MASTER | REP_F_CLIENT);
	DB_ASSERT(env, cur_repflags);
	if (FLD_ISSET(cur_repflags, REP_F_MASTER) &&
	    flags == DB_REP_CLIENT)
		ret = __repmgr_become_client(env);
	else if (FLD_ISSET(cur_repflags, REP_F_CLIENT) &&
	    flags == DB_REP_MASTER)
		ret = __repmgr_become_master(env);
	if (ret != 0)
		return (ret);

	if (nthreads == 0)
		return (0);
	nth = (u_int)nthreads;
	LOCK_MUTEX(db_rep->mutex);
	locked = TRUE;
	min = RESERVED_MSG_TH(env) + db_rep->non_rep_th;
	if (nth < min)
		nth = min;

	if (nth > db_rep->nthreads) {
		/*
		 * To increase the number of threads, first allocate more space,
		 * unless we already have enough unused space available.
		 */
		if (db_rep->athreads < nth) {
			if ((ret = __os_realloc(env,
			    sizeof(REPMGR_RUNNABLE *) * nth,
			    &db_rep->messengers)) != 0)
				goto out;
			db_rep->athreads = nth;
		}
		ret = __repmgr_start_msg_threads(env, nth);
	} else if (nth < db_rep->nthreads) {
		/*
		 * Remove losers from array, and then wait for each of them.  We
		 * have to make an array copy, because we have to drop the mutex
		 * to wait for the threads to complete, and if we left the real
		 * array in the handle in the pending state while waiting,
		 * another thread could come along wanting to make another
		 * change, and would make a mess.
		 *     The alternative is about as inelegant: we could do these
		 * one at a time here if we added another field to the handle,
		 * to keep track of both the actual number of threads and the
		 * user's desired number of threads.
		 */
		/*
		 * Make sure signalling the condition variable works, before
		 * making a mess of the data structures.  Although it may seem a
		 * little backwards, it doesn't really matter since we're
		 * holding the mutex.  Once we allocate the temp array and grab
		 * ownership of the loser thread structs, we must continue
		 * trying (even if errors) so that we definitely free the
		 * memory.
		 */
		if ((ret = __repmgr_wake_msngers(env, nth)) != 0)
			goto out;
		delta = db_rep->nthreads - nth;
		if ((ret = __os_calloc(env, (size_t)delta,
		    sizeof(REPMGR_RUNNABLE *), &th)) != 0)
			goto out;
		for (i = 0; i < delta; i++) {
			th[i] = db_rep->messengers[nth + i];
			th[i]->quit_requested = TRUE;
			db_rep->messengers[nth + i] = NULL;
		}
		db_rep->nthreads = nth;
		UNLOCK_MUTEX(db_rep->mutex);
		locked = FALSE;

		DB_ASSERT(env, ret == 0);
		for (i = 0; i < delta; i++) {
			if ((t_ret = __repmgr_thread_join(th[i])) != 0 &&
			    ret == 0)
				ret = t_ret;
			__os_free(env, th[i]);
		}
		__os_free(env, th);
	}

out:	if (locked)
		UNLOCK_MUTEX(db_rep->mutex);
	return (ret);
}

/*
 * PUBLIC: int __repmgr_autostart __P((ENV *));
 *
 * Preconditions: rep_start() has been called; we're within an ENV_ENTER.
 */
int
__repmgr_autostart(env)
	ENV *env;
{
	DB_REP *db_rep;
	REP *rep;
	int ret;

	db_rep = env->rep_handle;
	rep = db_rep->region;

	DB_ASSERT(env, REP_ON(env));
	LOCK_MUTEX(db_rep->mutex);

	if (REPMGR_INITED(db_rep))
		ret = 0;
	else
		ret = __repmgr_init(env);
	if (ret != 0)
		goto out;

	RPRINT(env, (env, DB_VERB_REPMGR_MISC,
	    "Automatically joining existing repmgr env"));

	/*
	 * We're only called if we're a master, which means we've had a
	 * rep_start() call, which means we must have had a previous
	 * rep_set_transport() call (in the region, in a separate env handle).
	 * We could therefore get away with simply poking in a pointer to our
	 * send function; but we need to dig up our EID value anyway, so we
	 * might as well set it properly.
	 */
	db_rep->self_eid = rep->eid;
	if ((ret = __rep_set_transport_int(env,
	    db_rep->self_eid, __repmgr_send)) != 0)
		goto out;

	if (db_rep->selector == NULL && db_rep->repmgr_status != running)
		ret = __repmgr_start_selector(env);

out:
	UNLOCK_MUTEX(db_rep->mutex);
	return (ret);
}

/*
 * PUBLIC: int __repmgr_start_selector __P((ENV *));
 */
int
__repmgr_start_selector(env)
	ENV *env;
{
	DB_REP *db_rep;
	REPMGR_RUNNABLE *selector;
	int ret;

	db_rep = env->rep_handle;
	if ((ret = __os_calloc(env, 1, sizeof(REPMGR_RUNNABLE), &selector))
	    != 0)
		return (ret);
	selector->run = __repmgr_select_thread;

	/*
	 * In case the select thread ever examines db_rep->selector, set it
	 * before starting the thread (since once we create it we could be
	 * racing with it).
	 */
	db_rep->selector = selector;
	if ((ret = __repmgr_thread_start(env, selector)) != 0) {
		__db_err(env, ret, DB_STR("3645",
		    "can't start selector thread"));
		__os_free(env, selector);
		db_rep->selector = NULL;
		return (ret);
	}

	return (0);
}

/*
 * PUBLIC: int __repmgr_close __P((ENV *));
 *
 * Close repmgr during env close.  It stops repmgr, frees sites array and
 * its addresses.
 */
int
__repmgr_close(env)
	ENV *env;
{
	DB_REP *db_rep;
	REPMGR_SITE *site;
	int ret;
	u_int i;

	db_rep = env->rep_handle;
	ret = 0;

	ret = __repmgr_stop(env);
	if (db_rep->sites != NULL) {
		for (i = 0; i < db_rep->site_cnt; i++) {
			site = &db_rep->sites[i];
			DB_ASSERT(env, TAILQ_EMPTY(&site->sub_conns));
			__repmgr_cleanup_netaddr(env, &site->net_addr);
		}
		__os_free(env, db_rep->sites);
		db_rep->sites = NULL;
	}

	return (ret);
}

/*
 * PUBLIC: int __repmgr_stop __P((ENV *));
 *
 * Stop repmgr either when closing the env or removing the current repmgr from
 * replication group.  It stops threads if necessary, frees resources allocated
 * after __repmgr_start, and cleans up site membership.
 */
int
__repmgr_stop(env)
	ENV *env;
{
	DB_REP *db_rep;
	REPMGR_SITE *site;
	int ret, t_ret;
	u_int i;

	ret = 0;
	db_rep = env->rep_handle;

	if (db_rep->selector != NULL) {
		if (db_rep->repmgr_status != stopped) {
			LOCK_MUTEX(db_rep->mutex);
			ret = __repmgr_stop_threads(env);
			UNLOCK_MUTEX(db_rep->mutex);
		}
		if ((t_ret = __repmgr_await_threads(env)) != 0 && ret == 0)
			ret = t_ret;
		RPRINT(env, (env, DB_VERB_REPMGR_MISC,
		    "Repmgr threads are finished"));
	}
	__repmgr_net_destroy(env, db_rep);
	if ((t_ret = __repmgr_deinit(env)) != 0 && ret == 0)
		ret = t_ret;
	if ((t_ret = __repmgr_queue_destroy(env)) != 0 && ret == 0)
		ret = t_ret;
	if (db_rep->restored_list != NULL) {
		__os_free(env, db_rep->restored_list);
		db_rep->restored_list = NULL;
	}
	/*
	 * Clean up current site membership and state, so that the obsolete
	 * membership won't mislead us for the next repmgr start.
	 */
	for (i = 0; i < db_rep->site_cnt; i++) {
		site = &db_rep->sites[i];
		site->state = SITE_IDLE;
		site->membership = 0;
	}

	return (ret);
}

/*
 * PUBLIC: int __repmgr_set_ack_policy __P((DB_ENV *, int));
 */
int
__repmgr_set_ack_policy(dbenv, policy)
	DB_ENV *dbenv;
	int policy;
{
	DB_REP *db_rep;
	DB_THREAD_INFO *ip;
	ENV *env;
	REP *rep;
	int ret;

	env = dbenv->env;
	db_rep = env->rep_handle;
	rep = db_rep->region;

	ENV_NOT_CONFIGURED(
	    env, db_rep->region, "DB_ENV->repmgr_set_ack_policy", DB_INIT_REP);

	if (APP_IS_BASEAPI(env))
		return (repmgr_only(env, "repmgr_set_ack_policy"));

	switch (policy) {
	case DB_REPMGR_ACKS_ALL:
	case DB_REPMGR_ACKS_ALL_AVAILABLE:
	case DB_REPMGR_ACKS_ALL_PEERS:
	case DB_REPMGR_ACKS_NONE:
	case DB_REPMGR_ACKS_ONE:
	case DB_REPMGR_ACKS_ONE_PEER:
	case DB_REPMGR_ACKS_QUORUM:
		if (REP_ON(env)) {
			if (rep->perm_policy != policy) {
				rep->perm_policy = policy;
				if ((ret = __repmgr_bcast_parm_refresh(env))
				    != 0)
					return (ret);
			}
		} else
			db_rep->perm_policy = policy;
		/*
		 * Setting an ack policy makes this a replication manager
		 * application.
		 */
		APP_SET_REPMGR(env);
		return (0);
	default:
		__db_errx(env, DB_STR("3646",
		    "unknown ack_policy in DB_ENV->repmgr_set_ack_policy"));
		return (EINVAL);
	}
}

/*
 * PUBLIC: int __repmgr_get_ack_policy __P((DB_ENV *, int *));
 */
int
__repmgr_get_ack_policy(dbenv, policy)
	DB_ENV *dbenv;
	int *policy;
{
	ENV *env;
	DB_REP *db_rep;
	REP *rep;

	env = dbenv->env;
	db_rep = env->rep_handle;
	rep = db_rep->region;

	*policy = REP_ON(env) ? rep->perm_policy : db_rep->perm_policy;
	return (0);
}

/*
 * PUBLIC: int __repmgr_env_create __P((ENV *, DB_REP *));
 */
int
__repmgr_env_create(env, db_rep)
	ENV *env;
	DB_REP *db_rep;
{
	int ret;

	/* Set some default values. */
	db_rep->ack_timeout = DB_REPMGR_DEFAULT_ACK_TIMEOUT;
	db_rep->connection_retry_wait = DB_REPMGR_DEFAULT_CONNECTION_RETRY;
	db_rep->election_retry_wait = DB_REPMGR_DEFAULT_ELECTION_RETRY;
	db_rep->config_nsites = 0;
	db_rep->perm_policy = DB_REPMGR_ACKS_QUORUM;
	FLD_SET(db_rep->config, REP_C_ELECTIONS);
	FLD_SET(db_rep->config, REP_C_2SITE_STRICT);

	db_rep->self_eid = DB_EID_INVALID;
	db_rep->listen_fd = INVALID_SOCKET;
	TAILQ_INIT(&db_rep->connections);
	TAILQ_INIT(&db_rep->retries);

	db_rep->input_queue.size = 0;
	STAILQ_INIT(&db_rep->input_queue.header);

	__repmgr_env_create_pf(db_rep);
	ret = __repmgr_create_mutex(env, &db_rep->mutex);

	return (ret);
}

/*
 * PUBLIC: void __repmgr_env_destroy __P((ENV *, DB_REP *));
 */
void
__repmgr_env_destroy(env, db_rep)
	ENV *env;
	DB_REP *db_rep;
{
	if (db_rep->mutex != NULL) {
		(void)__repmgr_destroy_mutex(env, db_rep->mutex);
		db_rep->mutex = NULL;
	}
}

/*
 * PUBLIC: int __repmgr_stop_threads __P((ENV *));
 *
 * Caller must hold mutex;
 */
int
__repmgr_stop_threads(env)
	ENV *env;
{
	DB_REP *db_rep;
	int ret;

	db_rep = env->rep_handle;

	db_rep->repmgr_status = stopped;
	RPRINT(env, (env, DB_VERB_REPMGR_MISC, "Stopping repmgr threads"));
	if ((ret = __repmgr_signal(&db_rep->check_election)) != 0)
		return (ret);

	/*
	 * Because we've set "finished", it's enough to wake msg_avail, even on
	 * Windows.  (We don't need to wake per-thread Event Objects here, as we
	 * did in the case of only wanting to stop a subset of msg threads.)
	 */
	if ((ret = __repmgr_signal(&db_rep->msg_avail)) != 0)
		return (ret);

	if ((ret = __repmgr_each_connection(env,
	    kick_blockers, NULL, TRUE)) != 0)
		return (ret);

	return (__repmgr_wake_main_thread(env));
}

static int
kick_blockers(env, conn, unused)
	ENV *env;
	REPMGR_CONNECTION *conn;
	void *unused;
{
	int ret, t_ret;

	COMPQUIET(unused, NULL);

	ret = __repmgr_signal(&conn->drained);
	if ((t_ret = __repmgr_wake_waiters(env,
	    &conn->response_waiters)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);
}

/*
 * "Joins" all repmgr background threads.
 */
static int
__repmgr_await_threads(env)
	ENV *env;
{
	DB_REP *db_rep;
	REPMGR_RUNNABLE *th;
	REPMGR_SITE *site;
	int ret, t_ret;
	u_int i;

	db_rep = env->rep_handle;
	ret = 0;

	/*
	 * First wait for the threads we started explicitly.  Then wait for
	 * those "remote descendent" threads that these first threads may have
	 * started.  This order is important, because, for example, the select
	 * thread, in its last gasp, may have started yet another new instance
	 * of a connector thread.
	 */

	/* Message processing threads. */
	for (i = 0;
	    i < db_rep->nthreads && db_rep->messengers[i] != NULL; i++) {
		th = db_rep->messengers[i];
		if ((t_ret = __repmgr_thread_join(th)) != 0 && ret == 0)
			ret = t_ret;
		__os_free(env, th);
	}
	__os_free(env, db_rep->messengers);
	db_rep->messengers = NULL;

	/* The select() loop thread. */
	if (db_rep->selector != NULL) {
		if ((t_ret = __repmgr_thread_join(db_rep->selector)) != 0 &&
		    ret == 0)
			ret = t_ret;
		__os_free(env, db_rep->selector);
		db_rep->selector = NULL;
	}

	/* Election threads. */
	for (i = 0; i < db_rep->aelect_threads; i++) {
		th = db_rep->elect_threads[i];
		if (th != NULL) {
			if ((t_ret = __repmgr_thread_join(th)) != 0 && ret == 0)
				ret = t_ret;
			__os_free(env, th);
		}
	}
	__os_free(env, db_rep->elect_threads);
	db_rep->aelect_threads = 0;

	/* Threads opening outgoing socket connections. */
	FOR_EACH_REMOTE_SITE_INDEX(i) {
		LOCK_MUTEX(db_rep->mutex);
		site = SITE_FROM_EID(i);
		th = site->connector;
		site->connector = NULL;
		UNLOCK_MUTEX(db_rep->mutex);
		if (th != NULL) {
			if ((t_ret = __repmgr_thread_join(th)) != 0 && ret == 0)
				ret = t_ret;
			__os_free(env, th);
		}
	}

	return (ret);
}

/*
 * PUBLIC: int __repmgr_local_site __P((DB_ENV *, DB_SITE **));
 */
int
__repmgr_local_site(dbenv, sitep)
	DB_ENV *dbenv;
	DB_SITE **sitep;
{
	DB_REP *db_rep;
	ENV *env;

	env = dbenv->env;
	db_rep = env->rep_handle;

	if (!IS_VALID_EID(db_rep->self_eid))
		return (DB_NOTFOUND);
	return (__repmgr_site_by_eid(dbenv, db_rep->self_eid, sitep));
}

static int
addr_chk(env, host, port)
	const ENV *env;
	const char *host;
	u_int port;
{
	if (host == NULL || host[0] == '\0') {
		__db_errx(env, DB_STR("3648",
		    "repmgr_site: a host name is required"));
		return (EINVAL);
	}
	if (port == 0 || port > UINT16_MAX) {
		__db_errx(env, DB_STR_A("3649",
		    "repmgr_site: port out of range [1,%u]", "%u"), UINT16_MAX);
		return (EINVAL);
	}
	return (0);
}

/*
 * PUBLIC: int __repmgr_channel __P((DB_ENV *, int, DB_CHANNEL **, u_int32_t));
 */
int
__repmgr_channel(dbenv, eid, dbchannelp, flags)
	DB_ENV *dbenv;
	int eid;
	DB_CHANNEL **dbchannelp;
	u_int32_t flags;
{
	ENV *env;
	DB_THREAD_INFO *ip;
	REP *rep;
	DB_REP *db_rep;
	DB_CHANNEL *dbchannel;
	CHANNEL *channel;
	REPMGR_CONNECTION *conn;
	int cur_eid, master, ret;

	channel = NULL;
	dbchannel = NULL;
	conn = NULL;

	env = dbenv->env;
	if ((ret = __db_fchk(env, "DB_ENV->repmgr_channel", flags, 0)) != 0)
		return (ret);
	db_rep = env->rep_handle;
	rep = db_rep->region;

	if (db_rep->selector == NULL) {
		__db_errx(env, DB_STR("3650",
    "DB_ENV->repmgr_channel: must be called after DB_ENV->repmgr_start"));
		return (EINVAL);
	}
	/*
	 * Note that repmgr_start() checks DB_INIT_REP, ENV_THREAD and
	 * APP_IS_BASEAPI.
	 */
	if (db_rep->repmgr_status == stopped) {
		__db_errx(env, DB_STR("3651", "repmgr is stopped"));
		return (EINVAL);
	}

	if (eid == DB_EID_MASTER) {
		if ((master = rep->master_id) == DB_EID_INVALID)
			return (DB_REP_UNAVAIL);
		cur_eid = master;
	} else if (IS_KNOWN_REMOTE_SITE(eid))
		cur_eid = eid;
	else {
		__db_errx(env, DB_STR_A("3652",
			"%d is not a valid remote EID", "%d"), eid);
		return (EINVAL);
	}

	ENV_ENTER(env, ip);
	if ((ret = __os_calloc(env, 1, sizeof(DB_CHANNEL), &dbchannel)) != 0 ||
	    (ret = __os_calloc(env, 1, sizeof(CHANNEL), &channel)) != 0)
		goto err;
	dbchannel->channel = channel;
	channel->db_channel = dbchannel;
	channel->env = env;

	/* Preserve EID as passed by the caller (not cur_eid). */
	dbchannel->eid = eid;
	dbchannel->timeout = DB_REPMGR_DEFAULT_CHANNEL_TIMEOUT;

	dbchannel->close = __repmgr_channel_close;
	dbchannel->send_msg = __repmgr_send_msg;
	dbchannel->send_request = __repmgr_send_request;
	dbchannel->set_timeout = __repmgr_channel_timeout;

	if (cur_eid != db_rep->self_eid &&
	    (ret = establish_connection(env, cur_eid, &conn)) != 0)
		goto err;

	if (IS_VALID_EID(eid)) {
		DB_ASSERT(env, conn != NULL);
		channel->c.conn = conn;
	} else {
		/*
		 * If the configured EID is one of the special ones (MASTER or
		 * BROADCAST) we need a mutex for dynamic messing with
		 * connections that could happen later.
		 */
		if ((ret = __repmgr_create_mutex(env,
		    &channel->c.conns.mutex)) != 0)
			goto err;

		if (conn != NULL) {
			/*
			 * Allocate enough array elements to use cur_eid as an
			 * index; save the number of slots allocated as "cnt."
			 */
			if ((ret = __os_calloc(env,
			    (u_int)cur_eid + 1, sizeof(REPMGR_CONNECTION *),
			    &channel->c.conns.array)) != 0)
				goto err;
			channel->c.conns.cnt = (u_int)cur_eid + 1;
			channel->c.conns.array[cur_eid] = conn;
		}
	}

	if (conn != NULL) {
		LOCK_MUTEX(db_rep->mutex);
		conn->ref_count++;
		UNLOCK_MUTEX(db_rep->mutex);
	}

	*dbchannelp = dbchannel;

err:
	if (ret != 0) {
		if (conn != NULL)
			(void)__repmgr_disable_connection(env, conn);
		if (channel != NULL) {
			if (!IS_VALID_EID(eid) &&
			    channel->c.conns.mutex != NULL)
				(void)__repmgr_destroy_mutex(env,
				    channel->c.conns.mutex);
			__os_free(env, channel);
		}
		if (dbchannel != NULL)
			__os_free(env, dbchannel);
	}
	ENV_LEAVE(env, ip);
	return (ret);
}

static int
get_shared_netaddr(env, eid, netaddr)
	ENV *env;
	int eid;
	repmgr_netaddr_t *netaddr;
{
	DB_REP *db_rep;
	REP *rep;
	REGINFO *infop;
	SITEINFO *base, *p;
	int ret;

	db_rep = env->rep_handle;
	rep = db_rep->region;

	MUTEX_LOCK(env, rep->mtx_repmgr);

	if ((u_int)eid >= rep->site_cnt) {
		ret = DB_NOTFOUND;
		goto err;
	}
	DB_ASSERT(env, rep->siteinfo_off != INVALID_ROFF);

	infop = env->reginfo;
	base = R_ADDR(infop, rep->siteinfo_off);
	p = &base[eid];
	netaddr->host = R_ADDR(infop, p->addr.host);
	netaddr->port = p->addr.port;
	ret = 0;

err:
	MUTEX_UNLOCK(env, rep->mtx_repmgr);
	return (ret);
}

static int
establish_connection(env, eid, connp)
	ENV *env;
	int eid;
	REPMGR_CONNECTION **connp;
{
	DB_REP *db_rep;
	REPMGR_CONNECTION *conn;
	DBT vi;
	repmgr_netaddr_t netaddr;
	__repmgr_msg_hdr_args msg_hdr;
	__repmgr_version_confirmation_args conf;
	int alloc, locked, ret, unused;

	db_rep = env->rep_handle;
	alloc = locked = FALSE;

	if ((ret = get_shared_netaddr(env, eid, &netaddr)) != 0)
		return (ret);

	if ((ret = __repmgr_connect(env, &netaddr, &conn, &unused)) != 0)
		return (ret);
	conn->type = APP_CONNECTION;

	/* Read a handshake msg, to get version confirmation and parameters. */
	if ((ret = __repmgr_read_conn(conn)) != 0)
		goto out;
	/*
	 * We can only get here after having read the full 9 bytes that we
	 * expect, so this can't fail.
	 */
	DB_ASSERT(env, conn->reading_phase == SIZES_PHASE);
	ret = __repmgr_msg_hdr_unmarshal(env, &msg_hdr,
	    conn->msg_hdr_buf, __REPMGR_MSG_HDR_SIZE, NULL);
	DB_ASSERT(env, ret == 0);
	__repmgr_iovec_init(&conn->iovecs);
	conn->reading_phase = DATA_PHASE;

	if ((ret = __repmgr_prepare_simple_input(env, conn, &msg_hdr)) != 0)
		goto out;
	alloc = TRUE;

	if ((ret = __repmgr_read_conn(conn)) != 0)
		goto out;

	/*
	 * Analyze the handshake msg, and stash relevant info.
	 */
	if ((ret = __repmgr_find_version_info(env, conn, &vi)) != 0)
		goto out;
	DB_ASSERT(env, vi.size > 0);
	if ((ret = __repmgr_version_confirmation_unmarshal(env,
	    &conf, vi.data, vi.size, NULL)) != 0)
		goto out;

	if (conf.version < CHANNEL_MIN_VERSION) {
		ret = DB_REP_UNAVAIL;
		goto out;
	}

	conn->version = conf.version;

	if ((ret = __repmgr_send_handshake(env,
	    conn, NULL, 0, APP_CHANNEL_CONNECTION)) != 0)
		goto out;
	conn->state = CONN_READY;
	__repmgr_reset_for_reading(conn);
	if ((ret = __repmgr_set_nonblock_conn(conn)) != 0) {
		__db_err(env, ret, DB_STR("3653", "set_nonblock channel"));
		goto out;
	}

	/*
	 * Turn over the responsibility for reading on this connection to the
	 * select() thread.
	 */
	LOCK_MUTEX(db_rep->mutex);
	locked = TRUE;
	if ((ret = __repmgr_wake_main_thread(env)) != 0)
		goto out;

	/*
	 * Share this new connection with the select thread, which will
	 * hereafter own the exclusive right to read input from it.  Once we get
	 * past this point, we can't unilaterally close and destroy the
	 * connection if a retryable connection error happens.  Fortunately,
	 * we're now at the point where everything has succeeded; so there will
	 * be no more errors.
	 */
	TAILQ_INSERT_TAIL(&db_rep->connections, conn, entries);
	conn->ref_count++;
	*connp = conn;

out:
	if (locked)
		UNLOCK_MUTEX(db_rep->mutex);

	if (ret != 0) {
		/*
		 * Since we can't have given the connection to the select()
		 * thread yet, clean-up is as simple as this:
		 */
		(void)__repmgr_close_connection(env, conn);
		(void)__repmgr_destroy_conn(env, conn);
	}

	if (alloc) {
		DB_ASSERT(env, conn->input.repmgr_msg.cntrl.size > 0);
		__os_free(env, conn->input.repmgr_msg.cntrl.data);
		DB_ASSERT(env, conn->input.repmgr_msg.rec.size > 0);
		__os_free(env, conn->input.repmgr_msg.rec.data);
	}
	return (ret);
}

/*
 * PUBLIC: int __repmgr_set_msg_dispatch __P((DB_ENV *,
 * PUBLIC:     void (*)(DB_ENV *, DB_CHANNEL *, DBT *, u_int32_t, u_int32_t),
 * PUBLIC:     u_int32_t));
 */
int
__repmgr_set_msg_dispatch(dbenv, dispatch, flags)
	DB_ENV *dbenv;
	void (*dispatch) __P((DB_ENV *,
		DB_CHANNEL *, DBT *, u_int32_t, u_int32_t));
	u_int32_t flags;
{
	ENV *env;
	DB_REP *db_rep;
	DB_THREAD_INFO *ip;
	int ret;

	env = dbenv->env;
	if ((ret = __db_fchk(env,
	    "DB_ENV->repmgr_msg_dispatch", flags, 0)) != 0)
		return (ret);
	if (APP_IS_BASEAPI(env))
		return (repmgr_only(env, "repmgr_msg_dispatch"));

	db_rep = env->rep_handle;
	db_rep->msg_dispatch = dispatch;
	APP_SET_REPMGR(env);
	return (0);
}

/*
 * Implementation of DB_CHANNEL->send_msg() method for use in a normal channel
 * explicitly created by the message-originator application.
 *
 * PUBLIC: int __repmgr_send_msg __P((DB_CHANNEL *,
 * PUBLIC:     DBT *, u_int32_t, u_int32_t));
 */
int
__repmgr_send_msg(db_channel, msg, nmsg, flags)
	DB_CHANNEL *db_channel;
	DBT *msg;
	u_int32_t nmsg;
	u_int32_t flags;
{
	ENV *env;
	DB_THREAD_INFO *ip;
	CHANNEL *channel;
	REPMGR_CONNECTION *conn;
	int ret;

	channel = db_channel->channel;
	env = channel->env;
	if ((ret = __db_fchk(env,
	    "DB_CHANNEL->send_msg", flags, 0)) != 0)
		return (ret);

	ENV_ENTER(env, ip);
	if ((ret = get_channel_connection(channel, &conn)) == 0)
		ret = send_msg_conn(env, conn, msg, nmsg);
	ENV_LEAVE(env, ip);
	return (ret);
}

/*
 * Sends an async msg on the given connection (or just copies it locally if conn
 * is NULL, since that means we're "sending to the master" when we ourselves are
 * the master).
 */
static int
send_msg_conn(env, conn, msg, nmsg)
	ENV *env;
	REPMGR_CONNECTION *conn;
	DBT *msg;
	u_int32_t nmsg;
{
	DB_REP *db_rep;
	REPMGR_IOVECS *iovecs;
	__repmgr_msg_metadata_args meta;
	int ret;

	db_rep = env->rep_handle;
	memset(&meta, 0, sizeof(meta));
	if (conn == NULL) {
		/* Sending to DB_EID_MASTER when we ourselves are master. */
		if ((ret = __repmgr_build_data_out(env,
		    msg, nmsg, &meta, &iovecs)) != 0)
			return (ret);
		ret = send_msg_self(env, iovecs, nmsg);
	} else {
		if ((ret = __repmgr_build_msg_out(env,
		    msg, nmsg, &meta, &iovecs)) != 0)
			return (ret);
		LOCK_MUTEX(db_rep->mutex);
		ret = __repmgr_send_many(env, conn, iovecs, 0);
		UNLOCK_MUTEX(db_rep->mutex);
	}

	__os_free(env, iovecs);
	return (ret);
}

/*
 * Simulate sending by simply copying the message into a msg struct to be
 * queued.  On input, iovecs is ready to "send", with first slot set aside for
 * message header.
 */
static int
send_msg_self(env, iovecs, nmsg)
	ENV *env;
	REPMGR_IOVECS *iovecs;
	u_int32_t nmsg;
{
	REPMGR_MESSAGE *msg;
	size_t align, bodysize, structsize;
	u_int8_t *membase;
	int ret;

	align = sizeof(double);
	bodysize = iovecs->total_bytes - __REPMGR_MSG_HDR_SIZE;
	structsize = (size_t)DB_ALIGN((size_t)(sizeof(REPMGR_MESSAGE) +
	    nmsg * sizeof(DBT)), align);
	if ((ret = __os_malloc(env, structsize + bodysize, &membase)) != 0)
		return (ret);

	msg = (void*)membase;
	membase += structsize;

	/*
	 * Build a msg struct that looks like what would be received in the
	 * usual case.
	 */
	msg->msg_hdr.type = REPMGR_APP_MESSAGE;
	APP_MSG_BUFFER_SIZE(msg->msg_hdr) = (u_int32_t)bodysize;
	APP_MSG_SEGMENT_COUNT(msg->msg_hdr) = nmsg;

	msg->v.appmsg.conn = NULL;

	/*
	 * The "buf" is the message body (as [if] transmitted); i.e., it
	 * excludes the header (which we've just constructed separately).  So,
	 * skip over slot 0 in the iovecs, which had been reserved for the hdr.
	 */
	DB_INIT_DBT(msg->v.appmsg.buf, membase, bodysize);
	copy_body(membase, iovecs);

	return (__repmgr_queue_put(env, msg));
}

/*
 * Copies a message body into a single contiguous buffer.  The given iovecs is
 * assumed to have the first slot reserved for a message header, and we skip
 * that part.
 */
static void
copy_body(membase, iovecs)
	u_int8_t *membase;
	REPMGR_IOVECS *iovecs;
{
	size_t sz;
	int i;

	for (i = 1; i < iovecs->count; i++) {
		if ((sz = (size_t)iovecs->vectors[i].iov_len) > 0) {
			memcpy(membase, iovecs->vectors[i].iov_base, sz);
			membase += sz;
		}
	}
}

/*
 * Gets a connection to be used for sending, either an async message or a
 * request.  On a DB_EID_MASTER channel this entails checking the current
 * master, and possibly opening a new connection if the master has changed.
 * Allow an old connection to stay intact, because responses to previous
 * requests could still be arriving (though often the connection will have died
 * anyway, if the master changed due to failure of the old master).
 *
 * If the local site is currently master, then for a master channel we return
 * (via connp) a NULL pointer.
 */
static int
get_channel_connection(channel, connp)
	CHANNEL *channel;
	REPMGR_CONNECTION **connp;
{
	ENV *env;
	DB_REP *db_rep;
	REP *rep;
	REPMGR_CONNECTION *conn;
	DB_CHANNEL *db_channel;
	int eid, ret;

	env = channel->env;
	db_rep = env->rep_handle;
	rep = db_rep->region;
	db_channel = channel->db_channel;

	/*
	 * On a specific-EID channel it's very simple, because there is only
	 * ever one connection, which was established when the channel was
	 * created.
	 */
	if (db_channel->eid >= 0) {
		*connp = channel->c.conn;
		return (0);
	}

	/*
	 * For now we only support one connection at a time.  When we support
	 * DB_EID_BROADCAST channels in the future, we will have to loop through
	 * all connected sites.
	 */
	DB_ASSERT(env, db_channel->eid == DB_EID_MASTER);
	eid = rep->master_id;
	if (eid == db_rep->self_eid) {
		*connp = NULL;
		return (0);
	}
	if (eid == DB_EID_INVALID)
		return (DB_REP_UNAVAIL);

	LOCK_MUTEX(channel->c.conns.mutex);
	if ((u_int)eid >= channel->c.conns.cnt) {
		/*
		 * Allocate an array big enough such that `eid' is a valid
		 * index; initialize the newly allocated (tail) portion.
		 */
		RPRINT(env, (env, DB_VERB_REPMGR_MISC,
		    "Grow master-channel array to accommodate EID %d", eid));
		if ((ret = __os_realloc(env,
		    sizeof(REPMGR_CONNECTION *) * ((u_int)eid + 1),
		    &channel->c.conns.array)) != 0)
			goto out;
		memset(&channel->c.conns.array[channel->c.conns.cnt],
		    0,
		    sizeof(REPMGR_CONNECTION *) *
		    (((u_int)eid + 1) - channel->c.conns.cnt));
		channel->c.conns.cnt = (u_int)eid + 1;
	}
	DB_ASSERT(env, (u_int)eid < channel->c.conns.cnt);

	if ((conn = channel->c.conns.array[eid]) == NULL) {
		if ((ret = establish_connection(env, eid, &conn)) != 0)
			goto out;

		/*
		 * Even though `conn' is a newly created object, by the time we
		 * get here it has already been given out to the select()
		 * thread, so we should hold the mutex while incrementing the
		 * ref count.
		 */
		LOCK_MUTEX(db_rep->mutex);
		channel->c.conns.array[eid] = conn;
		conn->ref_count++;
		UNLOCK_MUTEX(db_rep->mutex);
	}

	*connp = conn;
	ret = 0;
out:
	UNLOCK_MUTEX(channel->c.conns.mutex);
	return (ret);
}

/*
 * PUBLIC: int __repmgr_send_request __P((DB_CHANNEL *,
 * PUBLIC:     DBT *, u_int32_t, DBT *, db_timeout_t, u_int32_t));
 */
int
__repmgr_send_request(db_channel, request, nrequest, response, timeout, flags)
	DB_CHANNEL *db_channel;
	DBT *request;
	u_int32_t nrequest;
	DBT *response;
	db_timeout_t timeout;
	u_int32_t flags;
{
	ENV *env;
	DB_THREAD_INFO *ip;
	DB_REP *db_rep;
	CHANNEL *channel;
	REPMGR_CONNECTION *conn;
	REPMGR_IOVECS *iovecs;
	REPMGR_RESPONSE *resp;
	struct response_wait ctx;
	__repmgr_msg_metadata_args meta;
	size_t sz;
	void *dummy;
	u_int32_t i, n;
	int ret;

	channel = db_channel->channel;
	env = channel->env;
	db_rep = env->rep_handle;

	if ((ret = __db_fchk(env,
	    "DB_CHANNEL->send_request", flags, DB_MULTIPLE)) != 0)
		return (ret);

	if (db_channel->eid == DB_EID_BROADCAST) {
		__db_errx(env, DB_STR("3654",
    "DB_CHANNEL->send_request() not supported on DB_EID_BROADCAST channel"));
		return (EINVAL);
	}

	ENV_ENTER(env, ip);
	ret = get_channel_connection(channel, &conn);
	ENV_LEAVE(env, ip);
	if (ret != 0)
		return (ret);

	if (conn == NULL)
		return (request_self(env, request, nrequest, response, flags));

	/* Find an available array slot, or grow the array if necessary. */
	LOCK_MUTEX(db_rep->mutex);
	for (i = 0; i < conn->aresp; i++)
		if (!(F_ISSET(&conn->responses[i], RESP_IN_USE)))
			break;
	if (i == conn->aresp) {
		n = conn->aresp == 0 ? 1 : conn->aresp * 2;
		ret = __os_realloc(env,
		    sizeof(REPMGR_RESPONSE) * n, &conn->responses);
		memset(&conn->responses[i], 0,
		    sizeof(REPMGR_RESPONSE) * (n - i));
		conn->aresp = n;
	}
	resp = &conn->responses[i];
	resp->flags = RESP_IN_USE | RESP_THREAD_WAITING;
	resp->dbt = *response;
	resp->ret = 0;
	UNLOCK_MUTEX(db_rep->mutex);

	/*
	 * The index "i" is stable, but the address in the "resp" pointer could
	 * change while we drop the mutex, if another thread has to grow the
	 * allocated array.  So we can't use "resp" again until after we set it
	 * again, from "i", under mutex protection.
	 */

	meta.tag = i;
	meta.flags = REPMGR_REQUEST_MSG_TYPE |
	    (LF_ISSET(DB_MULTIPLE) ? REPMGR_MULTI_RESP : 0) |
	    (F_ISSET(response, DB_DBT_USERMEM) ? REPMGR_RESPONSE_LIMIT : 0);
	meta.limit = response->ulen;

	/*
	 * Build an iovecs structure describing the request message, and then
	 * send it.
	 */
	if ((ret = __repmgr_build_msg_out(env,
	    request, nrequest, &meta, &iovecs)) != 0) {
		/*
		 * Since we haven't sent the message yet, there's no chance the
		 * select thread has started relying on the REPMGR_RESPONSE, so
		 * it's easy to deallocate it.
		 */
		LOCK_MUTEX(db_rep->mutex);
		F_CLR(&conn->responses[i], RESP_IN_USE | RESP_THREAD_WAITING);
		UNLOCK_MUTEX(db_rep->mutex);
		return (ret);
	}

	timeout = timeout > 0 ? timeout : db_channel->timeout;
	LOCK_MUTEX(db_rep->mutex);
	ret = __repmgr_send_many(env, conn, iovecs, timeout);
	if (ret == DB_TIMEOUT)
		F_CLR(&conn->responses[i], RESP_IN_USE | RESP_THREAD_WAITING);
	UNLOCK_MUTEX(db_rep->mutex);
	__os_free(env, iovecs);
	if (ret != 0) {
		/*
		 * An error while writing will force the connection to be
		 * closed, busted, abandoned.  Since there could be a few app
		 * threads waiting, *any* abandoning of a connection will have
		 * to wake up those threads, with a COMPLETE indication and an
		 * error code.  That's more than we want to tackle here.
		 */
		return (ret);
	}

	/*
	 * Here, we've successfully sent the request.  Once we've gotten this
	 * far, the select thread owns the REPMGR_RESPONSE slot until it marks
	 * it complete.
	 */
	ctx.conn = conn;
	ctx.index = i;
	LOCK_MUTEX(db_rep->mutex);
	ret = __repmgr_await_cond(env,
	    response_complete, &ctx, timeout, &conn->response_waiters);

	resp = &conn->responses[i];
	if (ret == 0) {
		DB_ASSERT(env, F_ISSET(resp, RESP_COMPLETE));
		*response = resp->dbt;
		if ((ret = resp->ret) == 0 && LF_ISSET(DB_MULTIPLE))
			adjust_bulk_response(env, response);
		F_CLR(resp, RESP_IN_USE | RESP_THREAD_WAITING);

	} else {
		F_CLR(resp, RESP_THREAD_WAITING);
		if (ret == DB_TIMEOUT && F_ISSET(resp, RESP_READING)) {
			/*
			 * The select thread is in the midst of reading the
			 * response, but we're about to yank the buffer out from
			 * under it.  So, replace it with a dummy buffer.
			 * (There's no way to abort the reading of a message
			 * part-way through.)
			 *
			 * Notice that whatever buffer the user is getting back,
			 * including her own in the case of USERMEM, may already
			 * have some partial data written into it.
			 *
			 * We always read responses in just one single chunk, so
			 * figuring out the needed buffer size is fairly simple.
			 */
			DB_ASSERT(env, conn->iovecs.offset == 0 &&
			    conn->iovecs.count == 1);
			sz = conn->iovecs.vectors[0].iov_len;

			if ((ret = __os_malloc(env, sz, &dummy)) != 0)
				goto out;
			__repmgr_iovec_init(&conn->iovecs);
			DB_INIT_DBT(resp->dbt, dummy, sz);
			__repmgr_add_dbt(&conn->iovecs, &resp->dbt);
			F_SET(resp, RESP_DUMMY_BUF);
		}
	}

out:
	UNLOCK_MUTEX(db_rep->mutex);
	return (ret);
}

static int
response_complete(env, ctx)
	ENV *env;
	void *ctx;
{
	REPMGR_CONNECTION *conn;
	struct response_wait *rw;

	COMPQUIET(env, NULL);

	rw = ctx;
	conn = rw->conn;
	return (F_ISSET(&conn->responses[rw->index], RESP_COMPLETE) ||
	    conn->state == CONN_DEFUNCT);
}

/*
 * "Send" a request to ourselves, by invoking the application's call-back
 * function directly, in the case where a channel directed to DB_EID_MASTER is
 * used on a master.
 */
static int
request_self(env, request, nrequest, response, flags)
	ENV *env;
	DBT *request;
	u_int32_t nrequest;
	DBT *response;
	u_int32_t flags;
{
	DB_REP *db_rep;
	DB_CHANNEL db_channel;
	CHANNEL channel;
	__repmgr_msg_metadata_args meta;

	db_rep = env->rep_handle;
	if (db_rep->msg_dispatch == NULL) {
		__db_errx(env, DB_STR("3655",
	    "No message dispatch call-back function has been configured"));
		return (DB_NOSERVER);
	}

	db_channel.channel = &channel;
	db_channel.send_msg = __repmgr_send_response;

	/* Supply stub functions for methods inapplicable in msg disp func. */
	db_channel.close = __repmgr_channel_close_inval;
	db_channel.send_request = __repmgr_send_request_inval;
	db_channel.set_timeout = __repmgr_channel_timeout_inval;

	channel.env = env;
	channel.c.conn = NULL;
	channel.responded = FALSE;
	channel.meta = &meta;
	channel.response.dbt = *response;

	meta.flags = REPMGR_REQUEST_MSG_TYPE |
	    (LF_ISSET(DB_MULTIPLE) ? REPMGR_MULTI_RESP : 0) |
	    (F_ISSET(response, DB_DBT_USERMEM) ? REPMGR_RESPONSE_LIMIT : 0);
	meta.limit = response->ulen;

	(*db_rep->msg_dispatch)(env->dbenv,
	    &db_channel, request, nrequest, DB_REPMGR_NEED_RESPONSE);

	if (!channel.responded) {
		__db_errx(env, DB_STR("3656",
		    "Application failed to provide a response"));
		return (DB_KEYEMPTY);
	} else {
		response->data = channel.response.dbt.data;
		response->size = channel.response.dbt.size;
		if (LF_ISSET(DB_MULTIPLE))
			adjust_bulk_response(env, response);
	}
	return (0);
}

static void
adjust_bulk_response(env, response)
	ENV *env;
	DBT *response;
{
	u_int32_t n, *p;

#ifndef	DIAGNOSTIC
	COMPQUIET(env, NULL);
#endif

	/*
	 * Convert bulk-buffer segment info to host byte-order, and count
	 * segments.  See the definition of DB_MULTIPLE_INIT for a reminder of
	 * the structure of a bulk buffer.  Each segment has both an offset and
	 * a length, so "n" ends up as the number of u_int32_t words we (might)
	 * need to shuffle, below.
	 */
	p = (u_int32_t *)((u_int8_t *)response->data +
	    response->size - sizeof(u_int32_t));
	for (n = 1; *p != (u_int32_t)-1; p -= 2) {
		DB_ASSERT(env, p > (u_int32_t *)response->data);
		p[0] = ntohl(p[0]);
		p[-1] = ntohl(p[-1]);
		n += 2;
	}
	/*
	 * The bulk pointers appear at the end of the transmitted response, so
	 * unless the buffer happened to be exactly the right size we need to
	 * shuffle them to the end of the buffer.
	 */
	if (F_ISSET(response, DB_DBT_USERMEM))
		memmove((u_int8_t *)response->data +
		    response->ulen - n * sizeof(u_int32_t),
		    p, n * sizeof(u_int32_t));
	else
		response->ulen = response->size;
}

/*
 * Implementation of DB_CHANNEL->send_msg() method for use in recipient's msg
 * dispatch callback function.
 *
 * PUBLIC: int __repmgr_send_response __P((DB_CHANNEL *,
 * PUBLIC:     DBT *, u_int32_t, u_int32_t));
 */
int
__repmgr_send_response(db_channel, msg, nmsg, flags)
	DB_CHANNEL *db_channel;
	DBT *msg;
	u_int32_t nmsg;
	u_int32_t flags;
{
	ENV *env;
	DB_REP *db_rep;
	CHANNEL *channel;
	REPMGR_CONNECTION *conn;
	REPMGR_IOVECS iovecs, *iovecsp;
	DBT *dbt;
	__repmgr_msg_hdr_args msg_hdr;
	u_int8_t msg_hdr_buf[__REPMGR_MSG_HDR_SIZE], *msg_hdr_buf_p;
	size_t sz;
	int alloc, ret;

	COMPQUIET(iovecsp, NULL);

	channel = db_channel->channel;
	env = channel->env;
	db_rep = env->rep_handle;
	conn = channel->c.conn;

	if ((ret = __db_fchk(env,
	    "DB_CHANNEL->send_msg", flags, 0)) != 0)
		return (ret);

	if (!F_ISSET(channel->meta, REPMGR_REQUEST_MSG_TYPE))
		return (send_msg_conn(env, conn, msg, nmsg));

	if (channel->responded) {
		__db_errx(env, DB_STR("3657",
		    "a response has already been sent"));
		return (EINVAL);
	}

	alloc = FALSE;
	if (F_ISSET(channel->meta, REPMGR_MULTI_RESP)) {
		/*
		 * Originator accepts bulk format: response can be any number of
		 * segments.
		 */
		if ((ret = __repmgr_build_data_out(env,
		    msg, nmsg, NULL, &iovecsp)) != 0)
			goto out;
		alloc = TRUE;

		/*
		 * Set buffer pointer to space we "know" build_data_out reserved
		 * for us.
		 */
		msg_hdr_buf_p = (u_int8_t *)iovecsp->vectors[0].iov_base;
		msg_hdr.type = REPMGR_APP_RESPONSE;
		APP_RESP_TAG(msg_hdr) = channel->meta->tag;
		APP_RESP_BUFFER_SIZE(msg_hdr) =
		    (u_int32_t)(iovecsp->total_bytes - __REPMGR_MSG_HDR_SIZE);
		__repmgr_msg_hdr_marshal(env, &msg_hdr, msg_hdr_buf_p);
	} else if (nmsg > 1) {
		__db_errx(env, DB_STR("3658",
		    "originator does not accept multi-segment response"));
		goto small;
	} else {
		iovecsp = &iovecs;
		__repmgr_iovec_init(iovecsp);
		msg_hdr.type = REPMGR_APP_RESPONSE;
		APP_RESP_TAG(msg_hdr) = channel->meta->tag;
		__repmgr_add_buffer(iovecsp,
		    msg_hdr_buf, __REPMGR_MSG_HDR_SIZE);
		if (nmsg == 0)
			APP_RESP_BUFFER_SIZE(msg_hdr) = 0;
		else if ((APP_RESP_BUFFER_SIZE(msg_hdr) = msg->size) > 0)
			__repmgr_add_dbt(iovecsp, msg);
		__repmgr_msg_hdr_marshal(env, &msg_hdr, msg_hdr_buf);
	}

	if (F_ISSET(channel->meta, REPMGR_RESPONSE_LIMIT) &&
	    (APP_RESP_BUFFER_SIZE(msg_hdr) > channel->meta->limit)) {
		__db_errx(env, DB_STR("3659",
		    "originator's USERMEM buffer too small"));
small:
		if (conn == NULL)
			channel->response.ret = DB_BUFFER_SMALL;
		else
			(void)__repmgr_send_err_resp(env,
			    channel, DB_BUFFER_SMALL);
		ret = EINVAL;
	} else {
		if (conn == NULL) {
			sz = APP_RESP_BUFFER_SIZE(msg_hdr);
			dbt = &channel->response.dbt;
			if (F_ISSET(dbt, DB_DBT_MALLOC))
				ret = __os_umalloc(env, sz, &dbt->data);
			else if (F_ISSET(dbt, DB_DBT_REALLOC)) {
				if (dbt->data == NULL || dbt->size < sz)
					ret = __os_urealloc(env,
					    sz, &dbt->data);
				else
					ret = 0;
			}
			dbt->size = (u_int32_t)sz;
			copy_body(dbt->data, iovecsp);
			channel->response.ret = 0;
			ret = 0;
		} else {
			LOCK_MUTEX(db_rep->mutex);
			ret = __repmgr_send_many(env, conn, iovecsp, 0);
			UNLOCK_MUTEX(db_rep->mutex);
		}
	}

out:
	if (alloc)
		__os_free(env, iovecsp);

	/*
	 * Once we've handed the tag back to the originator it becomes
	 * meaningless, so we can't use it again.  Note the fact that we've
	 * responded, so that we don't try.
	 */
	channel->responded = TRUE;

	return (ret);
}

static int
__repmgr_build_msg_out(env, msg, nmsg, meta, iovecsp)
	ENV *env;
	DBT *msg;
	u_int32_t nmsg;
	__repmgr_msg_metadata_args *meta;
	REPMGR_IOVECS **iovecsp;
{
	REPMGR_IOVECS *iovecs;
	__repmgr_msg_hdr_args msg_hdr;
	u_int8_t *msg_hdr_buf;
	int ret;

	if ((ret = __repmgr_build_data_out(env, msg, nmsg, meta, &iovecs)) != 0)
		return (ret);

	/*
	 * The IOVECS holds the entire message to be transmitted, including the
	 * 9-byte header.  The header contains the length of the remaining part
	 * of the message.  The header buffer area is of course pointed to by
	 * the first of the io vectors.
	 */
	msg_hdr_buf = (u_int8_t *)iovecs->vectors[0].iov_base;
	msg_hdr.type = REPMGR_APP_MESSAGE;
	APP_MSG_BUFFER_SIZE(msg_hdr) =
	    (u_int32_t)(iovecs->total_bytes - __REPMGR_MSG_HDR_SIZE);
	APP_MSG_SEGMENT_COUNT(msg_hdr) = nmsg;
	__repmgr_msg_hdr_marshal(env, &msg_hdr, msg_hdr_buf);

	*iovecsp = iovecs;
	return (0);
}

/*
 * Allocate and build most of an outgoing message, leaving it up to the caller
 * to fill in the header afterwards.
 */
static int
__repmgr_build_data_out(env, msg, nmsg, meta, iovecsp)
	ENV *env;
	DBT *msg;
	u_int32_t nmsg;
	__repmgr_msg_metadata_args *meta;
	REPMGR_IOVECS **iovecsp;
{
	REPMGR_IOVECS *iovecs;
	u_int32_t *bulk_base, *bulk_ptr, i, n;
	u_int8_t *membase, *meta_buf, *msg_hdr_buf, *p, *pad;
	void *inc_p;
	size_t align, bulk_area_sz, memsize, segments, sz, offset;
	int ret;

	COMPQUIET(pad, NULL);

	/*
	 * The actual message as it will be sent on the wire is composed of the
	 * following parts:
	 *
	 * (a) the 9-byte header
	 * (b) for each msg DBT ('nmsg' of them):
	 *     (b.1) the data itself, and
	 *     (b.2) an alignment pad, if necessary
	 * (c) trailing section for bulk-style pointers (2 words per segment,
	 *     plus a -1 end-marker)
	 * (d) message meta-data (optionally)
	 *
	 * Note that nmsg could be 0.
	 */

	/* First, count how many segments need padding. */
	n = 0;
	align = sizeof(double);
	for (i = 0; i < nmsg; i++) {
		p = msg[i].data;
		p = &p[msg[i].size];
		inc_p = ALIGNP_INC(p, align);
		if ((u_int8_t *)inc_p > p)
			n++;
	}

	/*
	 * Here we allocate memory to hold the actual pieces of the message we
	 * will send, plus the iovecs structure that points to those pieces.  We
	 * don't include the memory for the user's data (item (b.1) from the
	 * above explanation), since the user is supplying them directly.  Also
	 * note that we can reuse just one padding buffer even if we need to
	 * send it (i.e., point to it from an iovec) more than once.
	 *
	 * According to the list of message segments explained above, the total
	 * number of iovec elements we need is (1 + nmsg + n + 1 + f(meta)).
	 */
	segments = nmsg + n + (meta == NULL ? 2 : 3);
	sz = segments > MIN_IOVEC ? REPMGR_IOVECS_ALLOC_SZ(segments) :
	    sizeof(REPMGR_IOVECS);

	bulk_area_sz = (nmsg * 2 + 1) * sizeof(u_int32_t);
	memsize = sz + __REPMGR_MSG_HDR_SIZE +
	    bulk_area_sz + (n > 0 ? align : 0) + __REPMGR_MSG_METADATA_SIZE;

	if ((ret = __os_malloc(env, memsize, &membase)) != 0)
		return (ret);
	p = membase;
	iovecs = (REPMGR_IOVECS *)p;
	p += sz;
	bulk_base = (u_int32_t *)p;
	p += bulk_area_sz;
	if (n > 0) {
		pad = p;
		memset(pad, 0, align);
		p += align;
	}
	msg_hdr_buf = p;
	p += __REPMGR_MSG_HDR_SIZE;
	meta_buf = p;

	/*
	 * The message header appears first (on the wire), so we have to add its
	 * buffer address to the iovec list first.  But we don't actually
	 * compose the content; that's the responsibility of the caller, after
	 * we return.
	 */
	__repmgr_iovec_init(iovecs);
	__repmgr_add_buffer(iovecs, msg_hdr_buf, __REPMGR_MSG_HDR_SIZE);

	offset = 0;
	bulk_ptr = &bulk_base[2*nmsg + 1]; /* Work backward from the end. */
	for (i = 0; i < nmsg; i++) {
		p = msg[i].data;
		sz = (size_t)msg[i].size;

		/*
		 * Format of bulk pointers is similar to the usage of
		 * DB_MULTIPLE_NEXT, but note that the lengths we pass are of
		 * course for the actual data itself, not including any
		 * padding.
		 */
		*--bulk_ptr = htonl((u_long)offset);
		*--bulk_ptr = htonl((u_long)sz);

		__repmgr_add_dbt(iovecs, &msg[i]);
		offset += sz;

		p = &p[sz];
		inc_p = ALIGNP_INC(p, align);
		if ((u_int8_t *)inc_p > p) {
			DB_ASSERT(env, n > 0);
			sz = (size_t)((u_int8_t *)inc_p - p);
			DB_ASSERT(env, sz <= align);
			__repmgr_add_buffer(iovecs, pad, sz);
			offset += sz;
		}
	}
	*--bulk_ptr = (u_int32_t)-1;
	__repmgr_add_buffer(iovecs, bulk_ptr, bulk_area_sz);

	if (meta != NULL) {
		__repmgr_msg_metadata_marshal(env, meta, meta_buf);
		__repmgr_add_buffer(iovecs,
		    meta_buf, __REPMGR_MSG_METADATA_SIZE);
	}

	*iovecsp = iovecs;
	return (0);
}

/*
 * PUBLIC: int __repmgr_channel_close __P((DB_CHANNEL *, u_int32_t));
 */
int
__repmgr_channel_close(dbchan, flags)
	DB_CHANNEL *dbchan;
	u_int32_t flags;
{
	ENV *env;
	DB_REP *db_rep;
	REPMGR_CONNECTION *conn;
	CHANNEL *channel;
	u_int32_t i;
	int ret, t_ret;

	channel = dbchan->channel;
	env = channel->env;
	ret = __db_fchk(env, "DB_CHANNEL->close", flags, 0);
	db_rep = env->rep_handle;

	/*
	 * Disable connection(s) (if not already done due to an error having
	 * occurred previously); release our reference to conn struct(s).
	 */
	LOCK_MUTEX(db_rep->mutex);
	if (dbchan->eid >= 0) {
		conn = channel->c.conn;
		if (conn->state != CONN_DEFUNCT &&
		    (t_ret = __repmgr_disable_connection(env, conn)) != 0 &&
		    ret == 0)
			ret = t_ret;
		if ((t_ret = __repmgr_decr_conn_ref(env, conn)) != 0 &&
		    ret == 0)
			ret = t_ret;
	} else if (channel->c.conns.cnt > 0) {
		for (i = 0; i < channel->c.conns.cnt; i++)
			if ((conn = channel->c.conns.array[i]) != NULL) {
				if (conn->state != CONN_DEFUNCT &&
				    (t_ret = __repmgr_disable_connection(env,
				    conn)) != 0 && ret == 0)
					ret = t_ret;
				if ((t_ret = __repmgr_decr_conn_ref(env,
				    conn)) != 0 && ret == 0)
					ret = t_ret;
			}
		__os_free(env, channel->c.conns.array);
	}
	UNLOCK_MUTEX(db_rep->mutex);

	if (!IS_VALID_EID(dbchan->eid) && channel->c.conns.mutex != NULL &&
	    (t_ret = __repmgr_destroy_mutex(env,
	    channel->c.conns.mutex)) != 0 && ret == 0)
		ret = t_ret;

	if ((t_ret = __repmgr_wake_main_thread(env)) != 0 && ret == 0)
		ret = t_ret;

	__os_free(env, channel);
	__os_free(env, dbchan);

	return (ret);
}

/*
 * PUBLIC: int __repmgr_channel_timeout __P((DB_CHANNEL *, db_timeout_t));
 */
int
__repmgr_channel_timeout(chan, timeout)
	DB_CHANNEL *chan;
	db_timeout_t timeout;
{
	chan->timeout = timeout;
	return (0);
}

/*
 * PUBLIC: int __repmgr_send_request_inval __P((DB_CHANNEL *,
 * PUBLIC:     DBT *, u_int32_t, DBT *, db_timeout_t, u_int32_t));
 */
int
__repmgr_send_request_inval(dbchan, request, nrequest, response, timeout, flags)
	DB_CHANNEL *dbchan;
	DBT *request;
	u_int32_t nrequest;
	DBT *response;
	db_timeout_t timeout;
	u_int32_t flags;
{
	COMPQUIET(request, NULL);
	COMPQUIET(nrequest, 0);
	COMPQUIET(response, NULL);
	COMPQUIET(timeout, 0);
	COMPQUIET(flags, 0);
	return (bad_callback_method(dbchan, "send_request"));
}

/*
 * PUBLIC: int __repmgr_channel_close_inval __P((DB_CHANNEL *, u_int32_t));
 */
int
__repmgr_channel_close_inval(dbchan, flags)
	DB_CHANNEL *dbchan;
	u_int32_t flags;
{
	COMPQUIET(flags, 0);
	return (bad_callback_method(dbchan, "close"));
}

/*
 * PUBLIC: int __repmgr_channel_timeout_inval __P((DB_CHANNEL *, db_timeout_t));
 */
int
__repmgr_channel_timeout_inval(dbchan, timeout)
	DB_CHANNEL *dbchan;
	db_timeout_t timeout;
{
	COMPQUIET(timeout, 0);
	return (bad_callback_method(dbchan, "set_timeout"));
}

static int
bad_callback_method(chan, method)
	DB_CHANNEL *chan;
	const char *method;
{
	__db_errx(chan->channel->env, DB_STR_A("3660",
	    "%s() invalid on DB_CHANNEL supplied to msg dispatch function",
	    "%s"), method);
	return (EINVAL);
}

static int
repmgr_only(env, method)
	ENV *env;
	const char *method;
{
	__db_errx(env, DB_STR_A("3661",
	    "%s: cannot call from base replication application",
	    "%s"), method);
	return (EINVAL);
}

/*
 * Attempts to join the replication group, by finding a remote "helper" site and
 * sending a request message to it.
 *
 * PUBLIC: int __repmgr_join_group __P((ENV *));
 */
int
__repmgr_join_group(env)
	ENV *env;
{
	DB_REP *db_rep;
	REPMGR_SITE *site;
	repmgr_netaddr_t addr;
	u_int i;
	int pass, ret;

	db_rep = env->rep_handle;

	/*
	 * Make two passes through the site list.  On the first pass, try
	 * joining via an existing, fully "present" site whom we've found in the
	 * membership database.  If that is fruitless, on the second pass try
	 * any site marked as a bootstrap helper.
	 *
	 * On the first attempt to join, when we have found no database, the
	 * first pass will produce nothing.  On a later attempt to rejoin after
	 * having been removed, it's better to give priority to existing
	 * remaining sites from the database, and only rely on bootstrap helpers
	 * as a last resort.
	 *
	 * pass 0 => present members
	 * pass 1 => helpers
	 */
	LOCK_MUTEX(db_rep->mutex);
	for (pass = 0; pass <= 1; pass++) {
		FOR_EACH_REMOTE_SITE_INDEX(i) {
			site = SITE_FROM_EID(i);
			if (pass == 0 && site->membership != SITE_PRESENT)
				continue;
			if (pass == 1 &&
			    !FLD_ISSET(site->config, DB_BOOTSTRAP_HELPER))
				continue;
			addr = site->net_addr;
			UNLOCK_MUTEX(db_rep->mutex);
			if ((ret = join_group_at_site(env,
			    &addr)) == DB_REP_UNAVAIL) {
				LOCK_MUTEX(db_rep->mutex);
				continue;
			}
			return (ret);
		}
	}
	UNLOCK_MUTEX(db_rep->mutex);
	return (DB_REP_UNAVAIL);
}

/*
 * Sends a request message to another site, asking for permission to join the
 * replication group.  Ideally the other site is the master, because only the
 * master can grant that request.  But since we're not currently part of the
 * group, we generally don't know which site is master.  If the target site is
 * not master, it will respond by telling us who is.
 */
static int
join_group_at_site(env, addrp)
	ENV *env;
	repmgr_netaddr_t *addrp;
{
	DB_REP *db_rep;
	REPMGR_CONNECTION *conn;
	SITE_STRING_BUFFER addr_buf;
	repmgr_netaddr_t addr, myaddr;
	__repmgr_gm_fwd_args fwd;
	__repmgr_site_info_args site_info;
	u_int8_t *p, *response_buf, siteinfo_buf[MAX_MSG_BUF];
	char host_buf[MAXHOSTNAMELEN + 1], *host;
	u_int32_t gen, type;
	size_t len;
	int ret, t_ret;

	db_rep = env->rep_handle;

	LOCK_MUTEX(db_rep->mutex);
	myaddr = SITE_FROM_EID(db_rep->self_eid)->net_addr;
	UNLOCK_MUTEX(db_rep->mutex);
	len = strlen(myaddr.host) + 1;
	DB_INIT_DBT(site_info.host, myaddr.host, len);
	site_info.port = myaddr.port;
	site_info.flags = 0;
	ret = __repmgr_site_info_marshal(env,
	    &site_info, siteinfo_buf, sizeof(siteinfo_buf), &len);
	DB_ASSERT(env, ret == 0);

	conn = NULL;
	response_buf = NULL;
	gen = 0;
	RPRINT(env, (env, DB_VERB_REPMGR_MISC, "try join request to site %s",
	    __repmgr_format_addr_loc(addrp, addr_buf)));
retry:
	if ((ret = make_request_conn(env, addrp, &conn)) != 0)
		return (ret);
	if ((ret = __repmgr_send_sync_msg(env, conn,
	    REPMGR_JOIN_REQUEST, siteinfo_buf, (u_int32_t)len)) != 0)
		goto err;

	if ((ret = read_own_msg(env,
	    conn, &type, &response_buf, &len)) != 0)
		goto err;

	if (type == REPMGR_GM_FAILURE) {
		ret = DB_REP_UNAVAIL;
		goto err;
	}
	if (type == REPMGR_GM_FORWARD) {
		/*
		 * The remote site we thought was master is telling us that some
		 * other site has become master.  Retry with the new master.
		 * However, in order to avoid an endless cycle, only continue
		 * retrying as long as the master gen is advancing.
		 */
		ret = __repmgr_close_connection(env, conn);
		if ((t_ret = __repmgr_destroy_conn(env, conn)) != 0 &&
		    ret == 0)
			ret = t_ret;
		conn = NULL;
		if (ret != 0)
			goto err;

		ret = __repmgr_gm_fwd_unmarshal(env, &fwd,
		    response_buf, len, &p);
		DB_ASSERT(env, ret == 0);
		if (fwd.gen > gen) {
			if (fwd.host.size > MAXHOSTNAMELEN + 1) {
				ret = DB_REP_UNAVAIL;
				goto err;
			}
			host = fwd.host.data;
			host[fwd.host.size-1] = '\0'; /* Just to be sure. */
			(void)strcpy(host_buf, host);
			addr.host = host_buf;
			addr.port = fwd.port;
			addrp = &addr;
			gen = fwd.gen;
			RPRINT(env, (env, DB_VERB_REPMGR_MISC,
		      "will retry join request at forwarded master %s, gen %lu",
				__repmgr_format_addr_loc(addrp, addr_buf),
				(u_long)gen));
			__os_free(env, response_buf);
			response_buf = NULL;
			goto retry;
		} else {
			ret = DB_REP_UNAVAIL;
			goto err;
		}
	}
	if (type == REPMGR_JOIN_SUCCESS)
		ret = __repmgr_refresh_membership(env, response_buf, len);
	else
		ret = DB_REP_UNAVAIL; /* Invalid response: protocol violation */

err:
	if (conn != NULL) {
		if ((t_ret = __repmgr_close_connection(env, conn)) != 0 &&
		    ret != 0)
			ret = t_ret;
		if ((t_ret = __repmgr_destroy_conn(env, conn)) != 0 &&
		    ret != 0)
			ret = t_ret;
	}
	if (response_buf != NULL)
		__os_free(env, response_buf);

	return (ret);
}

/*
 * Reads a whole message, when we expect to get a REPMGR_OWN_MSG.
 */
static int
read_own_msg(env, conn, typep, bufp, lenp)
	ENV *env;
	REPMGR_CONNECTION *conn;
	u_int32_t *typep;
	u_int8_t **bufp;
	size_t *lenp;
{
	__repmgr_msg_hdr_args msg_hdr;
	u_int8_t *buf;
	u_int32_t type;
	size_t size;
	int ret;

	__repmgr_reset_for_reading(conn);
	if ((ret = __repmgr_read_conn(conn)) != 0)
		goto err;
	ret = __repmgr_msg_hdr_unmarshal(env, &msg_hdr,
	    conn->msg_hdr_buf, __REPMGR_MSG_HDR_SIZE, NULL);
	DB_ASSERT(env, ret == 0);

	if ((conn->msg_type = msg_hdr.type) != REPMGR_OWN_MSG) {
		ret = DB_REP_UNAVAIL; /* Protocol violation. */
		goto err;
	}
	type = REPMGR_OWN_MSG_TYPE(msg_hdr);
	if ((size = (size_t)REPMGR_OWN_BUF_SIZE(msg_hdr)) > 0) {
		conn->reading_phase = DATA_PHASE;
		__repmgr_iovec_init(&conn->iovecs);

		if ((ret = __os_malloc(env, size, &buf)) != 0)
			goto err;
		conn->input.rep_message = NULL;

		__repmgr_add_buffer(&conn->iovecs, buf, size);
		if ((ret = __repmgr_read_conn(conn)) != 0) {
			__os_free(env, buf);
			goto err;
		}
		*bufp = buf;
	}

	*typep = type;
	*lenp = size;

err:
	return (ret);
}

static int
make_request_conn(env, addr, connp)
	ENV *env;
	repmgr_netaddr_t *addr;
	REPMGR_CONNECTION **connp;
{
	DBT vi;
	__repmgr_msg_hdr_args msg_hdr;
	__repmgr_version_confirmation_args conf;
	REPMGR_CONNECTION *conn;
	int alloc, ret, unused;

	alloc = FALSE;
	if ((ret = __repmgr_connect(env, addr, &conn, &unused)) != 0)
		return (ret);
	conn->type = APP_CONNECTION;

	/* Read a handshake msg, to get version confirmation and parameters. */
	if ((ret = __repmgr_read_conn(conn)) != 0)
		goto err;
	/*
	 * We can only get here after having read the full 9 bytes that we
	 * expect, so this can't fail.
	 */
	DB_ASSERT(env, conn->reading_phase == SIZES_PHASE);
	ret = __repmgr_msg_hdr_unmarshal(env, &msg_hdr,
	    conn->msg_hdr_buf, __REPMGR_MSG_HDR_SIZE, NULL);
	DB_ASSERT(env, ret == 0);
	__repmgr_iovec_init(&conn->iovecs);
	conn->reading_phase = DATA_PHASE;

	if ((ret = __repmgr_prepare_simple_input(env, conn, &msg_hdr)) != 0)
		goto err;
	alloc = TRUE;

	if ((ret = __repmgr_read_conn(conn)) != 0)
		goto err;

	/*
	 * Analyze the handshake msg, and stash relevant info.
	 */
	if ((ret = __repmgr_find_version_info(env, conn, &vi)) != 0)
		goto err;
	DB_ASSERT(env, vi.size > 0);
	if ((ret = __repmgr_version_confirmation_unmarshal(env,
	    &conf, vi.data, vi.size, NULL)) != 0)
		goto err;

	if (conf.version < GM_MIN_VERSION) {
		ret = DB_REP_UNAVAIL;
		goto err;
	}
	conn->version = conf.version;

err:
	if (alloc) {
		DB_ASSERT(env, conn->input.repmgr_msg.cntrl.size > 0);
		__os_free(env, conn->input.repmgr_msg.cntrl.data);
		DB_ASSERT(env, conn->input.repmgr_msg.rec.size > 0);
		__os_free(env, conn->input.repmgr_msg.rec.data);
	}
	__repmgr_reset_for_reading(conn);
	if (ret == 0)
		*connp = conn;
	else {
		(void)__repmgr_close_connection(env, conn);
		(void)__repmgr_destroy_conn(env, conn);
	}
	return (ret);
}

/*
 * PUBLIC: int __repmgr_site __P((DB_ENV *,
 * PUBLIC:     const char *, u_int, DB_SITE **, u_int32_t));
 */
int
__repmgr_site(dbenv, host, port, sitep, flags)
	DB_ENV *dbenv;
	const char *host;
	u_int port;
	DB_SITE **sitep;
	u_int32_t flags;
{
	int ret;

	if ((ret = __db_fchk(dbenv->env, "repmgr_site", flags, 0)) == 0)
		ret = site_by_addr(dbenv->env, host, port, sitep);

	return ret;
}

static int
site_by_addr(env, host, port, sitep)
	ENV *env;
	const char *host;
	u_int port;
	DB_SITE **sitep;
{
	DB_THREAD_INFO *ip;
	DB_REP *db_rep;
	DB_SITE *dbsite;
	REPMGR_SITE *site;
	int eid, locked, ret;

	COMPQUIET(ip, NULL);
	PANIC_CHECK(env);
	db_rep = env->rep_handle;
	ENV_NOT_CONFIGURED(env, db_rep->region, "repmgr_site", DB_INIT_REP);
	if (APP_IS_BASEAPI(env))
		return (repmgr_only(env, "repmgr_site"));
	if ((ret = addr_chk(env, host, port)) != 0)
		return (ret);

	if (REP_ON(env)) {
		LOCK_MUTEX(db_rep->mutex);
		ENV_ENTER(env, ip);
		locked = TRUE;
	} else
		locked = FALSE;
	ret = __repmgr_find_site(env, host, port, &eid);
	DB_ASSERT(env, IS_VALID_EID(eid));
	site = SITE_FROM_EID(eid);
	/*
	 * Point to the stable, permanent copy of the host name.  That's the one
	 * we want the DB_SITE handle to point to; just like site_by_eid() does.
	 */
	host = site->net_addr.host;
	if (locked) {
		ENV_LEAVE(env, ip);
		UNLOCK_MUTEX(db_rep->mutex);
	}
	if (ret != 0)
		return (ret);

	if ((ret = init_dbsite(env, eid, host, port, &dbsite)) != 0)
		return (ret);

	/* Manipulating a site makes this a replication manager application. */
	APP_SET_REPMGR(env);
	*sitep = dbsite;
	return (0);
}

/*
 * PUBLIC: int __repmgr_site_by_eid __P((DB_ENV *, int, DB_SITE **));
 */
int
__repmgr_site_by_eid(dbenv, eid, sitep)
	DB_ENV *dbenv;
	int eid;
	DB_SITE **sitep;
{
	ENV *env;
	DB_REP *db_rep;
	REPMGR_SITE *site;
	DB_SITE *dbsite;
	int ret;

	env = dbenv->env;
	PANIC_CHECK(env);
	db_rep = env->rep_handle;

	if (eid < 0 || eid >= (int)db_rep->site_cnt)
		return (DB_NOTFOUND);
	site = SITE_FROM_EID(eid);

	if ((ret = init_dbsite(env, eid,
	    site->net_addr.host, site->net_addr.port, &dbsite)) != 0)
		return (ret);
	*sitep = dbsite;
	return (0);
}

static int
init_dbsite(env, eid, host, port, sitep)
	ENV *env;
	int eid;
	const char *host;
	u_int port;
	DB_SITE **sitep;
{
	DB_SITE *dbsite;
	int ret;

	if ((ret = __os_calloc(env, 1, sizeof(DB_SITE), &dbsite)) != 0)
		return (ret);

	dbsite->env = env;
	dbsite->eid = eid;
	dbsite->host = host;
	dbsite->port = port;
	dbsite->flags = (REP_ON(env) ? 0 : DB_SITE_PREOPEN);

	dbsite->get_address = __repmgr_get_site_address;
	dbsite->get_config = __repmgr_get_config;
	dbsite->get_eid = __repmgr_get_eid;
	dbsite->set_config = __repmgr_site_config;
	dbsite->remove = __repmgr_remove_site_pp;
	dbsite->close = __repmgr_site_close;

	*sitep = dbsite;
	return (0);
}

/*
 * PUBLIC: int __repmgr_get_site_address __P((DB_SITE *,
 * PUBLIC:     const char **, u_int *));
 */
int
__repmgr_get_site_address(dbsite, hostp, port)
	DB_SITE *dbsite;
	const char **hostp;
	u_int *port;
{
	if (hostp != NULL)
		*hostp = dbsite->host;
	if (port != NULL)
		*port = dbsite->port;
	return (0);
}

/*
 * PUBLIC: int __repmgr_get_eid __P((DB_SITE *, int *));
 */
int
__repmgr_get_eid(dbsite, eidp)
	DB_SITE *dbsite;
	int *eidp;
{
	int ret;

	if ((ret = refresh_site(dbsite)) != 0)
		return (ret);

	if (F_ISSET(dbsite, DB_SITE_PREOPEN)) {
		__db_errx(dbsite->env, DB_STR("3662",
		    "Can't determine EID before env open"));
		return (EINVAL);
	}
	*eidp = dbsite->eid;
	return (0);
}

/*
 * PUBLIC: int __repmgr_get_config __P((DB_SITE *, u_int32_t, u_int32_t *));
 */
int
__repmgr_get_config(dbsite, which, valuep)
	DB_SITE *dbsite;
	u_int32_t which;
	u_int32_t *valuep;
{
	DB_REP *db_rep;
	DB_THREAD_INFO *ip;
	ENV *env;
	REGINFO *infop;
	REP *rep;
	REPMGR_SITE *site;
	SITEINFO *sites;
	int ret;

	env = dbsite->env;
	db_rep = env->rep_handle;

	if ((ret = refresh_site(dbsite)) != 0)
		return (ret);
	LOCK_MUTEX(db_rep->mutex);
	DB_ASSERT(env, IS_VALID_EID(dbsite->eid));
	site = SITE_FROM_EID(dbsite->eid);
	if (REP_ON(env)) {
		rep = db_rep->region;
		infop = env->reginfo;

		ENV_ENTER(env, ip);
		MUTEX_LOCK(env, rep->mtx_repmgr);
		sites = R_ADDR(infop, rep->siteinfo_off);

		site->config = sites[dbsite->eid].config;

		MUTEX_UNLOCK(env, rep->mtx_repmgr);
		ENV_LEAVE(env, ip);
	}
	*valuep = FLD_ISSET(site->config, which) ? 1 : 0;
	UNLOCK_MUTEX(db_rep->mutex);
	return (0);
}

/*
 * PUBLIC: int __repmgr_site_config __P((DB_SITE *, u_int32_t, u_int32_t));
 */
int
__repmgr_site_config(dbsite, which, value)
	DB_SITE *dbsite;
	u_int32_t which;
	u_int32_t value;
{
	DB_REP *db_rep;
	DB_THREAD_INFO *ip;
	ENV *env;
	REGINFO *infop;
	REP *rep;
	REPMGR_SITE *site;
	SITEINFO *sites;
	int ret;

	env = dbsite->env;
	db_rep = env->rep_handle;

	if ((ret = refresh_site(dbsite)) != 0)
		return (ret);
	switch (which) {
	case DB_BOOTSTRAP_HELPER:
	case DB_REPMGR_PEER:
		if (dbsite->eid == db_rep->self_eid) {
			__db_errx(env, DB_STR("3663",
			    "Site config value not applicable to local site"));
			return (EINVAL);
		}
		break;
	case DB_GROUP_CREATOR:
		/*
		 * Ignore if this is set on remote site.  Users will often
		 * copy and edit a DB_CONFIG for all sites.
		 */
		break;
	case DB_LEGACY:
		/* Applicable to either local or remote site. */
		break;
	case DB_LOCAL_SITE:
		/*
		 * This special case needs extra processing, to set the
		 * "self_eid" index in addition to the flag bit.
		 */
		if ((ret = set_local_site(dbsite, value)) != 0)
			return (ret);
		break;
	default:
		__db_errx(env,
		    DB_STR("3665", "Unrecognized site config value"));
		return (EINVAL);
	}

	DB_ASSERT(env, IS_VALID_EID(dbsite->eid));
	if (REP_ON(env)) {
		rep = db_rep->region;
		infop = env->reginfo;

		LOCK_MUTEX(db_rep->mutex);
		ENV_ENTER(env, ip);
		MUTEX_LOCK(env, rep->mtx_repmgr);
		sites = R_ADDR(infop, rep->siteinfo_off);
		site = SITE_FROM_EID(dbsite->eid);

		/*
		 * Make sure we're up to date with shared memory version.  After
		 * env open, we never set private without also updating shared.
		 * But another process could have set the shared one, so shared
		 * is always "best."
		 */
		site->config = sites[dbsite->eid].config;
		if (value)
			FLD_SET(site->config, which);
		else
			FLD_CLR(site->config, which);
		if (site->config != sites[dbsite->eid].config) {
			sites[dbsite->eid].config = site->config;
			rep->siteinfo_seq++;
		}
		MUTEX_UNLOCK(env, rep->mtx_repmgr);
		ENV_LEAVE(env, ip);
		UNLOCK_MUTEX(db_rep->mutex);
	} else {
		site = SITE_FROM_EID(dbsite->eid);
		if (value)
			FLD_SET(site->config, which);
		else
			FLD_CLR(site->config, which);
	}
	return (0);
}

static int
set_local_site(dbsite, value)
	DB_SITE *dbsite;
	u_int32_t value;
{
	DB_REP *db_rep;
	DB_THREAD_INFO *ip;
	ENV *env;
	REP *rep;
	REPMGR_SITE *site;
	int locked, ret;

	COMPQUIET(rep, NULL);
	COMPQUIET(ip, NULL);
	env = dbsite->env;
	db_rep = env->rep_handle;
	ret = 0;

	locked = FALSE;
	if (REP_ON(env)) {
		rep = db_rep->region;
		LOCK_MUTEX(db_rep->mutex);
		ENV_ENTER(env, ip);
		MUTEX_LOCK(env, rep->mtx_repmgr);
		locked = TRUE;
		/* Make sure we're in sync first. */
		if (IS_VALID_EID(rep->self_eid))
			db_rep->self_eid = rep->self_eid;
	}
	if (!value && db_rep->self_eid == dbsite->eid) {
		__db_errx(env, DB_STR("3666",
		    "A previously given local site may not be unset"));
		ret = EINVAL;
	} else if (IS_VALID_EID(db_rep->self_eid) &&
	    db_rep->self_eid != dbsite->eid) {
		__db_errx(env, DB_STR("3667",
		    "A (different) local site has already been set"));
		ret = EINVAL;
	} else {
		DB_ASSERT(env, IS_VALID_EID(dbsite->eid));
		site = SITE_FROM_EID(dbsite->eid);
		if (FLD_ISSET(site->config,
		    DB_BOOTSTRAP_HELPER | DB_REPMGR_PEER)) {
			__db_errx(env, DB_STR("3668",
		    "Local site cannot have HELPER or PEER attributes"));
			ret = EINVAL;
		}
	}
	if (ret == 0) {
		db_rep->self_eid = dbsite->eid;
		if (locked) {
			rep->self_eid = dbsite->eid;
			rep->siteinfo_seq++;
		}
	}
	if (locked) {
		MUTEX_UNLOCK(env, rep->mtx_repmgr);
		ENV_LEAVE(env, ip);
		UNLOCK_MUTEX(db_rep->mutex);
	}
	return (ret);
}

/*
 * Brings the dbsite's EID up to date, in case it got shuffled around across an
 * env open.
 */
static int
refresh_site(dbsite)
	DB_SITE *dbsite;
{
	DB_REP *db_rep;
	ENV *env;
	REPMGR_SITE *site;

	env = dbsite->env;
	PANIC_CHECK(env);
	if (F_ISSET(dbsite, DB_SITE_PREOPEN) && REP_ON(env)) {
		db_rep = env->rep_handle;
		LOCK_MUTEX(db_rep->mutex);
		site = __repmgr_lookup_site(env, dbsite->host, dbsite->port);
		DB_ASSERT(env, site != NULL);
		dbsite->eid = EID_FROM_SITE(site);
		F_CLR(dbsite, DB_SITE_PREOPEN);
		UNLOCK_MUTEX(db_rep->mutex);
	}
	return (0);
}

static int
__repmgr_remove_site_pp(dbsite)
	DB_SITE *dbsite;
{
	int ret, t_ret;

	ret = __repmgr_remove_site(dbsite);
	/*
	 * The remove() method is documented as a destructor, which means that
	 * absolutely all calls must deallocate the handle, including error
	 * cases, even mutex failures.
	 */
	if ((t_ret = __repmgr_site_close(dbsite)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);
}

static int
__repmgr_remove_site(dbsite)
	DB_SITE *dbsite;
{
	ENV *env;
	DB_REP *db_rep;
	REP *rep;
	REPMGR_CONNECTION *conn;
	repmgr_netaddr_t addr;
	__repmgr_site_info_args site_info;
	u_int8_t *response_buf, siteinfo_buf[MAX_MSG_BUF];
	size_t len;
	u_int32_t type;
	int master, ret, t_ret;

	if ((ret = refresh_site(dbsite)) != 0)
		return (ret);
	env = dbsite->env;
	db_rep = env->rep_handle;
	rep = db_rep->region;

	if (db_rep->repmgr_status != running || !SELECTOR_RUNNING(db_rep)) {
		__db_errx(env, DB_STR("3669", "repmgr is not running"));
		return (EINVAL);
	}

	if (!IS_VALID_EID((master = rep->master_id)))
		return (DB_REP_UNAVAIL);
	LOCK_MUTEX(db_rep->mutex);
	DB_ASSERT(env, IS_VALID_EID(master));
	addr = SITE_FROM_EID(master)->net_addr;
	UNLOCK_MUTEX(db_rep->mutex);

	len = strlen(dbsite->host) + 1;
	DB_INIT_DBT(site_info.host, dbsite->host, len);
	site_info.port = dbsite->port;
	site_info.flags = 0;
	ret = __repmgr_site_info_marshal(env,
	    &site_info, siteinfo_buf, sizeof(siteinfo_buf), &len);
	DB_ASSERT(env, ret == 0);

	conn = NULL;
	response_buf = NULL;
	if ((ret = make_request_conn(env, &addr, &conn)) != 0)
		return (ret);
	if ((ret = __repmgr_send_sync_msg(env, conn,
	    REPMGR_REMOVE_REQUEST, siteinfo_buf, (u_int32_t)len)) != 0)
		goto err;
	if ((ret = read_own_msg(env,
	    conn, &type, &response_buf, &len)) != 0)
		goto err;
	ret = type == REPMGR_REMOVE_SUCCESS ? 0 : DB_REP_UNAVAIL;
err:
	if (conn != NULL) {
		if ((t_ret = __repmgr_close_connection(env, conn)) != 0 &&
		    ret != 0)
			ret = t_ret;
		if ((t_ret = __repmgr_destroy_conn(env, conn)) != 0 &&
		    ret != 0)
			ret = t_ret;
	}
	if (response_buf != NULL)
		__os_free(env, response_buf);
	return (ret);
}

/*
 * PUBLIC: int __repmgr_site_close __P((DB_SITE *));
 */
int
__repmgr_site_close(dbsite)
	DB_SITE *dbsite;
{
	__os_free(dbsite->env, dbsite);
	return (0);
}
