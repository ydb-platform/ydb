/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_am.h"

#ifdef HAVE_STATISTICS
static int __mutex_print_all __P((ENV *, u_int32_t));
static const char *__mutex_print_id __P((int));
static int __mutex_print_stats __P((ENV *, u_int32_t));
static void __mutex_print_summary __P((ENV *));
static int __mutex_stat __P((ENV *, DB_MUTEX_STAT **, u_int32_t));

/*
 * __mutex_stat_pp --
 *	ENV->mutex_stat pre/post processing.
 *
 * PUBLIC: int __mutex_stat_pp __P((DB_ENV *, DB_MUTEX_STAT **, u_int32_t));
 */
int
__mutex_stat_pp(dbenv, statp, flags)
	DB_ENV *dbenv;
	DB_MUTEX_STAT **statp;
	u_int32_t flags;
{
	DB_THREAD_INFO *ip;
	ENV *env;
	int ret;

	env = dbenv->env;

	ENV_REQUIRES_CONFIG(env,
	    env->mutex_handle, "DB_ENV->mutex_stat", DB_INIT_MUTEX);

	if ((ret = __db_fchk(env,
	    "DB_ENV->mutex_stat", flags, DB_STAT_CLEAR)) != 0)
		return (ret);

	ENV_ENTER(env, ip);
	REPLICATION_WRAP(env, (__mutex_stat(env, statp, flags)), 0, ret);
	ENV_LEAVE(env, ip);
	return (ret);
}

/*
 * __mutex_stat --
 *	ENV->mutex_stat.
 */
static int
__mutex_stat(env, statp, flags)
	ENV *env;
	DB_MUTEX_STAT **statp;
	u_int32_t flags;
{
	DB_MUTEXMGR *mtxmgr;
	DB_MUTEXREGION *mtxregion;
	DB_MUTEX_STAT *stats;
	int ret;

	*statp = NULL;
	mtxmgr = env->mutex_handle;
	mtxregion = mtxmgr->reginfo.primary;

	if ((ret = __os_umalloc(env, sizeof(DB_MUTEX_STAT), &stats)) != 0)
		return (ret);

	MUTEX_SYSTEM_LOCK(env);

	/*
	 * Most fields are maintained in the underlying region structure.
	 * Region size and region mutex are not.
	 */
	*stats = mtxregion->stat;
	stats->st_regsize = mtxmgr->reginfo.rp->size;
	stats->st_regmax = mtxmgr->reginfo.rp->max;
	__mutex_set_wait_info(env, mtxregion->mtx_region,
	    &stats->st_region_wait, &stats->st_region_nowait);
	if (LF_ISSET(DB_STAT_CLEAR))
		__mutex_clear(env, mtxregion->mtx_region);

	MUTEX_SYSTEM_UNLOCK(env);

	*statp = stats;
	return (0);
}

/*
 * __mutex_stat_print_pp --
 *	ENV->mutex_stat_print pre/post processing.
 *
 * PUBLIC: int __mutex_stat_print_pp __P((DB_ENV *, u_int32_t));
 */
int
__mutex_stat_print_pp(dbenv, flags)
	DB_ENV *dbenv;
	u_int32_t flags;
{
	DB_THREAD_INFO *ip;
	ENV *env;
	int ret;

	env = dbenv->env;

	ENV_REQUIRES_CONFIG(env,
	    env->mutex_handle, "DB_ENV->mutex_stat_print", DB_INIT_MUTEX);

	if ((ret = __db_fchk(env, "DB_ENV->mutex_stat_print",
	    flags, DB_STAT_ALL | DB_STAT_ALLOC | DB_STAT_CLEAR)) != 0)
		return (ret);

	ENV_ENTER(env, ip);
	REPLICATION_WRAP(env, (__mutex_stat_print(env, flags)), 0, ret);
	ENV_LEAVE(env, ip);
	return (ret);
}

/*
 * __mutex_stat_print
 *	ENV->mutex_stat_print method.
 *
 * PUBLIC: int __mutex_stat_print __P((ENV *, u_int32_t));
 */
int
__mutex_stat_print(env, flags)
	ENV *env;
	u_int32_t flags;
{
	u_int32_t orig_flags;
	int ret;

	orig_flags = flags;
	LF_CLR(DB_STAT_CLEAR | DB_STAT_SUBSYSTEM);
	if (flags == 0 || LF_ISSET(DB_STAT_ALL)) {
		ret = __mutex_print_stats(env, orig_flags);
		__mutex_print_summary(env);
		if (flags == 0 || ret != 0)
			return (ret);
	}

	if (LF_ISSET(DB_STAT_ALL))
		ret = __mutex_print_all(env, orig_flags);

	return (0);
}

static void
__mutex_print_summary(env)
	ENV *env;
{
	DB_MUTEX *mutexp;
	DB_MUTEXMGR *mtxmgr;
	DB_MUTEXREGION *mtxregion;
	void *chunk;
	db_mutex_t i;
	u_int32_t counts[MTX_MAX_ENTRY + 2];
	uintmax_t size;
	int alloc_id;

	mtxmgr = env->mutex_handle;
	mtxregion = mtxmgr->reginfo.primary;
	memset(counts, 0, sizeof(counts));
	size = 0;

	if (F_ISSET(env, ENV_PRIVATE)) {
		mutexp = (DB_MUTEX *)mtxmgr->mutex_array + 1;
		chunk = NULL;
		size = __env_elem_size(env,
		    ROFF_TO_P(mtxregion->mutex_off_alloc));
		size -= sizeof(*mutexp);
	} else
		mutexp = MUTEXP_SET(env, 1);
	for (i = 1; i <= mtxregion->stat.st_mutex_cnt; ++i) {
		if (!F_ISSET(mutexp, DB_MUTEX_ALLOCATED))
			counts[0]++;
		else if (mutexp->alloc_id > MTX_MAX_ENTRY)
			counts[MTX_MAX_ENTRY + 1]++;
		else
			counts[mutexp->alloc_id]++;

		mutexp++;
		if (F_ISSET(env, ENV_PRIVATE) &&
		    (size -= sizeof(*mutexp)) < sizeof(*mutexp)) {
			mutexp =
			    __env_get_chunk(&mtxmgr->reginfo, &chunk, &size);
		}
		mutexp = ALIGNP_INC(mutexp, mtxregion->stat.st_mutex_align);
	}
	__db_msg(env, "Mutex counts");
	__db_msg(env, "%d\tUnallocated", counts[0]);
	for (alloc_id = 1; alloc_id <= MTX_TXN_REGION + 1; alloc_id++)
		if (counts[alloc_id] != 0)
			__db_msg(env, "%lu\t%s",
			    (u_long)counts[alloc_id],
			    __mutex_print_id(alloc_id));

}

/*
 * __mutex_print_stats --
 *	Display default mutex region statistics.
 */
static int
__mutex_print_stats(env, flags)
	ENV *env;
	u_int32_t flags;
{
	DB_MUTEX_STAT *sp;
	int ret;

	if ((ret = __mutex_stat(env, &sp, LF_ISSET(DB_STAT_CLEAR))) != 0)
		return (ret);

	if (LF_ISSET(DB_STAT_ALL))
		__db_msg(env, "Default mutex region information:");

	__db_dlbytes(env, "Mutex region size",
	    (u_long)0, (u_long)0, (u_long)sp->st_regsize);
	__db_dlbytes(env, "Mutex region max size",
	    (u_long)0, (u_long)0, (u_long)sp->st_regmax);
	__db_dl_pct(env,
	    "The number of region locks that required waiting",
	    (u_long)sp->st_region_wait, DB_PCT(sp->st_region_wait,
	    sp->st_region_wait + sp->st_region_nowait), NULL);
	STAT_ULONG("Mutex alignment", sp->st_mutex_align);
	STAT_ULONG("Mutex test-and-set spins", sp->st_mutex_tas_spins);
	STAT_ULONG("Mutex initial count", sp->st_mutex_init);
	STAT_ULONG("Mutex total count", sp->st_mutex_cnt);
	STAT_ULONG("Mutex max count", sp->st_mutex_max);
	STAT_ULONG("Mutex free count", sp->st_mutex_free);
	STAT_ULONG("Mutex in-use count", sp->st_mutex_inuse);
	STAT_ULONG("Mutex maximum in-use count", sp->st_mutex_inuse_max);

	__os_ufree(env, sp);

	return (0);
}

/*
 * __mutex_print_all --
 *	Display debugging mutex region statistics.
 */
static int
__mutex_print_all(env, flags)
	ENV *env;
	u_int32_t flags;
{
	static const FN fn[] = {
		{ DB_MUTEX_ALLOCATED,		"alloc" },
		{ DB_MUTEX_LOCKED,		"locked" },
		{ DB_MUTEX_LOGICAL_LOCK,	"logical" },
		{ DB_MUTEX_PROCESS_ONLY,	"process-private" },
		{ DB_MUTEX_SELF_BLOCK,		"self-block" },
		{ 0,				NULL }
	};
	DB_MSGBUF mb, *mbp;
	DB_MUTEX *mutexp;
	DB_MUTEXMGR *mtxmgr;
	DB_MUTEXREGION *mtxregion;
	db_mutex_t i;
	uintmax_t size;
	void *chunk;

	DB_MSGBUF_INIT(&mb);
	mbp = &mb;

	mtxmgr = env->mutex_handle;
	mtxregion = mtxmgr->reginfo.primary;

	__db_print_reginfo(env, &mtxmgr->reginfo, "Mutex", flags);
	__db_msg(env, "%s", DB_GLOBAL(db_line));

	__db_msg(env, "DB_MUTEXREGION structure:");
	__mutex_print_debug_single(env,
	    "DB_MUTEXREGION region mutex", mtxregion->mtx_region, flags);
	STAT_ULONG("Size of the aligned mutex", mtxregion->mutex_size);
	STAT_ULONG("Next free mutex", mtxregion->mutex_next);

	/*
	 * The OOB mutex (MUTEX_INVALID) is 0, skip it.
	 *
	 * We're not holding the mutex region lock, so we're racing threads of
	 * control allocating mutexes.  That's OK, it just means we display or
	 * clear statistics while mutexes are moving.
	 */
	__db_msg(env, "%s", DB_GLOBAL(db_line));
	__db_msg(env, "mutex\twait/nowait, pct wait, holder, flags");
	size = 0;
	if (F_ISSET(env, ENV_PRIVATE)) {
		mutexp = (DB_MUTEX *)mtxmgr->mutex_array + 1;
		chunk = NULL;
		size = __env_elem_size(env,
		    ROFF_TO_P(mtxregion->mutex_off_alloc));
		size -= sizeof(*mutexp);
	} else
		mutexp = MUTEXP_SET(env, 1);
	for (i = 1; i <= mtxregion->stat.st_mutex_cnt; ++i) {
		if (F_ISSET(mutexp, DB_MUTEX_ALLOCATED)) {
			__db_msgadd(env, mbp, "%5lu\t", (u_long)i);

			__mutex_print_debug_stats(env, mbp,
			    F_ISSET(env, ENV_PRIVATE) ?
			    (db_mutex_t)mutexp : i, flags);

			if (mutexp->alloc_id != 0)
				__db_msgadd(env, mbp,
				    ", %s", __mutex_print_id(mutexp->alloc_id));

			__db_prflags(env, mbp, mutexp->flags, fn, " (", ")");

			DB_MSGBUF_FLUSH(env, mbp);
		}

		mutexp++;
		if (F_ISSET(env, ENV_PRIVATE) &&
		    (size -= sizeof(*mutexp)) < sizeof(*mutexp)) {
			mutexp =
			    __env_get_chunk(&mtxmgr->reginfo, &chunk, &size);
		}
		mutexp = ALIGNP_INC(mutexp, mtxregion->stat.st_mutex_align);
	}

	return (0);
}

/*
 * __mutex_print_debug_single --
 *	Print mutex internal debugging statistics for a single mutex on a
 *	single output line.
 *
 * PUBLIC: void __mutex_print_debug_single
 * PUBLIC:          __P((ENV *, const char *, db_mutex_t, u_int32_t));
 */
void
__mutex_print_debug_single(env, tag, mutex, flags)
	ENV *env;
	const char *tag;
	db_mutex_t mutex;
	u_int32_t flags;
{
	DB_MSGBUF mb, *mbp;

	DB_MSGBUF_INIT(&mb);
	mbp = &mb;

	if (LF_ISSET(DB_STAT_SUBSYSTEM))
		LF_CLR(DB_STAT_CLEAR);
	__db_msgadd(env, mbp, "%lu\t%s ", (u_long)mutex, tag);
	__mutex_print_debug_stats(env, mbp, mutex, flags);
	DB_MSGBUF_FLUSH(env, mbp);
}

/*
 * __mutex_print_debug_stats --
 *	Print mutex internal debugging statistics, that is, the statistics
 *	in the [] square brackets.
 *
 * PUBLIC: void __mutex_print_debug_stats
 * PUBLIC:          __P((ENV *, DB_MSGBUF *, db_mutex_t, u_int32_t));
 */
void
__mutex_print_debug_stats(env, mbp, mutex, flags)
	ENV *env;
	DB_MSGBUF *mbp;
	db_mutex_t mutex;
	u_int32_t flags;
{
	DB_ENV *dbenv;
	DB_MUTEX *mutexp;
	u_long value;
	char buf[DB_THREADID_STRLEN];
#if defined(HAVE_SHARED_LATCHES) && (defined(HAVE_MUTEX_HYBRID) || \
    !defined(HAVE_MUTEX_PTHREADS))
	int sharecount;
#endif

	if (mutex == MUTEX_INVALID) {
		__db_msgadd(env, mbp, "[!Set]");
		return;
	}

	dbenv = env->dbenv;
	mutexp = MUTEXP_SET(env, mutex);

	__db_msgadd(env, mbp, "[");
	if ((value = mutexp->mutex_set_wait) < 10000000)
		__db_msgadd(env, mbp, "%lu", value);
	else
		__db_msgadd(env, mbp, "%luM", value / 1000000);
	if ((value = mutexp->mutex_set_nowait) < 10000000)
		__db_msgadd(env, mbp, "/%lu", value);
	else
		__db_msgadd(env, mbp, "/%luM", value / 1000000);

	__db_msgadd(env, mbp, " %d%% ",
	    DB_PCT(mutexp->mutex_set_wait,
	    mutexp->mutex_set_wait + mutexp->mutex_set_nowait));

#if defined(HAVE_SHARED_LATCHES)
	if (F_ISSET(mutexp, DB_MUTEX_SHARED)) {
		__db_msgadd(env, mbp, " rd ");
		if ((value = mutexp->mutex_set_rd_wait) < 10000000)
			__db_msgadd(env, mbp, "%lu", value);
		else
			__db_msgadd(env, mbp, "%luM", value / 1000000);
		if ((value = mutexp->mutex_set_rd_nowait) < 10000000)
			__db_msgadd(env, mbp, "/%lu", value);
		else
			__db_msgadd(env, mbp, "/%luM", value / 1000000);
		__db_msgadd(env, mbp, " %d%% ",
		    DB_PCT(mutexp->mutex_set_rd_wait,
		    mutexp->mutex_set_rd_wait + mutexp->mutex_set_rd_nowait));
	}
#endif

	if (F_ISSET(mutexp, DB_MUTEX_LOCKED))
		__db_msgadd(env, mbp, "%s]",
		    dbenv->thread_id_string(dbenv,
		    mutexp->pid, mutexp->tid, buf));
	/* Pthreads-based shared latches do not expose the share count. */
#if defined(HAVE_SHARED_LATCHES) && (defined(HAVE_MUTEX_HYBRID) || \
    !defined(HAVE_MUTEX_PTHREADS))
	else if (F_ISSET(mutexp, DB_MUTEX_SHARED) &&
	    (sharecount = atomic_read(&mutexp->sharecount)) != 0) {
		if (sharecount == 1)
			__db_msgadd(env, mbp, "1 reader");
		else
			__db_msgadd(env, mbp, "%d readers", sharecount);
		/* Show the thread which last acquired the latch. */
		__db_msgadd(env, mbp, " %s]",
		    dbenv->thread_id_string(dbenv,
		    mutexp->pid, mutexp->tid, buf));
	}
#endif
	else
		__db_msgadd(env, mbp, "!Own]");

#ifdef HAVE_MUTEX_HYBRID
	if (mutexp->hybrid_wait != 0 || mutexp->hybrid_wakeup != 0)
		__db_msgadd(env, mbp, " <wakeups %d/%d>",
		    mutexp->hybrid_wait, mutexp->hybrid_wakeup);
#endif

	if (LF_ISSET(DB_STAT_CLEAR))
		__mutex_clear(env, mutex);
}

static const char *
__mutex_print_id(alloc_id)
	int alloc_id;
{
	switch (alloc_id) {
	case MTX_APPLICATION:		return ("application allocated");
	case MTX_ATOMIC_EMULATION:	return ("atomic emulation");
	case MTX_DB_HANDLE:		return ("db handle");
	case MTX_ENV_DBLIST:		return ("env dblist");
	case MTX_ENV_EXCLDBLIST:	return ("env exclusive dblist");
	case MTX_ENV_HANDLE:		return ("env handle");
	case MTX_ENV_REGION:		return ("env region");
	case MTX_LOCK_REGION:		return ("lock region");
	case MTX_LOGICAL_LOCK:		return ("logical lock");
	case MTX_LOG_FILENAME:		return ("log filename");
	case MTX_LOG_FLUSH:		return ("log flush");
	case MTX_LOG_HANDLE:		return ("log handle");
	case MTX_LOG_REGION:		return ("log region");
	case MTX_MPOOLFILE_HANDLE:	return ("mpoolfile handle");
	case MTX_MPOOL_BH:		return ("mpool buffer");
	case MTX_MPOOL_FH:		return ("mpool filehandle");
	case MTX_MPOOL_FILE_BUCKET:	return ("mpool file bucket");
	case MTX_MPOOL_HANDLE:		return ("mpool handle");
	case MTX_MPOOL_HASH_BUCKET:	return ("mpool hash bucket");
	case MTX_MPOOL_REGION:		return ("mpool region");
	case MTX_MUTEX_REGION:		return ("mutex region");
	case MTX_MUTEX_TEST:		return ("mutex test");
	case MTX_REPMGR:		return ("replication manager");
	case MTX_REP_CHKPT:		return ("replication checkpoint");
	case MTX_REP_DATABASE:		return ("replication database");
	case MTX_REP_DIAG:		return ("replication diagnostics");
	case MTX_REP_EVENT:		return ("replication event");
	case MTX_REP_REGION:		return ("replication region");
	case MTX_REP_START:		return ("replication role config");
	case MTX_REP_WAITER:		return ("replication txn apply");
	case MTX_SEQUENCE:		return ("sequence");
	case MTX_TWISTER:		return ("twister");
	case MTX_TCL_EVENTS:		return ("Tcl events");
	case MTX_TXN_ACTIVE:		return ("txn active list");
	case MTX_TXN_CHKPT:		return ("transaction checkpoint");
	case MTX_TXN_COMMIT:		return ("txn commit");
	case MTX_TXN_MVCC:		return ("txn mvcc");
	case MTX_TXN_REGION:		return ("txn region");
	default:			return ("unknown mutex type");
	/* NOTREACHED */
	}
}

/*
 * __mutex_set_wait_info --
 *	Return mutex statistics.
 *
 * PUBLIC: void __mutex_set_wait_info
 * PUBLIC:	__P((ENV *, db_mutex_t, uintmax_t *, uintmax_t *));
 */
void
__mutex_set_wait_info(env, mutex, waitp, nowaitp)
	ENV *env;
	db_mutex_t mutex;
	uintmax_t *waitp, *nowaitp;
{
	DB_MUTEX *mutexp;

	if (mutex == MUTEX_INVALID) {
		*waitp = 0;
		*nowaitp = 0;
		return;
	}
	mutexp = MUTEXP_SET(env, mutex);

	*waitp = mutexp->mutex_set_wait;
	*nowaitp = mutexp->mutex_set_nowait;
}

/*
 * __mutex_clear --
 *	Clear mutex statistics.
 *
 * PUBLIC: void __mutex_clear __P((ENV *, db_mutex_t));
 */
void
__mutex_clear(env, mutex)
	ENV *env;
	db_mutex_t mutex;
{
	DB_MUTEX *mutexp;

	if (!MUTEX_ON(env))
		return;

	mutexp = MUTEXP_SET(env, mutex);

	mutexp->mutex_set_wait = mutexp->mutex_set_nowait = 0;
#ifdef HAVE_SHARED_LATCHES
	mutexp->mutex_set_rd_wait = mutexp->mutex_set_rd_nowait = 0;
#endif
#ifdef HAVE_MUTEX_HYBRID
	mutexp->hybrid_wait = mutexp->hybrid_wakeup = 0;
#endif
}

#else /* !HAVE_STATISTICS */

int
__mutex_stat_pp(dbenv, statp, flags)
	DB_ENV *dbenv;
	DB_MUTEX_STAT **statp;
	u_int32_t flags;
{
	COMPQUIET(statp, NULL);
	COMPQUIET(flags, 0);

	return (__db_stat_not_built(dbenv->env));
}

int
__mutex_stat_print_pp(dbenv, flags)
	DB_ENV *dbenv;
	u_int32_t flags;
{
	COMPQUIET(flags, 0);

	return (__db_stat_not_built(dbenv->env));
}
#endif
