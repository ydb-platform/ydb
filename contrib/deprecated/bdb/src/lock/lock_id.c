/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/lock.h"
#include "dbinc/log.h"

static int __lock_freelocker_int
    __P((DB_LOCKTAB *, DB_LOCKREGION *, DB_LOCKER *, int));

/*
 * __lock_id_pp --
 *	ENV->lock_id pre/post processing.
 *
 * PUBLIC: int __lock_id_pp __P((DB_ENV *, u_int32_t *));
 */
int
__lock_id_pp(dbenv, idp)
	DB_ENV *dbenv;
	u_int32_t *idp;
{
	DB_THREAD_INFO *ip;
	ENV *env;
	int ret;

	env = dbenv->env;

	ENV_REQUIRES_CONFIG(env,
	    env->lk_handle, "DB_ENV->lock_id", DB_INIT_LOCK);

	ENV_ENTER(env, ip);
	REPLICATION_WRAP(env, (__lock_id(env, idp, NULL)), 0, ret);
	ENV_LEAVE(env, ip);
	return (ret);
}

/*
 * __lock_id --
 *	ENV->lock_id.
 *
 * PUBLIC: int  __lock_id __P((ENV *, u_int32_t *, DB_LOCKER **));
 */
int
__lock_id(env, idp, lkp)
	ENV *env;
	u_int32_t *idp;
	DB_LOCKER **lkp;
{
	DB_LOCKER *lk;
	DB_LOCKREGION *region;
	DB_LOCKTAB *lt;
	u_int32_t id, *ids;
	int nids, ret;

	lk = NULL;
	lt = env->lk_handle;
	region = lt->reginfo.primary;
	id = DB_LOCK_INVALIDID;
	ret = 0;

	id = DB_LOCK_INVALIDID;
	lk = NULL;

	LOCK_LOCKERS(env, region);

	/*
	 * Allocate a new lock id.  If we wrap around then we find the minimum
	 * currently in use and make sure we can stay below that.  This code is
	 * similar to code in __txn_begin_int for recovering txn ids.
	 *
	 * Our current valid range can span the maximum valid value, so check
	 * for it and wrap manually.
	 */
	if (region->lock_id == DB_LOCK_MAXID &&
	    region->cur_maxid != DB_LOCK_MAXID)
		region->lock_id = DB_LOCK_INVALIDID;
	if (region->lock_id == region->cur_maxid) {
		if ((ret = __os_malloc(env,
		    sizeof(u_int32_t) * region->nlockers, &ids)) != 0)
			goto err;
		nids = 0;
		SH_TAILQ_FOREACH(lk, &region->lockers, ulinks, __db_locker)
			ids[nids++] = lk->id;
		region->lock_id = DB_LOCK_INVALIDID;
		region->cur_maxid = DB_LOCK_MAXID;
		if (nids != 0)
			__db_idspace(ids, nids,
			    &region->lock_id, &region->cur_maxid);
		__os_free(env, ids);
	}
	id = ++region->lock_id;

	/* Allocate a locker for this id. */
	ret = __lock_getlocker_int(lt, id, 1, &lk);

err:	UNLOCK_LOCKERS(env, region);

	if (idp != NULL)
		*idp = id;
	if (lkp != NULL)
		*lkp = lk;

	return (ret);
}

/*
 * __lock_set_thread_id --
 *	Set the thread_id in an existing locker.
 * PUBLIC: void __lock_set_thread_id __P((void *, pid_t, db_threadid_t));
 */
void
__lock_set_thread_id(lref_arg, pid, tid)
	void *lref_arg;
	pid_t pid;
	db_threadid_t tid;
{
	DB_LOCKER *lref;

	lref = lref_arg;
	lref->pid = pid;
	lref->tid = tid;
}

/*
 * __lock_id_free_pp --
 *	ENV->lock_id_free pre/post processing.
 *
 * PUBLIC: int __lock_id_free_pp __P((DB_ENV *, u_int32_t));
 */
int
__lock_id_free_pp(dbenv, id)
	DB_ENV *dbenv;
	u_int32_t id;
{
	DB_LOCKER *sh_locker;
	DB_LOCKREGION *region;
	DB_LOCKTAB *lt;
	DB_THREAD_INFO *ip;
	ENV *env;
	int handle_check, ret, t_ret;

	env = dbenv->env;

	ENV_REQUIRES_CONFIG(env,
	    env->lk_handle, "DB_ENV->lock_id_free", DB_INIT_LOCK);

	ENV_ENTER(env, ip);

	/* Check for replication block. */
	handle_check = IS_ENV_REPLICATED(env);
	if (handle_check && (ret = __env_rep_enter(env, 0)) != 0) {
		handle_check = 0;
		goto err;
	}

	lt = env->lk_handle;
	region = lt->reginfo.primary;

	LOCK_LOCKERS(env, region);
	if ((ret =
	     __lock_getlocker_int(env->lk_handle, id, 0, &sh_locker)) == 0) {
		if (sh_locker != NULL)
			ret = __lock_freelocker_int(lt, region, sh_locker, 1);
		else {
			__db_errx(env, DB_STR_A("2045",
			    "Unknown locker id: %lx", "%lx"), (u_long)id);
			ret = EINVAL;
		}
	}
	UNLOCK_LOCKERS(env, region);

	if (handle_check && (t_ret = __env_db_rep_exit(env)) != 0 && ret == 0)
		ret = t_ret;

err:	ENV_LEAVE(env, ip);
	return (ret);
}

/*
 * __lock_id_free --
 *	Free a locker id.
 *
 * PUBLIC: int  __lock_id_free __P((ENV *, DB_LOCKER *));
 */
int
__lock_id_free(env, sh_locker)
	ENV *env;
	DB_LOCKER *sh_locker;
{
	DB_LOCKREGION *region;
	DB_LOCKTAB *lt;
	int ret;

	lt = env->lk_handle;
	region = lt->reginfo.primary;
	ret = 0;

	if (sh_locker->nlocks != 0) {
		__db_errx(env, DB_STR("2046",
		    "Locker still has locks"));
		ret = EINVAL;
		goto err;
	}

	LOCK_LOCKERS(env, region);
	ret = __lock_freelocker_int(lt, region, sh_locker, 1);
	UNLOCK_LOCKERS(env, region);

err:	return (ret);
}

/*
 * __lock_id_set --
 *	Set the current locker ID and current maximum unused ID (for
 *	testing purposes only).
 *
 * PUBLIC: int __lock_id_set __P((ENV *, u_int32_t, u_int32_t));
 */
int
__lock_id_set(env, cur_id, max_id)
	ENV *env;
	u_int32_t cur_id, max_id;
{
	DB_LOCKREGION *region;
	DB_LOCKTAB *lt;

	ENV_REQUIRES_CONFIG(env,
	    env->lk_handle, "lock_id_set", DB_INIT_LOCK);

	lt = env->lk_handle;
	region = lt->reginfo.primary;
	region->lock_id = cur_id;
	region->cur_maxid = max_id;

	return (0);
}

/*
 * __lock_getlocker --
 *	Get a locker in the locker hash table.  The create parameter
 * indicates if the locker should be created if it doesn't exist in
 * the table.
 *
 * This must be called with the locker mutex lock if create == 1.
 *
 * PUBLIC: int __lock_getlocker __P((DB_LOCKTAB *,
 * PUBLIC:     u_int32_t, int, DB_LOCKER **));
 * PUBLIC: int __lock_getlocker_int __P((DB_LOCKTAB *,
 * PUBLIC:     u_int32_t, int, DB_LOCKER **));
 */
int
__lock_getlocker(lt, locker, create, retp)
	DB_LOCKTAB *lt;
	u_int32_t locker;
	int create;
	DB_LOCKER **retp;
{
	DB_LOCKREGION *region;
	ENV *env;
	int ret;

	COMPQUIET(region, NULL);
	env = lt->env;
	region = lt->reginfo.primary;

	LOCK_LOCKERS(env, region);
	ret = __lock_getlocker_int(lt, locker, create, retp);
	UNLOCK_LOCKERS(env, region);

	return (ret);
}

int
__lock_getlocker_int(lt, locker, create, retp)
	DB_LOCKTAB *lt;
	u_int32_t locker;
	int create;
	DB_LOCKER **retp;
{
	DB_LOCKER *sh_locker;
	DB_LOCKREGION *region;
	DB_THREAD_INFO *ip;
	ENV *env;
	db_mutex_t mutex;
	u_int32_t i, indx, nlockers;
	int ret;

	env = lt->env;
	region = lt->reginfo.primary;

	LOCKER_HASH(lt, region, locker, indx);

	/*
	 * If we find the locker, then we can just return it.  If we don't find
	 * the locker, then we need to create it.
	 */
	SH_TAILQ_FOREACH(sh_locker, &lt->locker_tab[indx], links, __db_locker)
		if (sh_locker->id == locker)
			break;
	if (sh_locker == NULL && create) {
		nlockers = 0;
		/* Create new locker and then insert it into hash table. */
		if ((ret = __mutex_alloc(env, MTX_LOGICAL_LOCK,
		    DB_MUTEX_LOGICAL_LOCK | DB_MUTEX_SELF_BLOCK,
		    &mutex)) != 0)
			return (ret);
		else
			MUTEX_LOCK(env, mutex);
		if ((sh_locker = SH_TAILQ_FIRST(
		    &region->free_lockers, __db_locker)) == NULL) {
			nlockers = region->stat.st_lockers >> 2;
			/* Just in case. */
			if (nlockers == 0)
				nlockers = 1;
			if (region->stat.st_maxlockers != 0 &&
			    region->stat.st_maxlockers <
			    region->stat.st_lockers + nlockers)
				nlockers = region->stat.st_maxlockers -
				region->stat.st_lockers;
			/*
			 * Don't hold lockers when getting the region,
			 * we could deadlock.  When creating a locker
			 * there is no race since the id allocation
			 * is synchronized.
			 */
			UNLOCK_LOCKERS(env, region);
			LOCK_REGION_LOCK(env);
			/*
			 * If the max memory is not sized for max objects,
			 * allocate as much as possible.
			 */
			F_SET(&lt->reginfo, REGION_TRACKED);
			while (__env_alloc(&lt->reginfo, nlockers *
			    sizeof(struct __db_locker), &sh_locker) != 0)
				if ((nlockers >> 1) == 0)
					break;
			F_CLR(&lt->reginfo, REGION_TRACKED);
			LOCK_REGION_UNLOCK(lt->env);
			LOCK_LOCKERS(env, region);
			for (i = 0; i < nlockers; i++) {
				SH_TAILQ_INSERT_HEAD(&region->free_lockers,
				    sh_locker, links, __db_locker);
				sh_locker++;
			}
			if (nlockers == 0)
				return (__lock_nomem(env, "locker entries"));
			region->stat.st_lockers += nlockers;
			sh_locker = SH_TAILQ_FIRST(
			    &region->free_lockers, __db_locker);
		}
		SH_TAILQ_REMOVE(
		    &region->free_lockers, sh_locker, links, __db_locker);
		++region->nlockers;
#ifdef HAVE_STATISTICS
		STAT_PERFMON2(env, lock, nlockers, region->nlockers, locker);
		if (region->nlockers > region->stat.st_maxnlockers)
			STAT_SET(env, lock, maxnlockers,
			    region->stat.st_maxnlockers,
			    region->nlockers, locker);
#endif
		sh_locker->id = locker;
		env->dbenv->thread_id(
		    env->dbenv, &sh_locker->pid, &sh_locker->tid);
		sh_locker->mtx_locker = mutex;
		sh_locker->dd_id = 0;
		sh_locker->master_locker = INVALID_ROFF;
		sh_locker->parent_locker = INVALID_ROFF;
		SH_LIST_INIT(&sh_locker->child_locker);
		sh_locker->flags = 0;
		SH_LIST_INIT(&sh_locker->heldby);
		sh_locker->nlocks = 0;
		sh_locker->nwrites = 0;
		sh_locker->priority = DB_LOCK_DEFPRIORITY;
		sh_locker->lk_timeout = 0;
		timespecclear(&sh_locker->tx_expire);
		timespecclear(&sh_locker->lk_expire);

		SH_TAILQ_INSERT_HEAD(
		    &lt->locker_tab[indx], sh_locker, links, __db_locker);
		SH_TAILQ_INSERT_HEAD(&region->lockers,
		    sh_locker, ulinks, __db_locker);
		ENV_GET_THREAD_INFO(env, ip);
#ifdef DIAGNOSTIC
		if (ip != NULL)
			ip->dbth_locker = R_OFFSET(&lt->reginfo, sh_locker);
#endif
	}

	*retp = sh_locker;
	return (0);
}

/*
 * __lock_addfamilylocker
 *	Put a locker entry in for a child transaction.
 *
 * PUBLIC: int __lock_addfamilylocker __P((ENV *,
 * PUBLIC:     u_int32_t, u_int32_t, u_int32_t));
 */
int
__lock_addfamilylocker(env, pid, id, is_family)
	ENV *env;
	u_int32_t pid, id, is_family;
{
	DB_LOCKER *lockerp, *mlockerp;
	DB_LOCKREGION *region;
	DB_LOCKTAB *lt;
	int ret;

	COMPQUIET(region, NULL);
	lt = env->lk_handle;
	region = lt->reginfo.primary;
	LOCK_LOCKERS(env, region);

	/* get/create the  parent locker info */
	if ((ret = __lock_getlocker_int(lt, pid, 1, &mlockerp)) != 0)
		goto err;

	/*
	 * We assume that only one thread can manipulate
	 * a single transaction family.
	 * Therefore the master locker cannot go away while
	 * we manipulate it, nor can another child in the
	 * family be created at the same time.
	 */
	if ((ret = __lock_getlocker_int(lt, id, 1, &lockerp)) != 0)
		goto err;

	/* Point to our parent. */
	lockerp->parent_locker = R_OFFSET(&lt->reginfo, mlockerp);

	/* See if this locker is the family master. */
	if (mlockerp->master_locker == INVALID_ROFF)
		lockerp->master_locker = R_OFFSET(&lt->reginfo, mlockerp);
	else {
		lockerp->master_locker = mlockerp->master_locker;
		mlockerp = R_ADDR(&lt->reginfo, mlockerp->master_locker);
	}

	/*
	 * Set the family locker flag, so it is possible to distinguish
	 * between locks held by subtransactions and those with compatible
	 * lockers.
	 */
	if (is_family)
		F_SET(mlockerp, DB_LOCKER_FAMILY_LOCKER);

	/*
	 * Link the child at the head of the master's list.
	 * The guess is when looking for deadlock that
	 * the most recent child is the one that's blocked.
	 */
	SH_LIST_INSERT_HEAD(
	    &mlockerp->child_locker, lockerp, child_link, __db_locker);

err:	UNLOCK_LOCKERS(env, region);

	return (ret);
}

/*
 * __lock_freelocker_int
 *      Common code for deleting a locker; must be called with the
 *	locker bucket locked.
 */
static int
__lock_freelocker_int(lt, region, sh_locker, reallyfree)
	DB_LOCKTAB *lt;
	DB_LOCKREGION *region;
	DB_LOCKER *sh_locker;
	int reallyfree;
{
	ENV *env;
	u_int32_t indx;
	int ret;

	env = lt->env;

	if (SH_LIST_FIRST(&sh_locker->heldby, __db_lock) != NULL) {
		__db_errx(env, DB_STR("2047",
		    "Freeing locker with locks"));
		return (EINVAL);
	}

	/* If this is part of a family, we must fix up its links. */
	if (sh_locker->master_locker != INVALID_ROFF) {
		SH_LIST_REMOVE(sh_locker, child_link, __db_locker);
		sh_locker->master_locker = INVALID_ROFF;
	}

	if (reallyfree) {
		LOCKER_HASH(lt, region, sh_locker->id, indx);
		SH_TAILQ_REMOVE(&lt->locker_tab[indx], sh_locker,
		    links, __db_locker);
		if (sh_locker->mtx_locker != MUTEX_INVALID &&
		    (ret = __mutex_free(env, &sh_locker->mtx_locker)) != 0)
			return (ret);
		SH_TAILQ_INSERT_HEAD(&region->free_lockers, sh_locker,
		    links, __db_locker);
		SH_TAILQ_REMOVE(&region->lockers, sh_locker,
		    ulinks, __db_locker);
		region->nlockers--;
		STAT_PERFMON2(env,
		    lock, nlockers, region->nlockers, sh_locker->id);
	}

	return (0);
}

/*
 * __lock_freelocker
 *	Remove a locker its family from the hash table.
 *
 * This must be called without the locker bucket locked.
 *
 * PUBLIC: int __lock_freelocker  __P((DB_LOCKTAB *, DB_LOCKER *));
 */
int
__lock_freelocker(lt, sh_locker)
	DB_LOCKTAB *lt;
	DB_LOCKER *sh_locker;
{
	DB_LOCKREGION *region;
	ENV *env;
	int ret;

	region = lt->reginfo.primary;
	env = lt->env;

	if (sh_locker == NULL)
		return (0);

	LOCK_LOCKERS(env, region);
	ret = __lock_freelocker_int(lt, region, sh_locker, 1);
	UNLOCK_LOCKERS(env, region);

	return (ret);
}

/*
 * __lock_familyremove
 *	Remove a locker from its family.
 *
 * This must be called without the locker bucket locked.
 *
 * PUBLIC: int __lock_familyremove  __P((DB_LOCKTAB *, DB_LOCKER *));
 */
int
__lock_familyremove(lt, sh_locker)
	DB_LOCKTAB *lt;
	DB_LOCKER *sh_locker;
{
	DB_LOCKREGION *region;
	ENV *env;
	int ret;

	region = lt->reginfo.primary;
	env = lt->env;

	LOCK_LOCKERS(env, region);
	ret = __lock_freelocker_int(lt, region, sh_locker, 0);
	UNLOCK_LOCKERS(env, region);

	return (ret);
}
