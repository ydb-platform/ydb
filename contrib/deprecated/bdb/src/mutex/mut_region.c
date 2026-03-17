/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/log.h"
#include "dbinc/lock.h"
#include "dbinc/mp.h"
#include "dbinc/txn.h"

static db_size_t __mutex_align_size __P((ENV *));
static int __mutex_region_init __P((ENV *, DB_MUTEXMGR *));
static size_t __mutex_region_size __P((ENV *));
static size_t __mutex_region_max __P((ENV *));

/*
 * __mutex_open --
 *	Open a mutex region.
 *
 * PUBLIC: int __mutex_open __P((ENV *, int));
 */
int
__mutex_open(env, create_ok)
	ENV *env;
	int create_ok;
{
	DB_ENV *dbenv;
	DB_MUTEXMGR *mtxmgr;
	DB_MUTEXREGION *mtxregion;
	size_t size;
	u_int32_t cpu_count;
	int ret;
#ifndef HAVE_ATOMIC_SUPPORT
	u_int i;
#endif

	dbenv = env->dbenv;
	if (dbenv->mutex_max == 0 &&
	    dbenv->mutex_cnt == 0 && dbenv->mutex_inc == 0 &&
	    F_ISSET(env, ENV_PRIVATE | ENV_THREAD) == ENV_PRIVATE)
		return (0);

	/*
	 * Initialize the ENV handle information if not already initialized.
	 *
	 * Align mutexes on the byte boundaries specified by the application.
	 */
	if (dbenv->mutex_align == 0)
		dbenv->mutex_align = MUTEX_ALIGN;
	if (dbenv->mutex_tas_spins == 0) {
		cpu_count = __os_cpu_count();
		if ((ret = __mutex_set_tas_spins(dbenv, cpu_count == 1 ?
		    cpu_count : cpu_count * MUTEX_SPINS_PER_PROCESSOR)) != 0)
			return (ret);
	}

	/*
	 * If the user didn't set an absolute value on the number of mutexes
	 * we'll need, figure it out.  We're conservative in our allocation,
	 * we need mutexes for DB handles, group-commit queues and other things
	 * applications allocate at run-time.  The application may have kicked
	 * up our count to allocate its own mutexes, add that in.
	 */
	if (dbenv->mutex_cnt == 0 &&
	    F_ISSET(env, ENV_PRIVATE | ENV_THREAD) != ENV_PRIVATE)
		dbenv->mutex_cnt =
		    __lock_region_mutex_count(env) +
		    __log_region_mutex_count(env) +
		    __memp_region_mutex_count(env) +
		    __txn_region_mutex_count(env);

	if (dbenv->mutex_max != 0 && dbenv->mutex_cnt > dbenv->mutex_max)
		dbenv->mutex_cnt = dbenv->mutex_max;

	/* Create/initialize the mutex manager structure. */
	if ((ret = __os_calloc(env, 1, sizeof(DB_MUTEXMGR), &mtxmgr)) != 0)
		return (ret);

	/* Join/create the mutex region. */
	mtxmgr->reginfo.env = env;
	mtxmgr->reginfo.type = REGION_TYPE_MUTEX;
	mtxmgr->reginfo.id = INVALID_REGION_ID;
	mtxmgr->reginfo.flags = REGION_JOIN_OK;
	size = __mutex_region_size(env);
	if (create_ok)
		F_SET(&mtxmgr->reginfo, REGION_CREATE_OK);
	if ((ret = __env_region_attach(env,
	    &mtxmgr->reginfo, size, size + __mutex_region_max(env))) != 0)
		goto err;

	/* If we created the region, initialize it. */
	if (F_ISSET(&mtxmgr->reginfo, REGION_CREATE))
		if ((ret = __mutex_region_init(env, mtxmgr)) != 0)
			goto err;

	/* Set the local addresses. */
	mtxregion = mtxmgr->reginfo.primary =
	    R_ADDR(&mtxmgr->reginfo, mtxmgr->reginfo.rp->primary);
	mtxmgr->mutex_array = R_ADDR(&mtxmgr->reginfo, mtxregion->mutex_off);

	env->mutex_handle = mtxmgr;

#ifndef HAVE_ATOMIC_SUPPORT
	/* If necessary allocate the atomic emulation mutexes.  */
	if (F_ISSET(&mtxmgr->reginfo, REGION_CREATE))
		for (i = 0; i != MAX_ATOMIC_MUTEXES; i++)
			if ((ret = __mutex_alloc_int(
			    env, 0, MTX_ATOMIC_EMULATION,
			    0, &mtxregion->mtx_atomic[i])) != 0)
				return (ret);
#endif

	return (0);

err:	env->mutex_handle = NULL;
	if (mtxmgr->reginfo.addr != NULL)
		(void)__env_region_detach(env, &mtxmgr->reginfo, 0);

	__os_free(env, mtxmgr);
	return (ret);
}

/*
 * __mutex_region_init --
 *	Initialize a mutex region in shared memory.
 */
static int
__mutex_region_init(env, mtxmgr)
	ENV *env;
	DB_MUTEXMGR *mtxmgr;
{
	DB_ENV *dbenv;
	DB_MUTEX *mutexp;
	DB_MUTEXREGION *mtxregion;
	db_mutex_t mutex;
	int ret;
	void *mutex_array;

	dbenv = env->dbenv;

	COMPQUIET(mutexp, NULL);

	if ((ret = __env_alloc(&mtxmgr->reginfo,
	    sizeof(DB_MUTEXREGION), &mtxmgr->reginfo.primary)) != 0) {
		__db_errx(env, DB_STR("2013",
		    "Unable to allocate memory for the mutex region"));
		return (ret);
	}
	mtxmgr->reginfo.rp->primary =
	    R_OFFSET(&mtxmgr->reginfo, mtxmgr->reginfo.primary);
	mtxregion = mtxmgr->reginfo.primary;
	memset(mtxregion, 0, sizeof(*mtxregion));

	mtxregion->mutex_size = __mutex_align_size(env);

	mtxregion->stat.st_mutex_align = dbenv->mutex_align;
	if (dbenv->mutex_cnt == 0)
		dbenv->mutex_cnt = 1;
	mtxregion->stat.st_mutex_init =
	     mtxregion->stat.st_mutex_cnt = dbenv->mutex_cnt;
	mtxregion->stat.st_mutex_max = dbenv->mutex_max;
	if (mtxregion->stat.st_mutex_max != 0)
		mtxregion->stat.st_mutex_max += dbenv->mutex_inc;
	mtxregion->stat.st_mutex_tas_spins = dbenv->mutex_tas_spins;

	/*
	 * Get a chunk of memory to be used for the mutexes themselves.  Each
	 * piece of the memory must be properly aligned, and that alignment
	 * may be more restrictive than the memory alignment returned by the
	 * underlying allocation code.  We already know how much memory each
	 * mutex in the array will take up, but we need to offset the first
	 * mutex in the array so the array begins properly aligned.
	 *
	 * The OOB mutex (MUTEX_INVALID) is 0.  To make this work, we ignore
	 * the first allocated slot when we build the free list.  We have to
	 * correct the count by 1 here, though, otherwise our counter will be
	 * off by 1.
	 */
	if ((ret = __env_alloc(&mtxmgr->reginfo,
	    mtxregion->stat.st_mutex_align +
	    (mtxregion->stat.st_mutex_cnt + 1) * mtxregion->mutex_size,
	    &mutex_array)) != 0) {
		__db_errx(env, DB_STR("2014",
		    "Unable to allocate memory for mutexes from the region"));
		return (ret);
	}

	mtxregion->mutex_off_alloc = R_OFFSET(&mtxmgr->reginfo, mutex_array);
	mutex_array = ALIGNP_INC(mutex_array, mtxregion->stat.st_mutex_align);
	mtxregion->mutex_off = R_OFFSET(&mtxmgr->reginfo, mutex_array);
	mtxmgr->mutex_array = mutex_array;

	/*
	 * Put the mutexes on a free list and clear the allocated flag.
	 *
	 * The OOB mutex (MUTEX_INVALID) is 0, skip it.
	 *
	 * The comparison is <, not <=, because we're looking ahead one
	 * in each link.
	 */
	env->mutex_handle = mtxmgr;
	if (F_ISSET(env, ENV_PRIVATE)) {
		mutexp = (DB_MUTEX *)mutex_array;
		mutexp++;
		mutexp = ALIGNP_INC(mutexp, mtxregion->stat.st_mutex_align);
		mtxregion->mutex_next = (db_mutex_t)mutexp;
	} else {
		mtxregion->mutex_next = 1;
		mutexp = MUTEXP_SET(env, 1);
	}
	for (mutex = 1; mutex < mtxregion->stat.st_mutex_cnt; ++mutex) {
		mutexp->flags = 0;
		if (F_ISSET(env, ENV_PRIVATE))
			mutexp->mutex_next_link = (db_mutex_t)(mutexp + 1);
		else
			mutexp->mutex_next_link = mutex + 1;
		mutexp++;
		mutexp = ALIGNP_INC(mutexp, mtxregion->stat.st_mutex_align);
	}
	mutexp->flags = 0;
	mutexp->mutex_next_link = MUTEX_INVALID;
	mtxregion->stat.st_mutex_free = mtxregion->stat.st_mutex_cnt;
	mtxregion->stat.st_mutex_inuse = mtxregion->stat.st_mutex_inuse_max = 0;
	if ((ret = __mutex_alloc(env, MTX_MUTEX_REGION, 0, &mutex)) != 0)
		return (ret);
	mtxmgr->reginfo.mtx_alloc = mtxregion->mtx_region = mutex;

	/*
	 * This is the first place we can test mutexes and we need to
	 * know if they're working.  (They CAN fail, for example on
	 * SunOS, when using fcntl(2) for locking and using an
	 * in-memory filesystem as the database environment directory.
	 * But you knew that, I'm sure -- it probably wasn't worth
	 * mentioning.)
	 */
	mutex = MUTEX_INVALID;
	if ((ret =
	    __mutex_alloc(env, MTX_MUTEX_TEST, 0, &mutex) != 0) ||
	    (ret = __mutex_lock(env, mutex)) != 0 ||
	    (ret = __mutex_unlock(env, mutex)) != 0 ||
	    (ret = __mutex_trylock(env, mutex)) != 0 ||
	    (ret = __mutex_unlock(env, mutex)) != 0 ||
	    (ret = __mutex_free(env, &mutex)) != 0) {
		__db_errx(env, DB_STR("2015",
	    "Unable to acquire/release a mutex; check configuration"));
		return (ret);
	}
#ifdef HAVE_SHARED_LATCHES
	if ((ret =
	    __mutex_alloc(env,
		MTX_MUTEX_TEST, DB_MUTEX_SHARED, &mutex) != 0) ||
	    (ret = __mutex_lock(env, mutex)) != 0 ||
	    (ret = __mutex_tryrdlock(env, mutex)) != DB_LOCK_NOTGRANTED ||
	    (ret = __mutex_unlock(env, mutex)) != 0 ||
	    (ret = __mutex_rdlock(env, mutex)) != 0 ||
	    (ret = __mutex_rdlock(env, mutex)) != 0 ||
	    (ret = __mutex_unlock(env, mutex)) != 0 ||
	    (ret = __mutex_unlock(env, mutex)) != 0 ||
	    (ret = __mutex_free(env, &mutex)) != 0) {
		__db_errx(env, DB_STR("2016",
    "Unable to acquire/release a shared latch; check configuration"));
		return (ret);
	}
#endif

	return (0);
}

/*
 * __mutex_env_refresh --
 *	Clean up after the mutex region on a close or failed open.
 *
 * PUBLIC: int __mutex_env_refresh __P((ENV *));
 */
int
__mutex_env_refresh(env)
	ENV *env;
{
	DB_MUTEXMGR *mtxmgr;
	DB_MUTEXREGION *mtxregion;
	REGINFO *reginfo;
	int ret;

	mtxmgr = env->mutex_handle;
	reginfo = &mtxmgr->reginfo;
	mtxregion = mtxmgr->reginfo.primary;

	/*
	 * If a private region, return the memory to the heap.  Not needed for
	 * filesystem-backed or system shared memory regions, that memory isn't
	 * owned by any particular process.
	 */
	if (F_ISSET(env, ENV_PRIVATE)) {
		reginfo->mtx_alloc = MUTEX_INVALID;

#ifdef HAVE_MUTEX_SYSTEM_RESOURCES
		/*
		 * If destroying the mutex region, return any system resources
		 * to the system.
		 */
		__mutex_resource_return(env, reginfo);
#endif
		/* Discard the mutex array. */
		__env_alloc_free(
		    reginfo, R_ADDR(reginfo, mtxregion->mutex_off_alloc));
	}

	/* Detach from the region. */
	ret = __env_region_detach(env, reginfo, 0);

	__os_free(env, mtxmgr);

	env->mutex_handle = NULL;

	return (ret);
}

/*
 * __mutex_align_size --
 *	Return how much memory each mutex will take up if an array of them
 *	are to be properly aligned, individually, within the array.
 */
static db_size_t
__mutex_align_size(env)
	ENV *env;
{
	DB_ENV *dbenv;

	dbenv = env->dbenv;

	return ((db_size_t)DB_ALIGN(sizeof(DB_MUTEX), dbenv->mutex_align));
}

/*
 * __mutex_region_size --
 *	 Return the amount of space needed for the mutex region.
 */
static size_t
__mutex_region_size(env)
	ENV *env;
{
	DB_ENV *dbenv;
	size_t s;

	dbenv = env->dbenv;

	s = sizeof(DB_MUTEXMGR) + 1024;

	/* We discard one mutex for the OOB slot. */
	s += __env_alloc_size(
	    (dbenv->mutex_cnt + 1) *__mutex_align_size(env));

	return (s);
}

/*
 * __mutex_region_max --
 *	 Return the amount of space needed to reach the maximum size.
 */
static size_t
__mutex_region_max(env)
	ENV *env;
{
	DB_ENV *dbenv;
	u_int32_t max;

	dbenv = env->dbenv;

	if ((max = dbenv->mutex_max) == 0) {
		if (F_ISSET(env, ENV_PRIVATE | ENV_THREAD) == ENV_PRIVATE)
			max = dbenv->mutex_inc + 1;
		else
			max = __lock_region_mutex_max(env) +
			    __txn_region_mutex_max(env) +
			    __log_region_mutex_max(env) +
			    dbenv->mutex_inc + 100;
	} else if (max <= dbenv->mutex_cnt)
		return (0);
	else
		max -= dbenv->mutex_cnt;

	return ( __env_alloc_size(max * __mutex_align_size(env)));
}

#ifdef	HAVE_MUTEX_SYSTEM_RESOURCES
/*
 * __mutex_resource_return
 *	Return any system-allocated mutex resources to the system.
 *
 * PUBLIC: void __mutex_resource_return __P((ENV *, REGINFO *));
 */
void
__mutex_resource_return(env, infop)
	ENV *env;
	REGINFO *infop;
{
	DB_MUTEX *mutexp;
	DB_MUTEXMGR *mtxmgr, mtxmgr_st;
	DB_MUTEXREGION *mtxregion;
	db_mutex_t i, indx;
	void *orig_handle, *chunk;
	uintmax_t size;

	/*
	 * This routine is called in two cases: when discarding the regions
	 * from a previous Berkeley DB run, during recovery, and two, when
	 * discarding regions as we shut down the database environment.
	 *
	 * Walk the list of mutexes and destroy any live ones.
	 *
	 * This is just like joining a region -- the REGINFO we're handed is
	 * the same as the one returned by __env_region_attach(), all we have
	 * to do is fill in the links.
	 *
	 * !!!
	 * The region may be corrupted, of course.  We're safe because the
	 * only things we look at are things that are initialized when the
	 * region is created, and never modified after that.
	 */
	memset(&mtxmgr_st, 0, sizeof(mtxmgr_st));
	mtxmgr = &mtxmgr_st;
	mtxmgr->reginfo = *infop;
	mtxregion = mtxmgr->reginfo.primary =
	    R_ADDR(&mtxmgr->reginfo, mtxmgr->reginfo.rp->primary);
	mtxmgr->mutex_array = R_ADDR(&mtxmgr->reginfo, mtxregion->mutex_off);

	/*
	 * This is a little strange, but the mutex_handle is what all of the
	 * underlying mutex routines will use to determine if they should do
	 * any work and to find their information.  Save/restore the handle
	 * around the work loop.
	 *
	 * The OOB mutex (MUTEX_INVALID) is 0, skip it.
	 */
	orig_handle = env->mutex_handle;
	env->mutex_handle = mtxmgr;
	if (F_ISSET(env, ENV_PRIVATE)) {
		mutexp = (DB_MUTEX *)mtxmgr->mutex_array + 1;
		chunk = NULL;
		size = __env_elem_size(env,
		    (void *)mtxregion->mutex_off_alloc);
		size -= sizeof(*mutexp);
	} else
		mutexp = MUTEXP_SET(env, 1);
	for (i = 1; i <= mtxregion->stat.st_mutex_cnt; ++i) {
		if (F_ISSET(env, ENV_PRIVATE))
			indx = (db_mutex_t)mutexp;
		else
			indx = i;
		if (F_ISSET(mutexp, DB_MUTEX_ALLOCATED))
			(void)__mutex_destroy(env, indx);
		mutexp++;
		if (F_ISSET(env, ENV_PRIVATE) &&
		    (size -= sizeof(*mutexp)) < sizeof(*mutexp)) {
			mutexp = __env_get_chunk(&mtxmgr->reginfo,
			    &chunk, &size);
		}
		mutexp = ALIGNP_INC(mutexp, mtxregion->stat.st_mutex_align);
	}
	env->mutex_handle = orig_handle;
}
#endif
