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

static int  __lock_region_init __P((ENV *, DB_LOCKTAB *));

/*
 * The conflict arrays are set up such that the row is the lock you are
 * holding and the column is the lock that is desired.
 */
#define	DB_LOCK_RIW_N	9
static const u_int8_t db_riw_conflicts[] = {
/*         N   R   W   WT  IW  IR  RIW DR  WW */
/*   N */  0,  0,  0,  0,  0,  0,  0,  0,  0,
/*   R */  0,  0,  1,  0,  1,  0,  1,  0,  1,
/*   W */  0,  1,  1,  1,  1,  1,  1,  1,  1,
/*  WT */  0,  0,  0,  0,  0,  0,  0,  0,  0,
/*  IW */  0,  1,  1,  0,  0,  0,  0,  1,  1,
/*  IR */  0,  0,  1,  0,  0,  0,  0,  0,  1,
/* RIW */  0,  1,  1,  0,  0,  0,  0,  1,  1,
/*  DR */  0,  0,  1,  0,  1,  0,  1,  0,  0,
/*  WW */  0,  1,  1,  0,  1,  1,  1,  0,  1
};

/*
 * This conflict array is used for concurrent db access (CDB).  It uses
 * the same locks as the db_riw_conflicts array, but adds an IW mode to
 * be used for write cursors.
 */
#define	DB_LOCK_CDB_N	5
static const u_int8_t db_cdb_conflicts[] = {
	/*		N	R	W	WT	IW */
	/*   N */	0,	0,	0,	0,	0,
	/*   R */	0,	0,	1,	0,	0,
	/*   W */	0,	1,	1,	1,	1,
	/*  WT */	0,	0,	0,	0,	0,
	/*  IW */	0,	0,	1,	0,	1
};

/*
 * __lock_open --
 *	Internal version of lock_open: only called from ENV->open.
 *
 * PUBLIC: int __lock_open __P((ENV *));
 */
int
__lock_open(env)
	ENV *env;
{
	DB_ENV *dbenv;
	DB_LOCKREGION *region;
	DB_LOCKTAB *lt;
	int region_locked, ret;

	dbenv = env->dbenv;
	region_locked = 0;

	/* Create the lock table structure. */
	if ((ret = __os_calloc(env, 1, sizeof(DB_LOCKTAB), &lt)) != 0)
		return (ret);
	lt->env = env;

	/* Join/create the lock region. */
	if ((ret = __env_region_share(env, &lt->reginfo)) != 0)
		goto err;

	/* If we created the region, initialize it. */
	if (F_ISSET(&lt->reginfo, REGION_CREATE))
		if ((ret = __lock_region_init(env, lt)) != 0)
			goto err;

	/* Set the local addresses. */
	region = lt->reginfo.primary =
	    R_ADDR(&lt->reginfo, ((REGENV *)env->reginfo->primary)->lt_primary);

	/* Set remaining pointers into region. */
	lt->conflicts = R_ADDR(&lt->reginfo, region->conf_off);
	lt->obj_tab = R_ADDR(&lt->reginfo, region->obj_off);
#ifdef HAVE_STATISTICS
	lt->obj_stat = R_ADDR(&lt->reginfo, region->stat_off);
#endif
	lt->part_array = R_ADDR(&lt->reginfo, region->part_off);
	lt->locker_tab = R_ADDR(&lt->reginfo, region->locker_off);

	env->lk_handle = lt;
	lt->reginfo.mtx_alloc = region->mtx_region;

	LOCK_REGION_LOCK(env);
	region_locked = 1;

	if (dbenv->lk_detect != DB_LOCK_NORUN) {
		/*
		 * Check for incompatible automatic deadlock detection requests.
		 * There are scenarios where changing the detector configuration
		 * is reasonable, but we disallow them guessing it is likely to
		 * be an application error.
		 *
		 * We allow applications to turn on the lock detector, and we
		 * ignore attempts to set it to the default or current value.
		 */
		if (region->detect != DB_LOCK_NORUN &&
		    dbenv->lk_detect != DB_LOCK_DEFAULT &&
		    region->detect != dbenv->lk_detect) {
			__db_errx(env, DB_STR("2041",
			    "lock_open: incompatible deadlock detector mode"));
			ret = EINVAL;
			goto err;
		}
		if (region->detect == DB_LOCK_NORUN)
			region->detect = dbenv->lk_detect;
	}

	/*
	 * A process joining the region may have reset the lock and transaction
	 * timeouts.
	 */
	if (dbenv->lk_timeout != 0)
		region->lk_timeout = dbenv->lk_timeout;
	if (dbenv->tx_timeout != 0)
		region->tx_timeout = dbenv->tx_timeout;

	LOCK_REGION_UNLOCK(env);
	region_locked = 0;

	return (0);

err:	if (lt->reginfo.addr != NULL) {
		if (region_locked)
			LOCK_REGION_UNLOCK(env);
		(void)__env_region_detach(env, &lt->reginfo, 0);
	}
	env->lk_handle = NULL;

	__os_free(env, lt);
	return (ret);
}

/*
 * __lock_region_init --
 *	Initialize the lock region.
 */
static int
__lock_region_init(env, lt)
	ENV *env;
	DB_LOCKTAB *lt;
{
	const u_int8_t *lk_conflicts;
	struct __db_lock *lp;
	DB_ENV *dbenv;
	DB_LOCKER *lidp;
	DB_LOCKOBJ *op;
	DB_LOCKREGION *region;
	DB_LOCKPART *part;
	u_int32_t extra_locks, extra_objects, i, j, max;
	u_int8_t *addr;
	int lk_modes, ret;

	dbenv = env->dbenv;

	if ((ret = __env_alloc(&lt->reginfo,
	    sizeof(DB_LOCKREGION), &lt->reginfo.primary)) != 0)
		goto mem_err;
	((REGENV *)env->reginfo->primary)->lt_primary =
	     R_OFFSET(&lt->reginfo, lt->reginfo.primary);
	region = lt->reginfo.primary;
	memset(region, 0, sizeof(*region));

	/* We share the region so we need the same mutex. */
	region->mtx_region = ((REGENV *)env->reginfo->primary)->mtx_regenv;

	/* Select a conflict matrix if none specified. */
	if (dbenv->lk_modes == 0)
		if (CDB_LOCKING(env)) {
			lk_modes = DB_LOCK_CDB_N;
			lk_conflicts = db_cdb_conflicts;
		} else {
			lk_modes = DB_LOCK_RIW_N;
			lk_conflicts = db_riw_conflicts;
		}
	else {
		lk_modes = dbenv->lk_modes;
		lk_conflicts = dbenv->lk_conflicts;
	}

	region->need_dd = 0;
	timespecclear(&region->next_timeout);
	region->detect = DB_LOCK_NORUN;
	region->lk_timeout = dbenv->lk_timeout;
	region->tx_timeout = dbenv->tx_timeout;
	region->locker_t_size = dbenv->locker_t_size;
	region->object_t_size = dbenv->object_t_size;
	region->part_t_size = dbenv->lk_partitions;
	region->lock_id = 0;
	region->cur_maxid = DB_LOCK_MAXID;
	region->nmodes = lk_modes;
	memset(&region->stat, 0, sizeof(region->stat));
	region->stat.st_maxlocks = dbenv->lk_max;
	region->stat.st_maxlockers = dbenv->lk_max_lockers;
	region->stat.st_maxobjects = dbenv->lk_max_objects;
	region->stat.st_initlocks = region->stat.st_locks = dbenv->lk_init;
	region->stat.st_initlockers =
	     region->stat.st_lockers = dbenv->lk_init_lockers;
	region->stat.st_initobjects  =
	     region->stat.st_objects = dbenv->lk_init_objects;
	region->stat.st_partitions = dbenv->lk_partitions;
	region->stat.st_tablesize = dbenv->object_t_size;

	/* Allocate room for the conflict matrix and initialize it. */
	if ((ret = __env_alloc(
	    &lt->reginfo, (size_t)(lk_modes * lk_modes), &addr)) != 0)
		goto mem_err;
	memcpy(addr, lk_conflicts, (size_t)(lk_modes * lk_modes));
	region->conf_off = R_OFFSET(&lt->reginfo, addr);

	/* Allocate room for the object hash table and initialize it. */
	if ((ret = __env_alloc(&lt->reginfo,
	    region->object_t_size * sizeof(DB_HASHTAB), &addr)) != 0)
		goto mem_err;
	__db_hashinit(addr, region->object_t_size);
	region->obj_off = R_OFFSET(&lt->reginfo, addr);

#ifdef HAVE_STATISTICS
	/* Allocate room for the object hash stats table and initialize it. */
	if ((ret = __env_alloc(&lt->reginfo,
	    region->object_t_size * sizeof(DB_LOCK_HSTAT), &addr)) != 0)
		goto mem_err;
	memset(addr, 0, region->object_t_size * sizeof(DB_LOCK_HSTAT));
	region->stat_off = R_OFFSET(&lt->reginfo, addr);
#endif

	/* Allocate room for the partition table and initialize its mutexes. */
	if ((ret = __env_alloc(&lt->reginfo,
	    region->part_t_size * sizeof(DB_LOCKPART), &part)) != 0)
		goto mem_err;
	memset(part, 0, region->part_t_size * sizeof(DB_LOCKPART));
	region->part_off = R_OFFSET(&lt->reginfo, part);
	for (i = 0; i < region->part_t_size; i++) {
		if ((ret = __mutex_alloc(
		    env, MTX_LOCK_REGION, 0, &part[i].mtx_part)) != 0)
			return (ret);
	}
	if ((ret = __mutex_alloc(
	    env, MTX_LOCK_REGION, 0, &region->mtx_dd)) != 0)
		return (ret);

	if ((ret = __mutex_alloc(
	    env, MTX_LOCK_REGION, 0, &region->mtx_lockers)) != 0)
		return (ret);

	/* Allocate room for the locker hash table and initialize it. */
	if ((ret = __env_alloc(&lt->reginfo,
	    region->locker_t_size * sizeof(DB_HASHTAB), &addr)) != 0)
		goto mem_err;
	__db_hashinit(addr, region->locker_t_size);
	region->locker_off = R_OFFSET(&lt->reginfo, addr);

	SH_TAILQ_INIT(&region->dd_objs);

	/*
	 * If the locks and objects don't divide evenly, spread them around.
	 */
	extra_locks = region->stat.st_locks -
	    ((region->stat.st_locks / region->part_t_size) *
	    region->part_t_size);
	extra_objects = region->stat.st_objects -
	    ((region->stat.st_objects / region->part_t_size) *
	    region->part_t_size);
	for (j = 0; j < region->part_t_size; j++) {
		/* Initialize locks onto a free list. */
		SH_TAILQ_INIT(&part[j].free_locks);
		max = region->stat.st_locks / region->part_t_size;
		if (extra_locks > 0) {
			max++;
			extra_locks--;
		}

		if ((ret =
			__env_alloc(&lt->reginfo,
			    sizeof(struct __db_lock) * max,
			    &lp)) != 0)
			goto mem_err;
		part[j].lock_mem_off = R_OFFSET(&lt->reginfo, lp);
		for (i = 0; i < max; ++i) {
			memset(lp, 0, sizeof(*lp));
			lp->status = DB_LSTAT_FREE;
			SH_TAILQ_INSERT_HEAD(
			    &part[j].free_locks, lp, links, __db_lock);
			++lp;
		}

		/* Initialize objects onto a free list.  */
		max = region->stat.st_objects / region->part_t_size;
		if (extra_objects > 0) {
			max++;
			extra_objects--;
		}
		SH_TAILQ_INIT(&part[j].free_objs);

		if ((ret =
			__env_alloc(&lt->reginfo,
			    sizeof(DB_LOCKOBJ) * max,
			    &op)) != 0)
			goto mem_err;
		part[j].lockobj_mem_off = R_OFFSET(&lt->reginfo, op);
		for (i = 0; i < max; ++i) {
			memset(op, 0, sizeof(*op));
			SH_TAILQ_INSERT_HEAD(
			    &part[j].free_objs, op, links, __db_lockobj);
			++op;
		}
	}

	/* Initialize lockers onto a free list.  */
	SH_TAILQ_INIT(&region->lockers);
	SH_TAILQ_INIT(&region->free_lockers);
	if ((ret =
		__env_alloc(&lt->reginfo,
		    sizeof(DB_LOCKER) * region->stat.st_lockers,
		    &lidp)) != 0)
		goto mem_err;

	region->locker_mem_off = R_OFFSET(&lt->reginfo, lidp);
	for (i = 0; i < region->stat.st_lockers; ++i) {
		SH_TAILQ_INSERT_HEAD(
			&region->free_lockers, lidp, links, __db_locker);
		++lidp;
	}
	return (0);
mem_err:		__db_errx(env, DB_STR("2042",
			    "unable to allocate memory for the lock table"));
			return (ret);
		}

/*
 * __lock_env_refresh --
 *	Clean up after the lock system on a close or failed open.
 *
 * PUBLIC: int __lock_env_refresh __P((ENV *));
 */
int
__lock_env_refresh(env)
	ENV *env;
{
	DB_LOCKREGION *lr;
	DB_LOCKTAB *lt;
	REGINFO *reginfo;
	u_int32_t j;
	int ret;

	lt = env->lk_handle;
	reginfo = &lt->reginfo;
	lr = reginfo->primary;

	/*
	 * If a private region, return the memory to the heap.  Not needed for
	 * filesystem-backed or system shared memory regions, that memory isn't
	 * owned by any particular process.
	 */
	if (F_ISSET(env, ENV_PRIVATE)) {
		reginfo->mtx_alloc = MUTEX_INVALID;
		/* Discard the conflict matrix. */
		__env_alloc_free(reginfo, R_ADDR(reginfo, lr->conf_off));

		/* Discard the object hash table. */
		__env_alloc_free(reginfo, R_ADDR(reginfo, lr->obj_off));

		/* Discard the locker hash table. */
		__env_alloc_free(reginfo, R_ADDR(reginfo, lr->locker_off));

		/* Discard the object hash stat table. */
		__env_alloc_free(reginfo, R_ADDR(reginfo, lr->stat_off));
		for (j = 0; j < lr->part_t_size; j++) {
			SH_TAILQ_INIT(&FREE_OBJS(lt, j));
			SH_TAILQ_INIT(&FREE_LOCKS(lt, j));
			__env_alloc_free(reginfo,
			    R_ADDR(reginfo,
				lt->part_array[j].lock_mem_off));
			__env_alloc_free(reginfo,
			    R_ADDR(reginfo,
				lt->part_array[j].lockobj_mem_off));
		}

		/* Discard the object partition array. */
		__env_alloc_free(reginfo, R_ADDR(reginfo, lr->part_off));
		SH_TAILQ_INIT(&lr->free_lockers);
		__env_alloc_free(reginfo,
		    R_ADDR(reginfo, lr->locker_mem_off));
	}

	/* Detach from the region. */
	ret = __env_region_detach(env, reginfo, 0);

	/* Discard DB_LOCKTAB. */
	__os_free(env, lt);
	env->lk_handle = NULL;

	return (ret);
}

/*
 * __lock_region_mutex_count --
 *	Return the number of mutexes the lock region will need.
 *
 * PUBLIC: u_int32_t __lock_region_mutex_count __P((ENV *));
 */
u_int32_t
__lock_region_mutex_count(env)
	ENV *env;
{
	DB_ENV *dbenv;

	dbenv = env->dbenv;

	/*
	 * We need one mutex per locker for it to block on.
	 */
	return (dbenv->lk_init_lockers + dbenv->lk_partitions + 3);
}
/*
 * __lock_region_mutex_max --
 *	Return the number of additional mutexes the lock region will need.
 *
 * PUBLIC: u_int32_t __lock_region_mutex_max __P((ENV *));
 */
u_int32_t
__lock_region_mutex_max(env)
	ENV *env;
{
	DB_ENV *dbenv;
	u_int32_t count;

	dbenv = env->dbenv;

	/*
	 * For backward compatibility, ensure enough mutexes.
	 * These might actually get used by other things.
	 */
	if ((count = dbenv->lk_max_lockers) == 0)
		count = DB_LOCK_DEFAULT_N;
	if (count > dbenv->lk_init_lockers)
		return (count - dbenv->lk_init_lockers);
	else
		return (0);
}

/*
 * __lock_region_max --
 *	Return the amount of extra memory to allocate for locking information.
 * PUBLIC: size_t __lock_region_max __P((ENV *));
 */
size_t
__lock_region_max(env)
	ENV *env;
{
	DB_ENV *dbenv;
	size_t retval;
	u_int32_t count;

	dbenv = env->dbenv;

	retval = 0;
	if ((count = dbenv->lk_max) == 0)
		count = DB_LOCK_DEFAULT_N;
	if (count > dbenv->lk_init)
		retval += __env_alloc_size(sizeof(struct __db_lock)) *
		    (count - dbenv->lk_init);
	if ((count = dbenv->lk_max_objects) == 0)
		count = DB_LOCK_DEFAULT_N;
	if (count > dbenv->lk_init_objects)
		retval += __env_alloc_size(sizeof(DB_LOCKOBJ)) *
		    (count - dbenv->lk_init_objects);
	if ((count = dbenv->lk_max_lockers) == 0)
		count = DB_LOCK_DEFAULT_N;
	if (count > dbenv->lk_init_lockers)
		retval += __env_alloc_size(sizeof(DB_LOCKER)) *
		    (count - dbenv->lk_init_lockers);

	/* And we keep getting this wrong, let's be generous. */
	retval += retval / 4;

	return (retval);
}

/*
 * __lock_region_size --
 *	Return the initial region size.
 * PUBLIC: size_t __lock_region_size __P((ENV *, size_t));
 */
size_t
__lock_region_size(env, other_alloc)
	ENV *env;
	size_t other_alloc;
{
	DB_ENV *dbenv;
	size_t retval;
	u_int32_t count;

	dbenv = env->dbenv;

	/* Make sure there is at least 5 objects and locks per partition. */
	if (dbenv->lk_init_objects < dbenv->lk_partitions * 5)
		dbenv->lk_init_objects = dbenv->lk_partitions * 5;
	if (dbenv->lk_init < dbenv->lk_partitions * 5)
		dbenv->lk_init = dbenv->lk_partitions * 5;
	/*
	 * Figure out how much space we're going to need.  This list should
	 * map one-to-one with the __env_alloc calls in __lock_region_init.
	 */
	retval = 0;
	retval += __env_alloc_size(sizeof(DB_LOCKREGION));
	retval += __env_alloc_size((size_t)(dbenv->lk_modes * dbenv->lk_modes));
	/*
	 * Try to figure out the size of the locker hash table.
	 */
	if (dbenv->lk_max_lockers != 0)
		dbenv->locker_t_size = __db_tablesize(dbenv->lk_max_lockers);
	else if (dbenv->tx_max != 0)
		dbenv->locker_t_size = __db_tablesize(dbenv->tx_max);
	else {
		if (dbenv->memory_max != 0)
			count = (u_int32_t)
			    (((dbenv->memory_max - other_alloc) / 10) /
				sizeof(DB_LOCKER));
		else
			count = DB_LOCK_DEFAULT_N / 10;
		if (count < dbenv->lk_init_lockers)
			count = dbenv->lk_init_lockers;
		dbenv->locker_t_size = __db_tablesize(count);
	}
	retval += __env_alloc_size(dbenv->locker_t_size * (sizeof(DB_HASHTAB)));
	retval += __env_alloc_size(sizeof(DB_LOCKER)) * dbenv->lk_init_lockers;
	retval += __env_alloc_size(sizeof(struct __db_lock) * dbenv->lk_init);
	other_alloc += retval;
	/*
	 * We want to allocate a object hash table that is big enough to
	 * avoid many collisions, but not too big for starters.  Arbitrarily
	 * pick the point 2/3s of the way to the max size.  If the max
	 * is not stated then guess that objects will fill 1/2 the memory.
	 * Failing to know how much memory there might we just wind up
	 * using the default value.  If this winds up being less than
	 * the init value then we just make the table fit the init value.
	 */
	if ((count = dbenv->lk_max_objects) == 0) {
		if (dbenv->memory_max != 0)
			count = (u_int32_t)(
			    ((dbenv->memory_max - other_alloc) / 2)
			    / sizeof(DB_LOCKOBJ));
		else
			count = DB_LOCK_DEFAULT_N;
		if (count < dbenv->lk_init_objects)
			count = dbenv->lk_init_objects;
	}
	count *= 2;
	count += dbenv->lk_init_objects;
	count /= 3;
	if (dbenv->object_t_size == 0)
		dbenv->object_t_size = __db_tablesize(count);
	retval += __env_alloc_size(
	    __db_tablesize(dbenv->object_t_size) * (sizeof(DB_HASHTAB)));
#ifdef HAVE_STATISTICS
	retval += __env_alloc_size(
	    __db_tablesize(dbenv->object_t_size) * (sizeof(DB_LOCK_HSTAT)));
#endif
	retval +=
	    __env_alloc_size(dbenv->lk_partitions * (sizeof(DB_LOCKPART)));
	retval += __env_alloc_size(sizeof(DB_LOCKOBJ) * dbenv->lk_init_objects);

	return (retval);
}
