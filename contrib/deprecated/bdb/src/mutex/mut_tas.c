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

static inline int __db_tas_mutex_lock_int
	    __P((ENV *, db_mutex_t, db_timeout_t, int));
static inline int __db_tas_mutex_readlock_int __P((ENV *, db_mutex_t, int));

/*
 * __db_tas_mutex_init --
 *	Initialize a test-and-set mutex.
 *
 * PUBLIC: int __db_tas_mutex_init __P((ENV *, db_mutex_t, u_int32_t));
 */
int
__db_tas_mutex_init(env, mutex, flags)
	ENV *env;
	db_mutex_t mutex;
	u_int32_t flags;
{
	DB_ENV *dbenv;
	DB_MUTEX *mutexp;
	int ret;

#ifndef HAVE_MUTEX_HYBRID
	COMPQUIET(flags, 0);
#endif

	dbenv = env->dbenv;
	mutexp = MUTEXP_SET(env, mutex);

	/* Check alignment. */
	if (((uintptr_t)mutexp & (dbenv->mutex_align - 1)) != 0) {
		__db_errx(env, DB_STR("2028",
		    "TAS: mutex not appropriately aligned"));
		return (EINVAL);
	}

#ifdef HAVE_SHARED_LATCHES
	if (F_ISSET(mutexp, DB_MUTEX_SHARED))
		atomic_init_(&mutexp->sharecount, 0);
	else
#endif
	if (MUTEX_INIT(&mutexp->tas)) {
		ret = __os_get_syserr();
		__db_syserr(env, ret, DB_STR("2029",
		    "TAS: mutex initialize"));
		return (__os_posix_err(ret));
	}
#ifdef HAVE_MUTEX_HYBRID
	if ((ret = __db_pthread_mutex_init(env,
	     mutex, flags | DB_MUTEX_SELF_BLOCK)) != 0)
		return (ret);
#endif
	return (0);
}

/*
 * __db_tas_mutex_lock_int
 *     Internal function to lock a mutex, or just try to lock it without waiting
 */
inline static int
__db_tas_mutex_lock_int(env, mutex, timeout, nowait)
	ENV *env;
	db_mutex_t mutex;
	db_timeout_t timeout;
	int nowait;
{
	DB_ENV *dbenv;
	DB_MUTEX *mutexp;
	DB_MUTEXMGR *mtxmgr;
	DB_MUTEXREGION *mtxregion;
	DB_THREAD_INFO *ip;
	db_timespec now, timespec;
	u_int32_t nspins;
	int ret;
#ifdef HAVE_MUTEX_HYBRID
	const u_long micros = 0;
#else
	u_long micros, max_micros;
	db_timeout_t time_left;
#endif

	dbenv = env->dbenv;

	if (!MUTEX_ON(env) || F_ISSET(dbenv, DB_ENV_NOLOCKING))
		return (0);

	mtxmgr = env->mutex_handle;
	mtxregion = mtxmgr->reginfo.primary;
	mutexp = MUTEXP_SET(env, mutex);

	CHECK_MTX_THREAD(env, mutexp);

#ifdef HAVE_STATISTICS
	if (F_ISSET(mutexp, DB_MUTEX_LOCKED))
		STAT_INC(env, mutex, set_wait, mutexp->mutex_set_wait, mutex);
	else
		STAT_INC(env,
		    mutex, set_nowait, mutexp->mutex_set_nowait, mutex);
#endif

#ifndef HAVE_MUTEX_HYBRID
	/*
	 * Wait 1ms initially, up to 10ms for mutexes backing logical database
	 * locks, and up to 25 ms for mutual exclusion data structure mutexes.
	 * SR: #7675
	 */
	micros = 1000;
	max_micros = F_ISSET(mutexp, DB_MUTEX_LOGICAL_LOCK) ? 10000 : 25000;
#endif

	/* Clear the ending timespec so it'll be initialed upon first need. */
	if (timeout != 0)
		timespecclear(&timespec);

	 /*
	 * Only check the thread state once, by initializing the thread
	 * control block pointer to null.  If it is not the failchk
	 * thread, then ip will have a valid value subsequent times
	 * in the loop.
	 */
	ip = NULL;

loop:	/* Attempt to acquire the resource for N spins. */
	for (nspins =
	    mtxregion->stat.st_mutex_tas_spins; nspins > 0; --nspins) {
#ifdef HAVE_MUTEX_S390_CC_ASSEMBLY
		tsl_t zero;

		zero = 0;
#endif

#ifdef HAVE_MUTEX_HPPA_MSEM_INIT
	relock:
#endif
		/*
		 * Avoid interlocked instructions until they're likely to
		 * succeed by first checking whether it is held
		 */
		if (MUTEXP_IS_BUSY(mutexp) || !MUTEXP_ACQUIRE(mutexp)) {
			if (F_ISSET(dbenv, DB_ENV_FAILCHK) &&
			    ip == NULL && dbenv->is_alive(dbenv,
			    mutexp->pid, mutexp->tid, 0) == 0) {
				ret = __env_set_state(env, &ip, THREAD_VERIFY);
				if (ret != 0 ||
				    ip->dbth_state == THREAD_FAILCHK)
					return (DB_RUNRECOVERY);
			}
			if (nowait)
				return (DB_LOCK_NOTGRANTED);
			/*
			 * Some systems (notably those with newer Intel CPUs)
			 * need a small pause here. [#6975]
			 */
			MUTEX_PAUSE
			continue;
		}

		MEMBAR_ENTER();

#ifdef HAVE_MUTEX_HPPA_MSEM_INIT
		/*
		 * HP semaphores are unlocked automatically when a holding
		 * process exits.  If the mutex appears to be locked
		 * (F_ISSET(DB_MUTEX_LOCKED)) but we got here, assume this
		 * has happened.  Set the pid and tid into the mutex and
		 * lock again.  (The default state of the mutexes used to
		 * block in __lock_get_internal is locked, so exiting with
		 * a locked mutex is reasonable behavior for a process that
		 * happened to initialize or use one of them.)
		 */
		if (F_ISSET(mutexp, DB_MUTEX_LOCKED)) {
			dbenv->thread_id(dbenv, &mutexp->pid, &mutexp->tid);
			goto relock;
		}
		/*
		 * If we make it here, the mutex isn't locked, the diagnostic
		 * won't fire, and we were really unlocked by someone calling
		 * the DB mutex unlock function.
		 */
#endif
#ifdef DIAGNOSTIC
		if (F_ISSET(mutexp, DB_MUTEX_LOCKED)) {
			char buf[DB_THREADID_STRLEN];
			__db_errx(env, DB_STR_A("2030",
		    "TAS lock failed: lock %ld currently in use: ID: %s",
			    "%ld %s"), (long)mutex,
			    dbenv->thread_id_string(dbenv,
			    mutexp->pid, mutexp->tid, buf));
			return (__env_panic(env, EACCES));
		}
#endif
		F_SET(mutexp, DB_MUTEX_LOCKED);
		dbenv->thread_id(dbenv, &mutexp->pid, &mutexp->tid);

#ifdef DIAGNOSTIC
		/*
		 * We want to switch threads as often as possible.  Yield
		 * every time we get a mutex to ensure contention.
		 */
		if (F_ISSET(dbenv, DB_ENV_YIELDCPU))
			__os_yield(env, 0, 0);
#endif
		return (0);
	}

	/*
	 * We need to wait for the lock to become available.
	 * Possibly setup timeouts if this is the first wait, or
	 * check expiration times for the second and subsequent waits.
	 */
	if (timeout != 0) {
		/* Set the expiration time if this is the first sleep . */
		if (!timespecisset(&timespec))
			__clock_set_expires(env, &timespec, timeout);
		else {
			timespecclear(&now);
			if (__clock_expired(env, &now, &timespec))
				return (DB_TIMEOUT);
#ifndef HAVE_MUTEX_HYBRID
			timespecsub(&now, &timespec);
			DB_TIMESPEC_TO_TIMEOUT(time_left, &now, 0);
			time_left = timeout - time_left;
			if (micros > time_left)
				micros = time_left;
#endif
		}
	}

	/*
	 * This yields for a while for tas mutexes, and just gives up the
	 * processor for hybrid mutexes.
	 * By yielding here we can get the other thread to give up the
	 * mutex before calling the more expensive library mutex call.
	 * Tests have shown this to be a big win when there is contention.
	 */
	PERFMON4(env, mutex, suspend, mutex, TRUE, mutexp->alloc_id, mutexp);
	__os_yield(env, 0, micros);
	PERFMON4(env, mutex, resume, mutex, TRUE, mutexp->alloc_id, mutexp);

#if defined(HAVE_MUTEX_HYBRID)
	if (!MUTEXP_IS_BUSY(mutexp))
		goto loop;
	/* Wait until the mutex can be obtained exclusively or it times out. */
	if ((ret = __db_hybrid_mutex_suspend(env,
	    mutex, timeout == 0 ? NULL : &timespec, TRUE)) != 0)
		return (ret);
#else
	if ((micros <<= 1) > max_micros)
		micros = max_micros;
#endif

	/*
	 * We're spinning.  The environment might be hung, and somebody else
	 * has already recovered it.  The first thing recovery does is panic
	 * the environment.  Check to see if we're never going to get this
	 * mutex.
	 */
	PANIC_CHECK(env);

	goto loop;
}

/*
 * __db_tas_mutex_lock
 *	Lock on a mutex, blocking if necessary.
 *
 * PUBLIC: int __db_tas_mutex_lock __P((ENV *, db_mutex_t, db_timeout_t));
 */
int
__db_tas_mutex_lock(env, mutex, timeout)
	ENV *env;
	db_mutex_t mutex;
	db_timeout_t timeout;
{
	return (__db_tas_mutex_lock_int(env, mutex, timeout, 0));
}

/*
 * __db_tas_mutex_trylock
 *	Try to exclusively lock a mutex without ever blocking - ever!
 *
 *	Returns 0 on success,
 *		DB_LOCK_NOTGRANTED on timeout
 *		Possibly DB_RUNRECOVERY if DB_ENV_FAILCHK or panic.
 *
 *	This will work for DB_MUTEX_SHARED, though it always tries
 *	for exclusive access.
 *
 * PUBLIC: int __db_tas_mutex_trylock __P((ENV *, db_mutex_t));
 */
int
__db_tas_mutex_trylock(env, mutex)
	ENV *env;
	db_mutex_t mutex;
{
	return (__db_tas_mutex_lock_int(env, mutex, 0, 1));
}

#if defined(HAVE_SHARED_LATCHES)
/*
 * __db_tas_mutex_readlock_int
 *    Internal function to get a shared lock on a latch, blocking if necessary.
 *
 */
static inline int
__db_tas_mutex_readlock_int(env, mutex, nowait)
	ENV *env;
	db_mutex_t mutex;
	int nowait;
{
	DB_ENV *dbenv;
	DB_MUTEX *mutexp;
	DB_MUTEXMGR *mtxmgr;
	DB_MUTEXREGION *mtxregion;
	DB_THREAD_INFO *ip;
	int lock;
	u_int32_t nspins;
	int ret;
#ifndef HAVE_MUTEX_HYBRID
	u_long micros, max_micros;
#endif
	dbenv = env->dbenv;

	if (!MUTEX_ON(env) || F_ISSET(dbenv, DB_ENV_NOLOCKING))
		return (0);

	mtxmgr = env->mutex_handle;
	mtxregion = mtxmgr->reginfo.primary;
	mutexp = MUTEXP_SET(env, mutex);

	CHECK_MTX_THREAD(env, mutexp);

	DB_ASSERT(env, F_ISSET(mutexp, DB_MUTEX_SHARED));
#ifdef HAVE_STATISTICS
	if (F_ISSET(mutexp, DB_MUTEX_LOCKED))
		STAT_INC(env,
		    mutex, set_rd_wait, mutexp->mutex_set_rd_wait, mutex);
	else
		STAT_INC(env,
		    mutex, set_rd_nowait, mutexp->mutex_set_rd_nowait, mutex);
#endif

#ifndef HAVE_MUTEX_HYBRID
	/*
	 * Wait 1ms initially, up to 10ms for mutexes backing logical database
	 * locks, and up to 25 ms for mutual exclusion data structure mutexes.
	 * SR: #7675
	 */
	micros = 1000;
	max_micros = F_ISSET(mutexp, DB_MUTEX_LOGICAL_LOCK) ? 10000 : 25000;
#endif

loop:	/* Attempt to acquire the resource for N spins. */
	for (nspins =
	    mtxregion->stat.st_mutex_tas_spins; nspins > 0; --nspins) {
		lock = atomic_read(&mutexp->sharecount);
		if (lock == MUTEX_SHARE_ISEXCLUSIVE ||
		    !atomic_compare_exchange(env,
			&mutexp->sharecount, lock, lock + 1)) {
			/*
			 * Some systems (notably those with newer Intel CPUs)
			 * need a small pause here. [#6975]
			 */
			MUTEX_PAUSE
			continue;
		}

		MEMBAR_ENTER();
		/* For shared latches the threadid is the last requestor's id.
		 */
		dbenv->thread_id(dbenv, &mutexp->pid, &mutexp->tid);

		return (0);
	}

	/*
	 * Waiting for the latched must be avoided when it could allow a
	 * 'failchk'ing thread to hang.
	 */
	if (F_ISSET(dbenv, DB_ENV_FAILCHK) &&
	    dbenv->is_alive(dbenv, mutexp->pid, mutexp->tid, 0) == 0) {
		ret = __env_set_state(env, &ip, THREAD_VERIFY);
		if (ret != 0 || ip->dbth_state == THREAD_FAILCHK)
			return (DB_RUNRECOVERY);
	}

	/*
	 * It is possible to spin out when the latch is just shared, due to
	 * many threads or interrupts interfering with the compare&exchange.
	 * Avoid spurious DB_LOCK_NOTGRANTED returns by retrying.
	 */
	if (nowait) {
		if (atomic_read(&mutexp->sharecount) != MUTEX_SHARE_ISEXCLUSIVE)
			goto loop;
		return (DB_LOCK_NOTGRANTED);
	}

	/* Wait for the lock to become available. */
#ifdef HAVE_MUTEX_HYBRID
	/*
	 * By yielding here we can get the other thread to give up the
	 * mutex before calling the more expensive library mutex call.
	 * Tests have shown this to be a big win when there is contention.
	 */
	PERFMON4(env, mutex, suspend, mutex, FALSE, mutexp->alloc_id, mutexp);
	__os_yield(env, 0, 0);
	PERFMON4(env, mutex, resume, mutex, FALSE, mutexp->alloc_id, mutexp);
	if (atomic_read(&mutexp->sharecount) != MUTEX_SHARE_ISEXCLUSIVE)
		goto loop;
	/* Wait until the mutex is no longer exclusively locked. */
	if ((ret = __db_hybrid_mutex_suspend(env, mutex, NULL, FALSE)) != 0)
		return (ret);
#else
	PERFMON4(env, mutex, suspend, mutex, FALSE, mutexp->alloc_id, mutexp);
	__os_yield(env, 0, micros);
	PERFMON4(env, mutex, resume, mutex, FALSE, mutexp->alloc_id, mutexp);
	if ((micros <<= 1) > max_micros)
		micros = max_micros;
#endif

	/*
	 * We're spinning.  The environment might be hung, and somebody else
	 * has already recovered it.  The first thing recovery does is panic
	 * the environment.  Check to see if we're never going to get this
	 * mutex.
	 */
	PANIC_CHECK(env);

	goto loop;
}

/*
 * __db_tas_mutex_readlock
 *	Get a shared lock on a latch, waiting if necessary.
 *
 * PUBLIC: #if defined(HAVE_SHARED_LATCHES)
 * PUBLIC: int __db_tas_mutex_readlock __P((ENV *, db_mutex_t));
 * PUBLIC: #endif
 */
int
__db_tas_mutex_readlock(env, mutex)
	ENV *env;
	db_mutex_t mutex;
{
	return (__db_tas_mutex_readlock_int(env, mutex, 0));
}

/*
 * __db_tas_mutex_tryreadlock
 *	Try to get a shared lock on a latch; don't wait when busy.
 *
 * PUBLIC: #if defined(HAVE_SHARED_LATCHES)
 * PUBLIC: int __db_tas_mutex_tryreadlock __P((ENV *, db_mutex_t));
 * PUBLIC: #endif
 */
int
__db_tas_mutex_tryreadlock(env, mutex)
	ENV *env;
	db_mutex_t mutex;
{
	return (__db_tas_mutex_readlock_int(env, mutex, 1));
}
#endif

/*
 * __db_tas_mutex_unlock --
 *	Release a mutex.
 *
 * PUBLIC: int __db_tas_mutex_unlock __P((ENV *, db_mutex_t));
 *
 * Hybrid shared latch wakeup
 *	When an exclusive requester waits for the last shared holder to
 *	release, it increments mutexp->wait and pthread_cond_wait()'s. The
 *	last shared unlock calls __db_pthread_mutex_unlock() to wake it.
 */
int
__db_tas_mutex_unlock(env, mutex)
    ENV *env;
	db_mutex_t mutex;
{
	DB_ENV *dbenv;
	DB_MUTEX *mutexp;
#ifdef HAVE_MUTEX_HYBRID
	int ret;
#ifdef MUTEX_DIAG
	int waiters;
#endif
#endif
#ifdef HAVE_SHARED_LATCHES
	int sharecount;
#endif
	dbenv = env->dbenv;

	if (!MUTEX_ON(env) || F_ISSET(dbenv, DB_ENV_NOLOCKING))
		return (0);

	mutexp = MUTEXP_SET(env, mutex);
#if defined(HAVE_MUTEX_HYBRID) && defined(MUTEX_DIAG)
	waiters = mutexp->wait;
#endif

#if defined(DIAGNOSTIC)
#if defined(HAVE_SHARED_LATCHES)
	if (F_ISSET(mutexp, DB_MUTEX_SHARED)) {
		if (atomic_read(&mutexp->sharecount) == 0) {
			__db_errx(env, DB_STR_A("2031",
			    "shared unlock %ld already unlocked", "%ld"),
			    (long)mutex);
			return (__env_panic(env, EACCES));
		}
	} else
#endif
	if (!F_ISSET(mutexp, DB_MUTEX_LOCKED)) {
		__db_errx(env, DB_STR_A("2032",
		    "unlock %ld already unlocked", "%ld"), (long)mutex);
		return (__env_panic(env, EACCES));
	}
#endif

#ifdef HAVE_SHARED_LATCHES
	if (F_ISSET(mutexp, DB_MUTEX_SHARED)) {
		sharecount = atomic_read(&mutexp->sharecount);
		/*MUTEX_MEMBAR(mutexp->sharecount);*/		/* XXX why? */
		if (sharecount == MUTEX_SHARE_ISEXCLUSIVE) {
			F_CLR(mutexp, DB_MUTEX_LOCKED);
			/* Flush flag update before zeroing count */
			MEMBAR_EXIT();
			atomic_init_(&mutexp->sharecount, 0);
		} else {
			DB_ASSERT(env, sharecount > 0);
			MEMBAR_EXIT();
			sharecount = atomic_dec(env, &mutexp->sharecount);
			DB_ASSERT(env, sharecount >= 0);
			if (sharecount > 0)
				return (0);
		}
	} else
#endif
	{
		F_CLR(mutexp, DB_MUTEX_LOCKED);
		MUTEX_UNSET(&mutexp->tas);
	}

#ifdef HAVE_MUTEX_HYBRID
#ifdef DIAGNOSTIC
	if (F_ISSET(dbenv, DB_ENV_YIELDCPU))
		__os_yield(env, 0, 0);
#endif

	/* Prevent the load of wait from being hoisted before MUTEX_UNSET */
	MUTEX_MEMBAR(mutexp->flags);
	if (mutexp->wait &&
	    (ret = __db_pthread_mutex_unlock(env, mutex)) != 0)
		    return (ret);

#ifdef MUTEX_DIAG
	if (mutexp->wait)
		printf("tas_unlock %ld %x waiters! busy %x waiters %d/%d\n",
		    mutex, pthread_self(),
		    MUTEXP_BUSY_FIELD(mutexp), waiters, mutexp->wait);
#endif
#endif

	return (0);
}

/*
 * __db_tas_mutex_destroy --
 *	Destroy a mutex.
 *
 * PUBLIC: int __db_tas_mutex_destroy __P((ENV *, db_mutex_t));
 */
int
__db_tas_mutex_destroy(env, mutex)
	ENV *env;
	db_mutex_t mutex;
{
	DB_MUTEX *mutexp;
#ifdef HAVE_MUTEX_HYBRID
	int ret;
#endif

	if (!MUTEX_ON(env))
		return (0);

	mutexp = MUTEXP_SET(env, mutex);

	MUTEX_DESTROY(&mutexp->tas);

#ifdef HAVE_MUTEX_HYBRID
	if ((ret = __db_pthread_mutex_destroy(env, mutex)) != 0)
		return (ret);
#endif

	COMPQUIET(mutexp, NULL);	/* MUTEX_DESTROY may not be defined. */
	return (0);
}
