/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1999, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/lock.h"

/*
 * This is where we load in architecture/compiler specific mutex code.
 */
#define	LOAD_ACTUAL_MUTEX_CODE

#ifdef HAVE_MUTEX_SOLARIS_LWP
#define	pthread_cond_destroy(x)		0
#define	pthread_cond_signal		_lwp_cond_signal
#define	pthread_cond_broadcast		_lwp_cond_broadcast
#define	pthread_cond_wait		_lwp_cond_wait
#define	pthread_mutex_destroy(x)	0
#define	pthread_mutex_lock		_lwp_mutex_lock
#define	pthread_mutex_trylock		_lwp_mutex_trylock
#define	pthread_mutex_unlock		_lwp_mutex_unlock
#endif
#ifdef HAVE_MUTEX_UI_THREADS
#define	pthread_cond_destroy(x)		cond_destroy
#define	pthread_cond_broadcast		cond_broadcast
#define	pthread_cond_wait		cond_wait
#define	pthread_mutex_destroy		mutex_destroy
#define	pthread_mutex_lock		mutex_lock
#define	pthread_mutex_trylock		mutex_trylock
#define	pthread_mutex_unlock		mutex_unlock
#endif

/*
 * According to HP-UX engineers contacted by Netscape,
 * pthread_mutex_unlock() will occasionally return EFAULT for no good reason
 * on mutexes in shared memory regions, and the correct caller behavior
 * is to try again.  Do so, up to EFAULT_RETRY_ATTEMPTS consecutive times.
 * Note that we don't bother to restrict this to HP-UX;
 * it should be harmless elsewhere. [#2471]
 */
#define	EFAULT_RETRY_ATTEMPTS	5
#define	RETRY_ON_EFAULT(func_invocation, ret) do {	\
	int i;						\
	i = EFAULT_RETRY_ATTEMPTS;			\
	do {						\
		RET_SET((func_invocation), ret);	\
	} while (ret == EFAULT && --i > 0);		\
} while (0)

/*
 * IBM's MVS pthread mutex implementation returns -1 and sets errno rather than
 * returning errno itself.  As -1 is not a valid errno value, assume functions
 * returning -1 have set errno.  If they haven't, return a random error value.
 */
#define	RET_SET(f, ret) do {						\
	if (((ret) = (f)) == -1 && ((ret) = errno) == 0)		\
		(ret) = EAGAIN;						\
} while (0)

/*
 * __db_pthread_mutex_init --
 *	Initialize a pthread mutex: either a native one or
 *	just the mutex for block/wakeup of a hybrid test-and-set mutex
 *
 *
 * PUBLIC: int __db_pthread_mutex_init __P((ENV *, db_mutex_t, u_int32_t));
 */
int
__db_pthread_mutex_init(env, mutex, flags)
	ENV *env;
	db_mutex_t mutex;
	u_int32_t flags;
{
	DB_MUTEX *mutexp;
	int ret;

	mutexp = MUTEXP_SET(env, mutex);
	ret = 0;

#ifndef HAVE_MUTEX_HYBRID
	/* Can't have self-blocking shared latches.  */
	DB_ASSERT(env, !LF_ISSET(DB_MUTEX_SELF_BLOCK) ||
	    !LF_ISSET(DB_MUTEX_SHARED));
#endif

#ifdef HAVE_MUTEX_PTHREADS
	{
#ifndef HAVE_MUTEX_THREAD_ONLY
	pthread_condattr_t condattr;
	pthread_mutexattr_t mutexattr;
#endif
	pthread_condattr_t *condattrp = NULL;
	pthread_mutexattr_t *mutexattrp = NULL;

#ifndef HAVE_MUTEX_HYBRID
	if (LF_ISSET(DB_MUTEX_SHARED)) {
#if defined(HAVE_SHARED_LATCHES)
		pthread_rwlockattr_t rwlockattr, *rwlockattrp = NULL;
#ifndef HAVE_MUTEX_THREAD_ONLY
		if (!LF_ISSET(DB_MUTEX_PROCESS_ONLY)) {
			RET_SET((pthread_rwlockattr_init(&rwlockattr)), ret);
			if (ret != 0)
				goto err;
			RET_SET((pthread_rwlockattr_setpshared(
			    &rwlockattr, PTHREAD_PROCESS_SHARED)), ret);
			rwlockattrp = &rwlockattr;
		}
#endif

		if (ret == 0)
			RET_SET((pthread_rwlock_init(&mutexp->u.rwlock,
			    rwlockattrp)), ret);
		if (rwlockattrp != NULL)
			(void)pthread_rwlockattr_destroy(rwlockattrp);

		F_SET(mutexp, DB_MUTEX_SHARED);
		/* For rwlocks, we're done - cannot use the mutex or cond */
		goto err;
#endif
	}
#endif
#ifndef HAVE_MUTEX_THREAD_ONLY
	if (!LF_ISSET(DB_MUTEX_PROCESS_ONLY)) {
		RET_SET((pthread_mutexattr_init(&mutexattr)), ret);
		if (ret != 0)
			goto err;
		RET_SET((pthread_mutexattr_setpshared(
		    &mutexattr, PTHREAD_PROCESS_SHARED)), ret);
		mutexattrp = &mutexattr;
	}
#endif

	if (ret == 0)
		RET_SET(
		    (pthread_mutex_init(&mutexp->u.m.mutex, mutexattrp)), ret);

	if (mutexattrp != NULL)
		(void)pthread_mutexattr_destroy(mutexattrp);
	if (ret != 0)
		goto err;
	if (LF_ISSET(DB_MUTEX_SELF_BLOCK)) {
#ifndef HAVE_MUTEX_THREAD_ONLY
		if (!LF_ISSET(DB_MUTEX_PROCESS_ONLY)) {
			RET_SET((pthread_condattr_init(&condattr)), ret);
			if (ret != 0)
				goto err;

			condattrp = &condattr;
			RET_SET((pthread_condattr_setpshared(
			    &condattr, PTHREAD_PROCESS_SHARED)), ret);
		}
#endif

		if (ret == 0)
			RET_SET((pthread_cond_init(
			    &mutexp->u.m.cond, condattrp)), ret);

		F_SET(mutexp, DB_MUTEX_SELF_BLOCK);
		if (condattrp != NULL)
			(void)pthread_condattr_destroy(condattrp);
	}

	}
#endif
#ifdef HAVE_MUTEX_SOLARIS_LWP
	/*
	 * XXX
	 * Gcc complains about missing braces in the static initializations of
	 * lwp_cond_t and lwp_mutex_t structures because the structures contain
	 * sub-structures/unions and the Solaris include file that defines the
	 * initialization values doesn't have surrounding braces.  There's not
	 * much we can do.
	 */
	if (LF_ISSET(DB_MUTEX_PROCESS_ONLY)) {
		static lwp_mutex_t mi = DEFAULTMUTEX;

		mutexp->mutex = mi;
	} else {
		static lwp_mutex_t mi = SHAREDMUTEX;

		mutexp->mutex = mi;
	}
	if (LF_ISSET(DB_MUTEX_SELF_BLOCK)) {
		if (LF_ISSET(DB_MUTEX_PROCESS_ONLY)) {
			static lwp_cond_t ci = DEFAULTCV;

			mutexp->cond = ci;
		} else {
			static lwp_cond_t ci = SHAREDCV;

			mutexp->cond = ci;
		}
		F_SET(mutexp, DB_MUTEX_SELF_BLOCK);
	}
#endif
#ifdef HAVE_MUTEX_UI_THREADS
	{
	int type;

	type = LF_ISSET(DB_MUTEX_PROCESS_ONLY) ? USYNC_THREAD : USYNC_PROCESS;

	ret = mutex_init(&mutexp->mutex, type, NULL);
	if (ret == 0 && LF_ISSET(DB_MUTEX_SELF_BLOCK)) {
		ret = cond_init(&mutexp->cond, type, NULL);

		F_SET(mutexp, DB_MUTEX_SELF_BLOCK);
	}}
#endif

err:	if (ret != 0) {
		__db_err(env, ret, DB_STR("2021",
		    "unable to initialize mutex"));
	}
	return (ret);
}

/*
 * __db_pthread_mutex_prep
 *	Prepare to use a pthread-based DB_MUTEX.
 *
 *	This exclusively locks a DB_MUTEX's pthread_mutex_t or pthread_rwlock_t,
 *	before locking, unlocking, or waiting for the DB mutex to be become
 *	available in the requested mode (exclusive == 1, shared == 0).
 *
 *	Test for failchk concerns here too, to avoid hanging on a dead pid/tid.
 */
inline static int
__db_pthread_mutex_prep(env, mutex, mutexp, exclusive)
	ENV *env;
	db_mutex_t mutex;
	DB_MUTEX *mutexp;
	int exclusive;
{
	DB_ENV *dbenv;
	DB_THREAD_INFO *ip;
	int ret;

	dbenv = env->dbenv;
	PERFMON4(env,
	    mutex, suspend, mutex, exclusive, mutexp->alloc_id, mutexp);
	if (F_ISSET(dbenv, DB_ENV_FAILCHK)) {
		for (;;) {
			RET_SET_PTHREAD_TRYLOCK(mutexp, ret);
			if (ret != EBUSY)
				break;
			if (dbenv->is_alive(dbenv,
			    mutexp->pid, mutexp->tid, 0) == 0) {
				ret = __env_set_state(env, &ip, THREAD_VERIFY);
				if (ret != 0 ||
				    ip->dbth_state == THREAD_FAILCHK) {
					ret = DB_RUNRECOVERY;
				} else {
					/*
					 * Some thread other than the true
					 * FAILCHK thread in this process is
					 * asking for the mutex held by the
					 * dead process/thread.  We will block
					 * here until someone else does the
					 * cleanup.  Same behavior as if we
					 * hadn't gone down the 'if
					 * DB_ENV_FAILCHK' path to start with.
					 */
				    RET_SET_PTHREAD_LOCK(mutexp, ret);
				    break;
				}
			}
		}
	} else
		RET_SET_PTHREAD_LOCK(mutexp, ret);

	PERFMON4(env,
	    mutex, resume, mutex, exclusive, mutexp->alloc_id, mutexp);
	COMPQUIET(mutex, 0);
	COMPQUIET(exclusive, 0);
	return (ret);
}

/*
 * __db_pthread_mutex_condwait
 *	Perform a pthread condition wait for a DB_MUTEX.
 *
 *	This will be a timed wait when a timespec has been specified. EINTR and
 *	spurious ETIME* values are mapped to 0, and hence success.  The
 *	mutexp->u.m.mutex must be locked upon entry. When returning a success
 *	or timeout status it will have been locked again.
 *
 *	Returns:
 *		0 if it is safe to retry to get the mutex
 *		DB_TIMEOUT if the timeout exceeded
 *		<other> a fatal error. The mutexp->u.m.mutex has been unlocked.
 */
inline static int
__db_pthread_mutex_condwait(env, mutex, mutexp, timespec)
	ENV *env;
	db_mutex_t mutex;
	DB_MUTEX *mutexp;
	db_timespec *timespec;
{
	int ret;

#ifdef MUTEX_DIAG
	printf("condwait %ld %x wait busy %x count %d\n",
	    mutex, pthread_self(), MUTEXP_BUSY_FIELD(mutexp), mutexp->wait);
#endif
	PERFMON4(env, mutex, suspend, mutex, TRUE, mutexp->alloc_id, mutexp);

	if (timespec != NULL) {
		RET_SET((pthread_cond_timedwait(&mutexp->u.m.cond,
		    &mutexp->u.m.mutex, (struct timespec *) timespec)), ret);
		if (ret == ETIMEDOUT) {
			ret = DB_TIMEOUT;
			goto ret;
		}
	} else
		RET_SET((pthread_cond_wait(&mutexp->u.m.cond,
		    &mutexp->u.m.mutex)), ret);
#ifdef MUTEX_DIAG
	printf("condwait %ld %x wait returns %d busy %x\n",
	    mutex, pthread_self(), ret, MUTEXP_BUSY_FIELD(mutexp));
#endif
	/*
	 * !!!
	 * Solaris bug workaround: pthread_cond_wait() sometimes returns ETIME
	 * -- out  of sheer paranoia, check both ETIME and ETIMEDOUT.  We
	 * believe this happens when the application uses SIGALRM for some
	 * purpose, e.g., the C library sleep call, and Solaris delivers the
	 * signal to the wrong  LWP.
	 */
	if (ret != 0) {
		if (ret == ETIMEDOUT ||
#ifdef ETIME
		    ret == ETIME ||
#endif
		    ret == EINTR)
			ret = 0;
		else
			/* Failure, caller shouldn't condwait again. */
			(void)pthread_mutex_unlock(&mutexp->u.m.mutex);
	}

ret:
	PERFMON4(env, mutex, resume, mutex, TRUE, mutexp->alloc_id, mutexp);

	COMPQUIET(mutex, 0);
	COMPQUIET(env, 0);
	return (ret);
}

#ifndef HAVE_MUTEX_HYBRID
/*
 * __db_pthread_mutex_lock
 *	Lock on a mutex, blocking if necessary.
 *	Timeouts are supported only for self-blocking mutexes.
 *
 *	Self-blocking shared latches are not supported.
 *
 * PUBLIC: #ifndef HAVE_MUTEX_HYBRID
 * PUBLIC: int __db_pthread_mutex_lock __P((ENV *, db_mutex_t, db_timeout_t));
 * PUBLIC: #endif
 */
int
__db_pthread_mutex_lock(env, mutex, timeout)
	ENV *env;
	db_mutex_t mutex;
	db_timeout_t timeout;
{
	DB_ENV *dbenv;
	DB_MUTEX *mutexp;
	db_timespec timespec;
	int ret, t_ret;

	dbenv = env->dbenv;

	if (!MUTEX_ON(env) || F_ISSET(dbenv, DB_ENV_NOLOCKING))
		return (0);

	t_ret = 0;
	mutexp = MUTEXP_SET(env, mutex);

	CHECK_MTX_THREAD(env, mutexp);

#if defined(HAVE_STATISTICS)
	/*
	 * We want to know which mutexes are contentious, but don't want to
	 * do an interlocked test here -- that's slower when the underlying
	 * system has adaptive mutexes and can perform optimizations like
	 * spinning only if the thread holding the mutex is actually running
	 * on a CPU.  Make a guess, using a normal load instruction.
	 */
	if (F_ISSET(mutexp, DB_MUTEX_LOCKED))
		STAT_INC(env, mutex, set_wait, mutexp->mutex_set_wait, mutex);
	else
		STAT_INC(env,
		    mutex, set_nowait, mutexp->mutex_set_nowait, mutex);
#endif

	/* Single-thread the next block, except during the possible condwait. */
	if ((ret = __db_pthread_mutex_prep(env, mutex, mutexp, TRUE)) != 0)
		goto err;

	if (F_ISSET(mutexp, DB_MUTEX_SELF_BLOCK)) {
		if (timeout != 0)
			timespecclear(&timespec);
		while (MUTEXP_IS_BUSY(mutexp)) {
			/* Set expiration timer upon first need. */
			if (timeout != 0 && !timespecisset(&timespec)) {
				timespecclear(&timespec);
				__clock_set_expires(env, &timespec, timeout);
			}
			t_ret = __db_pthread_mutex_condwait(env,
			    mutex, mutexp, timeout == 0 ? NULL : &timespec);
			if (t_ret != 0) {
				if (t_ret == DB_TIMEOUT)
					goto out;
				ret = t_ret;
				goto err;
			}
		}

		F_SET(mutexp, DB_MUTEX_LOCKED);
		dbenv->thread_id(dbenv, &mutexp->pid, &mutexp->tid);
out:
		/* #2471: HP-UX can sporadically return EFAULT. See above */
		RETRY_ON_EFAULT(pthread_mutex_unlock(&mutexp->u.m.mutex), ret);
		if (ret != 0)
			goto err;
	} else {
#ifdef DIAGNOSTIC
		if (F_ISSET(mutexp, DB_MUTEX_LOCKED)) {
			char buf[DB_THREADID_STRLEN];
			(void)dbenv->thread_id_string(dbenv,
			    mutexp->pid, mutexp->tid, buf);
			__db_errx(env, DB_STR_A("2022",
		    "pthread lock failed: lock currently in use: pid/tid: %s",
			    "%s"), buf);
			ret = EINVAL;
			goto err;
		}
#endif
		F_SET(mutexp, DB_MUTEX_LOCKED);
		dbenv->thread_id(dbenv, &mutexp->pid, &mutexp->tid);
	}

#ifdef DIAGNOSTIC
	/*
	 * We want to switch threads as often as possible.  Yield every time
	 * we get a mutex to ensure contention.
	 */
	if (F_ISSET(dbenv, DB_ENV_YIELDCPU))
		__os_yield(env, 0, 0);
#endif
	return (t_ret);

err:
	__db_err(env, ret, DB_STR("2023", "pthread lock failed"));
	return (__env_panic(env, ret));
}
#endif

#if defined(HAVE_SHARED_LATCHES) && !defined(HAVE_MUTEX_HYBRID)
/*
 * __db_pthread_mutex_readlock
 *	Take a shared lock on a mutex, blocking if necessary.
 *
 * PUBLIC: #if defined(HAVE_SHARED_LATCHES)
 * PUBLIC: int __db_pthread_mutex_readlock __P((ENV *, db_mutex_t));
 * PUBLIC: #endif
 */
int
__db_pthread_mutex_readlock(env, mutex)
	ENV *env;
	db_mutex_t mutex;
{
	DB_ENV *dbenv;
	DB_MUTEX *mutexp;
	int ret;

	dbenv = env->dbenv;

	if (!MUTEX_ON(env) || F_ISSET(dbenv, DB_ENV_NOLOCKING))
		return (0);

	mutexp = MUTEXP_SET(env, mutex);
	DB_ASSERT(env, F_ISSET(mutexp, DB_MUTEX_SHARED));

	CHECK_MTX_THREAD(env, mutexp);

#if defined(HAVE_STATISTICS)
	/*
	 * We want to know which mutexes are contentious, but don't want to
	 * do an interlocked test here -- that's slower when the underlying
	 * system has adaptive mutexes and can perform optimizations like
	 * spinning only if the thread holding the mutex is actually running
	 * on a CPU.  Make a guess, using a normal load instruction.
	 */
	if (F_ISSET(mutexp, DB_MUTEX_LOCKED))
		STAT_INC(env,
		    mutex, set_rd_wait, mutexp->mutex_set_rd_wait, mutex);
	else
		STAT_INC(env,
		    mutex, set_rd_nowait, mutexp->mutex_set_rd_nowait, mutex);
#endif

	PERFMON4(env, mutex, suspend, mutex, FALSE, mutexp->alloc_id, mutexp);
	RET_SET((pthread_rwlock_rdlock(&mutexp->u.rwlock)), ret);
	PERFMON4(env, mutex, resume, mutex, FALSE, mutexp->alloc_id, mutexp);
	DB_ASSERT(env, !F_ISSET(mutexp, DB_MUTEX_LOCKED));
	if (ret != 0)
		goto err;

#ifdef DIAGNOSTIC
	/*
	 * We want to switch threads as often as possible.  Yield every time
	 * we get a mutex to ensure contention.
	 */
	if (F_ISSET(dbenv, DB_ENV_YIELDCPU))
		__os_yield(env, 0, 0);
#endif
	return (0);

err:	__db_err(env, ret, DB_STR("2024", "pthread readlock failed"));
	return (__env_panic(env, ret));
}
#endif

#ifdef HAVE_MUTEX_HYBRID
/*
 * __db_hybrid_mutex_suspend
 *	Suspend this thread until the mutex is free enough to give the caller a
 *	good chance of getting the mutex in the requested exclusivity mode.
 *
 *	The major difference between this and the old __db_pthread_mutex_lock()
 *	is the additional 'exclusive' parameter.
 *
 * PUBLIC: #ifdef HAVE_MUTEX_HYBRID
 * PUBLIC: int __db_hybrid_mutex_suspend
 * PUBLIC:	__P((ENV *, db_mutex_t, db_timespec *, int));
 * PUBLIC: #endif
 */
int
__db_hybrid_mutex_suspend(env, mutex, timespec, exclusive)
	ENV *env;
	db_mutex_t mutex;
	db_timespec *timespec;
	int exclusive;
{
	DB_MUTEX *mutexp;
	int ret, t_ret;

	t_ret = 0;
	mutexp = MUTEXP_SET(env, mutex);

	if (!exclusive)
		DB_ASSERT(env, F_ISSET(mutexp, DB_MUTEX_SHARED));
	DB_ASSERT(env, F_ISSET(mutexp, DB_MUTEX_SELF_BLOCK));

	if ((ret = __db_pthread_mutex_prep(env, mutex, mutexp, exclusive)) != 0)
		goto err;

	/*
	 * Since this is only for hybrid mutexes the pthread mutex
	 * is only used to wait after spinning on the TAS mutex.
	 * Set the wait flag before checking to see if the mutex
	 * is still locked.  The holder will clear DB_MUTEX_LOCKED
	 * before checking the wait counter.
	 */
	mutexp->wait++;
	MUTEX_MEMBAR(mutexp->wait);
	while (exclusive ? MUTEXP_IS_BUSY(mutexp) :
	    atomic_read(&mutexp->sharecount) == MUTEX_SHARE_ISEXCLUSIVE) {
		t_ret = __db_pthread_mutex_condwait(env,
		    mutex, mutexp, timespec);
		if (t_ret != 0) {
			if (t_ret == DB_TIMEOUT)
				break;
			ret = t_ret;
			goto err;
		}
		MUTEX_MEMBAR(mutexp->flags);
	}

	mutexp->wait--;

	/* #2471: HP-UX can sporadically return EFAULT. See above */
	RETRY_ON_EFAULT(pthread_mutex_unlock(&mutexp->u.m.mutex), ret);
	if (ret != 0)
		goto err;

	PERFMON4(env,
	    mutex, resume, mutex, exclusive, mutexp->alloc_id, mutexp);

#ifdef DIAGNOSTIC
	/*
	 * We want to switch threads as often as possible.  Yield every time
	 * we get a mutex to ensure contention.
	 */
	if (F_ISSET(env->dbenv, DB_ENV_YIELDCPU))
		__os_yield(env, 0, 0);
#endif
	return (t_ret);

err:
	PERFMON4(env,
	    mutex, resume, mutex, exclusive, mutexp->alloc_id, mutexp);
	__db_err(env, ret, "pthread suspend failed");
	return (__env_panic(env, ret));
}
#endif

/*
 * __db_pthread_mutex_unlock --
 *	Release a mutex, or, if hybrid, wake a thread up from a suspend.
 *
 * PUBLIC: int __db_pthread_mutex_unlock __P((ENV *, db_mutex_t));
 */
int
__db_pthread_mutex_unlock(env, mutex)
	ENV *env;
	db_mutex_t mutex;
{
	DB_ENV *dbenv;
	DB_MUTEX *mutexp;
	int ret;
#if defined(MUTEX_DIAG) && defined(HAVE_MUTEX_HYBRID)
	int waiters;
#endif

	dbenv = env->dbenv;

	if (!MUTEX_ON(env) || F_ISSET(dbenv, DB_ENV_NOLOCKING))
		return (0);

	mutexp = MUTEXP_SET(env, mutex);
#if defined(MUTEX_DIAG) && defined(HAVE_MUTEX_HYBRID)
	waiters = mutexp->wait;
#endif

#if !defined(HAVE_MUTEX_HYBRID) && defined(DIAGNOSTIC)
	if (!F_ISSET(mutexp, DB_MUTEX_LOCKED | DB_MUTEX_SHARED)) {
		__db_errx(env, DB_STR("2025",
		    "pthread unlock failed: lock already unlocked"));
		return (__env_panic(env, EACCES));
	}
#endif
	if (F_ISSET(mutexp, DB_MUTEX_SELF_BLOCK)) {
		ret = __db_pthread_mutex_prep(env, mutex, mutexp, TRUE);
		if (ret != 0)
			goto err;

#ifdef HAVE_MUTEX_HYBRID
		STAT_INC(env,
		    mutex, hybrid_wakeup, mutexp->hybrid_wakeup, mutex);
#else
		F_CLR(mutexp, DB_MUTEX_LOCKED);	/* nop if DB_MUTEX_SHARED */
#endif

		if (F_ISSET(mutexp, DB_MUTEX_SHARED))
			RET_SET(
			    (pthread_cond_broadcast(&mutexp->u.m.cond)), ret);
		else
			RET_SET((pthread_cond_signal(&mutexp->u.m.cond)), ret);
		if (ret != 0)
			goto err;
	} else {
#ifndef HAVE_MUTEX_HYBRID
		F_CLR(mutexp, DB_MUTEX_LOCKED);
#endif
	}

	/* See comment above; workaround for [#2471]. */
#if defined(HAVE_SHARED_LATCHES) && !defined(HAVE_MUTEX_HYBRID)
	if (F_ISSET(mutexp, DB_MUTEX_SHARED))
		RETRY_ON_EFAULT(pthread_rwlock_unlock(&mutexp->u.rwlock), ret);
	else
#endif
		RETRY_ON_EFAULT(pthread_mutex_unlock(&mutexp->u.m.mutex), ret);

err:	if (ret != 0) {
		__db_err(env, ret, "pthread unlock failed");
		return (__env_panic(env, ret));
	}
#if defined(MUTEX_DIAG) && defined(HAVE_MUTEX_HYBRID)
	if (!MUTEXP_IS_BUSY(mutexp) && mutexp->wait != 0)
		printf("unlock %ld %x busy %x waiters %d/%d\n",
		    mutex, pthread_self(), ret,
		    MUTEXP_BUSY_FIELD(mutexp), waiters, mutexp->wait);
#endif
	return (ret);
}

/*
 * __db_pthread_mutex_destroy --
 *	Destroy a mutex.
 *	If it is a native shared latch (not hybrid) then
 *	destroy only one half of the rwlock/mutex&cond union,
 *	depending whether it was allocated as shared
 *
 * PUBLIC: int __db_pthread_mutex_destroy __P((ENV *, db_mutex_t));
 */
int
__db_pthread_mutex_destroy(env, mutex)
	ENV *env;
	db_mutex_t mutex;
{
	DB_MUTEX *mutexp;
	DB_THREAD_INFO *ip;
	int ret, t_ret, failchk_thread;

	if (!MUTEX_ON(env))
		return (0);

	mutexp = MUTEXP_SET(env, mutex);

	ret = 0;
	failchk_thread = FALSE;
	/* Get information to determine if we are really the failchk thread. */
	if (F_ISSET(env->dbenv, DB_ENV_FAILCHK)) {
		ret = __env_set_state(env, &ip, THREAD_VERIFY);
		if (ip != NULL && ip->dbth_state == THREAD_FAILCHK)
			failchk_thread = TRUE;
	}

#ifndef HAVE_MUTEX_HYBRID
	if (F_ISSET(mutexp, DB_MUTEX_SHARED)) {
#if defined(HAVE_SHARED_LATCHES)
		/*
		 * If there were dead processes waiting on the condition
		 * we may not be able to destroy it.  Let failchk thread skip
		 * this, unless destroy is required.
		 * XXX What operating system resources might this leak?
		 */
#ifdef HAVE_PTHREAD_RWLOCK_REINIT_OKAY
		if (!failchk_thread)
#endif
			RET_SET(
			    (pthread_rwlock_destroy(&mutexp->u.rwlock)), ret);
		/* For rwlocks, we're done - must not destroy rest of union */
		return (ret);
#endif
	}
#endif
	if (F_ISSET(mutexp, DB_MUTEX_SELF_BLOCK)) {
		/*
		 * If there were dead processes waiting on the condition
		 * we may not be able to destroy it.  Let failchk thread
		 * skip this, unless destroy is required.
		 */
#ifdef HAVE_PTHREAD_COND_REINIT_OKAY
		if (!failchk_thread)
#endif
			RET_SET((pthread_cond_destroy(&mutexp->u.m.cond)), ret);
		if (ret != 0)
			__db_err(env, ret, DB_STR("2026",
			    "unable to destroy cond"));
	}
	RET_SET((pthread_mutex_destroy(&mutexp->u.m.mutex)), t_ret);
	if (t_ret != 0 && !failchk_thread) {
		__db_err(env, t_ret, DB_STR("2027",
		    "unable to destroy mutex"));
		if (ret == 0)
			ret = t_ret;
	}
	return (ret);
}
