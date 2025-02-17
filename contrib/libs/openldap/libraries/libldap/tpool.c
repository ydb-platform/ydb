/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 1998-2024 The OpenLDAP Foundation.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */

#include "portable.h"

#include <stdio.h>

#include <ac/signal.h>
#include <ac/stdarg.h>
#include <ac/stdlib.h>
#include <ac/string.h>
#include <ac/time.h>
#include <ac/errno.h>

#include "ldap-int.h"

#ifdef LDAP_R_COMPILE

#include "ldap_pvt_thread.h" /* Get the thread interface */
#include "ldap_queue.h"
#define LDAP_THREAD_POOL_IMPLEMENTATION
#include "ldap_thr_debug.h"  /* May rename symbols defined below */

#ifndef LDAP_THREAD_HAVE_TPOOL

#ifndef CACHELINE
#define CACHELINE	64
#endif

/* Thread-specific key with data and optional free function */
typedef struct ldap_int_tpool_key_s {
	void *ltk_key;
	void *ltk_data;
	ldap_pvt_thread_pool_keyfree_t *ltk_free;
} ldap_int_tpool_key_t;

/* Max number of thread-specific keys we store per thread.
 * We don't expect to use many...
 */
#define	MAXKEYS	32

/* Max number of threads */
#define	LDAP_MAXTHR	1024	/* must be a power of 2 */

/* (Theoretical) max number of pending requests */
#define MAX_PENDING (INT_MAX/2)	/* INT_MAX - (room to avoid overflow) */

/* pool->ltp_pause values */
enum { NOT_PAUSED = 0, WANT_PAUSE = 1, PAUSED = 2 };

/* Context: thread ID and thread-specific key/data pairs */
typedef struct ldap_int_thread_userctx_s {
	struct ldap_int_thread_poolq_s *ltu_pq;
	ldap_pvt_thread_t ltu_id;
	ldap_int_tpool_key_t ltu_key[MAXKEYS];
} ldap_int_thread_userctx_t;


/* Simple {thread ID -> context} hash table; key=ctx->ltu_id.
 * Protected by ldap_pvt_thread_pool_mutex.
 */
static struct {
	ldap_int_thread_userctx_t *ctx;
	/* ctx is valid when not NULL or DELETED_THREAD_CTX */
#	define DELETED_THREAD_CTX (&ldap_int_main_thrctx + 1) /* dummy addr */
} thread_keys[LDAP_MAXTHR];

#define	TID_HASH(tid, hash) do { \
	unsigned const char *ptr_ = (unsigned const char *)&(tid); \
	unsigned i_; \
	for (i_ = 0, (hash) = ptr_[0]; ++i_ < sizeof(tid);) \
		(hash) += ((hash) << 5) ^ ptr_[i_]; \
} while(0)


/* Task for a thread to perform */
typedef struct ldap_int_thread_task_s {
	union {
		LDAP_STAILQ_ENTRY(ldap_int_thread_task_s) q;
		LDAP_SLIST_ENTRY(ldap_int_thread_task_s) l;
	} ltt_next;
	ldap_pvt_thread_start_t *ltt_start_routine;
	void *ltt_arg;
	struct ldap_int_thread_poolq_s *ltt_queue;
} ldap_int_thread_task_t;

typedef LDAP_STAILQ_HEAD(tcq, ldap_int_thread_task_s) ldap_int_tpool_plist_t;

struct ldap_int_thread_poolq_s {
	void *ltp_free;

	struct ldap_int_thread_pool_s *ltp_pool;

	/* protect members below */
	ldap_pvt_thread_mutex_t ltp_mutex;

	/* not paused and something to do for pool_<wrapper/pause/destroy>()
	 * Used for normal pool operation, to synch between submitter and
	 * worker threads. Not used for pauses. In normal operation multiple
	 * queues can rendezvous without acquiring the main pool lock.
	 */
	ldap_pvt_thread_cond_t ltp_cond;

	/* ltp_pause == 0 ? &ltp_pending_list : &empty_pending_list,
	 * maintained to reduce work for pool_wrapper()
	 */
	ldap_int_tpool_plist_t *ltp_work_list;

	/* pending tasks, and unused task objects */
	ldap_int_tpool_plist_t ltp_pending_list;
	LDAP_SLIST_HEAD(tcl, ldap_int_thread_task_s) ltp_free_list;

	/* Max number of threads in this queue */
	int ltp_max_count;

	/* Max pending + paused + idle tasks, negated when ltp_finishing */
	int ltp_max_pending;

	int ltp_pending_count;		/* Pending + paused + idle tasks */
	int ltp_active_count;		/* Active, not paused/idle tasks */
	int ltp_open_count;			/* Number of threads */
	int ltp_starting;			/* Currently starting threads */
};

struct ldap_int_thread_pool_s {
	LDAP_STAILQ_ENTRY(ldap_int_thread_pool_s) ltp_next;

	struct ldap_int_thread_poolq_s **ltp_wqs;

	/* number of poolqs */
	int ltp_numqs;

	/* protect members below */
	ldap_pvt_thread_mutex_t ltp_mutex;

	/* paused and waiting for resume
	 * When a pause is in effect all workers switch to waiting on
	 * this cond instead of their per-queue cond.
	 */
	ldap_pvt_thread_cond_t ltp_cond;

	/* ltp_active_queues < 1 && ltp_pause */
	ldap_pvt_thread_cond_t ltp_pcond;

	/* number of active queues */
	int ltp_active_queues;

	/* The pool is finishing, waiting for its threads to close.
	 * They close when ltp_pending_list is done.  pool_submit()
	 * rejects new tasks.  ltp_max_pending = -(its old value).
	 */
	int ltp_finishing;

	/* Some active task needs to be the sole active task.
	 * Atomic variable so ldap_pvt_thread_pool_pausing() can read it.
	 */
	volatile sig_atomic_t ltp_pause;

	/* Max number of threads in pool */
	int ltp_max_count;

	/* Configured max number of threads in pool, 0 for default (LDAP_MAXTHR) */
	int ltp_conf_max_count;

	/* Max pending + paused + idle tasks, negated when ltp_finishing */
	int ltp_max_pending;
};

static ldap_int_tpool_plist_t empty_pending_list =
	LDAP_STAILQ_HEAD_INITIALIZER(empty_pending_list);

static int ldap_int_has_thread_pool = 0;
static LDAP_STAILQ_HEAD(tpq, ldap_int_thread_pool_s)
	ldap_int_thread_pool_list =
	LDAP_STAILQ_HEAD_INITIALIZER(ldap_int_thread_pool_list);

static ldap_pvt_thread_mutex_t ldap_pvt_thread_pool_mutex;

static void *ldap_int_thread_pool_wrapper( void *pool );

static ldap_pvt_thread_key_t	ldap_tpool_key;

/* Context of the main thread */
static ldap_int_thread_userctx_t ldap_int_main_thrctx;

int
ldap_int_thread_pool_startup ( void )
{
	ldap_int_main_thrctx.ltu_id = ldap_pvt_thread_self();
	ldap_pvt_thread_key_create( &ldap_tpool_key );
	return ldap_pvt_thread_mutex_init(&ldap_pvt_thread_pool_mutex);
}

int
ldap_int_thread_pool_shutdown ( void )
{
	struct ldap_int_thread_pool_s *pool;

	while ((pool = LDAP_STAILQ_FIRST(&ldap_int_thread_pool_list)) != NULL) {
		(ldap_pvt_thread_pool_destroy)(&pool, 0); /* ignore thr_debug macro */
	}
	ldap_pvt_thread_mutex_destroy(&ldap_pvt_thread_pool_mutex);
	ldap_pvt_thread_key_destroy( ldap_tpool_key );
	return(0);
}


/* Create a thread pool */
int
ldap_pvt_thread_pool_init_q (
	ldap_pvt_thread_pool_t *tpool,
	int max_threads,
	int max_pending,
	int numqs )
{
	ldap_pvt_thread_pool_t pool;
	struct ldap_int_thread_poolq_s *pq;
	int i, rc, rem_thr, rem_pend;

	/* multiple pools are currently not supported (ITS#4943) */
	assert(!ldap_int_has_thread_pool);

	if (! (0 <= max_threads && max_threads <= LDAP_MAXTHR))
		max_threads = 0;
	if (! (1 <= max_pending && max_pending <= MAX_PENDING))
		max_pending = MAX_PENDING;

	*tpool = NULL;
	pool = (ldap_pvt_thread_pool_t) LDAP_CALLOC(1,
		sizeof(struct ldap_int_thread_pool_s));

	if (pool == NULL) return(-1);

	pool->ltp_wqs = LDAP_MALLOC(numqs * sizeof(struct ldap_int_thread_poolq_s *));
	if (pool->ltp_wqs == NULL) {
		LDAP_FREE(pool);
		return(-1);
	}

	for (i=0; i<numqs; i++) {
		char *ptr = LDAP_CALLOC(1, sizeof(struct ldap_int_thread_poolq_s) + CACHELINE-1);
		if (ptr == NULL) {
			for (--i; i>=0; i--)
				LDAP_FREE(pool->ltp_wqs[i]->ltp_free);
			LDAP_FREE(pool->ltp_wqs);
			LDAP_FREE(pool);
			return(-1);
		}
		pool->ltp_wqs[i] = (struct ldap_int_thread_poolq_s *)(((size_t)ptr + CACHELINE-1) & ~(CACHELINE-1));
		pool->ltp_wqs[i]->ltp_free = ptr;
	}

	pool->ltp_numqs = numqs;
	pool->ltp_conf_max_count = max_threads;
	if ( !max_threads )
		max_threads = LDAP_MAXTHR;

	rc = ldap_pvt_thread_mutex_init(&pool->ltp_mutex);
	if (rc != 0) {
fail:
		for (i=0; i<numqs; i++)
			LDAP_FREE(pool->ltp_wqs[i]->ltp_free);
		LDAP_FREE(pool->ltp_wqs);
		LDAP_FREE(pool);
		return(rc);
	}

	rc = ldap_pvt_thread_cond_init(&pool->ltp_cond);
	if (rc != 0)
		goto fail;

	rc = ldap_pvt_thread_cond_init(&pool->ltp_pcond);
	if (rc != 0)
		goto fail;

	rem_thr = max_threads % numqs;
	rem_pend = max_pending % numqs;
	for ( i=0; i<numqs; i++ ) {
		pq = pool->ltp_wqs[i];
		pq->ltp_pool = pool;
		rc = ldap_pvt_thread_mutex_init(&pq->ltp_mutex);
		if (rc != 0)
			return(rc);
		rc = ldap_pvt_thread_cond_init(&pq->ltp_cond);
		if (rc != 0)
			return(rc);
		LDAP_STAILQ_INIT(&pq->ltp_pending_list);
		pq->ltp_work_list = &pq->ltp_pending_list;
		LDAP_SLIST_INIT(&pq->ltp_free_list);

		pq->ltp_max_count = max_threads / numqs;
		if ( rem_thr ) {
			pq->ltp_max_count++;
			rem_thr--;
		}
		pq->ltp_max_pending = max_pending / numqs;
		if ( rem_pend ) {
			pq->ltp_max_pending++;
			rem_pend--;
		}
	}

	ldap_int_has_thread_pool = 1;

	pool->ltp_max_count = max_threads;
	pool->ltp_max_pending = max_pending;

	ldap_pvt_thread_mutex_lock(&ldap_pvt_thread_pool_mutex);
	LDAP_STAILQ_INSERT_TAIL(&ldap_int_thread_pool_list, pool, ltp_next);
	ldap_pvt_thread_mutex_unlock(&ldap_pvt_thread_pool_mutex);

	/* Start no threads just yet.  That can break if the process forks
	 * later, as slapd does in order to daemonize.  On at least POSIX,
	 * only the forking thread would survive in the child.  Yet fork()
	 * can't unlock/clean up other threads' locks and data structures,
	 * unless pthread_atfork() handlers have been set up to do so.
	 */

	*tpool = pool;
	return(0);
}

int
ldap_pvt_thread_pool_init (
	ldap_pvt_thread_pool_t *tpool,
	int max_threads,
	int max_pending )
{
	return ldap_pvt_thread_pool_init_q( tpool, max_threads, max_pending, 1 );
}

/* Submit a task to be performed by the thread pool */
int
ldap_pvt_thread_pool_submit (
	ldap_pvt_thread_pool_t *tpool,
	ldap_pvt_thread_start_t *start_routine, void *arg )
{
	return ldap_pvt_thread_pool_submit2( tpool, start_routine, arg, NULL );
}

/* Submit a task to be performed by the thread pool */
int
ldap_pvt_thread_pool_submit2 (
	ldap_pvt_thread_pool_t *tpool,
	ldap_pvt_thread_start_t *start_routine, void *arg,
	void **cookie )
{
	struct ldap_int_thread_pool_s *pool;
	struct ldap_int_thread_poolq_s *pq;
	ldap_int_thread_task_t *task;
	ldap_pvt_thread_t thr;
	int i, j;

	if (tpool == NULL)
		return(-1);

	pool = *tpool;

	if (pool == NULL)
		return(-1);

	if ( pool->ltp_numqs > 1 ) {
		int min = pool->ltp_wqs[0]->ltp_max_pending + pool->ltp_wqs[0]->ltp_max_count;
		int min_x = 0, cnt;
		for ( i = 0; i < pool->ltp_numqs; i++ ) {
			/* take first queue that has nothing active */
			if ( !pool->ltp_wqs[i]->ltp_active_count ) {
				min_x = i;
				break;
			}
			cnt = pool->ltp_wqs[i]->ltp_active_count + pool->ltp_wqs[i]->ltp_pending_count;
			if ( cnt < min ) {
				min = cnt;
				min_x = i;
			}
		}
		i = min_x;
	} else
		i = 0;

	j = i;
	while(1) {
		ldap_pvt_thread_mutex_lock(&pool->ltp_wqs[i]->ltp_mutex);
		if (pool->ltp_wqs[i]->ltp_pending_count < pool->ltp_wqs[i]->ltp_max_pending) {
			break;
		}
		ldap_pvt_thread_mutex_unlock(&pool->ltp_wqs[i]->ltp_mutex);
		i++;
		i %= pool->ltp_numqs;
		if ( i == j )
			return -1;
	}

	pq = pool->ltp_wqs[i];
	task = LDAP_SLIST_FIRST(&pq->ltp_free_list);
	if (task) {
		LDAP_SLIST_REMOVE_HEAD(&pq->ltp_free_list, ltt_next.l);
	} else {
		task = (ldap_int_thread_task_t *) LDAP_MALLOC(sizeof(*task));
		if (task == NULL)
			goto failed;
	}

	task->ltt_start_routine = start_routine;
	task->ltt_arg = arg;
	task->ltt_queue = pq;
	if ( cookie )
		*cookie = task;

	pq->ltp_pending_count++;
	LDAP_STAILQ_INSERT_TAIL(&pq->ltp_pending_list, task, ltt_next.q);

	if (pool->ltp_pause)
		goto done;

	/* should we open (create) a thread? */
	if (pq->ltp_open_count < pq->ltp_active_count+pq->ltp_pending_count &&
		pq->ltp_open_count < pq->ltp_max_count)
	{
		pq->ltp_starting++;
		pq->ltp_open_count++;

		if (0 != ldap_pvt_thread_create(
			&thr, 1, ldap_int_thread_pool_wrapper, pq))
		{
			/* couldn't create thread.  back out of
			 * ltp_open_count and check for even worse things.
			 */
			pq->ltp_starting--;
			pq->ltp_open_count--;

			if (pq->ltp_open_count == 0) {
				/* no open threads at all?!?
				 */
				ldap_int_thread_task_t *ptr;

				/* let pool_close know there are no more threads */
				ldap_pvt_thread_cond_signal(&pq->ltp_cond);

				LDAP_STAILQ_FOREACH(ptr, &pq->ltp_pending_list, ltt_next.q)
					if (ptr == task) break;
				if (ptr == task) {
					/* no open threads, task not handled, so
					 * back out of ltp_pending_count, free the task,
					 * report the error.
					 */
					pq->ltp_pending_count--;
					LDAP_STAILQ_REMOVE(&pq->ltp_pending_list, task,
						ldap_int_thread_task_s, ltt_next.q);
					LDAP_SLIST_INSERT_HEAD(&pq->ltp_free_list, task,
						ltt_next.l);
					goto failed;
				}
			}
			/* there is another open thread, so this
			 * task will be handled eventually.
			 */
		}
	}
	ldap_pvt_thread_cond_signal(&pq->ltp_cond);

 done:
	ldap_pvt_thread_mutex_unlock(&pq->ltp_mutex);
	return(0);

 failed:
	ldap_pvt_thread_mutex_unlock(&pq->ltp_mutex);
	return(-1);
}

static void *
no_task( void *ctx, void *arg )
{
	return NULL;
}

/* Cancel a pending task that was previously submitted.
 * Return 1 if the task was successfully cancelled, 0 if
 * not found, -1 for invalid parameters
 */
int
ldap_pvt_thread_pool_retract (
	void *cookie )
{
	ldap_int_thread_task_t *task, *ttmp;
	struct ldap_int_thread_poolq_s *pq;

	if (cookie == NULL)
		return(-1);

	ttmp = cookie;
	pq = ttmp->ltt_queue;
	if (pq == NULL)
		return(-1);

	ldap_pvt_thread_mutex_lock(&pq->ltp_mutex);
	LDAP_STAILQ_FOREACH(task, &pq->ltp_pending_list, ltt_next.q)
		if (task == ttmp) {
			/* Could LDAP_STAILQ_REMOVE the task, but that
			 * walks ltp_pending_list again to find it.
			 */
			task->ltt_start_routine = no_task;
			task->ltt_arg = NULL;
			break;
		}
	ldap_pvt_thread_mutex_unlock(&pq->ltp_mutex);
	return task != NULL;
}

/* Walk the pool and allow tasks to be retracted, only to be called while the
 * pool is paused */
int
ldap_pvt_thread_pool_walk(
	ldap_pvt_thread_pool_t *tpool,
	ldap_pvt_thread_start_t *start,
	ldap_pvt_thread_walk_t *cb, void *arg )
{
	struct ldap_int_thread_pool_s *pool;
	struct ldap_int_thread_poolq_s *pq;
	ldap_int_thread_task_t *task;
	int i;

	if (tpool == NULL)
		return(-1);

	pool = *tpool;

	if (pool == NULL)
		return(-1);

	ldap_pvt_thread_mutex_lock(&pool->ltp_mutex);
	assert(pool->ltp_pause == PAUSED);
	ldap_pvt_thread_mutex_unlock(&pool->ltp_mutex);

	for (i=0; i<pool->ltp_numqs; i++) {
		pq = pool->ltp_wqs[i];
		LDAP_STAILQ_FOREACH(task, &pq->ltp_pending_list, ltt_next.q) {
			if ( task->ltt_start_routine == start ) {
				if ( cb( task->ltt_start_routine, task->ltt_arg, arg ) ) {
					/* retract */
					task->ltt_start_routine = no_task;
					task->ltt_arg = NULL;
				}
			}
		}
	}
	return 0;
}

/* Set number of work queues in this pool. Should not be
 * more than the number of CPUs. */
int
ldap_pvt_thread_pool_queues(
	ldap_pvt_thread_pool_t *tpool,
	int numqs )
{
	struct ldap_int_thread_pool_s *pool;
	struct ldap_int_thread_poolq_s *pq;
	int i, rc, rem_thr, rem_pend;

	if (numqs < 1 || tpool == NULL)
		return(-1);

	pool = *tpool;

	if (pool == NULL)
		return(-1);

	if (numqs < pool->ltp_numqs) {
		for (i=numqs; i<pool->ltp_numqs; i++)
			pool->ltp_wqs[i]->ltp_max_count = 0;
	} else if (numqs > pool->ltp_numqs) {
		struct ldap_int_thread_poolq_s **wqs;
		wqs = LDAP_REALLOC(pool->ltp_wqs, numqs * sizeof(struct ldap_int_thread_poolq_s *));
		if (wqs == NULL)
			return(-1);
		pool->ltp_wqs = wqs;
		for (i=pool->ltp_numqs; i<numqs; i++) {
			char *ptr = LDAP_CALLOC(1, sizeof(struct ldap_int_thread_poolq_s) + CACHELINE-1);
			if (ptr == NULL) {
				for (; i<numqs; i++)
					pool->ltp_wqs[i] = NULL;
				return(-1);
			}
			pq = (struct ldap_int_thread_poolq_s *)(((size_t)ptr + CACHELINE-1) & ~(CACHELINE-1));
			pq->ltp_free = ptr;
			pool->ltp_wqs[i] = pq;
			pq->ltp_pool = pool;
			rc = ldap_pvt_thread_mutex_init(&pq->ltp_mutex);
			if (rc != 0)
				return(rc);
			rc = ldap_pvt_thread_cond_init(&pq->ltp_cond);
			if (rc != 0)
				return(rc);
			LDAP_STAILQ_INIT(&pq->ltp_pending_list);
			pq->ltp_work_list = &pq->ltp_pending_list;
			LDAP_SLIST_INIT(&pq->ltp_free_list);
		}
	}
	rem_thr = pool->ltp_max_count % numqs;
	rem_pend = pool->ltp_max_pending % numqs;
	for ( i=0; i<numqs; i++ ) {
		pq = pool->ltp_wqs[i];
		pq->ltp_max_count = pool->ltp_max_count / numqs;
		if ( rem_thr ) {
			pq->ltp_max_count++;
			rem_thr--;
		}
		pq->ltp_max_pending = pool->ltp_max_pending / numqs;
		if ( rem_pend ) {
			pq->ltp_max_pending++;
			rem_pend--;
		}
	}
	pool->ltp_numqs = numqs;
	return 0;
}

/* Set max #threads.  value <= 0 means max supported #threads (LDAP_MAXTHR) */
int
ldap_pvt_thread_pool_maxthreads(
	ldap_pvt_thread_pool_t *tpool,
	int max_threads )
{
	struct ldap_int_thread_pool_s *pool;
	struct ldap_int_thread_poolq_s *pq;
	int remthr, i;

	if (! (0 <= max_threads && max_threads <= LDAP_MAXTHR))
		max_threads = 0;

	if (tpool == NULL)
		return(-1);

	pool = *tpool;

	if (pool == NULL)
		return(-1);

	pool->ltp_conf_max_count = max_threads;
	if ( !max_threads )
		max_threads = LDAP_MAXTHR;
	pool->ltp_max_count = max_threads;

	remthr = max_threads % pool->ltp_numqs;
	max_threads /= pool->ltp_numqs;

	for (i=0; i<pool->ltp_numqs; i++) {
		pq = pool->ltp_wqs[i];
		pq->ltp_max_count = max_threads;
		if (remthr) {
			pq->ltp_max_count++;
			remthr--;
		}
	}
	return(0);
}

/* Inspect the pool */
int
ldap_pvt_thread_pool_query(
	ldap_pvt_thread_pool_t *tpool,
	ldap_pvt_thread_pool_param_t param,
	void *value )
{
	struct ldap_int_thread_pool_s	*pool;
	int				count = -1;

	if ( tpool == NULL || value == NULL ) {
		return -1;
	}

	pool = *tpool;

	if ( pool == NULL ) {
		return 0;
	}

	switch ( param ) {
	case LDAP_PVT_THREAD_POOL_PARAM_MAX:
		count = pool->ltp_conf_max_count;
		break;

	case LDAP_PVT_THREAD_POOL_PARAM_MAX_PENDING:
		count = pool->ltp_max_pending;
		if (count < 0)
			count = -count;
		if (count == MAX_PENDING)
			count = 0;
		break;

	case LDAP_PVT_THREAD_POOL_PARAM_PAUSING:
		ldap_pvt_thread_mutex_lock(&pool->ltp_mutex);
		count = (pool->ltp_pause != 0);
		ldap_pvt_thread_mutex_unlock(&pool->ltp_mutex);
		break;

	case LDAP_PVT_THREAD_POOL_PARAM_OPEN:
	case LDAP_PVT_THREAD_POOL_PARAM_STARTING:
	case LDAP_PVT_THREAD_POOL_PARAM_ACTIVE:
	case LDAP_PVT_THREAD_POOL_PARAM_PENDING:
	case LDAP_PVT_THREAD_POOL_PARAM_BACKLOAD:
		{
			int i;
			count = 0;
			for (i=0; i<pool->ltp_numqs; i++) {
				struct ldap_int_thread_poolq_s *pq = pool->ltp_wqs[i];
				ldap_pvt_thread_mutex_lock(&pq->ltp_mutex);
				switch(param) {
					case LDAP_PVT_THREAD_POOL_PARAM_OPEN:
						count += pq->ltp_open_count;
						break;
					case LDAP_PVT_THREAD_POOL_PARAM_STARTING:
						count += pq->ltp_starting;
						break;
					case LDAP_PVT_THREAD_POOL_PARAM_ACTIVE:
						count += pq->ltp_active_count;
						break;
					case LDAP_PVT_THREAD_POOL_PARAM_PENDING:
						count += pq->ltp_pending_count;
						break;
					case LDAP_PVT_THREAD_POOL_PARAM_BACKLOAD:
						count += pq->ltp_pending_count + pq->ltp_active_count;
						break;
					default:
						break;
				}
				ldap_pvt_thread_mutex_unlock(&pq->ltp_mutex);
			}
			if (count < 0)
				count = -count;
		}
		break;

	case LDAP_PVT_THREAD_POOL_PARAM_ACTIVE_MAX:
		break;

	case LDAP_PVT_THREAD_POOL_PARAM_PENDING_MAX:
		break;

	case LDAP_PVT_THREAD_POOL_PARAM_BACKLOAD_MAX:
		break;

	case LDAP_PVT_THREAD_POOL_PARAM_STATE:
		if (pool->ltp_pause)
			*((char **)value) = "pausing";
		else if (!pool->ltp_finishing)
			*((char **)value) = "running";
		else {
			int i;
			for (i=0; i<pool->ltp_numqs; i++)
				if (pool->ltp_wqs[i]->ltp_pending_count) break;
			if (i<pool->ltp_numqs)
				*((char **)value) = "finishing";
			else
				*((char **)value) = "stopping";
		}
		break;

	case LDAP_PVT_THREAD_POOL_PARAM_PAUSED:
		ldap_pvt_thread_mutex_lock(&pool->ltp_mutex);
		count = (pool->ltp_pause == PAUSED);
		ldap_pvt_thread_mutex_unlock(&pool->ltp_mutex);
		break;

	case LDAP_PVT_THREAD_POOL_PARAM_UNKNOWN:
		break;
	}

	if ( count > -1 ) {
		*((int *)value) = count;
	}

	return ( count == -1 ? -1 : 0 );
}

/*
 * true if pool is pausing; does not lock any mutex to check.
 * 0 if not pause, 1 if pause, -1 if error or no pool.
 */
int
ldap_pvt_thread_pool_pausing( ldap_pvt_thread_pool_t *tpool )
{
	int rc = -1;
	struct ldap_int_thread_pool_s *pool;

	if ( tpool != NULL && (pool = *tpool) != NULL ) {
		rc = (pool->ltp_pause != 0);
	}

	return rc;
}

/*
 * wrapper for ldap_pvt_thread_pool_query(), left around
 * for backwards compatibility
 */
int
ldap_pvt_thread_pool_backload ( ldap_pvt_thread_pool_t *tpool )
{
	int	rc, count;

	rc = ldap_pvt_thread_pool_query( tpool,
		LDAP_PVT_THREAD_POOL_PARAM_BACKLOAD, (void *)&count );

	if ( rc == 0 ) {
		return count;
	}

	return rc;
}


/*
 * wrapper for ldap_pvt_thread_pool_close+free(), left around
 * for backwards compatibility
 */
int
ldap_pvt_thread_pool_destroy ( ldap_pvt_thread_pool_t *tpool, int run_pending )
{
	int rc;

	if ( (rc = ldap_pvt_thread_pool_close( tpool, run_pending )) ) {
		return rc;
	}

	return ldap_pvt_thread_pool_free( tpool );
}

/* Shut down the pool making its threads finish */
int
ldap_pvt_thread_pool_close ( ldap_pvt_thread_pool_t *tpool, int run_pending )
{
	struct ldap_int_thread_pool_s *pool, *pptr;
	struct ldap_int_thread_poolq_s *pq;
	ldap_int_thread_task_t *task;
	int i;

	if (tpool == NULL)
		return(-1);

	pool = *tpool;

	if (pool == NULL) return(-1);

	ldap_pvt_thread_mutex_lock(&ldap_pvt_thread_pool_mutex);
	LDAP_STAILQ_FOREACH(pptr, &ldap_int_thread_pool_list, ltp_next)
		if (pptr == pool) break;
	ldap_pvt_thread_mutex_unlock(&ldap_pvt_thread_pool_mutex);

	if (pool != pptr) return(-1);

	ldap_pvt_thread_mutex_lock(&pool->ltp_mutex);

	pool->ltp_finishing = 1;
	if (pool->ltp_max_pending > 0)
		pool->ltp_max_pending = -pool->ltp_max_pending;

	ldap_pvt_thread_cond_broadcast(&pool->ltp_cond);
	ldap_pvt_thread_mutex_unlock(&pool->ltp_mutex);

	for (i=0; i<pool->ltp_numqs; i++) {
		pq = pool->ltp_wqs[i];
		ldap_pvt_thread_mutex_lock(&pq->ltp_mutex);
		if (pq->ltp_max_pending > 0)
			pq->ltp_max_pending = -pq->ltp_max_pending;
		if (!run_pending) {
			while ((task = LDAP_STAILQ_FIRST(&pq->ltp_pending_list)) != NULL) {
				LDAP_STAILQ_REMOVE_HEAD(&pq->ltp_pending_list, ltt_next.q);
				LDAP_FREE(task);
			}
			pq->ltp_pending_count = 0;
		}

		while (pq->ltp_open_count) {
			ldap_pvt_thread_cond_broadcast(&pq->ltp_cond);
			ldap_pvt_thread_cond_wait(&pq->ltp_cond, &pq->ltp_mutex);
		}

		while ((task = LDAP_SLIST_FIRST(&pq->ltp_free_list)) != NULL)
		{
			LDAP_SLIST_REMOVE_HEAD(&pq->ltp_free_list, ltt_next.l);
			LDAP_FREE(task);
		}
		ldap_pvt_thread_mutex_unlock(&pq->ltp_mutex);
	}

	return(0);
}

/* Destroy the pool, everything must have already shut down */
int
ldap_pvt_thread_pool_free ( ldap_pvt_thread_pool_t *tpool )
{
	struct ldap_int_thread_pool_s *pool, *pptr;
	struct ldap_int_thread_poolq_s *pq;
	int i;

	if (tpool == NULL)
		return(-1);

	pool = *tpool;

	if (pool == NULL) return(-1);

	ldap_pvt_thread_mutex_lock(&ldap_pvt_thread_pool_mutex);
	LDAP_STAILQ_FOREACH(pptr, &ldap_int_thread_pool_list, ltp_next)
		if (pptr == pool) break;
	if (pptr == pool)
		LDAP_STAILQ_REMOVE(&ldap_int_thread_pool_list, pool,
			ldap_int_thread_pool_s, ltp_next);
	ldap_pvt_thread_mutex_unlock(&ldap_pvt_thread_pool_mutex);

	if (pool != pptr) return(-1);

	ldap_pvt_thread_cond_destroy(&pool->ltp_pcond);
	ldap_pvt_thread_cond_destroy(&pool->ltp_cond);
	ldap_pvt_thread_mutex_destroy(&pool->ltp_mutex);
	for (i=0; i<pool->ltp_numqs; i++) {
		pq = pool->ltp_wqs[i];

		assert( !pq->ltp_open_count );
		assert( LDAP_SLIST_EMPTY(&pq->ltp_free_list) );
		ldap_pvt_thread_cond_destroy(&pq->ltp_cond);
		ldap_pvt_thread_mutex_destroy(&pq->ltp_mutex);
		if (pq->ltp_free) {
			LDAP_FREE(pq->ltp_free);
		}
	}
	LDAP_FREE(pool->ltp_wqs);
	LDAP_FREE(pool);
	*tpool = NULL;
	ldap_int_has_thread_pool = 0;
	return(0);
}

/* Thread loop.  Accept and handle submitted tasks. */
static void *
ldap_int_thread_pool_wrapper ( 
	void *xpool )
{
	struct ldap_int_thread_poolq_s *pq = xpool;
	struct ldap_int_thread_pool_s *pool = pq->ltp_pool;
	ldap_int_thread_task_t *task;
	ldap_int_tpool_plist_t *work_list;
	ldap_int_thread_userctx_t ctx, *kctx;
	unsigned i, keyslot, hash;
	int pool_lock = 0, freeme = 0;

	assert(pool != NULL);

	for ( i=0; i<MAXKEYS; i++ ) {
		ctx.ltu_key[i].ltk_key = NULL;
	}

	ctx.ltu_pq = pq;
	ctx.ltu_id = ldap_pvt_thread_self();
	TID_HASH(ctx.ltu_id, hash);

	ldap_pvt_thread_key_setdata( ldap_tpool_key, &ctx );

	if (pool->ltp_pause) {
		ldap_pvt_thread_mutex_lock(&pool->ltp_mutex);
		/* thread_keys[] is read-only when paused */
		while (pool->ltp_pause)
			ldap_pvt_thread_cond_wait(&pool->ltp_cond, &pool->ltp_mutex);
		ldap_pvt_thread_mutex_unlock(&pool->ltp_mutex);
	}

	/* find a key slot to give this thread ID and store a
	 * pointer to our keys there; start at the thread ID
	 * itself (mod LDAP_MAXTHR) and look for an empty slot.
	 */
	ldap_pvt_thread_mutex_lock(&ldap_pvt_thread_pool_mutex);
	for (keyslot = hash & (LDAP_MAXTHR-1);
		(kctx = thread_keys[keyslot].ctx) && kctx != DELETED_THREAD_CTX;
		keyslot = (keyslot+1) & (LDAP_MAXTHR-1));
	thread_keys[keyslot].ctx = &ctx;
	ldap_pvt_thread_mutex_unlock(&ldap_pvt_thread_pool_mutex);

	ldap_pvt_thread_mutex_lock(&pq->ltp_mutex);
	pq->ltp_starting--;
	pq->ltp_active_count++;

	for (;;) {
		work_list = pq->ltp_work_list; /* help the compiler a bit */
		task = LDAP_STAILQ_FIRST(work_list);
		if (task == NULL) {	/* paused or no pending tasks */
			if (--(pq->ltp_active_count) < 1) {
				if (pool->ltp_pause) {
					ldap_pvt_thread_mutex_unlock(&pq->ltp_mutex);
					ldap_pvt_thread_mutex_lock(&pool->ltp_mutex);
					pool_lock = 1;
					if (--(pool->ltp_active_queues) < 1) {
						/* Notify pool_pause it is the sole active thread. */
						ldap_pvt_thread_cond_signal(&pool->ltp_pcond);
					}
				}
			}

			do {
				if (pool->ltp_finishing || pq->ltp_open_count > pq->ltp_max_count) {
					/* Not paused, and either finishing or too many
					 * threads running (can happen if ltp_max_count
					 * was reduced).  Let this thread die.
					 */
					goto done;
				}

				/* We could check an idle timer here, and let the
				 * thread die if it has been inactive for a while.
				 * Only die if there are other open threads (i.e.,
				 * always have at least one thread open).
				 * The check should be like this:
				 *   if (pool->ltp_open_count>1 && pool->ltp_starting==0)
				 *       check timer, wait if ltp_pause, leave thread;
				 *
				 * Just use pthread_cond_timedwait() if we want to
				 * check idle time.
				 */
				if (pool_lock) {
					ldap_pvt_thread_cond_wait(&pool->ltp_cond, &pool->ltp_mutex);
					if (!pool->ltp_pause) {
						ldap_pvt_thread_mutex_unlock(&pool->ltp_mutex);
						ldap_pvt_thread_mutex_lock(&pq->ltp_mutex);
						pool_lock = 0;
					}
				} else
					ldap_pvt_thread_cond_wait(&pq->ltp_cond, &pq->ltp_mutex);

				work_list = pq->ltp_work_list;
				task = LDAP_STAILQ_FIRST(work_list);
			} while (task == NULL);

			if (pool_lock) {
				ldap_pvt_thread_mutex_unlock(&pool->ltp_mutex);
				ldap_pvt_thread_mutex_lock(&pq->ltp_mutex);
				pool_lock = 0;
			}
			pq->ltp_active_count++;
		}

		LDAP_STAILQ_REMOVE_HEAD(work_list, ltt_next.q);
		pq->ltp_pending_count--;
		ldap_pvt_thread_mutex_unlock(&pq->ltp_mutex);

		task->ltt_start_routine(&ctx, task->ltt_arg);

		ldap_pvt_thread_mutex_lock(&pq->ltp_mutex);
		LDAP_SLIST_INSERT_HEAD(&pq->ltp_free_list, task, ltt_next.l);
	}
 done:

	ldap_pvt_thread_mutex_lock(&ldap_pvt_thread_pool_mutex);

	/* The pool_mutex lock protects ctx->ltu_key from pool_purgekey()
	 * during this call, since it prevents new pauses. */
	ldap_pvt_thread_pool_context_reset(&ctx);

	thread_keys[keyslot].ctx = DELETED_THREAD_CTX;
	ldap_pvt_thread_mutex_unlock(&ldap_pvt_thread_pool_mutex);

	pq->ltp_open_count--;
	if (pq->ltp_open_count == 0) {
		if (pool->ltp_finishing)
			/* let pool_destroy know we're all done */
			ldap_pvt_thread_cond_signal(&pq->ltp_cond);
		else
			freeme = 1;
	}

	if (pool_lock)
		ldap_pvt_thread_mutex_unlock(&pool->ltp_mutex);
	else
		ldap_pvt_thread_mutex_unlock(&pq->ltp_mutex);

	if (freeme) {
		ldap_pvt_thread_cond_destroy(&pq->ltp_cond);
		ldap_pvt_thread_mutex_destroy(&pq->ltp_mutex);
		LDAP_FREE(pq->ltp_free);
		pq->ltp_free = NULL;
	}
	ldap_pvt_thread_exit(NULL);
	return(NULL);
}

/* Arguments > ltp_pause to handle_pause(,PAUSE_ARG()).  arg=PAUSE_ARG
 * ensures (arg-ltp_pause) sets GO_* at need and keeps DO_PAUSE/GO_*.
 */
#define GO_IDLE		8
#define GO_UNIDLE	16
#define CHECK_PAUSE	32	/* if ltp_pause: GO_IDLE; wait; GO_UNIDLE */
#define DO_PAUSE	64	/* CHECK_PAUSE; pause the pool */
#define PAUSE_ARG(a) \
		((a) | ((a) & (GO_IDLE|GO_UNIDLE) ? GO_IDLE-1 : CHECK_PAUSE))

static int
handle_pause( ldap_pvt_thread_pool_t *tpool, int pause_type )
{
	struct ldap_int_thread_pool_s *pool;
	struct ldap_int_thread_poolq_s *pq;
	int ret = 0, pause, max_ltp_pause;

	if (tpool == NULL)
		return(-1);

	pool = *tpool;

	if (pool == NULL)
		return(0);

	if (pause_type == CHECK_PAUSE && !pool->ltp_pause)
		return(0);

	{
		ldap_int_thread_userctx_t *ctx = ldap_pvt_thread_pool_context();
		pq = ctx->ltu_pq;
		if ( !pq )
			return(-1);
	}

	/* Let pool_unidle() ignore requests for new pauses */
	max_ltp_pause = pause_type==PAUSE_ARG(GO_UNIDLE) ? WANT_PAUSE : NOT_PAUSED;

	ldap_pvt_thread_mutex_lock(&pool->ltp_mutex);

	pause = pool->ltp_pause;	/* NOT_PAUSED, WANT_PAUSE or PAUSED */

	/* If ltp_pause and not GO_IDLE|GO_UNIDLE: Set GO_IDLE,GO_UNIDLE */
	pause_type -= pause;

	if (pause_type & GO_IDLE) {
		int do_pool = 0;
		ldap_pvt_thread_mutex_lock(&pq->ltp_mutex);
		pq->ltp_pending_count++;
		pq->ltp_active_count--;
		if (pause && pq->ltp_active_count < 1) {
			do_pool = 1;
		}
		ldap_pvt_thread_mutex_unlock(&pq->ltp_mutex);
		if (do_pool) {
			pool->ltp_active_queues--;
			if (pool->ltp_active_queues < 1)
			/* Tell the task waiting to DO_PAUSE it can proceed */
				ldap_pvt_thread_cond_signal(&pool->ltp_pcond);
		}
	}

	if (pause_type & GO_UNIDLE) {
		/* Wait out pause if any, then cancel GO_IDLE */
		if (pause > max_ltp_pause) {
			ret = 1;
			do {
				ldap_pvt_thread_cond_wait(&pool->ltp_cond, &pool->ltp_mutex);
			} while (pool->ltp_pause > max_ltp_pause);
		}
		ldap_pvt_thread_mutex_lock(&pq->ltp_mutex);
		pq->ltp_pending_count--;
		pq->ltp_active_count++;
		ldap_pvt_thread_mutex_unlock(&pq->ltp_mutex);
	}

	if (pause_type & DO_PAUSE) {
		int i, j;
		/* Tell everyone else to pause or finish, then await that */
		ret = 0;
		assert(!pool->ltp_pause);
		pool->ltp_pause = WANT_PAUSE;
		pool->ltp_active_queues = 0;

		for (i=0; i<pool->ltp_numqs; i++)
			if (pool->ltp_wqs[i] == pq) break;

		ldap_pvt_thread_mutex_lock(&pq->ltp_mutex);
		/* temporarily remove ourself from active count */
		pq->ltp_active_count--;

		j=i;
		do {
			pq = pool->ltp_wqs[j];
			if (j != i)
				ldap_pvt_thread_mutex_lock(&pq->ltp_mutex);

			/* Hide pending tasks from ldap_pvt_thread_pool_wrapper() */
			pq->ltp_work_list = &empty_pending_list;

			if (pq->ltp_active_count > 0)
				pool->ltp_active_queues++;

			ldap_pvt_thread_mutex_unlock(&pq->ltp_mutex);
			if (pool->ltp_numqs > 1) {
				j++;
				j %= pool->ltp_numqs;
			}
		} while (j != i);

		/* Wait for this task to become the sole active task */
		while (pool->ltp_active_queues > 0)
			ldap_pvt_thread_cond_wait(&pool->ltp_pcond, &pool->ltp_mutex);

		/* restore us to active count */
		pool->ltp_wqs[i]->ltp_active_count++;

		assert(pool->ltp_pause == WANT_PAUSE);
		pool->ltp_pause = PAUSED;
	}
	ldap_pvt_thread_mutex_unlock(&pool->ltp_mutex);

	return(ret);
}

/* Consider this task idle: It will not block pool_pause() in other tasks. */
void
ldap_pvt_thread_pool_idle( ldap_pvt_thread_pool_t *tpool )
{
	handle_pause(tpool, PAUSE_ARG(GO_IDLE));
}

/* Cancel pool_idle(). If the pool is paused, wait it out first. */
void
ldap_pvt_thread_pool_unidle( ldap_pvt_thread_pool_t *tpool )
{
	handle_pause(tpool, PAUSE_ARG(GO_UNIDLE));
}

/*
 * If a pause was requested, wait for it.  If several threads
 * are waiting to pause, let through one or more pauses.
 * The calling task must be active, not idle.
 * Return 1 if we waited, 0 if not, -1 at parameter error.
 */
int
ldap_pvt_thread_pool_pausewait( ldap_pvt_thread_pool_t *tpool )
{
	return handle_pause(tpool, PAUSE_ARG(CHECK_PAUSE));
}

/* Return 1 if a pause has been requested */
int
ldap_pvt_thread_pool_pausequery( ldap_pvt_thread_pool_t *tpool )
{
	struct ldap_int_thread_pool_s *pool;
	if ( !tpool )
		return -1;

	pool = *tpool;
	if ( !pool )
		return 0;

	return pool->ltp_pause != 0;
}

/*
 * Wait for a pause, from a non-pooled thread.
 */
int
ldap_pvt_thread_pool_pausecheck_native( ldap_pvt_thread_pool_t *tpool )
{
	struct ldap_int_thread_pool_s *pool;

	if (tpool == NULL)
		return(-1);

	pool = *tpool;

	if (pool == NULL)
		return(0);

	if (!pool->ltp_pause)
		return(0);

	ldap_pvt_thread_mutex_lock(&pool->ltp_mutex);
	while (pool->ltp_pause)
			ldap_pvt_thread_cond_wait(&pool->ltp_cond, &pool->ltp_mutex);
	ldap_pvt_thread_mutex_unlock(&pool->ltp_mutex);
	return 1;
}

/*
 * Pause the pool.  The calling task must be active, not idle.
 * Return when all other tasks are paused or idle.
 */
int
ldap_pvt_thread_pool_pause( ldap_pvt_thread_pool_t *tpool )
{
	return handle_pause(tpool, PAUSE_ARG(DO_PAUSE));
}

/* End a pause */
int
ldap_pvt_thread_pool_resume ( 
	ldap_pvt_thread_pool_t *tpool )
{
	struct ldap_int_thread_pool_s *pool;
	struct ldap_int_thread_poolq_s *pq;
	int i;

	if (tpool == NULL)
		return(-1);

	pool = *tpool;

	if (pool == NULL)
		return(0);

	ldap_pvt_thread_mutex_lock(&pool->ltp_mutex);
	assert(pool->ltp_pause == PAUSED);
	pool->ltp_pause = 0;
	for (i=0; i<pool->ltp_numqs; i++) {
		pq = pool->ltp_wqs[i];
		pq->ltp_work_list = &pq->ltp_pending_list;
		ldap_pvt_thread_cond_broadcast(&pq->ltp_cond);
	}
	ldap_pvt_thread_cond_broadcast(&pool->ltp_cond);
	ldap_pvt_thread_mutex_unlock(&pool->ltp_mutex);
	return(0);
}

/*
 * Get the key's data and optionally free function in the given context.
 */
int ldap_pvt_thread_pool_getkey(
	void *xctx,
	void *key,
	void **data,
	ldap_pvt_thread_pool_keyfree_t **kfree )
{
	ldap_int_thread_userctx_t *ctx = xctx;
	int i;

	if ( !ctx || !key || !data ) return EINVAL;

	for ( i=0; i<MAXKEYS && ctx->ltu_key[i].ltk_key; i++ ) {
		if ( ctx->ltu_key[i].ltk_key == key ) {
			*data = ctx->ltu_key[i].ltk_data;
			if ( kfree ) *kfree = ctx->ltu_key[i].ltk_free;
			return 0;
		}
	}
	return ENOENT;
}

static void
clear_key_idx( ldap_int_thread_userctx_t *ctx, int i )
{
	for ( ; i < MAXKEYS-1 && ctx->ltu_key[i+1].ltk_key; i++ )
		ctx->ltu_key[i] = ctx->ltu_key[i+1];
	ctx->ltu_key[i].ltk_key = NULL;
}

/*
 * Set or remove data for the key in the given context.
 * key can be any unique pointer.
 * kfree() is an optional function to free the data (but not the key):
 *   pool_context_reset() and pool_purgekey() call kfree(key, data),
 *   but pool_setkey() does not.  For pool_setkey() it is the caller's
 *   responsibility to free any existing data with the same key.
 *   kfree() must not call functions taking a tpool argument.
 */
int ldap_pvt_thread_pool_setkey(
	void *xctx,
	void *key,
	void *data,
	ldap_pvt_thread_pool_keyfree_t *kfree,
	void **olddatap,
	ldap_pvt_thread_pool_keyfree_t **oldkfreep )
{
	ldap_int_thread_userctx_t *ctx = xctx;
	int i, found;

	if ( !ctx || !key ) return EINVAL;

	for ( i=found=0; i<MAXKEYS; i++ ) {
		if ( ctx->ltu_key[i].ltk_key == key ) {
			found = 1;
			break;
		} else if ( !ctx->ltu_key[i].ltk_key ) {
			break;
		}
	}

	if ( olddatap ) {
		if ( found ) {
			*olddatap = ctx->ltu_key[i].ltk_data;
		} else {
			*olddatap = NULL;
		}
	}

	if ( oldkfreep ) {
		if ( found ) {
			*oldkfreep = ctx->ltu_key[i].ltk_free;
		} else {
			*oldkfreep = 0;
		}
	}

	if ( data || kfree ) {
		if ( i>=MAXKEYS )
			return ENOMEM;
		ctx->ltu_key[i].ltk_key = key;
		ctx->ltu_key[i].ltk_data = data;
		ctx->ltu_key[i].ltk_free = kfree;
	} else if ( found ) {
		clear_key_idx( ctx, i );
	}

	return 0;
}

/* Free all elements with this key, no matter which thread they're in.
 * May only be called while the pool is paused.
 */
void ldap_pvt_thread_pool_purgekey( void *key )
{
	int i, j;
	ldap_int_thread_userctx_t *ctx;

	assert ( key != NULL );

	ldap_pvt_thread_mutex_lock(&ldap_pvt_thread_pool_mutex);
	for ( i=0; i<LDAP_MAXTHR; i++ ) {
		ctx = thread_keys[i].ctx;
		if ( ctx && ctx != DELETED_THREAD_CTX ) {
			for ( j=0; j<MAXKEYS && ctx->ltu_key[j].ltk_key; j++ ) {
				if ( ctx->ltu_key[j].ltk_key == key ) {
					if (ctx->ltu_key[j].ltk_free)
						ctx->ltu_key[j].ltk_free( ctx->ltu_key[j].ltk_key,
						ctx->ltu_key[j].ltk_data );
					clear_key_idx( ctx, j );
					break;
				}
			}
		}
	}
	ldap_pvt_thread_mutex_unlock(&ldap_pvt_thread_pool_mutex);
}

/*
 * Find the context of the current thread.
 * This is necessary if the caller does not have access to the
 * thread context handle (for example, a slapd plugin calling
 * slapi_search_internal()). No doubt it is more efficient
 * for the application to keep track of the thread context
 * handles itself.
 */
void *ldap_pvt_thread_pool_context( )
{
	void *ctx = NULL;

	ldap_pvt_thread_key_getdata( ldap_tpool_key, &ctx );
	return ctx ? ctx : (void *) &ldap_int_main_thrctx;
}

/*
 * Free the context's keys.
 * Must not call functions taking a tpool argument (because this
 * thread already holds ltp_mutex when called from pool_wrapper()).
 */
void ldap_pvt_thread_pool_context_reset( void *vctx )
{
	ldap_int_thread_userctx_t *ctx = vctx;
	int i;

	for ( i=MAXKEYS-1; i>=0; i--) {
		if ( !ctx->ltu_key[i].ltk_key )
			continue;
		if ( ctx->ltu_key[i].ltk_free )
			ctx->ltu_key[i].ltk_free( ctx->ltu_key[i].ltk_key,
			ctx->ltu_key[i].ltk_data );
		ctx->ltu_key[i].ltk_key = NULL;
	}
}

ldap_pvt_thread_t ldap_pvt_thread_pool_tid( void *vctx )
{
	ldap_int_thread_userctx_t *ctx = vctx;

	return ctx->ltu_id;
}
#endif /* LDAP_THREAD_HAVE_TPOOL */

#endif /* LDAP_R_COMPILE */
