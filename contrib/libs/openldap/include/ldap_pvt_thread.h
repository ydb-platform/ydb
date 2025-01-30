/* ldap_pvt_thread.h - ldap threads header file */
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

#ifndef _LDAP_PVT_THREAD_H
#define _LDAP_PVT_THREAD_H /* libldap/ldap_thr_debug.h #undefines this */

#include "ldap_cdefs.h"
#include "ldap_int_thread.h"

LDAP_BEGIN_DECL

#ifndef LDAP_PVT_THREAD_H_DONE
typedef ldap_int_thread_t			ldap_pvt_thread_t;
#ifdef LDAP_THREAD_DEBUG_WRAP
typedef ldap_debug_thread_mutex_t	ldap_pvt_thread_mutex_t;
typedef ldap_debug_thread_cond_t	ldap_pvt_thread_cond_t;
typedef ldap_debug_thread_rdwr_t	ldap_pvt_thread_rdwr_t;
#define LDAP_PVT_MUTEX_FIRSTCREATE	LDAP_DEBUG_MUTEX_FIRSTCREATE
#define LDAP_PVT_MUTEX_NULL			LDAP_DEBUG_MUTEX_NULL
#else
typedef ldap_int_thread_mutex_t		ldap_pvt_thread_mutex_t;
typedef ldap_int_thread_cond_t		ldap_pvt_thread_cond_t;
typedef ldap_int_thread_rdwr_t		ldap_pvt_thread_rdwr_t;
#define LDAP_PVT_MUTEX_FIRSTCREATE	LDAP_INT_MUTEX_FIRSTCREATE
#define LDAP_PVT_MUTEX_NULL			LDAP_INT_MUTEX_NULL
#endif
typedef ldap_int_thread_key_t	ldap_pvt_thread_key_t;
#endif /* !LDAP_PVT_THREAD_H_DONE */

#define ldap_pvt_thread_equal		ldap_int_thread_equal

LDAP_F( int )
ldap_pvt_thread_initialize LDAP_P(( void ));

LDAP_F( int )
ldap_pvt_thread_destroy LDAP_P(( void ));

LDAP_F( unsigned int )
ldap_pvt_thread_sleep LDAP_P(( unsigned int s ));

LDAP_F( int )
ldap_pvt_thread_get_concurrency LDAP_P(( void ));

LDAP_F( int )
ldap_pvt_thread_set_concurrency LDAP_P(( int ));

#define LDAP_PVT_THREAD_CREATE_JOINABLE 0
#define LDAP_PVT_THREAD_CREATE_DETACHED 1

#ifndef LDAP_PVT_THREAD_H_DONE
#define	LDAP_PVT_THREAD_SET_STACK_SIZE
/* The size may be explicitly #defined to zero to disable it. */
#if defined( LDAP_PVT_THREAD_STACK_SIZE ) && LDAP_PVT_THREAD_STACK_SIZE == 0
#	undef LDAP_PVT_THREAD_SET_STACK_SIZE
#elif !defined( LDAP_PVT_THREAD_STACK_SIZE )
	/* LARGE stack. Will be twice as large on 64 bit machine. */
#	define LDAP_PVT_THREAD_STACK_SIZE ( 1 * 1024 * 1024 * sizeof(void *) )
#endif
#endif /* !LDAP_PVT_THREAD_H_DONE */

LDAP_F( int )
ldap_pvt_thread_create LDAP_P((
	ldap_pvt_thread_t * thread,
	int	detach,
	void *(*start_routine)( void * ),
	void *arg));

LDAP_F( void )
ldap_pvt_thread_exit LDAP_P(( void *retval ));

LDAP_F( int )
ldap_pvt_thread_join LDAP_P(( ldap_pvt_thread_t thread, void **status ));

LDAP_F( int )
ldap_pvt_thread_kill LDAP_P(( ldap_pvt_thread_t thread, int signo ));

LDAP_F( int )
ldap_pvt_thread_yield LDAP_P(( void ));

LDAP_F( int )
ldap_pvt_thread_cond_init LDAP_P(( ldap_pvt_thread_cond_t *cond ));

LDAP_F( int )
ldap_pvt_thread_cond_destroy LDAP_P(( ldap_pvt_thread_cond_t *cond ));

LDAP_F( int )
ldap_pvt_thread_cond_signal LDAP_P(( ldap_pvt_thread_cond_t *cond ));

LDAP_F( int )
ldap_pvt_thread_cond_broadcast LDAP_P(( ldap_pvt_thread_cond_t *cond ));

LDAP_F( int )
ldap_pvt_thread_cond_wait LDAP_P((
	ldap_pvt_thread_cond_t *cond,
	ldap_pvt_thread_mutex_t *mutex ));

LDAP_F( int )
ldap_pvt_thread_mutex_init LDAP_P(( ldap_pvt_thread_mutex_t *mutex ));

LDAP_F( int )
ldap_pvt_thread_mutex_recursive_init LDAP_P(( ldap_pvt_thread_mutex_t *mutex ));

LDAP_F( int )
ldap_pvt_thread_mutex_destroy LDAP_P(( ldap_pvt_thread_mutex_t *mutex ));

LDAP_F( int )
ldap_pvt_thread_mutex_lock LDAP_P(( ldap_pvt_thread_mutex_t *mutex ));

LDAP_F( int )
ldap_pvt_thread_mutex_trylock LDAP_P(( ldap_pvt_thread_mutex_t *mutex ));

LDAP_F( int )
ldap_pvt_thread_mutex_unlock LDAP_P(( ldap_pvt_thread_mutex_t *mutex ));

LDAP_F( ldap_pvt_thread_t )
ldap_pvt_thread_self LDAP_P(( void ));

#ifdef	LDAP_INT_THREAD_ASSERT_MUTEX_OWNER
#define	LDAP_PVT_THREAD_ASSERT_MUTEX_OWNER LDAP_INT_THREAD_ASSERT_MUTEX_OWNER
#else
#define	LDAP_PVT_THREAD_ASSERT_MUTEX_OWNER(mutex) ((void) 0)
#endif

LDAP_F( int )
ldap_pvt_thread_rdwr_init LDAP_P((ldap_pvt_thread_rdwr_t *rdwrp));

LDAP_F( int )
ldap_pvt_thread_rdwr_destroy LDAP_P((ldap_pvt_thread_rdwr_t *rdwrp));

LDAP_F( int )
ldap_pvt_thread_rdwr_rlock LDAP_P((ldap_pvt_thread_rdwr_t *rdwrp));

LDAP_F( int )
ldap_pvt_thread_rdwr_rtrylock LDAP_P((ldap_pvt_thread_rdwr_t *rdwrp));

LDAP_F( int )
ldap_pvt_thread_rdwr_runlock LDAP_P((ldap_pvt_thread_rdwr_t *rdwrp));

LDAP_F( int )
ldap_pvt_thread_rdwr_wlock LDAP_P((ldap_pvt_thread_rdwr_t *rdwrp));

LDAP_F( int )
ldap_pvt_thread_rdwr_wtrylock LDAP_P((ldap_pvt_thread_rdwr_t *rdwrp));

LDAP_F( int )
ldap_pvt_thread_rdwr_wunlock LDAP_P((ldap_pvt_thread_rdwr_t *rdwrp));

LDAP_F( int )
ldap_pvt_thread_key_create LDAP_P((ldap_pvt_thread_key_t *keyp));

LDAP_F( int )
ldap_pvt_thread_key_destroy LDAP_P((ldap_pvt_thread_key_t key));

LDAP_F( int )
ldap_pvt_thread_key_setdata LDAP_P((ldap_pvt_thread_key_t key, void *data));

LDAP_F( int )
ldap_pvt_thread_key_getdata LDAP_P((ldap_pvt_thread_key_t key, void **data));

#ifdef LDAP_DEBUG
LDAP_F( int )
ldap_pvt_thread_rdwr_readers LDAP_P((ldap_pvt_thread_rdwr_t *rdwrp));

LDAP_F( int )
ldap_pvt_thread_rdwr_writers LDAP_P((ldap_pvt_thread_rdwr_t *rdwrp));

LDAP_F( int )
ldap_pvt_thread_rdwr_active LDAP_P((ldap_pvt_thread_rdwr_t *rdwrp));
#endif /* LDAP_DEBUG */

#define LDAP_PVT_THREAD_EINVAL EINVAL
#define LDAP_PVT_THREAD_EBUSY EINVAL

#ifndef LDAP_PVT_THREAD_H_DONE
typedef ldap_int_thread_pool_t ldap_pvt_thread_pool_t;

typedef void * (ldap_pvt_thread_start_t) LDAP_P((void *ctx, void *arg));
typedef int (ldap_pvt_thread_walk_t) LDAP_P((ldap_pvt_thread_start_t *start, void *start_arg, void *arg));
typedef void (ldap_pvt_thread_pool_keyfree_t) LDAP_P((void *key, void *data));
#endif /* !LDAP_PVT_THREAD_H_DONE */

LDAP_F( int )
ldap_pvt_thread_pool_init LDAP_P((
	ldap_pvt_thread_pool_t *pool_out,
	int max_threads,
	int max_pending ));

LDAP_F( int )
ldap_pvt_thread_pool_init_q LDAP_P((
	ldap_pvt_thread_pool_t *pool_out,
	int max_threads,
	int max_pending,
	int num_qs ));

LDAP_F( int )
ldap_pvt_thread_pool_submit LDAP_P((
	ldap_pvt_thread_pool_t *pool,
	ldap_pvt_thread_start_t *start,
	void *arg ));

LDAP_F( int )
ldap_pvt_thread_pool_submit2 LDAP_P((
	ldap_pvt_thread_pool_t *pool,
	ldap_pvt_thread_start_t *start,
	void *arg,
	void **cookie ));

LDAP_F( int )
ldap_pvt_thread_pool_retract LDAP_P((
	void *cookie ));

LDAP_F( int )
ldap_pvt_thread_pool_walk LDAP_P((
	ldap_pvt_thread_pool_t *pool,
	ldap_pvt_thread_start_t *start,
	ldap_pvt_thread_walk_t *cb,
	void *arg ));

LDAP_F( int )
ldap_pvt_thread_pool_maxthreads LDAP_P((
	ldap_pvt_thread_pool_t *pool,
	int max_threads ));

LDAP_F( int )
ldap_pvt_thread_pool_queues LDAP_P((
	ldap_pvt_thread_pool_t *pool,
	int numqs ));

#ifndef LDAP_PVT_THREAD_H_DONE
typedef enum {
	LDAP_PVT_THREAD_POOL_PARAM_UNKNOWN = -1,
	LDAP_PVT_THREAD_POOL_PARAM_MAX,
	LDAP_PVT_THREAD_POOL_PARAM_MAX_PENDING,
	LDAP_PVT_THREAD_POOL_PARAM_OPEN,
	LDAP_PVT_THREAD_POOL_PARAM_STARTING,
	LDAP_PVT_THREAD_POOL_PARAM_ACTIVE,
	LDAP_PVT_THREAD_POOL_PARAM_PAUSING,
	LDAP_PVT_THREAD_POOL_PARAM_PENDING,
	LDAP_PVT_THREAD_POOL_PARAM_BACKLOAD,
	LDAP_PVT_THREAD_POOL_PARAM_ACTIVE_MAX,
	LDAP_PVT_THREAD_POOL_PARAM_PENDING_MAX,
	LDAP_PVT_THREAD_POOL_PARAM_BACKLOAD_MAX,
	LDAP_PVT_THREAD_POOL_PARAM_STATE,
	LDAP_PVT_THREAD_POOL_PARAM_PAUSED
} ldap_pvt_thread_pool_param_t;
#endif /* !LDAP_PVT_THREAD_H_DONE */

LDAP_F( int )
ldap_pvt_thread_pool_query LDAP_P((
	ldap_pvt_thread_pool_t *pool,
	ldap_pvt_thread_pool_param_t param, void *value ));

LDAP_F( int )
ldap_pvt_thread_pool_pausing LDAP_P((
	ldap_pvt_thread_pool_t *pool ));

LDAP_F( int )
ldap_pvt_thread_pool_backload LDAP_P((
	ldap_pvt_thread_pool_t *pool ));

LDAP_F( void )
ldap_pvt_thread_pool_idle LDAP_P((
	ldap_pvt_thread_pool_t *pool ));

LDAP_F( void )
ldap_pvt_thread_pool_unidle LDAP_P((
	ldap_pvt_thread_pool_t *pool ));

LDAP_F( int )
ldap_pvt_thread_pool_pausequery LDAP_P((
	ldap_pvt_thread_pool_t *pool ));

LDAP_F( int )
ldap_pvt_thread_pool_pausewait LDAP_P((
	ldap_pvt_thread_pool_t *pool ));

LDAP_F( int )
ldap_pvt_thread_pool_pausecheck_native LDAP_P((
	ldap_pvt_thread_pool_t *pool ));

LDAP_F( int )
ldap_pvt_thread_pool_pause LDAP_P((
	ldap_pvt_thread_pool_t *pool ));

LDAP_F( int )
ldap_pvt_thread_pool_resume LDAP_P((
	ldap_pvt_thread_pool_t *pool ));

LDAP_F( int )
ldap_pvt_thread_pool_destroy LDAP_P((
	ldap_pvt_thread_pool_t *pool,
	int run_pending ));

LDAP_F( int )
ldap_pvt_thread_pool_close LDAP_P((
	ldap_pvt_thread_pool_t *pool,
	int run_pending ));

LDAP_F( int )
ldap_pvt_thread_pool_free LDAP_P((
	ldap_pvt_thread_pool_t *pool ));

LDAP_F( int )
ldap_pvt_thread_pool_getkey LDAP_P((
	void *ctx,
	void *key,
	void **data,
	ldap_pvt_thread_pool_keyfree_t **kfree ));

LDAP_F( int )
ldap_pvt_thread_pool_setkey LDAP_P((
	void *ctx,
	void *key,
	void *data,
	ldap_pvt_thread_pool_keyfree_t *kfree,
	void **olddatap,
	ldap_pvt_thread_pool_keyfree_t **oldkfreep ));

LDAP_F( void )
ldap_pvt_thread_pool_purgekey LDAP_P(( void *key ));

LDAP_F( void *)
ldap_pvt_thread_pool_context LDAP_P(( void ));

LDAP_F( void )
ldap_pvt_thread_pool_context_reset LDAP_P(( void *key ));

LDAP_F( ldap_pvt_thread_t )
ldap_pvt_thread_pool_tid LDAP_P(( void *ctx ));

LDAP_END_DECL

#define LDAP_PVT_THREAD_H_DONE
#endif /* _LDAP_PVT_THREAD_H */
