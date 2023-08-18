/* ldap_thr_debug.h - preprocessor magic for LDAP_THREAD_DEBUG */
/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 2005-2022 The OpenLDAP Foundation.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */

#ifdef LDAP_THREAD_DEBUG

/*
 * libldap .c files should include this file after ldap_pvt_thread.h,
 * with the appropriate LDAP_THREAD*_IMPLEMENTATION macro(s) defined.
 */

#ifndef _LDAP_PVT_THREAD_H
#error "ldap_pvt_thread.h" must be included before "ldap_thr_debug.h"
#endif

/*
 * Support for thr_debug.c:
 *
 * thr_debug.c defines ldap_pvt_thread_* as wrappers around the real
 * ldap_pvt_thread_* implementation, which this file renames to
 * ldap_int_thread_*.
 *
 * Implementation:
 *
 * This file re#defines selected ldap_pvt_thread_* names to
 * ldap_int_thread_*, which will be used from wrappers in thr_debug.c.
 * Two ldap_int_*() calls are redirected to call ldap_debug_*(): These
 * are wrappers around the originals, whose definitions are not renamed.
 * This file then #includes ldap_pvt_thread.h to declare the renamed
 * functions/types.  If #included from thr_debug.c it finally #undefines
 * the macros again.
 *
 * include/ldap_pvt_thread.h declares the typedefs ldap_pvt_thread*_t as
 * either wrapper types ldap_debug_thread*_t or their usual definitions
 * ldap_int_thread*_t, depending on the LDAP_THREAD_DEBUG_WRAP option.
 * When defining the underlying implementation, this file then redirects
 * the type names back to the original ldap_int_thread*_t types.
 * include/ldap_<int,pvt>_thread.h also do some thr_debug magic.
 *
 * So,
 * libldap/<not thr_debug.c> thus define ldap_int_thread_*() instead
 * of ldap_pvt_thread_*().
 * thr_debug.c defines the ldap_pvt_*() and ldap_debug_*() functions.
 * In thread.c, ldap_pvt_thread_<initialize/destroy>() will call
 * ldap_debug_thread_*() instead of ldap_int_thread_*().
 * In tpool.c, ldap_int_thread_pool_shutdown() has explicit thr_debug.c
 * support which treats ldap_pvt_thread_pool_destroy() the same way.
 */

#ifndef LDAP_THREAD_IMPLEMENTATION		/* for first part of threads.c */
#define	ldap_int_thread_initialize		ldap_debug_thread_initialize
#define	ldap_int_thread_destroy			ldap_debug_thread_destroy
#else /* LDAP_THREAD_IMPLEMENTATION 	-- for thr_*.c and end of threads.c */
#undef	ldap_int_thread_initialize
#undef	ldap_int_thread_destroy
#ifdef LDAP_THREAD_DEBUG_WRAP			/* see ldap_pvt_thread.h */
#define	ldap_pvt_thread_mutex_t			ldap_int_thread_mutex_t
#define	ldap_pvt_thread_cond_t			ldap_int_thread_cond_t
#endif
#define	ldap_pvt_thread_sleep			ldap_int_thread_sleep
#define	ldap_pvt_thread_get_concurrency	ldap_int_thread_get_concurrency
#define	ldap_pvt_thread_set_concurrency	ldap_int_thread_set_concurrency
#define	ldap_pvt_thread_create			ldap_int_thread_create
#define	ldap_pvt_thread_exit			ldap_int_thread_exit
#define	ldap_pvt_thread_join			ldap_int_thread_join
#define	ldap_pvt_thread_kill			ldap_int_thread_kill
#define	ldap_pvt_thread_yield			ldap_int_thread_yield
#define	ldap_pvt_thread_cond_init		ldap_int_thread_cond_init
#define	ldap_pvt_thread_cond_destroy	ldap_int_thread_cond_destroy
#define	ldap_pvt_thread_cond_signal		ldap_int_thread_cond_signal
#define	ldap_pvt_thread_cond_broadcast	ldap_int_thread_cond_broadcast
#define	ldap_pvt_thread_cond_wait		ldap_int_thread_cond_wait
#define	ldap_pvt_thread_mutex_init		ldap_int_thread_mutex_init
#define	ldap_pvt_thread_mutex_recursive_init		ldap_int_thread_mutex_recursive_init
#define	ldap_pvt_thread_mutex_destroy	ldap_int_thread_mutex_destroy
#define	ldap_pvt_thread_mutex_lock		ldap_int_thread_mutex_lock
#define	ldap_pvt_thread_mutex_trylock	ldap_int_thread_mutex_trylock
#define	ldap_pvt_thread_mutex_unlock	ldap_int_thread_mutex_unlock
#define	ldap_pvt_thread_self			ldap_int_thread_self
#endif /* LDAP_THREAD_IMPLEMENTATION */

#ifdef LDAP_THREAD_RDWR_IMPLEMENTATION	/* rdwr.c, thr_debug.c */
#ifdef LDAP_THREAD_DEBUG_WRAP			/* see ldap_pvt_thread.h */
#define	ldap_pvt_thread_rdwr_t			ldap_int_thread_rdwr_t
#endif
#define	ldap_pvt_thread_rdwr_init		ldap_int_thread_rdwr_init
#define	ldap_pvt_thread_rdwr_destroy	ldap_int_thread_rdwr_destroy
#define	ldap_pvt_thread_rdwr_rlock		ldap_int_thread_rdwr_rlock
#define	ldap_pvt_thread_rdwr_rtrylock	ldap_int_thread_rdwr_rtrylock
#define	ldap_pvt_thread_rdwr_runlock	ldap_int_thread_rdwr_runlock
#define	ldap_pvt_thread_rdwr_wlock		ldap_int_thread_rdwr_wlock
#define	ldap_pvt_thread_rdwr_wtrylock	ldap_int_thread_rdwr_wtrylock
#define	ldap_pvt_thread_rdwr_wunlock	ldap_int_thread_rdwr_wunlock
#define	ldap_pvt_thread_rdwr_readers	ldap_int_thread_rdwr_readers
#define	ldap_pvt_thread_rdwr_writers	ldap_int_thread_rdwr_writers
#define	ldap_pvt_thread_rdwr_active		ldap_int_thread_rdwr_active
#endif /* LDAP_THREAD_RDWR_IMPLEMENTATION */

#ifdef LDAP_THREAD_POOL_IMPLEMENTATION	/* tpool.c, thr_debug.c */
#ifdef LDAP_THREAD_DEBUG_WRAP			/* see ldap_pvt_thread.h */
#define	ldap_pvt_thread_pool_t			ldap_int_thread_pool_t
#endif
#define	ldap_pvt_thread_pool_init		ldap_int_thread_pool_init
#define	ldap_pvt_thread_pool_submit		ldap_int_thread_pool_submit
#define	ldap_pvt_thread_pool_maxthreads	ldap_int_thread_pool_maxthreads
#define	ldap_pvt_thread_pool_backload	ldap_int_thread_pool_backload
#define	ldap_pvt_thread_pool_pause		ldap_int_thread_pool_pause
#define	ldap_pvt_thread_pool_resume		ldap_int_thread_pool_resume
#define	ldap_pvt_thread_pool_destroy	ldap_int_thread_pool_destroy
#define	ldap_pvt_thread_pool_close	ldap_int_thread_pool_close
#define	ldap_pvt_thread_pool_free	ldap_int_thread_pool_free
#define	ldap_pvt_thread_pool_getkey		ldap_int_thread_pool_getkey
#define	ldap_pvt_thread_pool_setkey	ldap_int_thread_pool_setkey
#define	ldap_pvt_thread_pool_purgekey	ldap_int_thread_pool_purgekey
#define	ldap_pvt_thread_pool_context	ldap_int_thread_pool_context
#define	ldap_pvt_thread_pool_context_reset ldap_int_thread_pool_context_reset
#endif /* LDAP_THREAD_POOL_IMPLEMENTATION */

#undef _LDAP_PVT_THREAD_H
#include "ldap_pvt_thread.h"

#ifdef LDAP_THREAD_POOL_IMPLEMENTATION	/* tpool.c */
/*
 * tpool.c:ldap_int_thread_pool_shutdown() needs this.  Could not
 * use it for ldap_pvt_thread.h above because of its use of LDAP_P().
 */
#undef	ldap_pvt_thread_pool_destroy
#define	ldap_pvt_thread_pool_destroy(p,r) ldap_int_thread_pool_destroy(p,r)
#endif

#ifdef LDAP_THREAD_DEBUG_IMPLEMENTATION	/* thr_debug.c */
#undef	ldap_pvt_thread_mutex_t
#undef	ldap_pvt_thread_cond_t
#undef	ldap_pvt_thread_sleep
#undef	ldap_pvt_thread_get_concurrency
#undef	ldap_pvt_thread_set_concurrency
#undef	ldap_pvt_thread_create
#undef	ldap_pvt_thread_exit
#undef	ldap_pvt_thread_join
#undef	ldap_pvt_thread_kill
#undef	ldap_pvt_thread_yield
#undef	ldap_pvt_thread_cond_init
#undef	ldap_pvt_thread_cond_destroy
#undef	ldap_pvt_thread_cond_signal
#undef	ldap_pvt_thread_cond_broadcast
#undef	ldap_pvt_thread_cond_wait
#undef	ldap_pvt_thread_mutex_init
#undef	ldap_pvt_thread_mutex_recursive_init
#undef	ldap_pvt_thread_mutex_destroy
#undef	ldap_pvt_thread_mutex_lock
#undef	ldap_pvt_thread_mutex_trylock
#undef	ldap_pvt_thread_mutex_unlock
#undef	ldap_pvt_thread_self
/* LDAP_THREAD_RDWR_IMPLEMENTATION: */
#undef	ldap_pvt_thread_rdwr_t
#undef	ldap_pvt_thread_rdwr_init
#undef	ldap_pvt_thread_rdwr_destroy
#undef	ldap_pvt_thread_rdwr_rlock
#undef	ldap_pvt_thread_rdwr_rtrylock
#undef	ldap_pvt_thread_rdwr_runlock
#undef	ldap_pvt_thread_rdwr_wlock
#undef	ldap_pvt_thread_rdwr_wtrylock
#undef	ldap_pvt_thread_rdwr_wunlock
#undef	ldap_pvt_thread_rdwr_readers
#undef	ldap_pvt_thread_rdwr_writers
#undef	ldap_pvt_thread_rdwr_active
/* LDAP_THREAD_POOL_IMPLEMENTATION: */
#undef	ldap_pvt_thread_pool_t
#undef	ldap_pvt_thread_pool_init
#undef	ldap_pvt_thread_pool_submit
#undef	ldap_pvt_thread_pool_maxthreads
#undef	ldap_pvt_thread_pool_backload
#undef	ldap_pvt_thread_pool_pause
#undef	ldap_pvt_thread_pool_resume
#undef	ldap_pvt_thread_pool_destroy
#undef	ldap_pvt_thread_pool_close
#undef	ldap_pvt_thread_pool_free
#undef	ldap_pvt_thread_pool_getkey
#undef	ldap_pvt_thread_pool_setkey
#undef	ldap_pvt_thread_pool_purgekey
#undef	ldap_pvt_thread_pool_context
#undef	ldap_pvt_thread_pool_context_reset
#endif /* LDAP_THREAD_DEBUG_IMPLEMENTATION */

#endif /* LDAP_THREAD_DEBUG */
