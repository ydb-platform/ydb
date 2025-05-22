/* ldap_int_thread.h - ldap internal thread wrappers header file */
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


LDAP_BEGIN_DECL

/* Can be done twice.  See libldap/ldap_thr_debug.h. */
LDAP_F(int) ldap_int_thread_initialize LDAP_P(( void ));
LDAP_F(int) ldap_int_thread_destroy    LDAP_P(( void ));

LDAP_END_DECL

#ifndef _LDAP_INT_THREAD_H
#define _LDAP_INT_THREAD_H

#if defined( HAVE_PTHREADS )
/**********************************
 *                                *
 * definitions for POSIX Threads  *
 *                                *
 **********************************/

#include <pthread.h>
#ifdef HAVE_SCHED_H
#include <sched.h>
#endif

LDAP_BEGIN_DECL

typedef pthread_t		ldap_int_thread_t;
typedef pthread_mutex_t		ldap_int_thread_mutex_t;
typedef pthread_cond_t		ldap_int_thread_cond_t;
typedef pthread_key_t		ldap_int_thread_key_t;

#define ldap_int_thread_equal(a, b)	pthread_equal((a), (b))

#if defined( _POSIX_REENTRANT_FUNCTIONS ) || \
	defined( _POSIX_THREAD_SAFE_FUNCTIONS ) || \
	defined( _POSIX_THREADSAFE_FUNCTIONS )
#define HAVE_REENTRANT_FUNCTIONS 1
#endif

#if defined( HAVE_PTHREAD_GETCONCURRENCY ) || \
	defined( HAVE_THR_GETCONCURRENCY )
#define LDAP_THREAD_HAVE_GETCONCURRENCY 1
#endif

#if defined( HAVE_PTHREAD_SETCONCURRENCY ) || \
	defined( HAVE_THR_SETCONCURRENCY )
#define LDAP_THREAD_HAVE_SETCONCURRENCY 1
#endif

#if defined( HAVE_PTHREAD_RWLOCK_DESTROY )
#define LDAP_THREAD_HAVE_RDWR 1
typedef pthread_rwlock_t ldap_int_thread_rdwr_t;
#endif

#ifndef LDAP_INT_MUTEX_NULL
#define LDAP_INT_MUTEX_NULL	PTHREAD_MUTEX_INITIALIZER
#define LDAP_INT_MUTEX_FIRSTCREATE(m)	((void) 0)
#endif

LDAP_END_DECL

#elif defined( HAVE_GNU_PTH )
/***********************************
 *                                 *
 * thread definitions for GNU Pth  *
 *                                 *
 ***********************************/

#define PTH_SYSCALL_SOFT 1
#include <pth.h>

LDAP_BEGIN_DECL

typedef pth_t		ldap_int_thread_t;
typedef pth_mutex_t	ldap_int_thread_mutex_t;
typedef pth_cond_t	ldap_int_thread_cond_t;
typedef pth_key_t	ldap_int_thread_key_t;

#if 0
#define LDAP_THREAD_HAVE_RDWR 1
typedef pth_rwlock_t ldap_int_thread_rdwr_t;
#endif

#ifndef LDAP_INT_MUTEX_NULL
#define LDAP_INT_MUTEX_NULL	PTH_MUTEX_INIT
#define LDAP_INT_MUTEX_FIRSTCREATE(m)	((void) 0)
#endif

LDAP_END_DECL

#elif defined( HAVE_THR )
/********************************************
 *                                          *
 * thread definitions for Solaris LWP (THR) *
 *                                          *
 ********************************************/

#include <thread.h>
#error #include <synch.h>

LDAP_BEGIN_DECL

typedef thread_t		ldap_int_thread_t;
typedef mutex_t			ldap_int_thread_mutex_t;
typedef cond_t			ldap_int_thread_cond_t;
typedef thread_key_t	ldap_int_thread_key_t;

#define HAVE_REENTRANT_FUNCTIONS 1

#ifdef HAVE_THR_GETCONCURRENCY
#define LDAP_THREAD_HAVE_GETCONCURRENCY 1
#endif
#ifdef HAVE_THR_SETCONCURRENCY
#define LDAP_THREAD_HAVE_SETCONCURRENCY 1
#endif

#ifndef LDAP_INT_MUTEX_NULL
#define LDAP_INT_MUTEX_NULL	DEFAULTMUTEX
#define LDAP_INT_MUTEX_FIRSTCREATE(m)	((void) 0)
#endif

#elif defined(HAVE_NT_THREADS)
/*************************************
 *                                   *
 * thread definitions for NT threads *
 *                                   *
 *************************************/

#include <process.h>
#include <windows.h>

LDAP_BEGIN_DECL

typedef unsigned long	ldap_int_thread_t;
typedef HANDLE	ldap_int_thread_mutex_t;
typedef HANDLE	ldap_int_thread_cond_t;
typedef DWORD	ldap_int_thread_key_t;

LDAP_F( int )
ldap_int_mutex_firstcreate LDAP_P(( ldap_int_thread_mutex_t *mutex ));

#ifndef LDAP_INT_MUTEX_NULL
#define LDAP_INT_MUTEX_NULL		((HANDLE)0)
#define LDAP_INT_MUTEX_FIRSTCREATE(m) \
		ldap_int_mutex_firstcreate(&(m))
#endif

LDAP_END_DECL

#else
/***********************************
 *                                 *
 * thread definitions for no       *
 * underlying library support      *
 *                                 *
 ***********************************/

#ifndef NO_THREADS
#define NO_THREADS 1
#endif

LDAP_BEGIN_DECL

typedef int			ldap_int_thread_t;
typedef int			ldap_int_thread_mutex_t;
typedef int			ldap_int_thread_cond_t;
typedef int			ldap_int_thread_key_t;

#define LDAP_THREAD_HAVE_TPOOL 1
typedef int			ldap_int_thread_pool_t;

#ifndef LDAP_INT_MUTEX_NULL
#define LDAP_INT_MUTEX_NULL				0
#define LDAP_INT_MUTEX_FIRSTCREATE(m)	((void) 0)
#endif

LDAP_END_DECL

#endif /* no threads support */


LDAP_BEGIN_DECL

#ifndef ldap_int_thread_equal
#define ldap_int_thread_equal(a, b)	((a) == (b))
#endif

#ifndef LDAP_THREAD_HAVE_RDWR
typedef struct ldap_int_thread_rdwr_s * ldap_int_thread_rdwr_t;
#endif

LDAP_F(int) ldap_int_thread_pool_startup ( void );
LDAP_F(int) ldap_int_thread_pool_shutdown ( void );

#ifndef LDAP_THREAD_HAVE_TPOOL
typedef struct ldap_int_thread_pool_s * ldap_int_thread_pool_t;
#endif
LDAP_END_DECL


#if defined(LDAP_THREAD_DEBUG) && !((LDAP_THREAD_DEBUG +0) & 2U)
#define LDAP_THREAD_DEBUG_WRAP 1
#endif

#ifdef LDAP_THREAD_DEBUG_WRAP
/**************************************
 *                                    *
 * definitions for type-wrapped debug *
 *                                    *
 **************************************/

LDAP_BEGIN_DECL

#ifndef LDAP_UINTPTR_T	/* May be configured in CPPFLAGS */
#define LDAP_UINTPTR_T	unsigned long
#endif

typedef enum {
	ldap_debug_magic =	-(int) (((unsigned)-1)/19)
} ldap_debug_magic_t;

typedef enum {
	/* Could fill in "locked" etc here later */
	ldap_debug_state_inited = (int) (((unsigned)-1)/11),
	ldap_debug_state_destroyed
} ldap_debug_state_t;

typedef struct {
	/* Enclosed in magic numbers in the hope of catching overwrites */
	ldap_debug_magic_t	magic;	/* bit pattern to recognize usages  */
	LDAP_UINTPTR_T		self;	/* ~(LDAP_UINTPTR_T)&(this struct) */
	union ldap_debug_mem_u {	/* Dummy memory reference */
		unsigned char	*ptr;
		LDAP_UINTPTR_T	num;
	} mem;
	ldap_debug_state_t	state;	/* doubles as another magic number */
} ldap_debug_usage_info_t;

typedef struct {
	ldap_int_thread_mutex_t	wrapped;
	ldap_debug_usage_info_t	usage;
	ldap_int_thread_t	owner;
} ldap_debug_thread_mutex_t;

#define	LDAP_DEBUG_MUTEX_NULL	{LDAP_INT_MUTEX_NULL, {0,0,{0},0} /*,owner*/}
#define	LDAP_DEBUG_MUTEX_FIRSTCREATE(m) \
	((void) ((m).usage.state || ldap_pvt_thread_mutex_init(&(m))))

typedef struct {
	ldap_int_thread_cond_t	wrapped;
	ldap_debug_usage_info_t	usage;
} ldap_debug_thread_cond_t;

typedef struct {
	ldap_int_thread_rdwr_t	wrapped;
	ldap_debug_usage_info_t	usage;
} ldap_debug_thread_rdwr_t;

#ifndef NDEBUG
#define	LDAP_INT_THREAD_ASSERT_MUTEX_OWNER(mutex) \
	ldap_debug_thread_assert_mutex_owner( \
		__FILE__, __LINE__, "owns(" #mutex ")", mutex )
LDAP_F(void) ldap_debug_thread_assert_mutex_owner LDAP_P((
	LDAP_CONST char *file,
	int line,
	LDAP_CONST char *msg,
	ldap_debug_thread_mutex_t *mutex ));
#endif /* NDEBUG */

LDAP_END_DECL

#endif /* LDAP_THREAD_DEBUG_WRAP */

#endif /* _LDAP_INT_THREAD_H */
