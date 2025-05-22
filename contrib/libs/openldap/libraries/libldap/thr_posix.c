/* thr_posix.c - wrapper around posix and posixish thread implementations.  */
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

#if defined( HAVE_PTHREADS )

#ifdef __GLIBC__
#undef _FEATURES_H
#define _XOPEN_SOURCE 500		/* For pthread_setconcurrency() on glibc */
#endif

#include <ac/errno.h>

#ifdef REPLACE_BROKEN_YIELD
#ifndef HAVE_NANOSLEEP
#include <ac/socket.h>
#endif
#include <ac/time.h>
#endif

#include "ldap_pvt_thread.h" /* Get the thread interface */
#define LDAP_THREAD_IMPLEMENTATION
#define LDAP_THREAD_RDWR_IMPLEMENTATION
#include "ldap_thr_debug.h"	 /* May rename the symbols defined below */
#include <signal.h>			 /* For pthread_kill() */

extern int ldap_int_stackguard;

#if HAVE_PTHREADS < 6
#  define LDAP_INT_THREAD_ATTR_DEFAULT		pthread_attr_default
#  define LDAP_INT_THREAD_CONDATTR_DEFAULT	pthread_condattr_default
#  define LDAP_INT_THREAD_MUTEXATTR_DEFAULT	pthread_mutexattr_default
#else
#  define LDAP_INT_THREAD_ATTR_DEFAULT		NULL
#  define LDAP_INT_THREAD_CONDATTR_DEFAULT	NULL
#  define LDAP_INT_THREAD_MUTEXATTR_DEFAULT NULL
#endif

#ifdef LDAP_THREAD_DEBUG
#  if defined LDAP_INT_THREAD_MUTEXATTR	/* May be defined in CPPFLAGS */
#  elif defined HAVE_PTHREAD_KILL_OTHER_THREADS_NP
	 /* LinuxThreads hack */
#    define LDAP_INT_THREAD_MUTEXATTR	PTHREAD_MUTEX_ERRORCHECK_NP
#  else
#    define LDAP_INT_THREAD_MUTEXATTR	PTHREAD_MUTEX_ERRORCHECK
#  endif
static pthread_mutexattr_t mutex_attr;
#  undef  LDAP_INT_THREAD_MUTEXATTR_DEFAULT
#  define LDAP_INT_THREAD_MUTEXATTR_DEFAULT &mutex_attr
#endif

static pthread_mutexattr_t mutex_attr_recursive;

#if HAVE_PTHREADS < 7
#define ERRVAL(val)	((val) < 0 ? errno : 0)
#else
#define ERRVAL(val)	(val)
#endif

int
ldap_int_thread_initialize( void )
{
#ifdef LDAP_INT_THREAD_MUTEXATTR
	pthread_mutexattr_init( &mutex_attr );
	pthread_mutexattr_settype( &mutex_attr, LDAP_INT_THREAD_MUTEXATTR );
#endif
	if (pthread_mutexattr_init(&mutex_attr_recursive))
		return -1;
	if (pthread_mutexattr_settype(&mutex_attr_recursive, PTHREAD_MUTEX_RECURSIVE))
		return -1;
	return 0;
}

int
ldap_int_thread_destroy( void )
{
#ifdef HAVE_PTHREAD_KILL_OTHER_THREADS_NP
	/* LinuxThreads: kill clones */
	pthread_kill_other_threads_np();
#endif
#ifdef LDAP_INT_THREAD_MUTEXATTR
	pthread_mutexattr_destroy( &mutex_attr );
#endif
	pthread_mutexattr_destroy( &mutex_attr_recursive );
	return 0;
}

#ifdef LDAP_THREAD_HAVE_SETCONCURRENCY
int
ldap_pvt_thread_set_concurrency(int n)
{
#ifdef HAVE_PTHREAD_SETCONCURRENCY
	return pthread_setconcurrency( n );
#elif defined(HAVE_THR_SETCONCURRENCY)
	return thr_setconcurrency( n );
#else
	return 0;
#endif
}
#endif

#ifdef LDAP_THREAD_HAVE_GETCONCURRENCY
int
ldap_pvt_thread_get_concurrency(void)
{
#ifdef HAVE_PTHREAD_GETCONCURRENCY
	return pthread_getconcurrency();
#elif defined(HAVE_THR_GETCONCURRENCY)
	return thr_getconcurrency();
#else
	return 0;
#endif
}
#endif

/* detachstate appeared in Draft 6, but without manifest constants.
 * in Draft 7 they were called PTHREAD_CREATE_UNDETACHED and ...DETACHED.
 * in Draft 8 on, ...UNDETACHED became ...JOINABLE.
 */
#ifndef PTHREAD_CREATE_JOINABLE
#ifdef PTHREAD_CREATE_UNDETACHED
#define	PTHREAD_CREATE_JOINABLE	PTHREAD_CREATE_UNDETACHED
#else
#define	PTHREAD_CREATE_JOINABLE	0
#endif
#endif

#ifndef PTHREAD_CREATE_DETACHED
#define	PTHREAD_CREATE_DETACHED	1
#endif

int 
ldap_pvt_thread_create( ldap_pvt_thread_t * thread,
	int detach,
	void *(*start_routine)( void * ),
	void *arg)
{
	int rtn;
	pthread_attr_t attr;

/* Always create the thread attrs, so we can set stacksize if we need to */
#if HAVE_PTHREADS > 5
	pthread_attr_init(&attr);
#else
	pthread_attr_create(&attr);
#endif

#ifdef LDAP_PVT_THREAD_SET_STACK_SIZE
	/* this should be tunable */
	pthread_attr_setstacksize( &attr, LDAP_PVT_THREAD_STACK_SIZE );
	if ( ldap_int_stackguard )
		pthread_attr_setguardsize( &attr, LDAP_PVT_THREAD_STACK_SIZE );
#endif

#if HAVE_PTHREADS > 5
	detach = detach ? PTHREAD_CREATE_DETACHED : PTHREAD_CREATE_JOINABLE;
#if HAVE_PTHREADS == 6
	pthread_attr_setdetachstate(&attr, &detach);
#else
	pthread_attr_setdetachstate(&attr, detach);
#endif
#endif

#if HAVE_PTHREADS < 5
	rtn = pthread_create( thread, attr, start_routine, arg );
#else
	rtn = pthread_create( thread, &attr, start_routine, arg );
#endif

#if HAVE_PTHREADS > 5
	pthread_attr_destroy(&attr);
#else
	pthread_attr_delete(&attr);
	if( detach ) {
		pthread_detach( thread );
	}
#endif

#if HAVE_PTHREADS < 7
	if ( rtn < 0 ) rtn = errno;
#endif
	return rtn;
}

void 
ldap_pvt_thread_exit( void *retval )
{
	pthread_exit( retval );
}

int 
ldap_pvt_thread_join( ldap_pvt_thread_t thread, void **thread_return )
{
#if HAVE_PTHREADS < 7
	void *dummy;
	if (thread_return==NULL)
	  thread_return=&dummy;
#endif
	return ERRVAL( pthread_join( thread, thread_return ) );
}

int 
ldap_pvt_thread_kill( ldap_pvt_thread_t thread, int signo )
{
#if defined(HAVE_PTHREAD_KILL) && HAVE_PTHREADS > 4
	/* MacOS 10.1 is detected as v10 but has no pthread_kill() */
	return ERRVAL( pthread_kill( thread, signo ) );
#else
	/* pthread package with DCE */
	if (kill( getpid(), signo )<0)
		return errno;
	return 0;
#endif
}

int 
ldap_pvt_thread_yield( void )
{
#ifdef REPLACE_BROKEN_YIELD
#ifdef HAVE_NANOSLEEP
	struct timespec t = { 0, 0 };
	nanosleep(&t, NULL);
#else
	struct timeval tv = {0,0};
	select( 0, NULL, NULL, NULL, &tv );
#endif
	return 0;

#elif defined(HAVE_THR_YIELD)
	thr_yield();
	return 0;

#elif HAVE_PTHREADS == 10
	return sched_yield();

#elif defined(_POSIX_THREAD_IS_GNU_PTH)
	sched_yield();
	return 0;

#elif HAVE_PTHREADS == 6
	pthread_yield(NULL);
	return 0;

#else
	pthread_yield();
	return 0;
#endif
}

int 
ldap_pvt_thread_cond_init( ldap_pvt_thread_cond_t *cond )
{
	return ERRVAL( pthread_cond_init(
		cond, LDAP_INT_THREAD_CONDATTR_DEFAULT ) );
}

int 
ldap_pvt_thread_cond_destroy( ldap_pvt_thread_cond_t *cond )
{
	return ERRVAL( pthread_cond_destroy( cond ) );
}
	
int 
ldap_pvt_thread_cond_signal( ldap_pvt_thread_cond_t *cond )
{
	return ERRVAL( pthread_cond_signal( cond ) );
}

int
ldap_pvt_thread_cond_broadcast( ldap_pvt_thread_cond_t *cond )
{
	return ERRVAL( pthread_cond_broadcast( cond ) );
}

int 
ldap_pvt_thread_cond_wait( ldap_pvt_thread_cond_t *cond, 
		      ldap_pvt_thread_mutex_t *mutex )
{
	return ERRVAL( pthread_cond_wait( cond, mutex ) );
}

int 
ldap_pvt_thread_mutex_init( ldap_pvt_thread_mutex_t *mutex )
{
	return ERRVAL( pthread_mutex_init(
		mutex, LDAP_INT_THREAD_MUTEXATTR_DEFAULT ) );
}

int 
ldap_pvt_thread_mutex_destroy( ldap_pvt_thread_mutex_t *mutex )
{
	return ERRVAL( pthread_mutex_destroy( mutex ) );
}

int 
ldap_pvt_thread_mutex_lock( ldap_pvt_thread_mutex_t *mutex )
{
	return ERRVAL( pthread_mutex_lock( mutex ) );
}

int 
ldap_pvt_thread_mutex_trylock( ldap_pvt_thread_mutex_t *mutex )
{
	return ERRVAL( pthread_mutex_trylock( mutex ) );
}

int 
ldap_pvt_thread_mutex_unlock( ldap_pvt_thread_mutex_t *mutex )
{
	return ERRVAL( pthread_mutex_unlock( mutex ) );
}

int
ldap_pvt_thread_mutex_recursive_init( ldap_pvt_thread_mutex_t *mutex )
{
	return ERRVAL( pthread_mutex_init( mutex, &mutex_attr_recursive ) );
}

ldap_pvt_thread_t ldap_pvt_thread_self( void )
{
	return pthread_self();
}

int
ldap_pvt_thread_key_create( ldap_pvt_thread_key_t *key )
{
	return pthread_key_create( key, NULL );
}

int
ldap_pvt_thread_key_destroy( ldap_pvt_thread_key_t key )
{
	return pthread_key_delete( key );
}

int
ldap_pvt_thread_key_setdata( ldap_pvt_thread_key_t key, void *data )
{
	return pthread_setspecific( key, data );
}

int
ldap_pvt_thread_key_getdata( ldap_pvt_thread_key_t key, void **data )
{
	*data = pthread_getspecific( key );
	return 0;
}

#ifdef LDAP_THREAD_HAVE_RDWR
#ifdef HAVE_PTHREAD_RWLOCK_DESTROY
int 
ldap_pvt_thread_rdwr_init( ldap_pvt_thread_rdwr_t *rw )
{
	return ERRVAL( pthread_rwlock_init( rw, NULL ) );
}

int 
ldap_pvt_thread_rdwr_destroy( ldap_pvt_thread_rdwr_t *rw )
{
	return ERRVAL( pthread_rwlock_destroy( rw ) );
}

int ldap_pvt_thread_rdwr_rlock( ldap_pvt_thread_rdwr_t *rw )
{
	return ERRVAL( pthread_rwlock_rdlock( rw ) );
}

int ldap_pvt_thread_rdwr_rtrylock( ldap_pvt_thread_rdwr_t *rw )
{
	return ERRVAL( pthread_rwlock_tryrdlock( rw ) );
}

int ldap_pvt_thread_rdwr_runlock( ldap_pvt_thread_rdwr_t *rw )
{
	return ERRVAL( pthread_rwlock_unlock( rw ) );
}

int ldap_pvt_thread_rdwr_wlock( ldap_pvt_thread_rdwr_t *rw )
{
	return ERRVAL( pthread_rwlock_wrlock( rw ) );
}

int ldap_pvt_thread_rdwr_wtrylock( ldap_pvt_thread_rdwr_t *rw )
{
	return ERRVAL( pthread_rwlock_trywrlock( rw ) );
}

int ldap_pvt_thread_rdwr_wunlock( ldap_pvt_thread_rdwr_t *rw )
{
	return ERRVAL( pthread_rwlock_unlock( rw ) );
}

#endif /* HAVE_PTHREAD_RWLOCK_DESTROY */
#endif /* LDAP_THREAD_HAVE_RDWR */
#endif /* HAVE_PTHREADS */

