/* thr_thr.c - wrappers around solaris threads */
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

#if defined( HAVE_THR )

#include "ldap_pvt_thread.h" /* Get the thread interface */
#define LDAP_THREAD_IMPLEMENTATION
#include "ldap_thr_debug.h"	 /* May rename the symbols defined below */

/*******************
 *                 *
 * Solaris Threads *
 *                 *
 *******************/

int
ldap_int_thread_initialize( void )
{
	return 0;
}

int
ldap_int_thread_destroy( void )
{
	return 0;
}

#ifdef LDAP_THREAD_HAVE_SETCONCURRENCY
int
ldap_pvt_thread_set_concurrency(int n)
{
	return thr_setconcurrency( n );
}
#endif

#ifdef LDAP_THREAD_HAVE_GETCONCURRENCY
int
ldap_pvt_thread_get_concurrency(void)
{
	return thr_getconcurrency();
}
#endif

int 
ldap_pvt_thread_create( ldap_pvt_thread_t * thread, 
	int detach,
	void *(*start_routine)( void *),
	void *arg)
{
	return( thr_create( NULL, LDAP_PVT_THREAD_STACK_SIZE, start_routine,
		arg, detach ? THR_DETACHED : 0, thread ) );
}

void 
ldap_pvt_thread_exit( void *retval )
{
	thr_exit( NULL );
}

int ldap_pvt_thread_join( ldap_pvt_thread_t thread, void **thread_return )
{
	thr_join( thread, NULL, thread_return );
	return 0;
}

int 
ldap_pvt_thread_kill( ldap_pvt_thread_t thread, int signo )
{
	thr_kill( thread, signo );
	return 0;
}
	
int 
ldap_pvt_thread_yield( void )
{
	thr_yield();
	return 0;
}

int 
ldap_pvt_thread_cond_init( ldap_pvt_thread_cond_t *cond )
{
	return( cond_init( cond, USYNC_THREAD, NULL ) );
}

int 
ldap_pvt_thread_cond_signal( ldap_pvt_thread_cond_t *cond )
{
	return( cond_signal( cond ) );
}

int
ldap_pvt_thread_cond_broadcast( ldap_pvt_thread_cond_t *cv )
{
	return( cond_broadcast( cv ) );
}

int 
ldap_pvt_thread_cond_wait( ldap_pvt_thread_cond_t *cond, 
	ldap_pvt_thread_mutex_t *mutex )
{
	return( cond_wait( cond, mutex ) );
}

int
ldap_pvt_thread_cond_destroy( ldap_pvt_thread_cond_t *cv )
{
	return( cond_destroy( cv ) );
}

int 
ldap_pvt_thread_mutex_init( ldap_pvt_thread_mutex_t *mutex )
{
	return( mutex_init( mutex, USYNC_THREAD, NULL ) );
}

int 
ldap_pvt_thread_mutex_destroy( ldap_pvt_thread_mutex_t *mutex )
{
	return( mutex_destroy( mutex ) );
}

int 
ldap_pvt_thread_mutex_lock( ldap_pvt_thread_mutex_t *mutex )
{
	return( mutex_lock( mutex ) );
}

int 
ldap_pvt_thread_mutex_unlock( ldap_pvt_thread_mutex_t *mutex )
{
	return( mutex_unlock( mutex ) );
}

int
ldap_pvt_thread_mutex_trylock( ldap_pvt_thread_mutex_t *mp )
{
	return( mutex_trylock( mp ) );
}

int
ldap_pvt_thread_mutex_recursive_init( ldap_pvt_thread_mutex_t *mutex )
{
	return( mutex_init( mutex, USYNC_THREAD | LOCK_RECURSIVE, NULL ) );
}

ldap_pvt_thread_t
ldap_pvt_thread_self( void )
{
	return thr_self();
}

int
ldap_pvt_thread_key_create( ldap_pvt_thread_key_t *key )
{
	return thr_keycreate( key, NULL );
}

int
ldap_pvt_thread_key_destroy( ldap_pvt_thread_key_t key )
{
	return( 0 );
}

int
ldap_pvt_thread_key_setdata( ldap_pvt_thread_key_t key, void *data )
{
	return thr_setspecific( key, data );
}

int
ldap_pvt_thread_key_getdata( ldap_pvt_thread_key_t key, void **data )
{
	return thr_getspecific( key, data );
}

#endif /* HAVE_THR */
