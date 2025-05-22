/* thr_pth.c - wrappers around GNU Pth */
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

#if defined( HAVE_GNU_PTH )

#include "ldap_pvt_thread.h" /* Get the thread interface */
#define LDAP_THREAD_IMPLEMENTATION
#define LDAP_THREAD_RDWR_IMPLEMENTATION
#include "ldap_thr_debug.h"	 /* May rename the symbols defined below */

#include <errno.h>

/*******************
 *                 *
 * GNU Pth Threads *
 *                 *
 *******************/

static pth_attr_t detach_attr;
static pth_attr_t joined_attr;

int
ldap_int_thread_initialize( void )
{
	if( !pth_init() ) {
		return -1;
	}
	detach_attr = pth_attr_new();
	joined_attr = pth_attr_new();
#ifdef LDAP_PVT_THREAD_SET_STACK_SIZE
	pth_attr_set( joined_attr, PTH_ATTR_STACK_SIZE, LDAP_PVT_THREAD_STACK_SIZE );
	pth_attr_set( detach_attr, PTH_ATTR_STACK_SIZE, LDAP_PVT_THREAD_STACK_SIZE );
#endif
	return pth_attr_set( detach_attr, PTH_ATTR_JOINABLE, FALSE );
}

int
ldap_int_thread_destroy( void )
{
	pth_attr_destroy(detach_attr);
	pth_kill();
	return 0;
}

int 
ldap_pvt_thread_create( ldap_pvt_thread_t * thread, 
	int detach,
	void *(*start_routine)( void *),
	void *arg)
{
	*thread = pth_spawn( detach ? detach_attr : joined_attr,
		start_routine, arg );

	return *thread == NULL ? errno : 0;
}

void 
ldap_pvt_thread_exit( void *retval )
{
	pth_exit( retval );
}

int ldap_pvt_thread_join( ldap_pvt_thread_t thread, void **thread_return )
{
	return pth_join( thread, thread_return ) ? 0 : errno;
}

int 
ldap_pvt_thread_kill( ldap_pvt_thread_t thread, int signo )
{
	return pth_raise( thread, signo ) ? 0 : errno;
}
	
int 
ldap_pvt_thread_yield( void )
{
	return pth_yield(NULL) ? 0 : errno;
}

int 
ldap_pvt_thread_cond_init( ldap_pvt_thread_cond_t *cond )
{
	return( pth_cond_init( cond ) ? 0 : errno );
}

int 
ldap_pvt_thread_cond_signal( ldap_pvt_thread_cond_t *cond )
{
	return( pth_cond_notify( cond, 0 ) ? 0 : errno );
}

int
ldap_pvt_thread_cond_broadcast( ldap_pvt_thread_cond_t *cond )
{
	return( pth_cond_notify( cond, 1 ) ? 0 : errno );
}

int 
ldap_pvt_thread_cond_wait( ldap_pvt_thread_cond_t *cond, 
	ldap_pvt_thread_mutex_t *mutex )
{
	return( pth_cond_await( cond, mutex, NULL ) ? 0 : errno );
}

int
ldap_pvt_thread_cond_destroy( ldap_pvt_thread_cond_t *cv )
{
	return 0;
}

int 
ldap_pvt_thread_mutex_init( ldap_pvt_thread_mutex_t *mutex )
{
	return( pth_mutex_init( mutex ) ? 0 : errno );
}

int
ldap_pvt_thread_mutex_recursive_init( ldap_pvt_thread_mutex_t *mutex )
{
	/* All pth mutexes are recursive */
	return ldap_pvt_thread_mutex_init( mutex );
}

int 
ldap_pvt_thread_mutex_destroy( ldap_pvt_thread_mutex_t *mutex )
{
	return 0;
}

int 
ldap_pvt_thread_mutex_lock( ldap_pvt_thread_mutex_t *mutex )
{
	return( pth_mutex_acquire( mutex, 0, NULL ) ? 0 : errno );
}

int 
ldap_pvt_thread_mutex_unlock( ldap_pvt_thread_mutex_t *mutex )
{
	return( pth_mutex_release( mutex ) ? 0 : errno );
}

int
ldap_pvt_thread_mutex_trylock( ldap_pvt_thread_mutex_t *mutex )
{
	return( pth_mutex_acquire( mutex, 1, NULL ) ? 0 : errno );
}

ldap_pvt_thread_t
ldap_pvt_thread_self( void )
{
	return pth_self();
}

int
ldap_pvt_thread_key_create( ldap_pvt_thread_key_t *key )
{
	return pth_key_create( key, NULL );
}

int
ldap_pvt_thread_key_destroy( ldap_pvt_thread_key_t key )
{
	return pth_key_delete( key );
}

int
ldap_pvt_thread_key_setdata( ldap_pvt_thread_key_t key, void *data )
{
	return pth_key_setdata( key, data );
}

int
ldap_pvt_thread_key_getdata( ldap_pvt_thread_key_t key, void **data )
{
	*data = pth_key_getdata( key );
	return 0;
}

#ifdef LDAP_THREAD_HAVE_RDWR
int 
ldap_pvt_thread_rdwr_init( ldap_pvt_thread_rdwr_t *rw )
{
	return pth_rwlock_init( rw ) ? 0 : errno;
}

int 
ldap_pvt_thread_rdwr_destroy( ldap_pvt_thread_rdwr_t *rw )
{
	return 0;
}

int ldap_pvt_thread_rdwr_rlock( ldap_pvt_thread_rdwr_t *rw )
{
	return pth_rwlock_acquire( rw, PTH_RWLOCK_RD, 0, NULL ) ? 0 : errno;
}

int ldap_pvt_thread_rdwr_rtrylock( ldap_pvt_thread_rdwr_t *rw )
{
	return pth_rwlock_acquire( rw, PTH_RWLOCK_RD, 1, NULL ) ? 0 : errno;
}

int ldap_pvt_thread_rdwr_runlock( ldap_pvt_thread_rdwr_t *rw )
{
	return pth_rwlock_release( rw ) ? 0 : errno;
}

int ldap_pvt_thread_rdwr_wlock( ldap_pvt_thread_rdwr_t *rw )
{
	return pth_rwlock_acquire( rw, PTH_RWLOCK_RW, 0, NULL ) ? 0 : errno;
}

int ldap_pvt_thread_rdwr_wtrylock( ldap_pvt_thread_rdwr_t *rw )
{
	return pth_rwlock_acquire( rw, PTH_RWLOCK_RW, 1, NULL ) ? 0 : errno;
}

int ldap_pvt_thread_rdwr_wunlock( ldap_pvt_thread_rdwr_t *rw )
{
	return pth_rwlock_release( rw ) ? 0 : errno;
}

#endif /* LDAP_THREAD_HAVE_RDWR */
#endif /* HAVE_GNU_PTH */
