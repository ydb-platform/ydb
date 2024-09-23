/* thr_nt.c - wrapper around NT threads */
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

#if defined( HAVE_NT_THREADS )

#define _WIN32_WINNT 0x0400
#include <windows.h>
#include <process.h>

#include "ldap_pvt_thread.h" /* Get the thread interface */
#define LDAP_THREAD_IMPLEMENTATION
#include "ldap_thr_debug.h"	 /* May rename the symbols defined below */

typedef struct ldap_int_thread_s {
	long tid;
	HANDLE thd;
} ldap_int_thread_s;

#ifndef NT_MAX_THREADS
#define NT_MAX_THREADS	1024
#endif

static ldap_int_thread_s tids[NT_MAX_THREADS];
static int ntids;


/* mingw compiler very sensitive about getting prototypes right */
typedef unsigned __stdcall thrfunc_t(void *);

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

int
ldap_int_mutex_firstcreate( ldap_int_thread_mutex_t *mutex )
{
	if ( *mutex == NULL ) {
		HANDLE p = CreateMutex( NULL, 0, NULL );
		if ( InterlockedCompareExchangePointer((PVOID*)mutex, (PVOID)p, NULL) != NULL)
			CloseHandle( p );
	}
	return 0;
}

int 
ldap_pvt_thread_create( ldap_pvt_thread_t * thread, 
	int detach,
	void *(*start_routine)( void *),
	void *arg)
{
	unsigned tid;
	HANDLE thd;
	int rc = -1;

	thd = (HANDLE) _beginthreadex(NULL, LDAP_PVT_THREAD_STACK_SIZE, (thrfunc_t *) start_routine,
				      arg, 0, &tid);

	if ( thd ) {
		*thread = (ldap_pvt_thread_t) tid;
		tids[ntids].tid = tid;
		tids[ntids].thd = thd;
		ntids++;
		rc = 0;
	}
	return rc;
}
	
void 
ldap_pvt_thread_exit( void *retval )
{
	_endthread( );
}

int 
ldap_pvt_thread_join( ldap_pvt_thread_t thread, void **thread_return )
{
	DWORD status;
	int i;

	for (i=0; i<ntids; i++) {
		if ( tids[i].tid == thread )
			break;
	}
	if ( i > ntids ) return -1;

	status = WaitForSingleObject( tids[i].thd, INFINITE );
	for (; i<ntids; i++) {
		tids[i] = tids[i+1];
	}
	ntids--;
	return status == WAIT_FAILED ? -1 : 0;
}

int 
ldap_pvt_thread_kill( ldap_pvt_thread_t thread, int signo )
{
	return 0;
}

int 
ldap_pvt_thread_yield( void )
{
	Sleep( 0 );
	return 0;
}

int 
ldap_pvt_thread_cond_init( ldap_pvt_thread_cond_t *cond )
{
	*cond = CreateEvent( NULL, FALSE, FALSE, NULL );
	return( 0 );
}

int
ldap_pvt_thread_cond_destroy( ldap_pvt_thread_cond_t *cv )
{
	CloseHandle( *cv );
	return( 0 );
}

int 
ldap_pvt_thread_cond_signal( ldap_pvt_thread_cond_t *cond )
{
	SetEvent( *cond );
	return( 0 );
}

int 
ldap_pvt_thread_cond_wait( ldap_pvt_thread_cond_t *cond, 
	ldap_pvt_thread_mutex_t *mutex )
{
	SignalObjectAndWait( *mutex, *cond, INFINITE, FALSE );
	WaitForSingleObject( *mutex, INFINITE );
	return( 0 );
}

int
ldap_pvt_thread_cond_broadcast( ldap_pvt_thread_cond_t *cond )
{
	while ( WaitForSingleObject( *cond, 0 ) == WAIT_TIMEOUT )
		SetEvent( *cond );
	return( 0 );
}

int 
ldap_pvt_thread_mutex_init( ldap_pvt_thread_mutex_t *mutex )
{
	*mutex = CreateMutex( NULL, 0, NULL );
	return ( 0 );
}

int
ldap_pvt_thread_mutex_recursive_init( ldap_pvt_thread_mutex_t *mutex )
{
	/* All NT mutexes are recursive */
	return ldap_pvt_thread_mutex_init( mutex );
}

int 
ldap_pvt_thread_mutex_destroy( ldap_pvt_thread_mutex_t *mutex )
{
	CloseHandle( *mutex );
	return ( 0 );	
}

int 
ldap_pvt_thread_mutex_lock( ldap_pvt_thread_mutex_t *mutex )
{
	DWORD status;
	status = WaitForSingleObject( *mutex, INFINITE );
	return status == WAIT_FAILED ? -1 : 0;
}

int 
ldap_pvt_thread_mutex_unlock( ldap_pvt_thread_mutex_t *mutex )
{
	ReleaseMutex( *mutex );
	return ( 0 );
}

int
ldap_pvt_thread_mutex_trylock( ldap_pvt_thread_mutex_t *mp )
{
	DWORD status;
	status = WaitForSingleObject( *mp, 0 );
	return status == WAIT_FAILED || status == WAIT_TIMEOUT
		? -1 : 0;
}

ldap_pvt_thread_t
ldap_pvt_thread_self( void )
{
	return GetCurrentThreadId();
}

int
ldap_pvt_thread_key_create( ldap_pvt_thread_key_t *keyp )
{
	DWORD key = TlsAlloc();
	if ( key != TLS_OUT_OF_INDEXES ) {
		*keyp = key;
		return 0;
	} else {
		return -1;
	}
}

int
ldap_pvt_thread_key_destroy( ldap_pvt_thread_key_t key )
{
	/* TlsFree returns 0 on failure */
	return( TlsFree( key ) == 0 );
}

int
ldap_pvt_thread_key_setdata( ldap_pvt_thread_key_t key, void *data )
{
	return ( TlsSetValue( key, data ) == 0 );
}

int
ldap_pvt_thread_key_getdata( ldap_pvt_thread_key_t key, void **data )
{
	void *ptr = TlsGetValue( key );
	*data = ptr;
	return( ptr ? GetLastError() : 0 );
}

#endif
