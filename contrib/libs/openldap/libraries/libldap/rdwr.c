/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 1998-2022 The OpenLDAP Foundation.
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
/* This work was initially developed by Kurt D. Zeilenga for inclusion
 * in OpenLDAP Software.  Additional significant contributors include:
 *     Stuart Lynne
 */

/*
 * This is an improved implementation of Reader/Writer locks does
 * not protect writers from starvation.  That is, if a writer is
 * currently waiting on a reader, any new reader will get
 * the lock before the writer.
 *
 * Does not support cancellation nor does any status checking.
 */
/* Adapted from publicly available examples for:
 *	"Programming with Posix Threads"
 *		by David R Butenhof, Addison-Wesley 
 *		http://cseng.aw.com/bookpage.taf?ISBN=0-201-63392-2
 */

#include "portable.h"

#include <ac/stdlib.h>

#include <ac/errno.h>
#include <ac/string.h>
#include <ac/time.h>

#include "ldap-int.h"

#ifdef LDAP_R_COMPILE

#include "ldap_pvt_thread.h" /* Get the thread interface */
#define LDAP_THREAD_RDWR_IMPLEMENTATION
#include "ldap_thr_debug.h"  /* May rename the symbols defined below */

/*
 * implementations that provide their own compatible 
 * reader/writer locks define LDAP_THREAD_HAVE_RDWR
 * in ldap_pvt_thread.h
 */
#ifndef LDAP_THREAD_HAVE_RDWR

struct ldap_int_thread_rdwr_s {
	ldap_pvt_thread_mutex_t ltrw_mutex;
	ldap_pvt_thread_cond_t ltrw_read;       /* wait for read */
	ldap_pvt_thread_cond_t ltrw_write;      /* wait for write */
	int ltrw_valid;
#define LDAP_PVT_THREAD_RDWR_VALID 0x0bad
	int ltrw_r_active;
	int ltrw_w_active;
	int ltrw_r_wait;
	int ltrw_w_wait;
#ifdef LDAP_RDWR_DEBUG
	/* keep track of who has these locks */
#define	MAX_READERS	32
	int ltrw_more_readers; /* Set if ltrw_readers[] is incomplete */
	ldap_pvt_thread_t ltrw_readers[MAX_READERS];
	ldap_pvt_thread_t ltrw_writer;
#endif
};

int 
ldap_pvt_thread_rdwr_init( ldap_pvt_thread_rdwr_t *rwlock )
{
	struct ldap_int_thread_rdwr_s *rw;

	assert( rwlock != NULL );

	rw = (struct ldap_int_thread_rdwr_s *) LDAP_CALLOC( 1,
		sizeof( struct ldap_int_thread_rdwr_s ) );
	if ( !rw )
		return LDAP_NO_MEMORY;

	/* we should check return results */
	ldap_pvt_thread_mutex_init( &rw->ltrw_mutex );
	ldap_pvt_thread_cond_init( &rw->ltrw_read );
	ldap_pvt_thread_cond_init( &rw->ltrw_write );

	rw->ltrw_valid = LDAP_PVT_THREAD_RDWR_VALID;

	*rwlock = rw;
	return 0;
}

int 
ldap_pvt_thread_rdwr_destroy( ldap_pvt_thread_rdwr_t *rwlock )
{
	struct ldap_int_thread_rdwr_s *rw;

	assert( rwlock != NULL );
	rw = *rwlock;

	assert( rw != NULL );
	assert( rw->ltrw_valid == LDAP_PVT_THREAD_RDWR_VALID );

	if( rw->ltrw_valid != LDAP_PVT_THREAD_RDWR_VALID )
		return LDAP_PVT_THREAD_EINVAL;

	ldap_pvt_thread_mutex_lock( &rw->ltrw_mutex );

	assert( rw->ltrw_w_active >= 0 ); 
	assert( rw->ltrw_w_wait >= 0 ); 
	assert( rw->ltrw_r_active >= 0 ); 
	assert( rw->ltrw_r_wait >= 0 ); 

	/* active threads? */
	if( rw->ltrw_r_active > 0 || rw->ltrw_w_active > 0) {
		ldap_pvt_thread_mutex_unlock( &rw->ltrw_mutex );
		return LDAP_PVT_THREAD_EBUSY;
	}

	/* waiting threads? */
	if( rw->ltrw_r_wait > 0 || rw->ltrw_w_wait > 0) {
		ldap_pvt_thread_mutex_unlock( &rw->ltrw_mutex );
		return LDAP_PVT_THREAD_EBUSY;
	}

	rw->ltrw_valid = 0;

	ldap_pvt_thread_mutex_unlock( &rw->ltrw_mutex );

	ldap_pvt_thread_mutex_destroy( &rw->ltrw_mutex );
	ldap_pvt_thread_cond_destroy( &rw->ltrw_read );
	ldap_pvt_thread_cond_destroy( &rw->ltrw_write );

	LDAP_FREE(rw);
	*rwlock = NULL;
	return 0;
}

int ldap_pvt_thread_rdwr_rlock( ldap_pvt_thread_rdwr_t *rwlock )
{
	struct ldap_int_thread_rdwr_s *rw;

	assert( rwlock != NULL );
	rw = *rwlock;

	assert( rw != NULL );
	assert( rw->ltrw_valid == LDAP_PVT_THREAD_RDWR_VALID );

	if( rw->ltrw_valid != LDAP_PVT_THREAD_RDWR_VALID )
		return LDAP_PVT_THREAD_EINVAL;

	ldap_pvt_thread_mutex_lock( &rw->ltrw_mutex );

	assert( rw->ltrw_w_active >= 0 ); 
	assert( rw->ltrw_w_wait >= 0 ); 
	assert( rw->ltrw_r_active >= 0 ); 
	assert( rw->ltrw_r_wait >= 0 ); 

	if( rw->ltrw_w_active > 0 ) {
		/* writer is active */

		rw->ltrw_r_wait++;

		do {
			ldap_pvt_thread_cond_wait(
				&rw->ltrw_read, &rw->ltrw_mutex );
		} while( rw->ltrw_w_active > 0 );

		rw->ltrw_r_wait--;
		assert( rw->ltrw_r_wait >= 0 ); 
	}

#ifdef LDAP_RDWR_DEBUG
	if( rw->ltrw_r_active < MAX_READERS )
		rw->ltrw_readers[rw->ltrw_r_active] = ldap_pvt_thread_self();
	else
		rw->ltrw_more_readers = 1;
#endif
	rw->ltrw_r_active++;


	ldap_pvt_thread_mutex_unlock( &rw->ltrw_mutex );

	return 0;
}

int ldap_pvt_thread_rdwr_rtrylock( ldap_pvt_thread_rdwr_t *rwlock )
{
	struct ldap_int_thread_rdwr_s *rw;

	assert( rwlock != NULL );
	rw = *rwlock;

	assert( rw != NULL );
	assert( rw->ltrw_valid == LDAP_PVT_THREAD_RDWR_VALID );

	if( rw->ltrw_valid != LDAP_PVT_THREAD_RDWR_VALID )
		return LDAP_PVT_THREAD_EINVAL;

	ldap_pvt_thread_mutex_lock( &rw->ltrw_mutex );

	assert( rw->ltrw_w_active >= 0 ); 
	assert( rw->ltrw_w_wait >= 0 ); 
	assert( rw->ltrw_r_active >= 0 ); 
	assert( rw->ltrw_r_wait >= 0 ); 

	if( rw->ltrw_w_active > 0) {
		ldap_pvt_thread_mutex_unlock( &rw->ltrw_mutex );
		return LDAP_PVT_THREAD_EBUSY;
	}

#ifdef LDAP_RDWR_DEBUG
	if( rw->ltrw_r_active < MAX_READERS )
		rw->ltrw_readers[rw->ltrw_r_active] = ldap_pvt_thread_self();
	else
		rw->ltrw_more_readers = 1;
#endif
	rw->ltrw_r_active++;

	ldap_pvt_thread_mutex_unlock( &rw->ltrw_mutex );

	return 0;
}

int ldap_pvt_thread_rdwr_runlock( ldap_pvt_thread_rdwr_t *rwlock )
{
	struct ldap_int_thread_rdwr_s *rw;

	assert( rwlock != NULL );
	rw = *rwlock;

	assert( rw != NULL );
	assert( rw->ltrw_valid == LDAP_PVT_THREAD_RDWR_VALID );

	if( rw->ltrw_valid != LDAP_PVT_THREAD_RDWR_VALID )
		return LDAP_PVT_THREAD_EINVAL;

	ldap_pvt_thread_mutex_lock( &rw->ltrw_mutex );

	rw->ltrw_r_active--;
#ifdef LDAP_RDWR_DEBUG
	/* Remove us from the list of readers */
	{
		ldap_pvt_thread_t self = ldap_pvt_thread_self();
		int i, j;
		for( i = j = rw->ltrw_r_active; i >= 0; i--) {
			if (rw->ltrw_readers[i] == self) {
				rw->ltrw_readers[i] = rw->ltrw_readers[j];
				rw->ltrw_readers[j] = 0;
				break;
			}
		}
		if( !rw->ltrw_more_readers )
			assert( i >= 0 );
		else if( j == 0 )
			rw->ltrw_more_readers = 0;
	}
#endif

	assert( rw->ltrw_w_active >= 0 ); 
	assert( rw->ltrw_w_wait >= 0 ); 
	assert( rw->ltrw_r_active >= 0 ); 
	assert( rw->ltrw_r_wait >= 0 ); 

	if (rw->ltrw_r_active == 0 && rw->ltrw_w_wait > 0 ) {
		ldap_pvt_thread_cond_signal( &rw->ltrw_write );
	}

	ldap_pvt_thread_mutex_unlock( &rw->ltrw_mutex );

	return 0;
}

int ldap_pvt_thread_rdwr_wlock( ldap_pvt_thread_rdwr_t *rwlock )
{
	struct ldap_int_thread_rdwr_s *rw;

	assert( rwlock != NULL );
	rw = *rwlock;

	assert( rw != NULL );
	assert( rw->ltrw_valid == LDAP_PVT_THREAD_RDWR_VALID );

	if( rw->ltrw_valid != LDAP_PVT_THREAD_RDWR_VALID )
		return LDAP_PVT_THREAD_EINVAL;

	ldap_pvt_thread_mutex_lock( &rw->ltrw_mutex );

	assert( rw->ltrw_w_active >= 0 ); 
	assert( rw->ltrw_w_wait >= 0 ); 
	assert( rw->ltrw_r_active >= 0 ); 
	assert( rw->ltrw_r_wait >= 0 ); 

	if ( rw->ltrw_w_active > 0 || rw->ltrw_r_active > 0 ) {
		rw->ltrw_w_wait++;

		do {
			ldap_pvt_thread_cond_wait(
				&rw->ltrw_write, &rw->ltrw_mutex );
		} while ( rw->ltrw_w_active > 0 || rw->ltrw_r_active > 0 );

		rw->ltrw_w_wait--;
		assert( rw->ltrw_w_wait >= 0 ); 
	}

#ifdef LDAP_RDWR_DEBUG
	rw->ltrw_writer = ldap_pvt_thread_self();
#endif
	rw->ltrw_w_active++;

	ldap_pvt_thread_mutex_unlock( &rw->ltrw_mutex );

	return 0;
}

int ldap_pvt_thread_rdwr_wtrylock( ldap_pvt_thread_rdwr_t *rwlock )
{
	struct ldap_int_thread_rdwr_s *rw;

	assert( rwlock != NULL );
	rw = *rwlock;

	assert( rw != NULL );
	assert( rw->ltrw_valid == LDAP_PVT_THREAD_RDWR_VALID );

	if( rw->ltrw_valid != LDAP_PVT_THREAD_RDWR_VALID )
		return LDAP_PVT_THREAD_EINVAL;

	ldap_pvt_thread_mutex_lock( &rw->ltrw_mutex );

	assert( rw->ltrw_w_active >= 0 ); 
	assert( rw->ltrw_w_wait >= 0 ); 
	assert( rw->ltrw_r_active >= 0 ); 
	assert( rw->ltrw_r_wait >= 0 ); 

	if ( rw->ltrw_w_active > 0 || rw->ltrw_r_active > 0 ) {
		ldap_pvt_thread_mutex_unlock( &rw->ltrw_mutex );
		return LDAP_PVT_THREAD_EBUSY;
	}

#ifdef LDAP_RDWR_DEBUG
	rw->ltrw_writer = ldap_pvt_thread_self();
#endif
	rw->ltrw_w_active++;

	ldap_pvt_thread_mutex_unlock( &rw->ltrw_mutex );

	return 0;
}

int ldap_pvt_thread_rdwr_wunlock( ldap_pvt_thread_rdwr_t *rwlock )
{
	struct ldap_int_thread_rdwr_s *rw;

	assert( rwlock != NULL );
	rw = *rwlock;

	assert( rw != NULL );
	assert( rw->ltrw_valid == LDAP_PVT_THREAD_RDWR_VALID );

	if( rw->ltrw_valid != LDAP_PVT_THREAD_RDWR_VALID )
		return LDAP_PVT_THREAD_EINVAL;

	ldap_pvt_thread_mutex_lock( &rw->ltrw_mutex );

	rw->ltrw_w_active--;

	assert( rw->ltrw_w_active >= 0 ); 
	assert( rw->ltrw_w_wait >= 0 ); 
	assert( rw->ltrw_r_active >= 0 ); 
	assert( rw->ltrw_r_wait >= 0 ); 

	if (rw->ltrw_r_wait > 0) {
		ldap_pvt_thread_cond_broadcast( &rw->ltrw_read );

	} else if (rw->ltrw_w_wait > 0) {
		ldap_pvt_thread_cond_signal( &rw->ltrw_write );
	}

#ifdef LDAP_RDWR_DEBUG
	assert( rw->ltrw_writer == ldap_pvt_thread_self() );
	rw->ltrw_writer = 0;
#endif
	ldap_pvt_thread_mutex_unlock( &rw->ltrw_mutex );

	return 0;
}

#ifdef LDAP_RDWR_DEBUG

/* just for testing, 
 * return 0 if false, suitable for assert(ldap_pvt_thread_rdwr_Xchk(rdwr))
 * 
 * Currently they don't check if the calling thread is the one 
 * that has the lock, just that there is a reader or writer.
 *
 * Basically sufficient for testing that places that should have
 * a lock are caught.
 */

int ldap_pvt_thread_rdwr_readers(ldap_pvt_thread_rdwr_t *rwlock)
{
	struct ldap_int_thread_rdwr_s *rw;

	assert( rwlock != NULL );
	rw = *rwlock;

	assert( rw != NULL );
	assert( rw->ltrw_valid == LDAP_PVT_THREAD_RDWR_VALID );
	assert( rw->ltrw_w_active >= 0 ); 
	assert( rw->ltrw_w_wait >= 0 ); 
	assert( rw->ltrw_r_active >= 0 ); 
	assert( rw->ltrw_r_wait >= 0 ); 

	return( rw->ltrw_r_active );
}

int ldap_pvt_thread_rdwr_writers(ldap_pvt_thread_rdwr_t *rwlock)
{
	struct ldap_int_thread_rdwr_s *rw;

	assert( rwlock != NULL );
	rw = *rwlock;

	assert( rw != NULL );
	assert( rw->ltrw_valid == LDAP_PVT_THREAD_RDWR_VALID );
	assert( rw->ltrw_w_active >= 0 ); 
	assert( rw->ltrw_w_wait >= 0 ); 
	assert( rw->ltrw_r_active >= 0 ); 
	assert( rw->ltrw_r_wait >= 0 ); 

	return( rw->ltrw_w_active );
}

int ldap_pvt_thread_rdwr_active(ldap_pvt_thread_rdwr_t *rwlock)
{
	struct ldap_int_thread_rdwr_s *rw;

	assert( rwlock != NULL );
	rw = *rwlock;

	assert( rw != NULL );
	assert( rw->ltrw_valid == LDAP_PVT_THREAD_RDWR_VALID );
	assert( rw->ltrw_w_active >= 0 ); 
	assert( rw->ltrw_w_wait >= 0 ); 
	assert( rw->ltrw_r_active >= 0 ); 
	assert( rw->ltrw_r_wait >= 0 ); 

	return(ldap_pvt_thread_rdwr_readers(rwlock) +
	       ldap_pvt_thread_rdwr_writers(rwlock));
}

#endif /* LDAP_RDWR_DEBUG */

#endif /* LDAP_THREAD_HAVE_RDWR */

#endif /* LDAP_R_COMPILE */
