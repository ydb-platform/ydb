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
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */
/* Portions Copyright (c) 1990 Regents of the University of Michigan.
 * All rights reserved.
 */

#include "portable.h"

#include <stdio.h>
#include <ac/stdlib.h>

#include <ac/socket.h>
#include <ac/string.h>
#include <ac/time.h>

#include "ldap-int.h"

/* An Unbind Request looks like this:
 *
 *	UnbindRequest ::= [APPLICATION 2] NULL
 *
 * and has no response.  (Source: RFC 4511)
 */

int
ldap_unbind_ext(
	LDAP *ld,
	LDAPControl **sctrls,
	LDAPControl **cctrls )
{
	int rc;

	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );

	/* check client controls */
	rc = ldap_int_client_controls( ld, cctrls );
	if( rc != LDAP_SUCCESS ) return rc;

	return ldap_ld_free( ld, 1, sctrls, cctrls );
}

int
ldap_unbind_ext_s(
	LDAP *ld,
	LDAPControl **sctrls,
	LDAPControl **cctrls )
{
	return ldap_unbind_ext( ld, sctrls, cctrls );
}

int
ldap_unbind( LDAP *ld )
{
	Debug0( LDAP_DEBUG_TRACE, "ldap_unbind\n" );

	return( ldap_unbind_ext( ld, NULL, NULL ) );
}


int
ldap_ld_free(
	LDAP *ld,
	int close,
	LDAPControl **sctrls,
	LDAPControl **cctrls )
{
	LDAPMessage	*lm, *next;
	int		err = LDAP_SUCCESS;

	LDAP_MUTEX_LOCK( &ld->ld_ldcmutex );
	/* Someone else is still using this ld. */
	if (ld->ld_ldcrefcnt > 1) {	/* but not last thread */
		/* clean up self only */
		ld->ld_ldcrefcnt--;
		if ( ld->ld_error != NULL ) {
			LDAP_FREE( ld->ld_error );
			ld->ld_error = NULL;
		}

		if ( ld->ld_matched != NULL ) {
			LDAP_FREE( ld->ld_matched );
			ld->ld_matched = NULL;
		}
		if ( ld->ld_referrals != NULL) {
			LDAP_VFREE(ld->ld_referrals);
			ld->ld_referrals = NULL;
		}
		LDAP_MUTEX_UNLOCK( &ld->ld_ldcmutex );
		LDAP_FREE( (char *) ld );
		return( err );
	}

	/* This ld is the last thread. */
	LDAP_MUTEX_UNLOCK( &ld->ld_ldcmutex );

	/* free LDAP structure and outstanding requests/responses */
	LDAP_MUTEX_LOCK( &ld->ld_req_mutex );
	ldap_tavl_free( ld->ld_requests, ldap_do_free_request );
	ld->ld_requests = NULL;
	LDAP_MUTEX_UNLOCK( &ld->ld_req_mutex );
	LDAP_MUTEX_LOCK( &ld->ld_conn_mutex );

	/* free and unbind from all open connections */
	while ( ld->ld_conns != NULL ) {
		ldap_free_connection( ld, ld->ld_conns, 1, close );
	}
	LDAP_MUTEX_UNLOCK( &ld->ld_conn_mutex );
	LDAP_MUTEX_LOCK( &ld->ld_res_mutex );
	for ( lm = ld->ld_responses; lm != NULL; lm = next ) {
		next = lm->lm_next;
		ldap_msgfree( lm );
	}

	if ( ld->ld_abandoned != NULL ) {
		LDAP_FREE( ld->ld_abandoned );
		ld->ld_abandoned = NULL;
	}
	LDAP_MUTEX_UNLOCK( &ld->ld_res_mutex );

	/* Should already be closed by ldap_free_connection which knows not to free
	 * this one */
	ber_int_sb_destroy( ld->ld_sb );
	LBER_FREE( ld->ld_sb );

	LDAP_MUTEX_LOCK( &ld->ld_ldopts_mutex );

	/* final close callbacks */
	{
		ldaplist *ll, *next;

		for ( ll = ld->ld_options.ldo_conn_cbs; ll; ll = next ) {
			ldap_conncb *cb = ll->ll_data;
			next = ll->ll_next;
			cb->lc_del( ld, NULL, cb );
			LDAP_FREE( ll );
		}
	}

	if ( ld->ld_error != NULL ) {
		LDAP_FREE( ld->ld_error );
		ld->ld_error = NULL;
	}

	if ( ld->ld_matched != NULL ) {
		LDAP_FREE( ld->ld_matched );
		ld->ld_matched = NULL;
	}

	if ( ld->ld_referrals != NULL) {
		LDAP_VFREE(ld->ld_referrals);
		ld->ld_referrals = NULL;
	}

	if ( ld->ld_selectinfo != NULL ) {
		ldap_free_select_info( ld->ld_selectinfo );
		ld->ld_selectinfo = NULL;
	}

	if ( ld->ld_options.ldo_defludp != NULL ) {
		ldap_free_urllist( ld->ld_options.ldo_defludp );
		ld->ld_options.ldo_defludp = NULL;
	}

	if ( ld->ld_options.ldo_local_ip_addrs.local_ip_addrs ) {
		LDAP_FREE( ld->ld_options.ldo_local_ip_addrs.local_ip_addrs );
		memset( & ld->ld_options.ldo_local_ip_addrs, 0,
			sizeof( ldapsourceip ) );
	}

#ifdef LDAP_CONNECTIONLESS
	if ( ld->ld_options.ldo_peer != NULL ) {
		LDAP_FREE( ld->ld_options.ldo_peer );
		ld->ld_options.ldo_peer = NULL;
	}

	if ( ld->ld_options.ldo_cldapdn != NULL ) {
		LDAP_FREE( ld->ld_options.ldo_cldapdn );
		ld->ld_options.ldo_cldapdn = NULL;
	}
#endif

	if ( ld->ld_options.ldo_defbase != NULL ) {
		LDAP_FREE( ld->ld_options.ldo_defbase );
		ld->ld_options.ldo_defbase = NULL;
	}

#ifdef HAVE_CYRUS_SASL
	if ( ld->ld_options.ldo_def_sasl_mech != NULL ) {
		LDAP_FREE( ld->ld_options.ldo_def_sasl_mech );
		ld->ld_options.ldo_def_sasl_mech = NULL;
	}

	if ( ld->ld_options.ldo_def_sasl_realm != NULL ) {
		LDAP_FREE( ld->ld_options.ldo_def_sasl_realm );
		ld->ld_options.ldo_def_sasl_realm = NULL;
	}

	if ( ld->ld_options.ldo_def_sasl_authcid != NULL ) {
		LDAP_FREE( ld->ld_options.ldo_def_sasl_authcid );
		ld->ld_options.ldo_def_sasl_authcid = NULL;
	}

	if ( ld->ld_options.ldo_def_sasl_authzid != NULL ) {
		LDAP_FREE( ld->ld_options.ldo_def_sasl_authzid );
		ld->ld_options.ldo_def_sasl_authzid = NULL;
	}
#endif

#ifdef HAVE_TLS
	ldap_int_tls_destroy( &ld->ld_options );
#endif

	if ( ld->ld_options.ldo_sctrls != NULL ) {
		ldap_controls_free( ld->ld_options.ldo_sctrls );
		ld->ld_options.ldo_sctrls = NULL;
	}

	if ( ld->ld_options.ldo_cctrls != NULL ) {
		ldap_controls_free( ld->ld_options.ldo_cctrls );
		ld->ld_options.ldo_cctrls = NULL;
	}
	LDAP_MUTEX_UNLOCK( &ld->ld_ldopts_mutex );

#ifdef LDAP_R_COMPILE
	ldap_pvt_thread_mutex_destroy( &ld->ld_msgid_mutex );
	ldap_pvt_thread_mutex_destroy( &ld->ld_conn_mutex );
	ldap_pvt_thread_mutex_destroy( &ld->ld_req_mutex );
	ldap_pvt_thread_mutex_destroy( &ld->ld_res_mutex );
	ldap_pvt_thread_mutex_destroy( &ld->ld_abandon_mutex );
	ldap_pvt_thread_mutex_destroy( &ld->ld_ldopts_mutex );
	ldap_pvt_thread_mutex_destroy( &ld->ld_ldcmutex );
#endif
#ifndef NDEBUG
	LDAP_TRASH(ld);
#endif
	LDAP_FREE( (char *) ld->ldc );
	LDAP_FREE( (char *) ld );

	return( err );
}

int
ldap_destroy( LDAP *ld )
{
	return ( ldap_ld_free( ld, 1, NULL, NULL ) );
}

int
ldap_unbind_s( LDAP *ld )
{
	return( ldap_unbind_ext( ld, NULL, NULL ) );
}

/* FIXME: this function is called only by ldap_free_connection(),
 * which, most of the times, is called with ld_req_mutex locked */
int
ldap_send_unbind(
	LDAP *ld,
	Sockbuf *sb,
	LDAPControl **sctrls,
	LDAPControl **cctrls )
{
	BerElement	*ber;
	ber_int_t	id;

	Debug0( LDAP_DEBUG_TRACE, "ldap_send_unbind\n" );

#ifdef LDAP_CONNECTIONLESS
	if (LDAP_IS_UDP(ld))
		return LDAP_SUCCESS;
#endif
	/* create a message to send */
	if ( (ber = ldap_alloc_ber_with_options( ld )) == NULL ) {
		return( ld->ld_errno );
	}

	LDAP_NEXT_MSGID(ld, id);

	/* fill it in */
	if ( ber_printf( ber, "{itn" /*}*/, id,
	    LDAP_REQ_UNBIND ) == -1 ) {
		ld->ld_errno = LDAP_ENCODING_ERROR;
		ber_free( ber, 1 );
		return( ld->ld_errno );
	}

	/* Put Server Controls */
	if( ldap_int_put_controls( ld, sctrls, ber ) != LDAP_SUCCESS ) {
		ber_free( ber, 1 );
		return ld->ld_errno;
	}

	if ( ber_printf( ber, /*{*/ "N}", LDAP_REQ_UNBIND ) == -1 ) {
		ld->ld_errno = LDAP_ENCODING_ERROR;
		ber_free( ber, 1 );
		return( ld->ld_errno );
	}

	ld->ld_errno = LDAP_SUCCESS;
	/* send the message */
	if ( ber_flush2( sb, ber, LBER_FLUSH_FREE_ALWAYS ) == -1 ) {
		ld->ld_errno = LDAP_SERVER_DOWN;
	}

	return( ld->ld_errno );
}
