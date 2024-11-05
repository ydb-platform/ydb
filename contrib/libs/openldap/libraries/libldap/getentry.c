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

/* ARGSUSED */
LDAPMessage *
ldap_first_entry( LDAP *ld, LDAPMessage *chain )
{
	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );
	assert( chain != NULL );

	return chain->lm_msgtype == LDAP_RES_SEARCH_ENTRY
		? chain
		: ldap_next_entry( ld, chain );
}

LDAPMessage *
ldap_next_entry( LDAP *ld, LDAPMessage *entry )
{
	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );
	assert( entry != NULL );

	for(
		entry = entry->lm_chain;
		entry != NULL;
		entry = entry->lm_chain )
	{
		if( entry->lm_msgtype == LDAP_RES_SEARCH_ENTRY ) {
			return( entry );
		}
	}

	return( NULL );
}

int
ldap_count_entries( LDAP *ld, LDAPMessage *chain )
{
	int	i;

	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );

	for ( i = 0; chain != NULL; chain = chain->lm_chain ) {
		if( chain->lm_msgtype == LDAP_RES_SEARCH_ENTRY ) {
			i++;
		}
	}

	return( i );
}

int
ldap_get_entry_controls(
	LDAP *ld,
	LDAPMessage *entry, 
	LDAPControl ***sctrls )
{
	int rc;
	BerElement be;

	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );
	assert( entry != NULL );
	assert( sctrls != NULL );

	if ( entry->lm_msgtype != LDAP_RES_SEARCH_ENTRY ) {
		return LDAP_PARAM_ERROR;
	}

	/* make a local copy of the BerElement */
	AC_MEMCPY(&be, entry->lm_ber, sizeof(be));

	if ( ber_scanf( &be, "{xx" /*}*/ ) == LBER_ERROR ) {
		rc = LDAP_DECODING_ERROR;
		goto cleanup_and_return;
	}

	rc = ldap_pvt_get_controls( &be, sctrls );

cleanup_and_return:
	if( rc != LDAP_SUCCESS ) {
		ld->ld_errno = rc;

		if( ld->ld_matched != NULL ) {
			LDAP_FREE( ld->ld_matched );
			ld->ld_matched = NULL;
		}

		if( ld->ld_error != NULL ) {
			LDAP_FREE( ld->ld_error );
			ld->ld_error = NULL;
		}
	}

	return rc;
}
