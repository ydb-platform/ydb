/* references.c */
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
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */

#include "portable.h"

#include <stdio.h>

#include <ac/stdlib.h>

#include <ac/socket.h>
#include <ac/string.h>
#include <ac/time.h>

#include "ldap-int.h"

LDAPMessage *
ldap_first_reference( LDAP *ld, LDAPMessage *chain )
{
	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );
	assert( chain != NULL );

	return chain->lm_msgtype == LDAP_RES_SEARCH_REFERENCE
		? chain
		: ldap_next_reference( ld, chain );
}

LDAPMessage *
ldap_next_reference( LDAP *ld, LDAPMessage *ref )
{
	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );
	assert( ref != NULL );

	for (
		ref = ref->lm_chain;
		ref != NULL;
		ref = ref->lm_chain )
	{
		if( ref->lm_msgtype == LDAP_RES_SEARCH_REFERENCE ) {
			return( ref );
		}
	}

	return( NULL );
}

int
ldap_count_references( LDAP *ld, LDAPMessage *chain )
{
	int	i;

	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );

	for ( i = 0; chain != NULL; chain = chain->lm_chain ) {
		if( chain->lm_msgtype == LDAP_RES_SEARCH_REFERENCE ) {
			i++;
		}
	}

	return( i );
}

int
ldap_parse_reference( 
	LDAP            *ld,    
	LDAPMessage     *ref,
	char            ***referralsp,
	LDAPControl     ***serverctrls,
	int             freeit)
{
	BerElement be;
	char **refs = NULL;
	int rc;

	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );
	assert( ref !=  NULL );

	if( ref->lm_msgtype != LDAP_RES_SEARCH_REFERENCE ) {
		return LDAP_PARAM_ERROR;
	}

	/* make a private copy of BerElement */
	AC_MEMCPY(&be, ref->lm_ber, sizeof(be));
	
	if ( ber_scanf( &be, "{v" /*}*/, &refs ) == LBER_ERROR ) {
		rc = LDAP_DECODING_ERROR;
		goto free_and_return;
	}

	if ( serverctrls == NULL ) {
		rc = LDAP_SUCCESS;
		goto free_and_return;
	}

	if ( ber_scanf( &be, /*{*/ "}" ) == LBER_ERROR ) {
		rc = LDAP_DECODING_ERROR;
		goto free_and_return;
	}

	rc = ldap_pvt_get_controls( &be, serverctrls );

free_and_return:

	if( referralsp != NULL ) {
		/* provide references regardless of return code */
		*referralsp = refs;

	} else {
		LDAP_VFREE( refs );
	}

	if( freeit ) {
		ldap_msgfree( ref );
	}

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
