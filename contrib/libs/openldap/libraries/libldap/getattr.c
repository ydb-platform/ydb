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

char *
ldap_first_attribute( LDAP *ld, LDAPMessage *entry, BerElement **berout )
{
	int rc;
	ber_tag_t tag;
	ber_len_t len = 0;
	char *attr;
	BerElement *ber;

	Debug0( LDAP_DEBUG_TRACE, "ldap_first_attribute\n" );

	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );
	assert( entry != NULL );
	assert( berout != NULL );

	*berout = NULL;

	ber = ldap_alloc_ber_with_options( ld );
	if( ber == NULL ) {
		return NULL;
	}

	*ber = *entry->lm_ber;

	/* 
	 * Skip past the sequence, dn, sequence of sequence leaving
	 * us at the first attribute.
	 */

	tag = ber_scanf( ber, "{xl{" /*}}*/, &len );
	if( tag == LBER_ERROR ) {
		ld->ld_errno = LDAP_DECODING_ERROR;
		ber_free( ber, 0 );
		return NULL;
	}

	/* set the length to avoid overrun */
	rc = ber_set_option( ber, LBER_OPT_REMAINING_BYTES, &len );
	if( rc != LBER_OPT_SUCCESS ) {
		ld->ld_errno = LDAP_LOCAL_ERROR;
		ber_free( ber, 0 );
		return NULL;
	}

	if ( ber_pvt_ber_remaining( ber ) == 0 ) {
		assert( len == 0 );
		ber_free( ber, 0 );
		return NULL;
	}
	assert( len != 0 );

	/* snatch the first attribute */
	tag = ber_scanf( ber, "{ax}", &attr );
	if( tag == LBER_ERROR ) {
		ld->ld_errno = LDAP_DECODING_ERROR;
		ber_free( ber, 0 );
		return NULL;
	}

	*berout = ber;
	return attr;
}

/* ARGSUSED */
char *
ldap_next_attribute( LDAP *ld, LDAPMessage *entry, BerElement *ber )
{
	ber_tag_t tag;
	char *attr;

	Debug0( LDAP_DEBUG_TRACE, "ldap_next_attribute\n" );

	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );
	assert( entry != NULL );
	assert( ber != NULL );

	if ( ber_pvt_ber_remaining( ber ) == 0 ) {
		return NULL;
	}

	/* skip sequence, snarf attribute type, skip values */
	tag = ber_scanf( ber, "{ax}", &attr ); 
	if( tag == LBER_ERROR ) {
		ld->ld_errno = LDAP_DECODING_ERROR;
		return NULL;
	}

	return attr;
}

/* Fetch attribute type and optionally fetch values. The type
 * and values are referenced in-place from the BerElement, they are
 * not dup'd into malloc'd memory.
 */
/* ARGSUSED */
int
ldap_get_attribute_ber( LDAP *ld, LDAPMessage *entry, BerElement *ber,
	BerValue *attr, BerVarray *vals )
{
	ber_tag_t tag;
	int rc = LDAP_SUCCESS;

	Debug0( LDAP_DEBUG_TRACE, "ldap_get_attribute_ber\n" );

	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );
	assert( entry != NULL );
	assert( ber != NULL );
	assert( attr != NULL );

	attr->bv_val = NULL;
	attr->bv_len = 0;

	if ( ber_pvt_ber_remaining( ber ) ) {
		ber_len_t siz = sizeof( BerValue );

		/* skip sequence, snarf attribute type */
		tag = ber_scanf( ber, vals ? "{mM}" : "{mx}", attr, vals,
			&siz, (ber_len_t)0 );
		if( tag == LBER_ERROR ) {
			rc = ld->ld_errno = LDAP_DECODING_ERROR;
		}
	}

	return rc;
}
