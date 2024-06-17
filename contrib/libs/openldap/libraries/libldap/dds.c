/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 2005-2024 The OpenLDAP Foundation.
 * Portions Copyright 2005-2006 SysNet s.n.c.
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
/* ACKNOWLEDGEMENTS:
 * This work was developed by Pierangelo Masarati for inclusion
 * in OpenLDAP Software */

#include "portable.h"

#include <stdio.h>
#include <ac/stdlib.h>
#include <ac/string.h>
#include <ac/time.h>

#include "ldap-int.h"

int
ldap_parse_refresh( LDAP *ld, LDAPMessage *res, ber_int_t *newttl )
{
	int		rc;
	struct berval	*retdata = NULL;
	ber_tag_t	tag;
	BerElement	*ber;

	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );
	assert( res != NULL );
	assert( newttl != NULL );

	*newttl = 0;

	rc = ldap_parse_extended_result( ld, res, NULL, &retdata, 0 );

	if ( rc != LDAP_SUCCESS ) {
		return rc;
	}

	if ( ld->ld_errno != LDAP_SUCCESS ) {
		return ld->ld_errno;
	}

	if ( retdata == NULL ) {
		rc = ld->ld_errno = LDAP_DECODING_ERROR;
		return rc;
	}

	ber = ber_init( retdata );
	if ( ber == NULL ) {
		rc = ld->ld_errno = LDAP_NO_MEMORY;
		goto done;
	}

	/* check the tag */
	tag = ber_scanf( ber, "{i}", newttl );
	ber_free( ber, 1 );

	if ( tag != LDAP_TAG_EXOP_REFRESH_RES_TTL ) {
		*newttl = 0;
		rc = ld->ld_errno = LDAP_DECODING_ERROR;
	}

done:;
	if ( retdata ) {
		ber_bvfree( retdata );
	}

	return rc;
}

int
ldap_refresh(
	LDAP		*ld,
	struct berval	*dn,
	ber_int_t		ttl,
	LDAPControl	**sctrls,
	LDAPControl	**cctrls,
	int		*msgidp )
{
	struct berval	bv = { 0, NULL };
        BerElement	*ber = NULL;
	int		rc;

	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );
	assert( dn != NULL );
	assert( msgidp != NULL );

	*msgidp = -1;

	ber = ber_alloc_t( LBER_USE_DER );

	if ( ber == NULL ) {
		ld->ld_errno = LDAP_NO_MEMORY;
		return ld->ld_errno;
	}

	ber_printf( ber, "{tOtiN}",
		LDAP_TAG_EXOP_REFRESH_REQ_DN, dn,
		LDAP_TAG_EXOP_REFRESH_REQ_TTL, ttl );

	rc = ber_flatten2( ber, &bv, 0 );

	if ( rc < 0 ) {
		rc = ld->ld_errno = LDAP_ENCODING_ERROR;
		goto done;
	}

	rc = ldap_extended_operation( ld, LDAP_EXOP_REFRESH, &bv,
		sctrls, cctrls, msgidp );

done:;
        ber_free( ber, 1 );

	return rc;
}

int
ldap_refresh_s(
	LDAP		*ld,
	struct berval	*dn,
	ber_int_t		ttl,
	ber_int_t		*newttl,
	LDAPControl	**sctrls,
	LDAPControl	**cctrls )
{
	int		rc;
	int		msgid;
	LDAPMessage	*res;

	rc = ldap_refresh( ld, dn, ttl, sctrls, cctrls, &msgid );
	if ( rc != LDAP_SUCCESS ) return rc;
	
	rc = ldap_result( ld, msgid, LDAP_MSG_ALL, (struct timeval *)NULL, &res );
	if( rc == -1 || !res ) return ld->ld_errno;

	rc = ldap_parse_refresh( ld, res, newttl );
	if( rc != LDAP_SUCCESS ) {
		ldap_msgfree( res );
		return rc;
	}

	return ldap_result2error( ld, res, 1 );
}

