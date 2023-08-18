/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 1998-2022 The OpenLDAP Foundation.
 * Portions Copyright 2007 Pierangelo Masarati.
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
 * This work was developed by Pierangelo Masarati for inclusion in
 * OpenLDAP Software.
 */

#include "portable.h"

#include <stdio.h>
#include <ac/stdlib.h>
#include <ac/string.h>
#include <ac/time.h>

#include "ldap-int.h"

#ifdef LDAP_CONTROL_X_SESSION_TRACKING

/*
 * Client-side of <draft-wahl-ldap-session-03>
 */

int
ldap_create_session_tracking_value(
	LDAP		*ld,
	char		*sessionSourceIp,
	char		*sessionSourceName,
	char		*formatOID,
	struct berval	*sessionTrackingIdentifier,
	struct berval	*value )
{
	BerElement	*ber = NULL;
	ber_tag_t	tag;

	struct berval	ip, name, oid, id;

	if ( ld == NULL ||
		formatOID == NULL ||
		value == NULL )
	{
param_error:;
		if ( ld ) {
			ld->ld_errno = LDAP_PARAM_ERROR;
		}

		return LDAP_PARAM_ERROR;
	}

	assert( LDAP_VALID( ld ) );
	ld->ld_errno = LDAP_SUCCESS;

	/* check sizes according to I.D. */
	if ( sessionSourceIp == NULL ) {
		BER_BVSTR( &ip, "" );

	} else {
		ber_str2bv( sessionSourceIp, 0, 0, &ip );
		/* NOTE: we're strict because we don't want
		 * to send out bad data */
		if ( ip.bv_len > 128 ) goto param_error;
	}

	if ( sessionSourceName == NULL ) {
		BER_BVSTR( &name, "" );

	} else {
		ber_str2bv( sessionSourceName, 0, 0, &name );
		/* NOTE: we're strict because we don't want
		 * to send out bad data */
		if ( name.bv_len > 65536 ) goto param_error;
	}

	ber_str2bv( formatOID, 0, 0, &oid );
	/* NOTE: we're strict because we don't want
	 * to send out bad data */
	if ( oid.bv_len > 1024 ) goto param_error;

	if ( sessionTrackingIdentifier == NULL ||
		sessionTrackingIdentifier->bv_val == NULL )
	{
		BER_BVSTR( &id, "" );

	} else {
		id = *sessionTrackingIdentifier;
	}

	/* prepare value */
	value->bv_val = NULL;
	value->bv_len = 0;

	ber = ldap_alloc_ber_with_options( ld );
	if ( ber == NULL ) {
		ld->ld_errno = LDAP_NO_MEMORY;
		return ld->ld_errno;
	}

	tag = ber_printf( ber, "{OOOO}", &ip, &name, &oid, &id );
	if ( tag == LBER_ERROR ) {
		ld->ld_errno = LDAP_ENCODING_ERROR;
		goto done;
	}

	if ( ber_flatten2( ber, value, 1 ) == -1 ) {
		ld->ld_errno = LDAP_NO_MEMORY;
	}

done:;
	if ( ber != NULL ) {
		ber_free( ber, 1 );
	}

	return ld->ld_errno;
}

/*
 * NOTE: this API is bad; it could be much more efficient...
 */
int
ldap_create_session_tracking_control(
	LDAP		*ld,
	char		*sessionSourceIp,
	char		*sessionSourceName,
	char		*formatOID,
	struct berval	*sessionTrackingIdentifier,
	LDAPControl	**ctrlp )
{
	struct berval	value;

	if ( ctrlp == NULL ) {
		ld->ld_errno = LDAP_PARAM_ERROR;
		return ld->ld_errno;
	}

	ld->ld_errno = ldap_create_session_tracking_value( ld,
		sessionSourceIp, sessionSourceName, formatOID,
		sessionTrackingIdentifier, &value );
	if ( ld->ld_errno == LDAP_SUCCESS ) {
		ld->ld_errno = ldap_control_create( LDAP_CONTROL_X_SESSION_TRACKING,
			0, &value, 0, ctrlp );
		if ( ld->ld_errno != LDAP_SUCCESS ) {
			LDAP_FREE( value.bv_val );
		}
	}

	return ld->ld_errno;
}

int
ldap_parse_session_tracking_control(
	LDAP *ld,
	LDAPControl *ctrl,
	struct berval *ip,
	struct berval *name,
	struct berval *oid,
	struct berval *id )
{
	BerElement	*ber;
	ber_tag_t	tag;
	ber_len_t	len;

	if ( ld == NULL || 
		ctrl == NULL || 
		ip == NULL ||
		name == NULL ||
		oid == NULL ||
		id == NULL )
	{
		if ( ld ) {
			ld->ld_errno = LDAP_PARAM_ERROR;
		}

		/* NOTE: we want the caller to get all or nothing;
		 * we could allow some of the pointers to be NULL,
		 * if one does not want part of the data */
		return LDAP_PARAM_ERROR;
	}

	BER_BVZERO( ip );
	BER_BVZERO( name );
	BER_BVZERO( oid );
	BER_BVZERO( id );

	ber = ber_init( &ctrl->ldctl_value );

	if ( ber == NULL ) {
		ld->ld_errno = LDAP_NO_MEMORY;
		return ld->ld_errno;
	}

	tag = ber_skip_tag( ber, &len );
	if ( tag != LBER_SEQUENCE ) {
		tag = LBER_ERROR;
		goto error;
	}

	/* sessionSourceIp */
	tag = ber_peek_tag( ber, &len );
	if ( tag == LBER_DEFAULT ) {
		tag = LBER_ERROR;
		goto error;
	}

	if ( len == 0 ) {
		tag = ber_skip_tag( ber, &len );

	} else {
		if ( len > 128 ) {
			/* should be LDAP_DECODING_ERROR,
			 * but we're liberal in what we accept */
		}
		tag = ber_scanf( ber, "o", ip );
	}

	/* sessionSourceName */
	tag = ber_peek_tag( ber, &len );
	if ( tag == LBER_DEFAULT ) {
		tag = LBER_ERROR;
		goto error;
	}

	if ( len == 0 ) {
		tag = ber_skip_tag( ber, &len );

	} else {
		if ( len > 65536 ) {
			/* should be LDAP_DECODING_ERROR,
			 * but we're liberal in what we accept */
		}
		tag = ber_scanf( ber, "o", name );
	}

	/* formatOID */
	tag = ber_peek_tag( ber, &len );
	if ( tag == LBER_DEFAULT ) {
		tag = LBER_ERROR;
		goto error;
	}

	if ( len == 0 ) {
		ld->ld_errno = LDAP_DECODING_ERROR;
		goto error;

	} else {
		if ( len > 1024 ) {
			/* should be LDAP_DECODING_ERROR,
			 * but we're liberal in what we accept */
		}
		tag = ber_scanf( ber, "o", oid );
	}

	/* FIXME: should check if it is an OID... leave it to the caller */

	/* sessionTrackingIdentifier */
	tag = ber_peek_tag( ber, &len );
	if ( tag == LBER_DEFAULT ) {
		tag = LBER_ERROR;
		goto error;
	}

	if ( len == 0 ) {
		tag = ber_skip_tag( ber, &len );

	} else {
#if 0
		if ( len > 65536 ) {
			/* should be LDAP_DECODING_ERROR,
			 * but we're liberal in what we accept */
		}
#endif
		tag = ber_scanf( ber, "o", id );
	}

	/* closure */
	tag = ber_skip_tag( ber, &len );
	if ( tag == LBER_DEFAULT && len == 0 ) {
		tag = 0;
	}

error:;
	(void)ber_free( ber, 1 );

	if ( tag == LBER_ERROR ) {
		return LDAP_DECODING_ERROR;
	}

	return ld->ld_errno;
}

#endif /* LDAP_CONTROL_X_SESSION_TRACKING */
