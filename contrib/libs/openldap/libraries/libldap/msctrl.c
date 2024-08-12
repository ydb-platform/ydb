/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 1998-2024 The OpenLDAP Foundation.
 * Portions Copyright 2018 Howard Chu.
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
 * This work was developed by Howard Chu for inclusion in
 * OpenLDAP Software.
 */

#include "portable.h"

#include <stdio.h>
#include <ac/stdlib.h>
#include <ac/string.h>
#include <ac/time.h>

#include "ldap-int.h"

/* MS Active Directory controls - not implemented in slapd(8) */

#ifdef LDAP_CONTROL_X_DIRSYNC

int
ldap_create_dirsync_value(
	LDAP		*ld,
	int		flags,
	int		maxAttrCount,
	struct berval	*cookie,
	struct berval	*value )
{
	BerElement	*ber = NULL;
	ber_tag_t	tag;

	if ( ld == NULL || cookie == NULL ||
		value == NULL )
	{
		if ( ld ) {
			ld->ld_errno = LDAP_PARAM_ERROR;
		}

		return LDAP_PARAM_ERROR;
	}

	assert( LDAP_VALID( ld ) );
	ld->ld_errno = LDAP_SUCCESS;

	/* maxAttrCount less than 0x100000 is treated as 0x100000 by server */

	/* prepare value */
	value->bv_val = NULL;
	value->bv_len = 0;

	ber = ldap_alloc_ber_with_options( ld );
	if ( ber == NULL ) {
		ld->ld_errno = LDAP_NO_MEMORY;
		return ld->ld_errno;
	}

	tag = ber_printf( ber, "{iiO}", flags, maxAttrCount, cookie );
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

int
ldap_create_dirsync_control(
	LDAP		*ld,
	int			flags,
	int			maxAttrCount,
	struct berval	*cookie,
	LDAPControl	**ctrlp )
{
	struct berval	value;

	if ( ctrlp == NULL ) {
		ld->ld_errno = LDAP_PARAM_ERROR;
		return ld->ld_errno;
	}

	ld->ld_errno = ldap_create_dirsync_value( ld,
		flags, maxAttrCount, cookie, &value );
	if ( ld->ld_errno == LDAP_SUCCESS ) {
		ld->ld_errno = ldap_control_create( LDAP_CONTROL_X_DIRSYNC,
			1, &value, 0, ctrlp );
		if ( ld->ld_errno != LDAP_SUCCESS ) {
			LDAP_FREE( value.bv_val );
		}
	}

	return ld->ld_errno;
}

int
ldap_parse_dirsync_control(
	LDAP *ld,
	LDAPControl *ctrl,
	int *continueFlag,
	struct berval *cookie )
{
	BerElement	*ber;
	ber_tag_t	tag;
	ber_len_t	len;
	int unused;

	if ( ld == NULL || 
		ctrl == NULL || 
		continueFlag == NULL ||
		cookie == NULL )
	{
		if ( ld ) {
			ld->ld_errno = LDAP_PARAM_ERROR;
		}

		/* NOTE: we want the caller to get all or nothing;
		 * we could allow some of the pointers to be NULL,
		 * if one does not want part of the data */
		return LDAP_PARAM_ERROR;
	}

	*continueFlag = 0;
	BER_BVZERO( cookie );

	ber = ber_init( &ctrl->ldctl_value );

	if ( ber == NULL ) {
		ld->ld_errno = LDAP_NO_MEMORY;
		return ld->ld_errno;
	}

	tag = ber_scanf( ber, "{iio}", continueFlag, &unused, cookie );
	if ( tag == LBER_DEFAULT )
		tag = LBER_ERROR;

	(void)ber_free( ber, 1 );

	if ( tag == LBER_ERROR ) {
		return LDAP_DECODING_ERROR;
	}

	return ld->ld_errno;
}

#endif /* LDAP_CONTROL_X_DIRSYNC */

#ifdef LDAP_CONTROL_X_SHOW_DELETED

int
ldap_create_show_deleted_control( LDAP *ld,
                                    LDAPControl **ctrlp )
{
	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );
	assert( ctrlp != NULL );

	ld->ld_errno = ldap_control_create( LDAP_CONTROL_X_SHOW_DELETED,
		0, NULL, 0, ctrlp );

	return ld->ld_errno;
}

#endif /* LDAP_CONTROL_X_SHOW_DELETED */

#ifdef LDAP_CONTROL_X_EXTENDED_DN

int
ldap_create_extended_dn_value(
	LDAP		*ld,
	int			flag,
	struct berval	*value )
{
	BerElement	*ber = NULL;
	ber_tag_t	tag;

	if ( ld == NULL ||
		value == NULL )
	{
		if ( ld ) {
			ld->ld_errno = LDAP_PARAM_ERROR;
		}

		return LDAP_PARAM_ERROR;
	}

	assert( LDAP_VALID( ld ) );
	ld->ld_errno = LDAP_SUCCESS;

	/* prepare value */
	value->bv_val = NULL;
	value->bv_len = 0;

	ber = ldap_alloc_ber_with_options( ld );
	if ( ber == NULL ) {
		ld->ld_errno = LDAP_NO_MEMORY;
		return ld->ld_errno;
	}
	tag = ber_printf( ber, "{i}", flag );
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

int
ldap_create_extended_dn_control(
	LDAP		*ld,
	int			flag,
	LDAPControl	**ctrlp )
{
	struct berval	value;

	if ( ctrlp == NULL ) {
		ld->ld_errno = LDAP_PARAM_ERROR;
		return ld->ld_errno;
	}

	ld->ld_errno = ldap_create_extended_dn_value( ld, flag, &value );
	if ( ld->ld_errno == LDAP_SUCCESS ) {
		ld->ld_errno = ldap_control_create( LDAP_CONTROL_X_EXTENDED_DN,
			0, &value, 0, ctrlp );
		if ( ld->ld_errno != LDAP_SUCCESS ) {
			LDAP_FREE( value.bv_val );
		}
	}

	return ld->ld_errno;
}

#endif /* LDAP_CONTROL_X_EXTENDED_DN */

#ifdef LDAP_CONTROL_X_SERVER_NOTIFICATION

int
ldap_create_server_notification_control( LDAP *ld,
                                    LDAPControl **ctrlp )
{
	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );
	assert( ctrlp != NULL );

	ld->ld_errno = ldap_control_create( LDAP_CONTROL_X_SERVER_NOTIFICATION,
		0, NULL, 0, ctrlp );

	return ld->ld_errno;
}

#endif /* LDAP_CONTROL_X_SERVER_NOTIFICATION */
