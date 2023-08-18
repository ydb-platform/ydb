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
#include <ac/string.h>
#include <ac/time.h>

#include "ldap-int.h"

int
ldap_create_assertion_control_value(
	LDAP		*ld,
	char		*assertion,
	struct berval	*value )
{
	BerElement		*ber = NULL;
	int			err;

	ld->ld_errno = LDAP_SUCCESS;

	if ( assertion == NULL || assertion[ 0 ] == '\0' ) {
		ld->ld_errno = LDAP_PARAM_ERROR;
		return ld->ld_errno;
	}

	if ( value == NULL ) {
		ld->ld_errno = LDAP_PARAM_ERROR;
		return ld->ld_errno;
	}

	BER_BVZERO( value );

	ber = ldap_alloc_ber_with_options( ld );
	if ( ber == NULL ) {
		ld->ld_errno = LDAP_NO_MEMORY;
		return ld->ld_errno;
	}

	err = ldap_pvt_put_filter( ber, assertion );
	if ( err < 0 ) {
		ld->ld_errno = LDAP_ENCODING_ERROR;
		goto done;
	}

	err = ber_flatten2( ber, value, 1 );
	if ( err < 0 ) {
		ld->ld_errno = LDAP_NO_MEMORY;
		goto done;
	}

done:;
	if ( ber != NULL ) {
		ber_free( ber, 1 );
	}

	return ld->ld_errno;
}

int
ldap_create_assertion_control(
	LDAP		*ld,
	char		*assertion,
	int		iscritical,
	LDAPControl	**ctrlp )
{
	struct berval	value;

	if ( ctrlp == NULL ) {
		ld->ld_errno = LDAP_PARAM_ERROR;
		return ld->ld_errno;
	}

	ld->ld_errno = ldap_create_assertion_control_value( ld,
		assertion, &value );
	if ( ld->ld_errno == LDAP_SUCCESS ) {
		ld->ld_errno = ldap_control_create( LDAP_CONTROL_ASSERT,
			iscritical, &value, 0, ctrlp );
		if ( ld->ld_errno != LDAP_SUCCESS ) {
			LDAP_FREE( value.bv_val );
		}
	}

	return ld->ld_errno;
}

