/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 1998-2024 The OpenLDAP Foundation.
 * Portions Copyright 2008 Pierangelo Masarati.
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
 * This work was initially developed by Pierangelo Masarati
 * for inclusion in OpenLDAP Software.
 */

#include "portable.h"

#include <stdio.h>
#include <ac/stdlib.h>
#include <ac/string.h>
#include <ac/time.h>

#include "ldap-int.h"

int
ldap_create_deref_control_value(
	LDAP		*ld,
	LDAPDerefSpec	*ds,
	struct berval	*value )
{
	BerElement	*ber = NULL;
	ber_tag_t	tag;
	int		i;

	if ( ld == NULL || value == NULL || ds == NULL )
	{
		if ( ld )
			ld->ld_errno = LDAP_PARAM_ERROR;
		return LDAP_PARAM_ERROR;
	}

	assert( LDAP_VALID( ld ) );

	value->bv_val = NULL;
	value->bv_len = 0;
	ld->ld_errno = LDAP_SUCCESS;

	ber = ldap_alloc_ber_with_options( ld );
	if ( ber == NULL ) {
		ld->ld_errno = LDAP_NO_MEMORY;
		return ld->ld_errno;
	}

	tag = ber_printf( ber, "{" /*}*/ );
	if ( tag == LBER_ERROR ) {
		ld->ld_errno = LDAP_ENCODING_ERROR;
		goto done;
	}

	for ( i = 0; ds[i].derefAttr != NULL; i++ ) {
		int j;

		tag = ber_printf( ber, "{s{" /*}}*/ , ds[i].derefAttr );
		if ( tag == LBER_ERROR ) {
			ld->ld_errno = LDAP_ENCODING_ERROR;
			goto done;
		}

		for ( j = 0; ds[i].attributes[j] != NULL; j++ ) {
			tag = ber_printf( ber, "s", ds[i].attributes[ j ] );
			if ( tag == LBER_ERROR ) {
				ld->ld_errno = LDAP_ENCODING_ERROR;
				goto done;
			}
		}

		tag = ber_printf( ber, /*{{*/ "}N}" );
		if ( tag == LBER_ERROR ) {
			ld->ld_errno = LDAP_ENCODING_ERROR;
			goto done;
		}
	}

	tag = ber_printf( ber, /*{*/ "}" );
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
ldap_create_deref_control(
	LDAP		*ld,
	LDAPDerefSpec	*ds,
	int		iscritical,
	LDAPControl	**ctrlp )
{
	struct berval	value;

	if ( ctrlp == NULL ) {
		ld->ld_errno = LDAP_PARAM_ERROR;
		return ld->ld_errno;
	}

	ld->ld_errno = ldap_create_deref_control_value( ld, ds, &value );
	if ( ld->ld_errno == LDAP_SUCCESS ) {
		ld->ld_errno = ldap_control_create( LDAP_CONTROL_X_DEREF,
			iscritical, &value, 0, ctrlp );
		if ( ld->ld_errno != LDAP_SUCCESS ) {
			LDAP_FREE( value.bv_val );
		}
	}

	return ld->ld_errno;
}

void
ldap_derefresponse_free( LDAPDerefRes *dr )
{
	for ( ; dr; ) {
		LDAPDerefRes *drnext = dr->next;
		LDAPDerefVal *dv;

		LDAP_FREE( dr->derefAttr );
		LDAP_FREE( dr->derefVal.bv_val );

		for ( dv = dr->attrVals; dv; ) {
			LDAPDerefVal *dvnext = dv->next;
			LDAP_FREE( dv->type );
			ber_bvarray_free( dv->vals );
			LDAP_FREE( dv );
			dv = dvnext;
		}

		LDAP_FREE( dr );

		dr = drnext;
	}
}

int
ldap_parse_derefresponse_control(
	LDAP		*ld,
	LDAPControl	*ctrl,
	LDAPDerefRes	**drp2 )
{
	BerElementBuffer berbuf;
	BerElement *ber = (BerElement *)&berbuf;
	ber_tag_t tag;
	ber_len_t len;
	char *last;
	LDAPDerefRes *drhead = NULL, **drp;

	if ( ld == NULL || ctrl == NULL || drp2 == NULL ) {
		if ( ld )
			ld->ld_errno = LDAP_PARAM_ERROR;
		return LDAP_PARAM_ERROR;
	}

	/* Set up a BerElement from the berval returned in the control. */
	ber_init2( ber, &ctrl->ldctl_value, 0 );

	/* Extract the count and cookie from the control. */
	drp = &drhead;
	for ( tag = ber_first_element( ber, &len, &last );
		tag != LBER_DEFAULT;
		tag = ber_next_element( ber, &len, last ) )
	{
		LDAPDerefRes *dr;
		LDAPDerefVal **dvp;
		char *last2;

		dr = LDAP_CALLOC( 1, sizeof(LDAPDerefRes) );
		if ( dr == NULL ) {
			ldap_derefresponse_free( drhead );
			*drp2 = NULL;
			ld->ld_errno = LDAP_NO_MEMORY;
			return ld->ld_errno;
		}
		dvp = &dr->attrVals;

		tag = ber_scanf( ber, "{ao", &dr->derefAttr, &dr->derefVal );
		if ( tag == LBER_ERROR ) {
			goto done;
		}

		tag = ber_peek_tag( ber, &len );
		if ( tag == (LBER_CONSTRUCTED|LBER_CLASS_CONTEXT) ) {
			for ( tag = ber_first_element( ber, &len, &last2 );
				tag != LBER_DEFAULT;
				tag = ber_next_element( ber, &len, last2 ) )
			{
				LDAPDerefVal *dv;

				dv = LDAP_CALLOC( 1, sizeof(LDAPDerefVal) );
				if ( dv == NULL ) {
					ldap_derefresponse_free( drhead );
					LDAP_FREE( dr );
					*drp2 = NULL;
					ld->ld_errno = LDAP_NO_MEMORY;
					return ld->ld_errno;
				}

				tag = ber_scanf( ber, "{a[W]}", &dv->type, &dv->vals );
				if ( tag == LBER_ERROR ) {
					goto done;
				}

				*dvp = dv;
				dvp = &dv->next;
			}
		}

		tag = ber_scanf( ber, "}" );
		if ( tag == LBER_ERROR ) {
			goto done;
		}

		*drp = dr;
		drp = &dr->next;
	}

	tag = 0;

done:;
	if ( tag == LBER_ERROR ) {
		if ( drhead != NULL ) {
			ldap_derefresponse_free( drhead );
		}

		*drp2 = NULL;
		ld->ld_errno = LDAP_DECODING_ERROR;

	} else {
		*drp2 = drhead;
		ld->ld_errno = LDAP_SUCCESS;
	}

	return ld->ld_errno;
}

int
ldap_parse_deref_control(
	LDAP		*ld,
	LDAPControl	**ctrls,
	LDAPDerefRes	**drp )
{
	LDAPControl *c;

	if ( drp == NULL ) {
		ld->ld_errno = LDAP_PARAM_ERROR;
		return ld->ld_errno;
	}

	*drp = NULL;

	if ( ctrls == NULL ) {
		ld->ld_errno =  LDAP_CONTROL_NOT_FOUND;
		return ld->ld_errno;
	}

	c = ldap_control_find( LDAP_CONTROL_X_DEREF, ctrls, NULL );
	if ( c == NULL ) {
		/* No deref control was found. */
		ld->ld_errno = LDAP_CONTROL_NOT_FOUND;
		return ld->ld_errno;
	}

	ld->ld_errno = ldap_parse_derefresponse_control( ld, c, drp );

	return ld->ld_errno;
}

