/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 1998-2022 The OpenLDAP Foundation.
 * Copyright 2006 Hans Leidekker
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

/* ---------------------------------------------------------------------------
    ldap_create_page_control_value

    Create and encode the value of the paged results control (RFC 2696).

    ld          (IN) An LDAP session handle
    pagesize    (IN) Page size requested
    cookie      (IN) Opaque structure used by the server to track its
                     location in the search results.  NULL on the
                     first call.
    value      (OUT) Control value, SHOULD be freed by calling
					 ldap_memfree() when done.
 
    pagedResultsControl ::= SEQUENCE {
            controlType     1.2.840.113556.1.4.319,
            criticality     BOOLEAN DEFAULT FALSE,
            controlValue    searchControlValue }

    searchControlValue ::= SEQUENCE {
            size            INTEGER (0..maxInt),
                                    -- requested page size from client
                                    -- result set size estimate from server
            cookie          OCTET STRING }

   ---------------------------------------------------------------------------*/

int
ldap_create_page_control_value(
	LDAP *ld,
	ber_int_t pagesize,
	struct berval	*cookie,
	struct berval	*value )
{
	BerElement	*ber = NULL;
	ber_tag_t	tag;
	struct berval	null_cookie = { 0, NULL };

	if ( ld == NULL || value == NULL ||
		pagesize < 1 || pagesize > LDAP_MAXINT )
	{
		if ( ld )
			ld->ld_errno = LDAP_PARAM_ERROR;
		return LDAP_PARAM_ERROR;
	}

	assert( LDAP_VALID( ld ) );

	value->bv_val = NULL;
	value->bv_len = 0;
	ld->ld_errno = LDAP_SUCCESS;

	if ( cookie == NULL ) {
		cookie = &null_cookie;
	}

	ber = ldap_alloc_ber_with_options( ld );
	if ( ber == NULL ) {
		ld->ld_errno = LDAP_NO_MEMORY;
		return ld->ld_errno;
	}

	tag = ber_printf( ber, "{iO}", pagesize, cookie );
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


/* ---------------------------------------------------------------------------
    ldap_create_page_control

    Create and encode a page control.

    ld          (IN) An LDAP session handle
    pagesize    (IN) Page size requested
    cookie      (IN) Opaque structure used by the server to track its
                     location in the search results.  NULL on the
                     first call.
    value      (OUT) Control value, SHOULD be freed by calling
					 ldap_memfree() when done.
    iscritical  (IN) Criticality
    ctrlp      (OUT) LDAP control, SHOULD be freed by calling
					 ldap_control_free() when done.
 
    pagedResultsControl ::= SEQUENCE {
            controlType     1.2.840.113556.1.4.319,
            criticality     BOOLEAN DEFAULT FALSE,
            controlValue    searchControlValue }

    searchControlValue ::= SEQUENCE {
            size            INTEGER (0..maxInt),
                                    -- requested page size from client
                                    -- result set size estimate from server
            cookie          OCTET STRING }

   ---------------------------------------------------------------------------*/

int
ldap_create_page_control(
	LDAP		*ld,
	ber_int_t	pagesize,
	struct berval	*cookie,
	int		iscritical,
	LDAPControl	**ctrlp )
{
	struct berval	value;

	if ( ctrlp == NULL ) {
		ld->ld_errno = LDAP_PARAM_ERROR;
		return ld->ld_errno;
	}

	ld->ld_errno = ldap_create_page_control_value( ld,
		pagesize, cookie, &value );
	if ( ld->ld_errno == LDAP_SUCCESS ) {
		ld->ld_errno = ldap_control_create( LDAP_CONTROL_PAGEDRESULTS,
			iscritical, &value, 0, ctrlp );
		if ( ld->ld_errno != LDAP_SUCCESS ) {
			LDAP_FREE( value.bv_val );
		}
	}

	return ld->ld_errno;
}


/* ---------------------------------------------------------------------------
    ldap_parse_pageresponse_control

    Decode a page control.

    ld          (IN) An LDAP session handle
    ctrl        (IN) The page response control
    count      (OUT) The number of entries in the page.
    cookie     (OUT) Opaque cookie.  Use ldap_memfree() to
                     free the bv_val member of this structure.

   ---------------------------------------------------------------------------*/

int
ldap_parse_pageresponse_control(
	LDAP *ld,
	LDAPControl *ctrl,
	ber_int_t *countp,
	struct berval *cookie )
{
	BerElement *ber;
	ber_tag_t tag;
	ber_int_t count;

	if ( ld == NULL || ctrl == NULL || cookie == NULL ) {
		if ( ld )
			ld->ld_errno = LDAP_PARAM_ERROR;
		return LDAP_PARAM_ERROR;
	}

	/* Create a BerElement from the berval returned in the control. */
	ber = ber_init( &ctrl->ldctl_value );

	if ( ber == NULL ) {
		ld->ld_errno = LDAP_NO_MEMORY;
		return ld->ld_errno;
	}

	/* Extract the count and cookie from the control. */
	tag = ber_scanf( ber, "{io}", &count, cookie );
        ber_free( ber, 1 );

	if ( tag == LBER_ERROR ) {
		ld->ld_errno = LDAP_DECODING_ERROR;
	} else {
		ld->ld_errno = LDAP_SUCCESS;

		if ( countp != NULL ) {
			*countp = (unsigned long)count;
		}
	}

	return ld->ld_errno;
}

/* ---------------------------------------------------------------------------
    ldap_parse_page_control

    Decode a page control.

    ld          (IN) An LDAP session handle
    ctrls       (IN) Response controls
    count      (OUT) The number of entries in the page.
    cookie     (OUT) Opaque cookie.  Use ldap_memfree() to
                     free the bv_val member of this structure.

   ---------------------------------------------------------------------------*/

int
ldap_parse_page_control(
	LDAP		*ld,
	LDAPControl	**ctrls,
	ber_int_t *countp,
	struct berval	**cookiep )
{
	LDAPControl *c;
	struct berval	cookie;

	if ( cookiep == NULL ) {
		ld->ld_errno = LDAP_PARAM_ERROR;
		return ld->ld_errno;
	}

	if ( ctrls == NULL ) {
		ld->ld_errno =  LDAP_CONTROL_NOT_FOUND;
		return ld->ld_errno;
	}

	c = ldap_control_find( LDAP_CONTROL_PAGEDRESULTS, ctrls, NULL );
	if ( c == NULL ) {
		/* No page control was found. */
		ld->ld_errno = LDAP_CONTROL_NOT_FOUND;
		return ld->ld_errno;
	}

	ld->ld_errno = ldap_parse_pageresponse_control( ld, c, countp, &cookie );
	if ( ld->ld_errno == LDAP_SUCCESS ) {
		*cookiep = LDAP_MALLOC( sizeof( struct berval ) );
		if ( *cookiep == NULL ) {
			ld->ld_errno = LDAP_NO_MEMORY;
		} else {
			**cookiep = cookie;
		}
	}

	return ld->ld_errno;
}

