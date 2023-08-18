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
#include "ldap_log.h"

/*
 * ldap_search_ext - initiate an ldap search operation.
 *
 * Parameters:
 *
 *	ld		LDAP descriptor
 *	base		DN of the base object
 *	scope		the search scope - one of
 *				LDAP_SCOPE_BASE (baseObject),
 *			    LDAP_SCOPE_ONELEVEL (oneLevel),
 *				LDAP_SCOPE_SUBTREE (subtree), or
 *				LDAP_SCOPE_SUBORDINATE (children) -- OpenLDAP extension
 *	filter		a string containing the search filter
 *			(e.g., "(|(cn=bob)(sn=bob))")
 *	attrs		list of attribute types to return for matches
 *	attrsonly	1 => attributes only 0 => attributes and values
 *
 * Example:
 *	char	*attrs[] = { "mail", "title", 0 };
 *	ldap_search_ext( ld, "dc=example,dc=com", LDAP_SCOPE_SUBTREE, "cn~=bob",
 *	    attrs, attrsonly, sctrls, ctrls, timeout, sizelimit,
 *		&msgid );
 */
int
ldap_search_ext(
	LDAP *ld,
	LDAP_CONST char *base,
	int scope,
	LDAP_CONST char *filter,
	char **attrs,
	int attrsonly,
	LDAPControl **sctrls,
	LDAPControl **cctrls,
	struct timeval *timeout,
	int sizelimit,
	int *msgidp )
{
	return ldap_pvt_search( ld, base, scope, filter, attrs,
		attrsonly, sctrls, cctrls, timeout, sizelimit, -1, msgidp );
}

int
ldap_pvt_search(
	LDAP *ld,
	LDAP_CONST char *base,
	int scope,
	LDAP_CONST char *filter,
	char **attrs,
	int attrsonly,
	LDAPControl **sctrls,
	LDAPControl **cctrls,
	struct timeval *timeout,
	int sizelimit,
	int deref,
	int *msgidp )
{
	int rc;
	BerElement	*ber;
	int timelimit;
	ber_int_t id;

	Debug0( LDAP_DEBUG_TRACE, "ldap_search_ext\n" );

	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );

	/* check client controls */
	rc = ldap_int_client_controls( ld, cctrls );
	if( rc != LDAP_SUCCESS ) return rc;

	/*
	 * if timeout is provided, both tv_sec and tv_usec must
	 * not be zero
	 */
	if( timeout != NULL ) {
		if( timeout->tv_sec == 0 && timeout->tv_usec == 0 ) {
			return LDAP_PARAM_ERROR;
		}

		/* timelimit must be non-zero if timeout is provided */
		timelimit = timeout->tv_sec != 0 ? timeout->tv_sec : 1;

	} else {
		/* no timeout, no timelimit */
		timelimit = -1;
	}

	ber = ldap_build_search_req( ld, base, scope, filter, attrs,
	    attrsonly, sctrls, cctrls, timelimit, sizelimit, deref, &id ); 

	if ( ber == NULL ) {
		return ld->ld_errno;
	}


	/* send the message */
	*msgidp = ldap_send_initial_request( ld, LDAP_REQ_SEARCH, base, ber, id );

	if( *msgidp < 0 )
		return ld->ld_errno;

	return LDAP_SUCCESS;
}

int
ldap_search_ext_s(
	LDAP *ld,
	LDAP_CONST char *base,
	int scope,
	LDAP_CONST char *filter,
	char **attrs,
	int attrsonly,
	LDAPControl **sctrls,
	LDAPControl **cctrls,
	struct timeval *timeout,
	int sizelimit,
	LDAPMessage **res )
{
	return ldap_pvt_search_s( ld, base, scope, filter, attrs,
		attrsonly, sctrls, cctrls, timeout, sizelimit, -1, res );
}

int
ldap_pvt_search_s(
	LDAP *ld,
	LDAP_CONST char *base,
	int scope,
	LDAP_CONST char *filter,
	char **attrs,
	int attrsonly,
	LDAPControl **sctrls,
	LDAPControl **cctrls,
	struct timeval *timeout,
	int sizelimit,
	int deref,
	LDAPMessage **res )
{
	int rc;
	int	msgid;

    *res = NULL;

	rc = ldap_pvt_search( ld, base, scope, filter, attrs, attrsonly,
		sctrls, cctrls, timeout, sizelimit, deref, &msgid );

	if ( rc != LDAP_SUCCESS ) {
		return( rc );
	}

	rc = ldap_result( ld, msgid, LDAP_MSG_ALL, timeout, res );

	if( rc <= 0 ) {
		/* error(-1) or timeout(0) */
		if ( ld->ld_errno == LDAP_TIMEOUT ) {
			/* cleanup request */
			(void) ldap_abandon( ld, msgid );
			ld->ld_errno = LDAP_TIMEOUT;
		}
		return( ld->ld_errno );
	}

	if( rc == LDAP_RES_SEARCH_REFERENCE || rc == LDAP_RES_INTERMEDIATE ) {
		return( ld->ld_errno );
	}

	return( ldap_result2error( ld, *res, 0 ) );
}

/*
 * ldap_search - initiate an ldap search operation.
 *
 * Parameters:
 *
 *	ld		LDAP descriptor
 *	base		DN of the base object
 *	scope		the search scope - one of
 *				LDAP_SCOPE_BASE (baseObject),
 *			    LDAP_SCOPE_ONELEVEL (oneLevel),
 *				LDAP_SCOPE_SUBTREE (subtree), or
 *				LDAP_SCOPE_SUBORDINATE (children) -- OpenLDAP extension
 *	filter		a string containing the search filter
 *			(e.g., "(|(cn=bob)(sn=bob))")
 *	attrs		list of attribute types to return for matches
 *	attrsonly	1 => attributes only 0 => attributes and values
 *
 * Example:
 *	char	*attrs[] = { "mail", "title", 0 };
 *	msgid = ldap_search( ld, "dc=example,dc=com", LDAP_SCOPE_SUBTREE, "cn~=bob",
 *	    attrs, attrsonly );
 */
int
ldap_search(
	LDAP *ld, LDAP_CONST char *base, int scope, LDAP_CONST char *filter,
	char **attrs, int attrsonly )
{
	BerElement	*ber;
	ber_int_t	id;

	Debug0( LDAP_DEBUG_TRACE, "ldap_search\n" );

	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );

	ber = ldap_build_search_req( ld, base, scope, filter, attrs,
	    attrsonly, NULL, NULL, -1, -1, -1, &id ); 

	if ( ber == NULL ) {
		return( -1 );
	}


	/* send the message */
	return ( ldap_send_initial_request( ld, LDAP_REQ_SEARCH, base, ber, id ));
}


BerElement *
ldap_build_search_req(
	LDAP *ld,
	LDAP_CONST char *base,
	ber_int_t scope,
	LDAP_CONST char *filter,
	char **attrs,
	ber_int_t attrsonly,
	LDAPControl **sctrls,
	LDAPControl **cctrls,
	ber_int_t timelimit,
	ber_int_t sizelimit,
	ber_int_t deref,
	ber_int_t *idp)
{
	BerElement	*ber;
	int		err;

	/*
	 * Create the search request.  It looks like this:
	 *	SearchRequest := [APPLICATION 3] SEQUENCE {
	 *		baseObject	DistinguishedName,
	 *		scope		ENUMERATED {
	 *			baseObject	(0),
	 *			singleLevel	(1),
	 *			wholeSubtree	(2)
	 *		},
	 *		derefAliases	ENUMERATED {
	 *			neverDerefaliases	(0),
	 *			derefInSearching	(1),
	 *			derefFindingBaseObj	(2),
	 *			alwaysDerefAliases	(3)
	 *		},
	 *		sizelimit	INTEGER (0 .. 65535),
	 *		timelimit	INTEGER (0 .. 65535),
	 *		attrsOnly	BOOLEAN,
	 *		filter		Filter,
	 *		attributes	SEQUENCE OF AttributeType
	 *	}
	 * wrapped in an ldap message.
	 */

	/* create a message to send */
	if ( (ber = ldap_alloc_ber_with_options( ld )) == NULL ) {
		return( NULL );
	}

	if ( base == NULL ) {
		/* no base provided, use session default base */
		base = ld->ld_options.ldo_defbase;

		if ( base == NULL ) {
			/* no session default base, use top */
			base = "";
		}
	}

	LDAP_NEXT_MSGID( ld, *idp );
#ifdef LDAP_CONNECTIONLESS
	if ( LDAP_IS_UDP(ld) ) {
		struct sockaddr_storage sa = {0};
		/* dummy, filled with ldo_peer in request.c */
	    err = ber_write( ber, (char *) &sa, sizeof( sa ), 0 );
	}
	if ( LDAP_IS_UDP(ld) && ld->ld_options.ldo_version == LDAP_VERSION2) {
	    char *dn = ld->ld_options.ldo_cldapdn;
	    if (!dn) dn = "";
	    err = ber_printf( ber, "{ist{seeiib", *idp, dn,
		LDAP_REQ_SEARCH, base, (ber_int_t) scope,
		(deref < 0) ? ld->ld_deref : deref,
		(sizelimit < 0) ? ld->ld_sizelimit : sizelimit,
		(timelimit < 0) ? ld->ld_timelimit : timelimit,
		attrsonly );
	} else
#endif
	{
	    err = ber_printf( ber, "{it{seeiib", *idp,
		LDAP_REQ_SEARCH, base, (ber_int_t) scope,
		(deref < 0) ? ld->ld_deref : deref,
		(sizelimit < 0) ? ld->ld_sizelimit : sizelimit,
		(timelimit < 0) ? ld->ld_timelimit : timelimit,
		attrsonly );
	}

	if ( err == -1 ) {
		ld->ld_errno = LDAP_ENCODING_ERROR;
		ber_free( ber, 1 );
		return( NULL );
	}

	if( filter == NULL ) {
		filter = "(objectclass=*)";
	}

	err = ldap_pvt_put_filter( ber, filter );

	if ( err  == -1 ) {
		ld->ld_errno = LDAP_FILTER_ERROR;
		ber_free( ber, 1 );
		return( NULL );
	}

#ifdef LDAP_DEBUG
	if ( ldap_debug & LDAP_DEBUG_ARGS ) {
		char	buf[ BUFSIZ ], *ptr = " *";

		if ( attrs != NULL ) {
			int	i, len, rest = sizeof( buf );

			for ( i = 0; attrs[ i ] != NULL && rest > 0; i++ ) {
				ptr = &buf[ sizeof( buf ) - rest ];
				len = snprintf( ptr, rest, " %s", attrs[ i ] );
				rest -= (len >= 0 ? len : (int) sizeof( buf ));
			}

			if ( rest <= 0 ) {
				AC_MEMCPY( &buf[ sizeof( buf ) - STRLENOF( "...(truncated)" ) - 1 ],
					"...(truncated)", STRLENOF( "...(truncated)" ) + 1 );
			} 
			ptr = buf;
		}

		Debug1( LDAP_DEBUG_ARGS, "ldap_build_search_req ATTRS:%s\n", ptr );
	}
#endif /* LDAP_DEBUG */

	if ( ber_printf( ber, /*{*/ "{v}N}", attrs ) == -1 ) {
		ld->ld_errno = LDAP_ENCODING_ERROR;
		ber_free( ber, 1 );
		return( NULL );
	}

	/* Put Server Controls */
	if( ldap_int_put_controls( ld, sctrls, ber ) != LDAP_SUCCESS ) {
		ber_free( ber, 1 );
		return( NULL );
	}

	if ( ber_printf( ber, /*{*/ "N}" ) == -1 ) {
		ld->ld_errno = LDAP_ENCODING_ERROR;
		ber_free( ber, 1 );
		return( NULL );
	}

	return( ber );
}

int
ldap_search_st(
	LDAP *ld, LDAP_CONST char *base, int scope,
	LDAP_CONST char *filter, char **attrs,
	int attrsonly, struct timeval *timeout, LDAPMessage **res )
{
	int	msgid;

    *res = NULL;

	if ( (msgid = ldap_search( ld, base, scope, filter, attrs, attrsonly ))
	    == -1 )
		return( ld->ld_errno );

	if ( ldap_result( ld, msgid, LDAP_MSG_ALL, timeout, res ) == -1 || !*res )
		return( ld->ld_errno );

	if ( ld->ld_errno == LDAP_TIMEOUT ) {
		(void) ldap_abandon( ld, msgid );
		ld->ld_errno = LDAP_TIMEOUT;
		return( ld->ld_errno );
	}

	return( ldap_result2error( ld, *res, 0 ) );
}

int
ldap_search_s(
	LDAP *ld,
	LDAP_CONST char *base,
	int scope,
	LDAP_CONST char *filter,
	char **attrs,
	int attrsonly,
	LDAPMessage **res )
{
	int	msgid;

    *res = NULL;

	if ( (msgid = ldap_search( ld, base, scope, filter, attrs, attrsonly ))
	    == -1 )
		return( ld->ld_errno );

	if ( ldap_result( ld, msgid, LDAP_MSG_ALL, (struct timeval *) NULL, res ) == -1 || !*res )
		return( ld->ld_errno );

	return( ldap_result2error( ld, *res, 0 ) );
}

static char escape[128] = {
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,

	0, 0, 0, 0, 0, 0, 0, 0,
	1, 1, 1, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,

	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 1, 0, 0, 0,

	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 1
};
#define	NEEDFLTESCAPE(c)	((c) & 0x80 || escape[ (unsigned)(c) ])

/*
 * compute the length of the escaped value
 */
ber_len_t
ldap_bv2escaped_filter_value_len( struct berval *in )
{
	ber_len_t	i, l;

	assert( in != NULL );

	if ( in->bv_len == 0 ) {
		return 0;
	}

	for( l = 0, i = 0; i < in->bv_len; l++, i++ ) {
		char c = in->bv_val[ i ];
		if ( NEEDFLTESCAPE( c ) ) {
			l += 2;
		}
	}

	return l;
}

int
ldap_bv2escaped_filter_value( struct berval *in, struct berval *out )
{
	return ldap_bv2escaped_filter_value_x( in, out, 0, NULL );
}

int
ldap_bv2escaped_filter_value_x( struct berval *in, struct berval *out, int inplace, void *ctx )
{
	ber_len_t	i, l;

	assert( in != NULL );
	assert( out != NULL );

	BER_BVZERO( out );

	if ( in->bv_len == 0 ) {
		return 0;
	}

	/* assume we'll escape everything */
	l = ldap_bv2escaped_filter_value_len( in );
	if ( l == in->bv_len ) {
		if ( inplace ) {
			*out = *in;
		} else {
			ber_dupbv( out, in );
		}
		return 0;
	}
	out->bv_val = LDAP_MALLOCX( l + 1, ctx );
	if ( out->bv_val == NULL ) {
		return -1;
	}

	for ( i = 0; i < in->bv_len; i++ ) {
		char c = in->bv_val[ i ];
		if ( NEEDFLTESCAPE( c ) ) {
			assert( out->bv_len < l - 2 );
			out->bv_val[out->bv_len++] = '\\';
			out->bv_val[out->bv_len++] = "0123456789ABCDEF"[0x0f & (c>>4)];
			out->bv_val[out->bv_len++] = "0123456789ABCDEF"[0x0f & c];

		} else {
			assert( out->bv_len < l );
			out->bv_val[out->bv_len++] = c;
		}
	}

	out->bv_val[out->bv_len] = '\0';

	return 0;
}

