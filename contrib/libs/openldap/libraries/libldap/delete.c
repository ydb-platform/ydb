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

#include <ac/socket.h>
#include <ac/string.h>
#include <ac/time.h>

#include "ldap-int.h"

/*
 * A delete request looks like this:
 *	DelRequest ::= DistinguishedName,
 */

BerElement *
ldap_build_delete_req(
	LDAP *ld,
	LDAP_CONST char *dn,
	LDAPControl **sctrls,
	LDAPControl **cctrls,
	int	*msgidp )
{
	BerElement	*ber;
	int rc;

	/* create a message to send */
	if ( (ber = ldap_alloc_ber_with_options( ld )) == NULL ) {
		return( NULL );
	}

	LDAP_NEXT_MSGID( ld, *msgidp );
	rc = ber_printf( ber, "{its", /* '}' */
		*msgidp, LDAP_REQ_DELETE, dn );
	if ( rc == -1 )
	{
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

/*
 * ldap_delete_ext - initiate an ldap extended delete operation. Parameters:
 *
 *	ld		LDAP descriptor
 *	dn		DN of the object to delete
 *	sctrls	Server Controls
 *	cctrls	Client Controls
 *	msgidp	Message Id Pointer
 *
 * Example:
 *	rc = ldap_delete( ld, dn, sctrls, cctrls, msgidp );
 */
int
ldap_delete_ext(
	LDAP *ld,
	LDAP_CONST char* dn,
	LDAPControl **sctrls,
	LDAPControl **cctrls,
	int *msgidp )
{
	int rc;
	BerElement	*ber;
	ber_int_t	id;

	Debug0( LDAP_DEBUG_TRACE, "ldap_delete_ext\n" );

	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );
	assert( dn != NULL );
	assert( msgidp != NULL );

	/* check client controls */
	rc = ldap_int_client_controls( ld, cctrls );
	if( rc != LDAP_SUCCESS ) return rc;

	ber = ldap_build_delete_req( ld, dn, sctrls, cctrls, &id );
	if( !ber )
		return ld->ld_errno;

	/* send the message */
	*msgidp = ldap_send_initial_request( ld, LDAP_REQ_DELETE, dn, ber, id );

	if(*msgidp < 0)
		return ld->ld_errno;

	return LDAP_SUCCESS;
}

int
ldap_delete_ext_s(
	LDAP *ld,
	LDAP_CONST char *dn,
	LDAPControl **sctrls,
	LDAPControl **cctrls )
{
	int	msgid;
	int rc;
	LDAPMessage	*res;

	rc = ldap_delete_ext( ld, dn, sctrls, cctrls, &msgid );
	
	if( rc != LDAP_SUCCESS )
		return( ld->ld_errno );

	if ( ldap_result( ld, msgid, LDAP_MSG_ALL, (struct timeval *) NULL, &res ) == -1 || !res )
		return( ld->ld_errno );

	return( ldap_result2error( ld, res, 1 ) );
}
/*
 * ldap_delete - initiate an ldap (and X.500) delete operation. Parameters:
 *
 *	ld		LDAP descriptor
 *	dn		DN of the object to delete
 *
 * Example:
 *	msgid = ldap_delete( ld, dn );
 */
int
ldap_delete( LDAP *ld, LDAP_CONST char *dn )
{
	int msgid;

	/*
	 * A delete request looks like this:
	 *	DelRequest ::= DistinguishedName,
	 */

	Debug0( LDAP_DEBUG_TRACE, "ldap_delete\n" );

	return ldap_delete_ext( ld, dn, NULL, NULL, &msgid ) == LDAP_SUCCESS
		? msgid : -1 ;
}


int
ldap_delete_s( LDAP *ld, LDAP_CONST char *dn )
{
	return ldap_delete_ext_s( ld, dn, NULL, NULL );
}
