/* add.c */
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

/* An LDAP Add Request/Response looks like this:
 *        AddRequest ::= [APPLICATION 8] SEQUENCE {
 *            entry           LDAPDN,
 *            attributes      AttributeList }
 *
 *        AttributeList ::= SEQUENCE OF attribute Attribute
 *
 *        Attribute ::= PartialAttribute(WITH COMPONENTS {
 *             ...,
 *             vals (SIZE(1..MAX))})
 *
 *        PartialAttribute ::= SEQUENCE {
 *             type       AttributeDescription,
 *             vals       SET OF value AttributeValue }
 *
 *        AttributeDescription ::= LDAPString           
 *             -- Constrained to <attributedescription> [RFC4512]
 *                                      
 *        AttributeValue ::= OCTET STRING
 *        
 *        AddResponse ::= [APPLICATION 9] LDAPResult 
 * (Source: RFC 4511)
 */

/*
 * ldap_add - initiate an ldap add operation.  Parameters:
 *
 *	ld		LDAP descriptor
 *	dn		DN of the entry to add
 *	mods		List of attributes for the entry.  This is a null-
 *			terminated array of pointers to LDAPMod structures.
 *			only the type and values in the structures need be
 *			filled in.
 *
 * Example:
 *	LDAPMod	*attrs[] = { 
 *			{ 0, "cn", { "babs jensen", "babs", 0 } },
 *			{ 0, "sn", { "jensen", 0 } },
 *			{ 0, "objectClass", { "person", 0 } },
 *			0
 *		}
 *	msgid = ldap_add( ld, dn, attrs );
 */
int
ldap_add( LDAP *ld, LDAP_CONST char *dn, LDAPMod **attrs )
{
	int rc;
	int msgid;

	rc = ldap_add_ext( ld, dn, attrs, NULL, NULL, &msgid );

	if ( rc != LDAP_SUCCESS )
		return -1;

	return msgid;
}


BerElement *
ldap_build_add_req(
	LDAP *ld,
	const char *dn,
	LDAPMod **attrs,
	LDAPControl **sctrls,
	LDAPControl **cctrls,
	ber_int_t	*msgidp )
{
	BerElement *ber;
	int i, rc;

	/* create a message to send */
	if ( (ber = ldap_alloc_ber_with_options( ld )) == NULL ) {
		return( NULL );
	}

	LDAP_NEXT_MSGID(ld, *msgidp);
	rc = ber_printf( ber, "{it{s{", /* '}}}' */
		*msgidp, LDAP_REQ_ADD, dn );

	if ( rc == -1 ) {
		ld->ld_errno = LDAP_ENCODING_ERROR;
		ber_free( ber, 1 );
		return( NULL );
	}

	/* allow attrs to be NULL ("touch"; should fail...) */
	if ( attrs ) {
		/* for each attribute in the entry... */
		for ( i = 0; attrs[i] != NULL; i++ ) {
			if ( ( attrs[i]->mod_op & LDAP_MOD_BVALUES) != 0 ) {
				int j;

				if ( attrs[i]->mod_bvalues == NULL ) {
					ld->ld_errno = LDAP_PARAM_ERROR;
					ber_free( ber, 1 );
					return( NULL );
				}

				for ( j = 0; attrs[i]->mod_bvalues[ j ] != NULL; j++ ) {
					if ( attrs[i]->mod_bvalues[ j ]->bv_val == NULL ) {
						ld->ld_errno = LDAP_PARAM_ERROR;
						ber_free( ber, 1 );
						return( NULL );
					}
				}

				rc = ber_printf( ber, "{s[V]N}", attrs[i]->mod_type,
				    attrs[i]->mod_bvalues );

			} else {
				if ( attrs[i]->mod_values == NULL ) {
					ld->ld_errno = LDAP_PARAM_ERROR;
					ber_free( ber, 1 );
					return( NULL );
				}

				rc = ber_printf( ber, "{s[v]N}", attrs[i]->mod_type,
				    attrs[i]->mod_values );
			}
			if ( rc == -1 ) {
				ld->ld_errno = LDAP_ENCODING_ERROR;
				ber_free( ber, 1 );
				return( NULL );
			}
		}
	}

	if ( ber_printf( ber, /*{{*/ "N}N}" ) == -1 ) {
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
 * ldap_add_ext - initiate an ldap extended add operation.  Parameters:
 *
 *	ld		LDAP descriptor
 *	dn		DN of the entry to add
 *	mods		List of attributes for the entry.  This is a null-
 *			terminated array of pointers to LDAPMod structures.
 *			only the type and values in the structures need be
 *			filled in.
 *	sctrl	Server Controls
 *	cctrl	Client Controls
 *	msgidp	Message ID pointer
 *
 * Example:
 *	LDAPMod	*attrs[] = {
 *			{ 0, "cn", { "babs jensen", "babs", 0 } },
 *			{ 0, "sn", { "jensen", 0 } },
 *			{ 0, "objectClass", { "person", 0 } },
 *			0
 *		}
 *	rc = ldap_add_ext( ld, dn, attrs, NULL, NULL, &msgid );
 */
int
ldap_add_ext(
	LDAP *ld,
	LDAP_CONST char *dn,
	LDAPMod **attrs,
	LDAPControl **sctrls,
	LDAPControl **cctrls,
	int	*msgidp )
{
	BerElement	*ber;
	int		rc;
	ber_int_t	id;

	Debug0( LDAP_DEBUG_TRACE, "ldap_add_ext\n" );
	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );
	assert( dn != NULL );
	assert( msgidp != NULL );

	/* check client controls */
	rc = ldap_int_client_controls( ld, cctrls );
	if( rc != LDAP_SUCCESS ) return rc;

	ber = ldap_build_add_req( ld, dn, attrs, sctrls, cctrls, &id );
	if( !ber )
		return ld->ld_errno;

	/* send the message */
	*msgidp = ldap_send_initial_request( ld, LDAP_REQ_ADD, dn, ber, id );

	if(*msgidp < 0)
		return ld->ld_errno;

	return LDAP_SUCCESS;
}

int
ldap_add_ext_s(
	LDAP *ld,
	LDAP_CONST char *dn,
	LDAPMod **attrs,
	LDAPControl **sctrls,
	LDAPControl **cctrls )
{
	int		msgid, rc;
	LDAPMessage	*res;

	rc = ldap_add_ext( ld, dn, attrs, sctrls, cctrls, &msgid );

	if ( rc != LDAP_SUCCESS )
		return( rc );

	if ( ldap_result( ld, msgid, LDAP_MSG_ALL, (struct timeval *) NULL, &res ) == -1 || !res )
		return( ld->ld_errno );

	return( ldap_result2error( ld, res, 1 ) );
}

int
ldap_add_s( LDAP *ld, LDAP_CONST char *dn, LDAPMod **attrs )
{
	return ldap_add_ext_s( ld, dn, attrs, NULL, NULL );
}

