/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 1998-2024 The OpenLDAP Foundation.
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

/* A modify request/response looks like this:
 *        ModifyRequest ::= [APPLICATION 6] SEQUENCE {              
 *             object          LDAPDN,
 *             changes         SEQUENCE OF change SEQUENCE {
 *                  operation       ENUMERATED {      
 *                       add     (0),                
 *                       delete  (1),                 
 *                       replace (2),
 *                       ...  },
 *                  modification    PartialAttribute } }                  
 *
 *        PartialAttribute ::= SEQUENCE {
 *             type       AttributeDescription,
 *             vals       SET OF value AttributeValue }
 *
 *        AttributeDescription ::= LDAPString           
 *              -- Constrained to <attributedescription> [RFC4512]
 *                                      
 *        AttributeValue ::= OCTET STRING
 *		
 *        ModifyResponse ::= [APPLICATION 7] LDAPResult
 *
 * (Source: RFC 4511)
 */

BerElement *
ldap_build_modify_req(
	LDAP *ld,
	LDAP_CONST char *dn,
	LDAPMod **mods,
	LDAPControl **sctrls,
	LDAPControl **cctrls,
	ber_int_t *msgidp )
{
	BerElement	*ber;
	int		i, rc;

	/* create a message to send */
	if ( (ber = ldap_alloc_ber_with_options( ld )) == NULL ) {
		return( NULL );
	}

	LDAP_NEXT_MSGID( ld, *msgidp );
	rc = ber_printf( ber, "{it{s{" /*}}}*/, *msgidp, LDAP_REQ_MODIFY, dn );
	if ( rc == -1 ) {
		ld->ld_errno = LDAP_ENCODING_ERROR;
		ber_free( ber, 1 );
		return( NULL );
	}

	/* allow mods to be NULL ("touch") */
	if ( mods ) {
		/* for each modification to be performed... */
		for ( i = 0; mods[i] != NULL; i++ ) {
			if (( mods[i]->mod_op & LDAP_MOD_BVALUES) != 0 ) {
				rc = ber_printf( ber, "{e{s[V]N}N}",
				    (ber_int_t) ( mods[i]->mod_op & ~LDAP_MOD_BVALUES ),
				    mods[i]->mod_type, mods[i]->mod_bvalues );
			} else {
				rc = ber_printf( ber, "{e{s[v]N}N}",
					(ber_int_t) mods[i]->mod_op,
				    mods[i]->mod_type, mods[i]->mod_values );
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
 * ldap_modify_ext - initiate an ldap extended modify operation.
 *
 * Parameters:
 *
 *	ld		LDAP descriptor
 *	dn		DN of the object to modify
 *	mods		List of modifications to make.  This is null-terminated
 *			array of struct ldapmod's, specifying the modifications
 *			to perform.
 *	sctrls	Server Controls
 *	cctrls	Client Controls
 *	msgidp	Message ID pointer
 *
 * Example:
 *	LDAPMod	*mods[] = {
 *			{ LDAP_MOD_ADD, "cn", { "babs jensen", "babs", 0 } },
 *			{ LDAP_MOD_REPLACE, "sn", { "babs jensen", "babs", 0 } },
 *			{ LDAP_MOD_DELETE, "ou", 0 },
 *			{ LDAP_MOD_INCREMENT, "uidNumber, { "1", 0 } }
 *			0
 *		}
 *	rc=  ldap_modify_ext( ld, dn, mods, sctrls, cctrls, &msgid );
 */
int
ldap_modify_ext( LDAP *ld,
	LDAP_CONST char *dn,
	LDAPMod **mods,
	LDAPControl **sctrls,
	LDAPControl **cctrls,
	int *msgidp )
{
	BerElement	*ber;
	int		rc;
	ber_int_t	id;

	Debug0( LDAP_DEBUG_TRACE, "ldap_modify_ext\n" );

	/* check client controls */
	rc = ldap_int_client_controls( ld, cctrls );
	if( rc != LDAP_SUCCESS ) return rc;

	ber = ldap_build_modify_req( ld, dn, mods, sctrls, cctrls, &id );
	if( !ber )
		return ld->ld_errno;

	/* send the message */
	*msgidp = ldap_send_initial_request( ld, LDAP_REQ_MODIFY, dn, ber, id );
	return( *msgidp < 0 ? ld->ld_errno : LDAP_SUCCESS );
}

/*
 * ldap_modify - initiate an ldap modify operation.
 *
 * Parameters:
 *
 *	ld		LDAP descriptor
 *	dn		DN of the object to modify
 *	mods		List of modifications to make.  This is null-terminated
 *			array of struct ldapmod's, specifying the modifications
 *			to perform.
 *
 * Example:
 *	LDAPMod	*mods[] = {
 *			{ LDAP_MOD_ADD, "cn", { "babs jensen", "babs", 0 } },
 *			{ LDAP_MOD_REPLACE, "sn", { "babs jensen", "babs", 0 } },
 *			{ LDAP_MOD_DELETE, "ou", 0 },
 *			{ LDAP_MOD_INCREMENT, "uidNumber, { "1", 0 } }
 *			0
 *		}
 *	msgid = ldap_modify( ld, dn, mods );
 */
int
ldap_modify( LDAP *ld, LDAP_CONST char *dn, LDAPMod **mods )
{
	int rc, msgid;

	Debug0( LDAP_DEBUG_TRACE, "ldap_modify\n" );

	rc = ldap_modify_ext( ld, dn, mods, NULL, NULL, &msgid );

	if ( rc != LDAP_SUCCESS )
		return -1;

	return msgid;
}

int
ldap_modify_ext_s( LDAP *ld, LDAP_CONST char *dn,
	LDAPMod **mods, LDAPControl **sctrl, LDAPControl **cctrl )
{
	int		rc;
	int		msgid;
	LDAPMessage	*res;

	rc = ldap_modify_ext( ld, dn, mods, sctrl, cctrl, &msgid );

	if ( rc != LDAP_SUCCESS )
		return( rc );

	if ( ldap_result( ld, msgid, LDAP_MSG_ALL, (struct timeval *) NULL, &res ) == -1 || !res )
		return( ld->ld_errno );

	return( ldap_result2error( ld, res, 1 ) );
}

int
ldap_modify_s( LDAP *ld, LDAP_CONST char *dn, LDAPMod **mods )
{
	return ldap_modify_ext_s( ld, dn, mods, NULL, NULL );
}

