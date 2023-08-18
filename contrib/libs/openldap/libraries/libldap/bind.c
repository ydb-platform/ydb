/* bind.c */
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
 *	BindRequest ::= SEQUENCE {
 *		version		INTEGER,
 *		name		DistinguishedName,	 -- who
 *		authentication	CHOICE {
 *			simple		[0] OCTET STRING -- passwd
 *			krbv42ldap	[1] OCTET STRING -- OBSOLETE
 *			krbv42dsa	[2] OCTET STRING -- OBSOLETE
 *			sasl		[3] SaslCredentials	-- LDAPv3
 *		}
 *	}
 *
 *	BindResponse ::= SEQUENCE {
 *		COMPONENTS OF LDAPResult,
 *		serverSaslCreds		OCTET STRING OPTIONAL -- LDAPv3
 *	}
 *
 * (Source: RFC 2251)
 */

/*
 * ldap_bind - bind to the ldap server (and X.500).  The dn and password
 * of the entry to which to bind are supplied, along with the authentication
 * method to use.  The msgid of the bind request is returned on success,
 * -1 if there's trouble.  ldap_result() should be called to find out the
 * outcome of the bind request.
 *
 * Example:
 *	ldap_bind( ld, "cn=manager, o=university of michigan, c=us", "secret",
 *	    LDAP_AUTH_SIMPLE )
 */

int
ldap_bind( LDAP *ld, LDAP_CONST char *dn, LDAP_CONST char *passwd, int authmethod )
{
	Debug0( LDAP_DEBUG_TRACE, "ldap_bind\n" );

	switch ( authmethod ) {
	case LDAP_AUTH_SIMPLE:
		return( ldap_simple_bind( ld, dn, passwd ) );

	case LDAP_AUTH_SASL:
		/* user must use ldap_sasl_bind */
		/* FALL-THRU */

	default:
		ld->ld_errno = LDAP_AUTH_UNKNOWN;
		return( -1 );
	}
}

/*
 * ldap_bind_s - bind to the ldap server (and X.500).  The dn and password
 * of the entry to which to bind are supplied, along with the authentication
 * method to use.  This routine just calls whichever bind routine is
 * appropriate and returns the result of the bind (e.g. LDAP_SUCCESS or
 * some other error indication).
 *
 * Examples:
 *	ldap_bind_s( ld, "cn=manager, o=university of michigan, c=us",
 *	    "secret", LDAP_AUTH_SIMPLE )
 *	ldap_bind_s( ld, "cn=manager, o=university of michigan, c=us",
 *	    NULL, LDAP_AUTH_KRBV4 )
 */
int
ldap_bind_s(
	LDAP *ld,
	LDAP_CONST char *dn,
	LDAP_CONST char *passwd,
	int authmethod )
{
	Debug0( LDAP_DEBUG_TRACE, "ldap_bind_s\n" );

	switch ( authmethod ) {
	case LDAP_AUTH_SIMPLE:
		return( ldap_simple_bind_s( ld, dn, passwd ) );

	case LDAP_AUTH_SASL:
		/* user must use ldap_sasl_bind */
		/* FALL-THRU */

	default:
		return( ld->ld_errno = LDAP_AUTH_UNKNOWN );
	}
}
