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
/* ACKNOWLEDGEMENTS:
 * This program was originally developed by Kurt D. Zeilenga for inclusion
 * in OpenLDAP Software.
 */

/*
 * LDAPv3 Cancel Operation Request
 */

#include "portable.h"

#include <stdio.h>
#include <ac/stdlib.h>

#include <ac/socket.h>
#include <ac/string.h>
#include <ac/time.h>

#include "ldap-int.h"
#include "ldap_log.h"

int
ldap_cancel(
	LDAP		*ld,
	int		cancelid,
	LDAPControl	**sctrls,
	LDAPControl	**cctrls,
	int		*msgidp )
{
	BerElement *cancelidber = NULL;
	struct berval cancelidvalp = { 0, NULL };
	int rc;

	cancelidber = ber_alloc_t( LBER_USE_DER );
	ber_printf( cancelidber, "{i}", cancelid );
	ber_flatten2( cancelidber, &cancelidvalp, 0 );
	rc = ldap_extended_operation( ld, LDAP_EXOP_CANCEL,
		&cancelidvalp, sctrls, cctrls, msgidp );
	ber_free( cancelidber, 1 );
	return rc;
}

int
ldap_cancel_s(
	LDAP		*ld,
	int		cancelid,
	LDAPControl	**sctrls,
	LDAPControl	**cctrls )
{
	BerElement *cancelidber = NULL;
	struct berval cancelidvalp = { 0, NULL };
	int rc;

	cancelidber = ber_alloc_t( LBER_USE_DER );
	ber_printf( cancelidber, "{i}", cancelid );
	ber_flatten2( cancelidber, &cancelidvalp, 0 );
	rc = ldap_extended_operation_s( ld, LDAP_EXOP_CANCEL,
		&cancelidvalp, sctrls, cctrls, NULL, NULL );
	ber_free( cancelidber, 1 );
	return rc;
}

