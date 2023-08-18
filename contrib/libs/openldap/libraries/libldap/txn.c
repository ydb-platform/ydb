/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 2006-2022 The OpenLDAP Foundation.
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
 * LDAPv3 Transactions (draft-zeilenga-ldap-txn)
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
ldap_txn_start(
	LDAP *ld,
	LDAPControl **sctrls,
	LDAPControl **cctrls,
	int *msgidp )
{
	return ldap_extended_operation( ld, LDAP_EXOP_TXN_START,
		NULL, sctrls, cctrls, msgidp );
}

int
ldap_txn_start_s(
	LDAP *ld,
	LDAPControl **sctrls,
	LDAPControl **cctrls,
	struct berval **txnid )
{
	assert( txnid != NULL );

	return ldap_extended_operation_s( ld, LDAP_EXOP_TXN_START,
		NULL, sctrls, cctrls, NULL, txnid );
}

int
ldap_txn_end(
	LDAP *ld,
	int commit,
	struct berval *txnid,
	LDAPControl **sctrls,
	LDAPControl **cctrls,
	int *msgidp )
{
	int rc;
	BerElement *txnber = NULL;
	struct berval txnval;

	assert( txnid != NULL );

	txnber = ber_alloc_t( LBER_USE_DER );

	if( commit ) {
		ber_printf( txnber, "{ON}", txnid );
	} else {
		ber_printf( txnber, "{bON}", commit, txnid );
	}

	ber_flatten2( txnber, &txnval, 0 );

	rc = ldap_extended_operation( ld, LDAP_EXOP_TXN_END,
		&txnval, sctrls, cctrls, msgidp );

	ber_free( txnber, 1 );
	return rc;
}

int
ldap_txn_end_s(
	LDAP *ld,
	int commit,
	struct berval *txnid,
	LDAPControl **sctrls,
	LDAPControl **cctrls,
	int *retidp )
{
	int rc;
	BerElement *txnber = NULL;
	struct berval txnval;
	struct berval *retdata = NULL;

	if ( retidp != NULL ) *retidp = -1;

	txnber = ber_alloc_t( LBER_USE_DER );

	if( commit ) {
		ber_printf( txnber, "{ON}", txnid );
	} else {
		ber_printf( txnber, "{bON}", commit, txnid );
	}

	ber_flatten2( txnber, &txnval, 0 );

	rc = ldap_extended_operation_s( ld, LDAP_EXOP_TXN_END,
		&txnval, sctrls, cctrls, NULL, &retdata );

	ber_free( txnber, 1 );

	/* parse retdata */
	if( retdata != NULL ) {
		BerElement *ber;
		ber_tag_t tag;
		ber_int_t retid;

		if( retidp == NULL ) goto done;

		ber = ber_init( retdata );

		if( ber == NULL ) {
			rc = ld->ld_errno = LDAP_NO_MEMORY;
			goto done;
		}

		tag = ber_scanf( ber, "i", &retid );
		ber_free( ber, 1 );

		if ( tag != LBER_INTEGER ) {
			rc = ld->ld_errno = LDAP_DECODING_ERROR;
			goto done;
		}

		*retidp = (int) retid;

done:
		ber_bvfree( retdata );
	}

	return rc;
}
