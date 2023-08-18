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
/* Portions Copyright (C) 1999, 2000 Novell, Inc. All Rights Reserved.
 *
 * THIS WORK IS SUBJECT TO U.S. AND INTERNATIONAL COPYRIGHT LAWS AND
 * TREATIES. USE, MODIFICATION, AND REDISTRIBUTION OF THIS WORK IS SUBJECT
 * TO VERSION 2.0.1 OF THE OPENLDAP PUBLIC LICENSE, A COPY OF WHICH IS
 * AVAILABLE AT HTTP://WWW.OPENLDAP.ORG/LICENSE.HTML OR IN THE FILE "LICENSE"
 * IN THE TOP-LEVEL DIRECTORY OF THE DISTRIBUTION. ANY USE OR EXPLOITATION
 * OF THIS WORK OTHER THAN AS AUTHORIZED IN VERSION 2.0.1 OF THE OPENLDAP
 * PUBLIC LICENSE, OR OTHER PRIOR WRITTEN CONSENT FROM NOVELL, COULD SUBJECT
 * THE PERPETRATOR TO CRIMINAL AND CIVIL LIABILITY.
 *---
 * Note: A verbatim copy of version 2.0.1 of the OpenLDAP Public License
 * can be found in the file "build/LICENSE-2.0.1" in this distribution
 * of OpenLDAP Software.
 */

#include "portable.h"

#include <stdio.h>
#include <ac/stdlib.h>
#include <ac/string.h>
#include <ac/time.h>

#include "ldap-int.h"

#define LDAP_VLVBYINDEX_IDENTIFIER     0xa0L
#define LDAP_VLVBYVALUE_IDENTIFIER     0x81L
#define LDAP_VLVCONTEXT_IDENTIFIER     0x04L


/*---
   ldap_create_vlv_control
   
   Create and encode the Virtual List View control.

   ld        (IN)  An LDAP session handle.
   
   vlvinfop  (IN)  The address of an LDAPVLVInfo structure whose contents 
				   are used to construct the value of the control
				   that is created.
   
   value     (OUT) A struct berval that contains the value to be assigned to the ldctl_value member
				   of an LDAPControl structure that contains the 
				   VirtualListViewRequest control.
				   The bv_val member of the berval structure
				   SHOULD be freed when it is no longer in use by
				   calling ldap_memfree().
					  
   
   Ber encoding
   
   VirtualListViewRequest ::= SEQUENCE {
		beforeCount  INTEGER (0 .. maxInt),
		afterCount   INTEGER (0 .. maxInt),
		CHOICE {
				byoffset [0] SEQUENCE, {
				offset        INTEGER (0 .. maxInt),
				contentCount  INTEGER (0 .. maxInt) }
				[1] greaterThanOrEqual assertionValue }
		contextID     OCTET STRING OPTIONAL }
	  
   
   Note:  The first time the VLV control is created, the ldvlv_context
		  field of the LDAPVLVInfo structure should be set to NULL.
		  The context obtained from calling ldap_parse_vlv_control()
		  should be used as the context in the next ldap_create_vlv_control
		  call.

 ---*/

int
ldap_create_vlv_control_value(
	LDAP *ld,
	LDAPVLVInfo *vlvinfop,
	struct berval *value )
{
	ber_tag_t tag;
	BerElement *ber;

	if ( ld == NULL || vlvinfop == NULL || value == NULL ) {
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

	tag = ber_printf( ber, "{ii" /*}*/,
		vlvinfop->ldvlv_before_count,
		vlvinfop->ldvlv_after_count );
	if ( tag == LBER_ERROR ) {
		goto error_return;
	}

	if ( vlvinfop->ldvlv_attrvalue == NULL ) {
		tag = ber_printf( ber, "t{iiN}",
			LDAP_VLVBYINDEX_IDENTIFIER,
			vlvinfop->ldvlv_offset,
			vlvinfop->ldvlv_count );
		if ( tag == LBER_ERROR ) {
			goto error_return;
		}

	} else {
		tag = ber_printf( ber, "tO",
			LDAP_VLVBYVALUE_IDENTIFIER,
			vlvinfop->ldvlv_attrvalue );
		if ( tag == LBER_ERROR ) {
			goto error_return;
		}
	}

	if ( vlvinfop->ldvlv_context ) {
		tag = ber_printf( ber, "tO",
			LDAP_VLVCONTEXT_IDENTIFIER,
			vlvinfop->ldvlv_context );
		if ( tag == LBER_ERROR ) {
			goto error_return;
		}
	}

	tag = ber_printf( ber, /*{*/ "N}" ); 
	if ( tag == LBER_ERROR ) {
		goto error_return;
	}

	if ( ber_flatten2( ber, value, 1 ) == -1 ) {
		ld->ld_errno = LDAP_NO_MEMORY;
	}

	if ( 0 ) {
error_return:;
		ld->ld_errno = LDAP_ENCODING_ERROR;
	}

	if ( ber != NULL ) {
		ber_free( ber, 1 );
	}

	return ld->ld_errno;
}

/*---
   ldap_create_vlv_control
   
   Create and encode the Virtual List View control.

   ld        (IN)  An LDAP session handle.
   
   vlvinfop  (IN)  The address of an LDAPVLVInfo structure whose contents 
				   are used to construct the value of the control
				   that is created.
   
   ctrlp     (OUT) A result parameter that will be assigned the address
				   of an LDAPControl structure that contains the 
				   VirtualListViewRequest control created by this function.
				   The memory occupied by the LDAPControl structure
				   SHOULD be freed when it is no longer in use by
				   calling ldap_control_free().
					  
   
   Ber encoding
   
   VirtualListViewRequest ::= SEQUENCE {
		beforeCount  INTEGER (0 .. maxInt),
		afterCount   INTEGER (0 .. maxInt),
		CHOICE {
				byoffset [0] SEQUENCE, {
				offset        INTEGER (0 .. maxInt),
				contentCount  INTEGER (0 .. maxInt) }
				[1] greaterThanOrEqual assertionValue }
		contextID     OCTET STRING OPTIONAL }
	  
   
   Note:  The first time the VLV control is created, the ldvlv_context
		  field of the LDAPVLVInfo structure should be set to NULL.
		  The context obtained from calling ldap_parse_vlv_control()
		  should be used as the context in the next ldap_create_vlv_control
		  call.

 ---*/

int
ldap_create_vlv_control(
	LDAP *ld,
	LDAPVLVInfo *vlvinfop,
	LDAPControl **ctrlp )
{
	struct berval	value;

	if ( ctrlp == NULL ) {
		ld->ld_errno = LDAP_PARAM_ERROR;
		return ld->ld_errno;
	}

	ld->ld_errno = ldap_create_vlv_control_value( ld, vlvinfop, &value );
	if ( ld->ld_errno == LDAP_SUCCESS ) {

		ld->ld_errno = ldap_control_create( LDAP_CONTROL_VLVREQUEST,
			1, &value, 0, ctrlp );
		if ( ld->ld_errno != LDAP_SUCCESS ) {
			LDAP_FREE( value.bv_val );
		}
	}

	return ld->ld_errno;
}


/*---
   ldap_parse_vlvresponse_control
   
   Decode the Virtual List View control return information.

   ld           (IN)   An LDAP session handle.
   
   ctrl         (IN)   The address of the LDAPControl structure.
   
   target_posp	(OUT)  This result parameter is filled in with the list
					   index of the target entry.  If this parameter is
					   NULL, the target position is not returned.
   
   list_countp  (OUT)  This result parameter is filled in with the server's
					   estimate of the size of the list.  If this parameter
					   is NULL, the size is not returned.
   
   contextp     (OUT)  This result parameter is filled in with the address
					   of a struct berval that contains the server-
					   generated context identifier if one was returned by
					   the server.  If the server did not return a context
					   identifier, this parameter will be set to NULL, even
					   if an error occurred.
					   The returned context SHOULD be used in the next call
					   to create a VLV sort control.  The struct berval
					   returned SHOULD be disposed of by calling ber_bvfree()
					   when it is no longer needed.  If NULL is passed for
					   contextp, the context identifier is not returned.
   
   errcodep     (OUT)  This result parameter is filled in with the VLV
					   result code.  If this parameter is NULL, the result
					   code is not returned.  
   
   
   Ber encoding
   
   VirtualListViewResponse ::= SEQUENCE {
		targetPosition    INTEGER (0 .. maxInt),
		contentCount     INTEGER (0 .. maxInt),
		virtualListViewResult ENUMERATED {
		success (0),
		operationsError (1),
		unwillingToPerform (53),
		insufficientAccessRights (50),
		busy (51),
		timeLimitExceeded (3),
		adminLimitExceeded (11),
		sortControlMissing (60),
		offsetRangeError (61),
		other (80) },
		contextID     OCTET STRING OPTIONAL }
   
---*/

int
ldap_parse_vlvresponse_control(
	LDAP *ld,
	LDAPControl *ctrl,
	ber_int_t *target_posp,
	ber_int_t *list_countp,
	struct berval  **contextp,
	ber_int_t *errcodep )
{
	BerElement  *ber;
	ber_int_t pos, count, err;
	ber_tag_t tag, berTag;
	ber_len_t berLen;

	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );

	if (contextp) {
		*contextp = NULL;	 /* Make sure we return a NULL if error occurs. */
	}

	if (ctrl == NULL) {
		ld->ld_errno = LDAP_PARAM_ERROR;
		return(ld->ld_errno);
	}

	if (strcmp(LDAP_CONTROL_VLVRESPONSE, ctrl->ldctl_oid) != 0) {
		/* Not VLV Response control */
		ld->ld_errno = LDAP_CONTROL_NOT_FOUND;
		return(ld->ld_errno);
	}

	/* Create a BerElement from the berval returned in the control. */
	ber = ber_init(&ctrl->ldctl_value);

	if (ber == NULL) {
		ld->ld_errno = LDAP_NO_MEMORY;
		return(ld->ld_errno);
	}

	/* Extract the data returned in the control. */
	tag = ber_scanf(ber, "{iie" /*}*/, &pos, &count, &err);

	if( tag == LBER_ERROR) {
		ber_free(ber, 1);
		ld->ld_errno = LDAP_DECODING_ERROR;
		return(ld->ld_errno);
	}


	/* Since the context is the last item encoded, if caller doesn't want
	   it returned, don't decode it. */
	if (contextp) {
		if (LDAP_VLVCONTEXT_IDENTIFIER == ber_peek_tag(ber, &berLen)) {
			tag = ber_scanf(ber, "tO", &berTag, contextp);

			if( tag == LBER_ERROR) {
				ber_free(ber, 1);
				ld->ld_errno = LDAP_DECODING_ERROR;
				return(ld->ld_errno);
			}
		}
	}

	ber_free(ber, 1);

	/* Return data to the caller for items that were requested. */
	if (target_posp) *target_posp = pos;
	if (list_countp) *list_countp = count;
	if (errcodep) *errcodep = err;

	ld->ld_errno = LDAP_SUCCESS;
	return(ld->ld_errno);
}
