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
 * This work was developed by Howard Chu for inclusion in
 * OpenLDAP Software.
 */

#include "portable.h"

#include <stdio.h>
#include <ac/stdlib.h>
#include <ac/string.h>
#include <ac/time.h>

#include "ldap-int.h"

/* Based on draft-ietf-ldapext-c-api-psearch-00 */

/* ---------------------------------------------------------------------------
   ldap_create_persistentsearch_control_value
   
   Create and encode the value of the server-side sort control.
   
   ld          (IN) An LDAP session handle, as obtained from a call to
					ldap_init().

   changetypes (IN) A bit-sensitive field that indicates which kinds of
					changes the client wants to be informed about.  Its
					value should be LDAP_CHANGETYPE_ANY, or any logical-OR
					combination of LDAP_CHANGETYPE_ADD,
					LDAP_CHANGETYPE_DELETE, LDAP_CHANGETYPE_MODIFY, and
					LDAP_CHANGETYPE_MODDN.  This field corresponds to the
					changeType element of the BER-encoded PersistentSearch
					control value itself.

   changesonly (IN) A Boolean field that indicates whether the client
					wishes to only receive searchResultEntry messages for
					entries that have been changed. If non-zero, only
					entries that result from changes are returned; other-
					wise, all of the static entries that match the search
					criteria are returned before the server begins change
					notification.  This field corresponds to the changes-
					Only element of the BER-encoded PersistentSearch con-
					trol value itself.

   return_echg_ctls (IN) A Boolean field that indicates whether the server
					should send back an Entry Change Notification control
					with each searchResultEntry that is returned due to a
					change to an entry.  If non-zero, Entry Change
					Notification controls are requested; if zero, they are
					not.  This field corresponds to the returnECs element
					of the BER-encoded PersistentSearch control value
					itself.
			   
   value      (OUT) Contains the control value; the bv_val member of the berval structure
					SHOULD be freed by calling ldap_memfree() when done.
   
   ---------------------------------------------------------------------------*/

int
ldap_create_persistentsearch_control_value(
	LDAP *ld,
	int changetypes,
	int changesonly,
	int return_echg_ctls,
	struct berval *value )
{
	int		i;
	BerElement	*ber = NULL;
	ber_tag_t	tag;

	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );

	if ( ld == NULL ) return LDAP_PARAM_ERROR;
	if ( value == NULL ) {
		ld->ld_errno = LDAP_PARAM_ERROR;
		return LDAP_PARAM_ERROR;
	}
	if (( changetypes & 0x0f ) != changetypes ) {
		ld->ld_errno = LDAP_PARAM_ERROR;
		return LDAP_PARAM_ERROR;
	}

	value->bv_val = NULL;
	value->bv_len = 0;
	ld->ld_errno = LDAP_SUCCESS;

	ber = ldap_alloc_ber_with_options( ld );
	if ( ber == NULL) {
		ld->ld_errno = LDAP_NO_MEMORY;
		return ld->ld_errno;
	}

	tag = ber_printf( ber, "{ibb}", changetypes, changesonly, return_echg_ctls );
	if ( tag == LBER_ERROR ) {
		goto error_return;
	}

	if ( ber_flatten2( ber, value, 1 ) == -1 ) {
		ld->ld_errno = LDAP_NO_MEMORY;
	}

	if ( 0 ) {
error_return:;
		ld->ld_errno =  LDAP_ENCODING_ERROR;
	}

	if ( ber != NULL ) {
		ber_free( ber, 1 );
	}

	return ld->ld_errno;
}


/* ---------------------------------------------------------------------------
   ldap_create_persistentsearch_control
   
   Create and encode the persistent search control.
   
   ld          (IN) An LDAP session handle, as obtained from a call to
					ldap_init().

   changetypes (IN) A bit-sensitive field that indicates which kinds of
					changes the client wants to be informed about.  Its
					value should be LDAP_CHANGETYPE_ANY, or any logical-OR
					combination of LDAP_CHANGETYPE_ADD,
					LDAP_CHANGETYPE_DELETE, LDAP_CHANGETYPE_MODIFY, and
					LDAP_CHANGETYPE_MODDN.  This field corresponds to the
					changeType element of the BER-encoded PersistentSearch
					control value itself.

   changesonly (IN) A Boolean field that indicates whether the client
					wishes to only receive searchResultEntry messages for
					entries that have been changed. If non-zero, only
					entries that result from changes are returned; other-
					wise, all of the static entries that match the search
					criteria are returned before the server begins change
					notification.  This field corresponds to the changes-
					Only element of the BER-encoded PersistentSearch con-
					trol value itself.

   return_echg_ctls (IN) A Boolean field that indicates whether the server
					should send back an Entry Change Notification control
					with each searchResultEntry that is returned due to a
					change to an entry.  If non-zero, Entry Change
					Notification controls are requested; if zero, they are
					not.  This field corresponds to the returnECs element
					of the BER-encoded PersistentSearch control value
					itself.

   isCritical  (IN) 0 - Indicates the control is not critical to the operation.
					non-zero - The control is critical to the operation.
					 
   ctrlp      (OUT) Returns a pointer to the LDAPControl created.  This control
					SHOULD be freed by calling ldap_control_free() when done.
   
   ---------------------------------------------------------------------------*/

int
ldap_create_persistentsearch_control(
	LDAP *ld,
	int changetypes,
	int changesonly,
	int return_echg_ctls,
	int isCritical,
	LDAPControl **ctrlp )
{
	struct berval	value;

	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );

	if ( ld == NULL ) {
		return LDAP_PARAM_ERROR;
	}

	if ( ctrlp == NULL ) {
		ld->ld_errno = LDAP_PARAM_ERROR;
		return ld->ld_errno;
	}

	ld->ld_errno = ldap_create_persistentsearch_control_value( ld, changetypes, changesonly, return_echg_ctls, &value );
	if ( ld->ld_errno == LDAP_SUCCESS ) {
		ld->ld_errno = ldap_control_create( LDAP_CONTROL_PERSIST_REQUEST,
			isCritical, &value, 0, ctrlp );
		if ( ld->ld_errno != LDAP_SUCCESS ) {
			LDAP_FREE( value.bv_val );
		}
	}

	return ld->ld_errno;
}


/* ---------------------------------------------------------------------------
   ldap_parse_entrychange_control
   
   Decode the entry change notification control return information.

   ld          (IN) An LDAP session handle, as obtained from a call to
					ldap_init().

   ctrl        (IN) The address of the LDAP Control Structure.

   chgtypep   (OUT) This result parameter is filled in with one of the
					following values to indicate the type of change that was
					made that caused the entry to be returned:
					LDAP_CONTROL_PERSIST_ENTRY_CHANGE_ADD (1),
					LDAP_CONTROL_PERSIST_ENTRY_CHANGE_DELETE (2),
					LDAP_CONTROL_PERSIST_ENTRY_CHANGE_MODIFY (4), or
					LDAP_CONTROL_PERSIST_ENTRY_CHANGE_RENAME (8).
					If this parameter is NULL, the change type information
					is not returned. 

   prevdnp    (OUT) This result parameter points to the DN the
   					entry had before it was renamed and/or moved by a
					modifyDN operation. It is set to NULL for other types
					of changes. If this parameter is NULL, the previous DN
					information is not returned. The returned value is a
					pointer to the contents of the control; it is not a
					copy of the data.

   chgnumpresentp (OUT) This result parameter is filled in with a non-zero
   					value if a change number was returned in the control
					(the change number is optional and servers MAY choose
					not to return it). If this parameter is NULL, no indication
					of whether the change number was present is returned.

   chgnump    (OUT) This result parameter is filled in with the change number
   					if one was returned in the control. If this parameter
					is NULL, the change number is not returned.
   
   ---------------------------------------------------------------------------*/

int
ldap_parse_entrychange_control(
	LDAP *ld,
	LDAPControl *ctrl,
	int *chgtypep,
	struct berval *prevdnp,
	int *chgnumpresentp,
	long *chgnump )
{
	BerElement *ber;
	ber_tag_t tag, berTag;
	ber_len_t berLen;
	ber_int_t chgtype;

	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );
	assert( ctrl != NULL );

	if (ld == NULL) {
		return LDAP_PARAM_ERROR;
	}

	if (ctrl == NULL) {
		ld->ld_errno =  LDAP_PARAM_ERROR;
		return(ld->ld_errno);
	}

	if ( !ctrl->ldctl_value.bv_val ) {
		ld->ld_errno = LDAP_DECODING_ERROR;
		return(ld->ld_errno);
	}

	/* Create a BerElement from the berval returned in the control. */
	ber = ber_init(&ctrl->ldctl_value);

	if (ber == NULL) {
		ld->ld_errno = LDAP_NO_MEMORY;
		return(ld->ld_errno);
	}

	if ( prevdnp != NULL ) {
		BER_BVZERO( prevdnp );
	}
	if ( chgnumpresentp != NULL )
		*chgnumpresentp = 0;
	if ( chgnump != NULL )
		*chgnump = 0;

	/* Extract the change type from the control. */
	tag = ber_scanf(ber, "{e" /*}*/, &chgtype);

	if( tag != LBER_ENUMERATED ) {
		ber_free(ber, 1);
		ld->ld_errno = LDAP_DECODING_ERROR;
		return(ld->ld_errno);
	}
	if ( chgtypep != NULL )
		*chgtypep = chgtype;

	tag = ber_peek_tag( ber, &berLen );
	if ( berLen ) {
		if (tag == LBER_OCTETSTRING) {
			if (prevdnp != NULL) {
				tag = ber_get_stringbv( ber, prevdnp, 0 );
			} else {
				struct berval bv;
				tag = ber_skip_element( ber, &bv );
			}
			if ( tag == LBER_ERROR ) {
				ber_free(ber, 1);
				ld->ld_errno = LDAP_DECODING_ERROR;
				return(ld->ld_errno);
			}
			tag = ber_peek_tag( ber, &berLen );
		}

		if ( chgnumpresentp != NULL || chgnump != NULL ) {
			ber_int_t chgnum = 0;
			int present = 0;
			if (tag == LBER_INTEGER) {
				present = 1;
				tag = ber_get_int( ber, &chgnum );
				if ( tag == LBER_ERROR ) {
					ber_free(ber, 1);
					ld->ld_errno = LDAP_DECODING_ERROR;
					return(ld->ld_errno);
				}
				if ( chgnumpresentp != NULL )
					*chgnumpresentp = present;
				if ( chgnump != NULL )
					*chgnump = chgnum;
			}
		}
	}

	ber_free(ber,1);

	ld->ld_errno = LDAP_SUCCESS;
	return(ld->ld_errno);
}
