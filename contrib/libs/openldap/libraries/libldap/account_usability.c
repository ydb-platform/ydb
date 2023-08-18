/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 2004-2022 The OpenLDAP Foundation.
 * Portions Copyright 2004 Hewlett-Packard Company.
 * Portions Copyright 2004 Howard Chu, Symas Corp.
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
 * OpenLDAP Software, based on prior work by Neil Dunbar (HP).
 * This work was sponsored by the Hewlett-Packard Company.
 */

#include "portable.h"

#include "ldap-int.h"

#ifdef LDAP_CONTROL_X_ACCOUNT_USABILITY

int
ldap_create_accountusability_control( LDAP *ld,
                                    LDAPControl **ctrlp )
{
	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );
	assert( ctrlp != NULL );

	ld->ld_errno = ldap_control_create( LDAP_CONTROL_X_ACCOUNT_USABILITY,
		0, NULL, 0, ctrlp );

	return ld->ld_errno;
}

int
ldap_parse_accountusability_control(
	LDAP           *ld,
	LDAPControl    *ctrl,
	int            *availablep,
	LDAPAccountUsability *usabilityp )
{
	BerElement  *ber;
	int available = 0;
	ber_tag_t tag;
	ber_len_t berLen;
	char *last;

	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );
	assert( ctrl != NULL );

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

	tag = ber_peek_tag( ber, &berLen );

	if ( tag == LDAP_TAG_X_ACCOUNT_USABILITY_AVAILABLE ) {
		available = 1;

		if ( usabilityp != NULL ) {
			if (ber_get_int( ber, &usabilityp->seconds_remaining ) == LBER_DEFAULT) goto exit;
		}
	} else if ( tag == LDAP_TAG_X_ACCOUNT_USABILITY_NOT_AVAILABLE ) {
		available = 0;
		LDAPAccountUsabilityMoreInfo more_info = { 0, 0, 0, -1, -1 };

		ber_skip_tag( ber, &berLen );
		while ( (tag = ber_peek_tag( ber, &berLen )) != LBER_DEFAULT ) {
			switch (tag) {
			case LDAP_TAG_X_ACCOUNT_USABILITY_INACTIVE:
				if (ber_get_boolean( ber, &more_info.inactive ) == LBER_DEFAULT) goto exit;
				break;
			case LDAP_TAG_X_ACCOUNT_USABILITY_RESET:
				if (ber_get_boolean( ber, &more_info.reset ) == LBER_DEFAULT) goto exit;
				break;
			case LDAP_TAG_X_ACCOUNT_USABILITY_EXPIRED:
				if (ber_get_boolean( ber, &more_info.expired ) == LBER_DEFAULT) goto exit;
				break;
			case LDAP_TAG_X_ACCOUNT_USABILITY_REMAINING_GRACE:
				if (ber_get_int( ber, &more_info.remaining_grace ) == LBER_DEFAULT) goto exit;
				break;
			case LDAP_TAG_X_ACCOUNT_USABILITY_UNTIL_UNLOCK:
				if (ber_get_int( ber, &more_info.seconds_before_unlock ) == LBER_DEFAULT) goto exit;
				break;
			default:
				goto exit;
			}
		}
		if ( usabilityp != NULL ) {
			usabilityp->more_info = more_info;
		}
	} else {
		goto exit;
	}
	if ( availablep != NULL ) {
		*availablep = available;
	}

	ber_free(ber, 1);

	ld->ld_errno = LDAP_SUCCESS;
	return(ld->ld_errno);

  exit:
	ber_free(ber, 1);
	ld->ld_errno = LDAP_DECODING_ERROR;
	return(ld->ld_errno);
}

#endif /* LDAP_CONTROL_X_ACCOUNT_USABILITY */
