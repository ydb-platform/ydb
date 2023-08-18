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

#include <stdio.h>
#include <ac/stdlib.h>
#include <ac/string.h>
#include <ac/time.h>

#include "ldap-int.h"

#ifdef LDAP_CONTROL_PASSWORDPOLICYREQUEST

/* IMPLICIT TAGS, all context-specific */
#define PPOLICY_WARNING 0xa0L	/* constructed + 0 */
#define PPOLICY_ERROR 0x81L		/* primitive + 1 */

#define PPOLICY_EXPIRE 0x80L	/* primitive + 0 */
#define PPOLICY_GRACE  0x81L	/* primitive + 1 */

/*---
   ldap_create_passwordpolicy_control
   
   Create and encode the Password Policy Request

   ld        (IN)  An LDAP session handle, as obtained from a call to
				   ldap_init().
   
   ctrlp     (OUT) A result parameter that will be assigned the address
				   of an LDAPControl structure that contains the 
				   passwordPolicyRequest control created by this function.
				   The memory occupied by the LDAPControl structure
				   SHOULD be freed when it is no longer in use by
				   calling ldap_control_free().
					  
   
   There is no control value for a password policy request
 ---*/

int
ldap_create_passwordpolicy_control( LDAP *ld,
                                    LDAPControl **ctrlp )
{
	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );
	assert( ctrlp != NULL );

	ld->ld_errno = ldap_control_create( LDAP_CONTROL_PASSWORDPOLICYREQUEST,
		0, NULL, 0, ctrlp );

	return ld->ld_errno;
}


/*---
   ldap_parse_passwordpolicy_control
   
   Decode the passwordPolicyResponse control and return information.

   ld           (IN)   An LDAP session handle.
   
   ctrl         (IN)   The address of an
					   LDAPControl structure, either obtained 
					   by running through the list of response controls or
					   by a call to ldap_control_find().

   exptimep     (OUT)  This result parameter is filled in with the number of seconds before
                                           the password will expire, if expiration is imminent
                                           (imminency defined by the password policy). If expiration
                                           is not imminent, the value is set to -1.

   gracep       (OUT)  This result parameter is filled in with the number of grace logins after
                                           the password has expired, before no further login attempts
                                           will be allowed.

   errorcodep   (OUT)  This result parameter is filled in with the error code of the password operation
                                           If no error was detected, this error is set to PP_noError.
   
   Ber encoding
   
   PasswordPolicyResponseValue ::= SEQUENCE {
       warning [0] CHOICE {
           timeBeforeExpiration [0] INTEGER (0 .. maxInt),
           graceLoginsRemaining [1] INTEGER (0 .. maxInt) } OPTIONAL
       error [1] ENUMERATED {
           passwordExpired        (0),
           accountLocked          (1),
           changeAfterReset       (2),
           passwordModNotAllowed  (3),
           mustSupplyOldPassword  (4),
           invalidPasswordSyntax  (5),
           passwordTooShort       (6),
           passwordTooYoung       (7),
           passwordInHistory      (8) } OPTIONAL }
           
---*/

int
ldap_parse_passwordpolicy_control(
	LDAP           *ld,
	LDAPControl    *ctrl,
	ber_int_t      *expirep,
	ber_int_t      *gracep,
	LDAPPasswordPolicyError *errorp )
{
	BerElement  *ber;
	int exp = -1, grace = -1;
	ber_tag_t tag;
	ber_len_t berLen;
        char *last;
	int err = PP_noError;
        
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
	if (tag != LBER_SEQUENCE) goto exit;

	for( tag = ber_first_element( ber, &berLen, &last );
		tag != LBER_DEFAULT;
		tag = ber_next_element( ber, &berLen, last ) )
	{
		switch (tag) {
		case PPOLICY_WARNING:
			ber_skip_tag(ber, &berLen );
			tag = ber_peek_tag( ber, &berLen );
			switch( tag ) {
			case PPOLICY_EXPIRE:
				if (ber_get_int( ber, &exp ) == LBER_DEFAULT) goto exit;
				break;
			case PPOLICY_GRACE:
				if (ber_get_int( ber, &grace ) == LBER_DEFAULT) goto exit;
				break;
			default:
				goto exit;
			}
			break;
		case PPOLICY_ERROR:
			if (ber_get_enum( ber, &err ) == LBER_DEFAULT) goto exit;
			break;
		default:
			goto exit;
		}
	}

	ber_free(ber, 1);

	/* Return data to the caller for items that were requested. */
	if (expirep) *expirep = exp;
	if (gracep) *gracep = grace;
	if (errorp) *errorp = err;
        
	ld->ld_errno = LDAP_SUCCESS;
	return(ld->ld_errno);

  exit:
	ber_free(ber, 1);
	ld->ld_errno = LDAP_DECODING_ERROR;
	return(ld->ld_errno);
}

const char *
ldap_passwordpolicy_err2txt( LDAPPasswordPolicyError err )
{
	switch(err) {
	case PP_passwordExpired: return "Password expired";
	case PP_accountLocked: return "Account locked";
	case PP_changeAfterReset: return "Password must be changed";
	case PP_passwordModNotAllowed: return "Policy prevents password modification";
	case PP_mustSupplyOldPassword: return "Policy requires old password in order to change password";
	case PP_insufficientPasswordQuality: return "Password fails quality checks";
	case PP_passwordTooShort: return "Password is too short for policy";
	case PP_passwordTooYoung: return "Password has been changed too recently";
	case PP_passwordInHistory: return "New password is in list of old passwords";
	case PP_passwordTooLong: return "Password is too long for policy";
	case PP_noError: return "No error";
	default: return "Unknown error code";
	}
}

#endif /* LDAP_CONTROL_PASSWORDPOLICYREQUEST */

#ifdef LDAP_CONTROL_X_PASSWORD_EXPIRING

int
ldap_parse_password_expiring_control(
	LDAP           *ld,
	LDAPControl    *ctrl,
	long           *secondsp )
{
	long seconds = 0;
	char buf[sizeof("-2147483648")];
	char *next;

	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );
	assert( ctrl != NULL );

	if ( BER_BVISEMPTY( &ctrl->ldctl_value ) ||
		ctrl->ldctl_value.bv_len >= sizeof(buf) ) {
		ld->ld_errno = LDAP_DECODING_ERROR;
		return(ld->ld_errno);
	}

	memcpy( buf, ctrl->ldctl_value.bv_val, ctrl->ldctl_value.bv_len );
	buf[ctrl->ldctl_value.bv_len] = '\0';

	seconds = strtol( buf, &next, 10 );
	if ( next == buf || next[0] != '\0' ) goto exit;

	if ( secondsp != NULL ) {
		*secondsp = seconds;
	}

	ld->ld_errno = LDAP_SUCCESS;
	return(ld->ld_errno);

  exit:
	ld->ld_errno = LDAP_DECODING_ERROR;
	return(ld->ld_errno);
}

#endif /* LDAP_CONTROL_X_PASSWORD_EXPIRING */
