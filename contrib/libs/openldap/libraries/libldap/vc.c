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
/* ACKNOWLEDGEMENTS:
 * This program was originally developed by Kurt D. Zeilenga for inclusion in
 * OpenLDAP Software.
 */

#include "portable.h"

#include <stdio.h>
#include <ac/stdlib.h>
#include <ac/string.h>
#include <ac/time.h>

#include "ldap-int.h"

/*
 * LDAP Verify Credentials operation
 *
 * The request is an extended request with OID 1.3.6.1.4.1.4203.666.6.5 with value of
 * the BER encoding of:
 *
 * VCRequest ::= SEQUENCE {
 *		cookie [0] OCTET STRING OPTIONAL,
 *		name	LDAPDN,
 *		authentication	AuthenticationChoice,
 *	    controls [2] Controls OPTIONAL
 * }
 *
 * where LDAPDN, AuthenticationChoice, and Controls are as defined in RFC 4511.
 *
 * The response is an extended response with no OID and a value of the BER encoding of
 *
 * VCResponse ::= SEQUENCE {
 *		resultCode ResultCode,
 *		diagnosticMessage LDAPString,
 *		cookie [0] OCTET STRING OPTIONAL,
 *		serverSaslCreds [1] OCTET STRING OPTIONAL,
 *	    controls [2] Controls OPTIONAL
 * }
 *
 * where ResultCode is the result code enumeration from RFC 4511, and LDAPString and Controls are as
 * defined in RFC 4511.
 */

int ldap_parse_verify_credentials(
	LDAP *ld,
	LDAPMessage *res,
	int * code,
	char ** diagmsg,
    struct berval **cookie,
	struct berval **screds,
	LDAPControl ***ctrls)
{
	int rc;
	char *retoid = NULL;
	struct berval *retdata = NULL;

	assert(ld != NULL);
	assert(LDAP_VALID(ld));
	assert(res != NULL);
	assert(code != NULL);
	assert(diagmsg != NULL);

	rc = ldap_parse_extended_result(ld, res, &retoid, &retdata, 0);

	if( rc != LDAP_SUCCESS ) {
		ldap_perror(ld, "ldap_parse_verify_credentials");
		return rc;
	}

	if (retdata) {
		ber_tag_t tag;
		ber_len_t len;
		ber_int_t i;
		BerElement * ber = ber_init(retdata);
		struct berval diagmsg_bv = BER_BVNULL;
		if (!ber) {
		    rc = ld->ld_errno = LDAP_NO_MEMORY;
			goto done;
		}

		rc = LDAP_DECODING_ERROR;

		if (ber_scanf(ber, "{im" /*"}"*/, &i, &diagmsg_bv) == LBER_ERROR) {
			goto ber_done;
		}
		if ( diagmsg != NULL ) {
			*diagmsg = LDAP_MALLOC( diagmsg_bv.bv_len + 1 );
			AC_MEMCPY( *diagmsg, diagmsg_bv.bv_val, diagmsg_bv.bv_len );
			(*diagmsg)[diagmsg_bv.bv_len] = '\0';
		}
		*code = i;

		tag = ber_peek_tag(ber, &len);
		if (tag == LDAP_TAG_EXOP_VERIFY_CREDENTIALS_COOKIE) {
			if (ber_scanf(ber, "O", cookie) == LBER_ERROR)
				goto ber_done;
			tag = ber_peek_tag(ber, &len);
		}

		if (tag == LDAP_TAG_EXOP_VERIFY_CREDENTIALS_SCREDS) {
			if (ber_scanf(ber, "O", screds) == LBER_ERROR)
				goto ber_done;
			tag = ber_peek_tag(ber, &len);
		}

		if (tag == LDAP_TAG_EXOP_VERIFY_CREDENTIALS_CONTROLS) {
		    int nctrls = 0;
			char * opaque;

		    *ctrls = LDAP_MALLOC(1 * sizeof(LDAPControl *));

			if (!*ctrls) {
				rc = LDAP_NO_MEMORY;
				goto ber_done;
			}

			*ctrls[nctrls] = NULL;

			for(tag = ber_first_element(ber, &len, &opaque);
				tag != LBER_ERROR;
				tag = ber_next_element(ber, &len, opaque))
		    {
				LDAPControl *tctrl;
				LDAPControl **tctrls;

				tctrl = LDAP_CALLOC(1, sizeof(LDAPControl));

				/* allocate pointer space for current controls (nctrls)
				 * + this control + extra NULL
				 */
				tctrls = !tctrl ? NULL : LDAP_REALLOC(*ctrls, (nctrls+2) * sizeof(LDAPControl *));

				if (!tctrls) {
					/* allocation failure */
					if (tctrl) LDAP_FREE(tctrl);
					ldap_controls_free(*ctrls);
					*ctrls = NULL;
				    rc = LDAP_NO_MEMORY;
				    goto ber_done;
				}

				tctrls[nctrls++] = tctrl;
				tctrls[nctrls] = NULL;

				tag = ber_scanf(ber, "{a" /*"}"*/, &tctrl->ldctl_oid);
				if (tag == LBER_ERROR) {
					*ctrls = NULL;
					ldap_controls_free(tctrls);
					goto ber_done;
				}

				tag = ber_peek_tag(ber, &len);
				if (tag == LBER_BOOLEAN) {
					ber_int_t crit;
					tag = ber_scanf(ber, "b", &crit);
					tctrl->ldctl_iscritical = crit ? (char) 0 : (char) ~0;
				    tag = ber_peek_tag(ber, &len);
				}

			    if (tag == LBER_OCTETSTRING) {
                    tag = ber_scanf( ber, "o", &tctrl->ldctl_value );
                } else {
                    BER_BVZERO( &tctrl->ldctl_value );
                }

                *ctrls = tctrls;
			}
	    }

		rc = LDAP_SUCCESS;

	ber_done:
	    ber_free(ber, 1);
    }

done:
	ber_bvfree(retdata);
	ber_memfree(retoid);
	return rc;
}

int
ldap_verify_credentials(LDAP *ld,
	struct berval	*cookie,
	LDAP_CONST char *dn,
	LDAP_CONST char *mechanism,
	struct berval	*cred,
    LDAPControl		**vcctrls,
	LDAPControl		**sctrls,
	LDAPControl		**cctrls,
	int				*msgidp)
{
	int rc;
	BerElement *ber;
	struct berval reqdata;

	assert(ld != NULL);
	assert(LDAP_VALID(ld));
	assert(msgidp != NULL);

	ber = ber_alloc_t(LBER_USE_DER);
	if (dn == NULL) dn = "";

	if (mechanism == LDAP_SASL_SIMPLE) {
		assert(!cookie);

		rc = ber_printf(ber, "{stO" /*"}"*/,
			dn, LDAP_AUTH_SIMPLE, cred);

	} else {
		if (!cred || BER_BVISNULL(cred)) {
			if (cookie) {
				rc = ber_printf(ber, "{tOst{sN}" /*"}"*/,
					LDAP_TAG_EXOP_VERIFY_CREDENTIALS_COOKIE, cookie,
					dn, LDAP_AUTH_SASL, mechanism);
			} else {
				rc = ber_printf(ber, "{st{sN}N" /*"}"*/,
					dn, LDAP_AUTH_SASL, mechanism);
			}
		} else {
			if (cookie) {
				rc = ber_printf(ber, "{tOst{sON}" /*"}"*/,
					LDAP_TAG_EXOP_VERIFY_CREDENTIALS_COOKIE, cookie,
					dn, LDAP_AUTH_SASL, mechanism, cred);
			} else {
				rc = ber_printf(ber, "{st{sON}" /*"}"*/,
					dn, LDAP_AUTH_SASL, mechanism, cred);
			}
		}
	}

	if (rc < 0) {
		rc = ld->ld_errno = LDAP_ENCODING_ERROR;
		goto done;
	}

	if (vcctrls && *vcctrls) {
		LDAPControl *const *c;

		rc = ber_printf(ber, "t{" /*"}"*/, LDAP_TAG_EXOP_VERIFY_CREDENTIALS_CONTROLS);

		for (c=vcctrls; *c; c++) {
			rc = ldap_pvt_put_control(*c, ber);
			if (rc != LDAP_SUCCESS) {
				rc = ld->ld_errno = LDAP_ENCODING_ERROR;
				goto done;
			}
		}

		rc = ber_printf(ber, /*"{{"*/ "}N}");

	} else {
		rc = ber_printf(ber, /*"{"*/ "N}");
	}

	if (rc < 0) {
		rc = ld->ld_errno = LDAP_ENCODING_ERROR;
		goto done;
	}


	rc = ber_flatten2(ber, &reqdata, 0);
	if (rc < 0) {
		rc = ld->ld_errno = LDAP_ENCODING_ERROR;
		goto done;
	}

	rc = ldap_extended_operation(ld, LDAP_EXOP_VERIFY_CREDENTIALS,
		&reqdata, sctrls, cctrls, msgidp);

done:
	ber_free(ber, 1);
	return rc;
}

int
ldap_verify_credentials_s(
	LDAP *ld,
	struct berval	*cookie,
	LDAP_CONST char *dn,
	LDAP_CONST char *mechanism,
	struct berval	*cred,
    LDAPControl		**vcictrls,
	LDAPControl		**sctrls,
	LDAPControl		**cctrls,
	int				*rcode,
	char 			**diagmsg,
	struct berval	**scookie,
	struct berval	**scred,
    LDAPControl		***vcoctrls)
{
	int				rc;
	int				msgid;
	LDAPMessage		*res;

	rc = ldap_verify_credentials(ld, cookie, dn, mechanism, cred, vcictrls, sctrls, cctrls, &msgid);
	if (rc != LDAP_SUCCESS) return rc;

	if (ldap_result(ld, msgid, LDAP_MSG_ALL, (struct timeval *) NULL, &res) == -1 || !res) {
		return ld->ld_errno;
	}

	rc = ldap_parse_verify_credentials(ld, res, rcode, diagmsg, scookie, scred, vcoctrls);
	if (rc != LDAP_SUCCESS) {
		ldap_msgfree(res);
		return rc;
	}

	return( ldap_result2error(ld, res, 1));
}

#ifdef LDAP_API_FEATURE_VERIFY_CREDENTIALS_INTERACTIVE
int
ldap_verify_credentials_interactive (
	LDAP *ld,
	LDAP_CONST char *dn, /* usually NULL */
	LDAP_CONST char *mech,
	LDAPControl **vcControls,
	LDAPControl **serverControls,
	LDAPControl **clientControls,

	/* should be client controls */
	unsigned flags,
	LDAP_SASL_INTERACT_PROC *proc,
	void *defaults,
    void *context;
	
	/* as obtained from ldap_result() */
	LDAPMessage *result,

	/* returned during bind processing */
	const char **rmech,
	int *msgid )
{
	if (!ld && context) {
		assert(!dn);
		assert(!mech);
		assert(!vcControls);
		assert(!serverControls);
		assert(!defaults);
		assert(!result);
		assert(!rmech);
		assert(!msgid);

		/* special case to avoid having to expose a separate dispose context API */
		sasl_dispose((sasl_conn_t)context);
        return LDAP_SUCCESS;
    }

    ld->ld_errno = LDAP_NOT_SUPPORTED;
    return ld->ld_errno;
}
#endif
