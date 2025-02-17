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
 * A copy of this license is available in file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */
/* Portions Copyright (c) 1990 Regents of the University of Michigan.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms are permitted
 * provided that this notice is preserved and that due credit is given
 * to the University of Michigan at Ann Arbor. The name of the University
 * may not be used to endorse or promote products derived from this
 * software without specific prior written permission. This software
 * is provided ``as is'' without express or implied warranty.
 */

#ifndef _LDAP_H
#define _LDAP_H

/* pull in lber */
#include <lber.h>

/* include version and API feature defines */
#include <ldap_features.h>

LDAP_BEGIN_DECL

#define LDAP_VERSION1	1
#define LDAP_VERSION2	2
#define LDAP_VERSION3	3

#define LDAP_VERSION_MIN	LDAP_VERSION2
#define	LDAP_VERSION		LDAP_VERSION2
#define LDAP_VERSION_MAX	LDAP_VERSION3

/*
 * We use 3000+n here because it is above 1823 (for RFC 1823),
 * above 2000+rev of IETF LDAPEXT draft (now quite dated),
 * yet below allocations for new RFCs (just in case there is
 * someday an RFC produced).
 */
#define LDAP_API_VERSION	3001
#define LDAP_VENDOR_NAME	"OpenLDAP"

/* OpenLDAP API Features */
#define LDAP_API_FEATURE_X_OPENLDAP LDAP_VENDOR_VERSION

#if defined( LDAP_API_FEATURE_X_OPENLDAP_REENTRANT )
#	define	LDAP_API_FEATURE_THREAD_SAFE 		1
#endif
#if defined( LDAP_API_FEATURE_X_OPENLDAP_THREAD_SAFE )
#	define  LDAP_API_FEATURE_SESSION_THREAD_SAFE	1
#	define  LDAP_API_FEATURE_OPERATION_THREAD_SAFE	1
#endif


#define LDAP_PORT		389		/* ldap:///		default LDAP port */
#define LDAPS_PORT		636		/* ldaps:///	default LDAP over TLS port */

#define LDAP_ROOT_DSE				""
#define LDAP_NO_ATTRS				"1.1"
#define LDAP_ALL_USER_ATTRIBUTES	"*"
#define LDAP_ALL_OPERATIONAL_ATTRIBUTES	"+" /* RFC 3673 */

/* RFC 4511:  maxInt INTEGER ::= 2147483647 -- (2^^31 - 1) -- */
#define LDAP_MAXINT (2147483647)

/*
 * LDAP_OPTions
 *	0x0000 - 0x0fff reserved for api options
 *	0x1000 - 0x3fff reserved for api extended options
 *	0x4000 - 0x7fff reserved for private and experimental options
 */

#define LDAP_OPT_API_INFO			0x0000
#define LDAP_OPT_DESC				0x0001 /* historic */
#define LDAP_OPT_DEREF				0x0002
#define LDAP_OPT_SIZELIMIT			0x0003
#define LDAP_OPT_TIMELIMIT			0x0004
/* 0x05 - 0x07 not defined */
#define LDAP_OPT_REFERRALS			0x0008
#define LDAP_OPT_RESTART			0x0009
/* 0x0a - 0x10 not defined */
#define LDAP_OPT_PROTOCOL_VERSION		0x0011
#define LDAP_OPT_SERVER_CONTROLS		0x0012
#define LDAP_OPT_CLIENT_CONTROLS		0x0013
/* 0x14 not defined */
#define LDAP_OPT_API_FEATURE_INFO		0x0015
/* 0x16 - 0x2f not defined */
#define LDAP_OPT_HOST_NAME			0x0030
#define LDAP_OPT_RESULT_CODE			0x0031
#define LDAP_OPT_ERROR_NUMBER			LDAP_OPT_RESULT_CODE
#define LDAP_OPT_DIAGNOSTIC_MESSAGE		0x0032
#define LDAP_OPT_ERROR_STRING			LDAP_OPT_DIAGNOSTIC_MESSAGE
#define LDAP_OPT_MATCHED_DN			0x0033
/* 0x0034 - 0x3fff not defined */
/* 0x0091 used by Microsoft for LDAP_OPT_AUTO_RECONNECT */
#define LDAP_OPT_SSPI_FLAGS			0x0092
/* 0x0093 used by Microsoft for LDAP_OPT_SSL_INFO */
/* 0x0094 used by Microsoft for LDAP_OPT_REF_DEREF_CONN_PER_MSG */
#define LDAP_OPT_SIGN				0x0095
#define LDAP_OPT_ENCRYPT			0x0096
#define LDAP_OPT_SASL_METHOD			0x0097
/* 0x0098 used by Microsoft for LDAP_OPT_AREC_EXCLUSIVE */
#define LDAP_OPT_SECURITY_CONTEXT		0x0099
/* 0x009A used by Microsoft for LDAP_OPT_ROOTDSE_CACHE */
/* 0x009B - 0x3fff not defined */

/* API Extensions */
#define LDAP_OPT_API_EXTENSION_BASE 0x4000  /* API extensions */

/* private and experimental options */
/* OpenLDAP specific options */
#define LDAP_OPT_DEBUG_LEVEL		0x5001	/* debug level */
#define LDAP_OPT_TIMEOUT			0x5002	/* default timeout */
#define LDAP_OPT_REFHOPLIMIT		0x5003	/* ref hop limit */
#define LDAP_OPT_NETWORK_TIMEOUT	0x5005	/* socket level timeout */
#define LDAP_OPT_URI				0x5006
#define LDAP_OPT_REFERRAL_URLS      0x5007  /* Referral URLs */
#define LDAP_OPT_SOCKBUF            0x5008  /* sockbuf */
#define LDAP_OPT_DEFBASE		0x5009	/* searchbase */
#define	LDAP_OPT_CONNECT_ASYNC		0x5010	/* create connections asynchronously */
#define	LDAP_OPT_CONNECT_CB			0x5011	/* connection callbacks */
#define	LDAP_OPT_SESSION_REFCNT		0x5012	/* session reference count */
#define	LDAP_OPT_KEEPCONN		0x5013	/* keep the connection on read error or NoD */
#define	LDAP_OPT_SOCKET_BIND_ADDRESSES	0x5014	/* user configured bind IPs */
#define	LDAP_OPT_TCP_USER_TIMEOUT	0x5015	/* set TCP_USER_TIMEOUT if the OS supports it, ignored otherwise */

/* OpenLDAP TLS options */
#define LDAP_OPT_X_TLS				0x6000
#define LDAP_OPT_X_TLS_CTX			0x6001	/* OpenSSL CTX* */
#define LDAP_OPT_X_TLS_CACERTFILE	0x6002
#define LDAP_OPT_X_TLS_CACERTDIR	0x6003
#define LDAP_OPT_X_TLS_CERTFILE		0x6004
#define LDAP_OPT_X_TLS_KEYFILE		0x6005
#define LDAP_OPT_X_TLS_REQUIRE_CERT	0x6006
#define LDAP_OPT_X_TLS_PROTOCOL_MIN	0x6007
#define LDAP_OPT_X_TLS_CIPHER_SUITE	0x6008
#define LDAP_OPT_X_TLS_RANDOM_FILE	0x6009
#define LDAP_OPT_X_TLS_SSL_CTX		0x600a	/* OpenSSL SSL* */
#define LDAP_OPT_X_TLS_CRLCHECK		0x600b
#define LDAP_OPT_X_TLS_CONNECT_CB	0x600c
#define LDAP_OPT_X_TLS_CONNECT_ARG	0x600d
#define LDAP_OPT_X_TLS_DHFILE		0x600e
#define LDAP_OPT_X_TLS_NEWCTX		0x600f
#define LDAP_OPT_X_TLS_CRLFILE		0x6010	/* GNUtls only */
#define LDAP_OPT_X_TLS_PACKAGE		0x6011
#define LDAP_OPT_X_TLS_ECNAME		0x6012
#define LDAP_OPT_X_TLS_VERSION		0x6013	/* read-only */
#define LDAP_OPT_X_TLS_CIPHER		0x6014	/* read-only */
#define LDAP_OPT_X_TLS_PEERCERT		0x6015	/* read-only */
#define LDAP_OPT_X_TLS_CACERT		0x6016
#define LDAP_OPT_X_TLS_CERT			0x6017
#define LDAP_OPT_X_TLS_KEY			0x6018
#define LDAP_OPT_X_TLS_PEERKEY_HASH	0x6019
#define LDAP_OPT_X_TLS_REQUIRE_SAN	0x601a
#define LDAP_OPT_X_TLS_PROTOCOL_MAX	0x601b

#define LDAP_OPT_X_TLS_NEVER	0
#define LDAP_OPT_X_TLS_HARD		1
#define LDAP_OPT_X_TLS_DEMAND	2
#define LDAP_OPT_X_TLS_ALLOW	3
#define LDAP_OPT_X_TLS_TRY		4

#define LDAP_OPT_X_TLS_CRL_NONE	0
#define LDAP_OPT_X_TLS_CRL_PEER	1
#define LDAP_OPT_X_TLS_CRL_ALL	2

/* for LDAP_OPT_X_TLS_PROTOCOL_MIN/MAX */
#define LDAP_OPT_X_TLS_PROTOCOL(maj,min)	(((maj) << 8) + (min))
#define LDAP_OPT_X_TLS_PROTOCOL_SSL2		(2 << 8)
#define LDAP_OPT_X_TLS_PROTOCOL_SSL3		(3 << 8)
#define LDAP_OPT_X_TLS_PROTOCOL_TLS1_0		((3 << 8) + 1)
#define LDAP_OPT_X_TLS_PROTOCOL_TLS1_1		((3 << 8) + 2)
#define LDAP_OPT_X_TLS_PROTOCOL_TLS1_2		((3 << 8) + 3)
#define LDAP_OPT_X_TLS_PROTOCOL_TLS1_3		((3 << 8) + 4)

#define LDAP_OPT_X_SASL_CBINDING_NONE		0
#define LDAP_OPT_X_SASL_CBINDING_TLS_UNIQUE	1
#define LDAP_OPT_X_SASL_CBINDING_TLS_ENDPOINT	2

/* OpenLDAP SASL options */
#define LDAP_OPT_X_SASL_MECH			0x6100
#define LDAP_OPT_X_SASL_REALM			0x6101
#define LDAP_OPT_X_SASL_AUTHCID			0x6102
#define LDAP_OPT_X_SASL_AUTHZID			0x6103
#define LDAP_OPT_X_SASL_SSF				0x6104 /* read-only */
#define LDAP_OPT_X_SASL_SSF_EXTERNAL	0x6105 /* write-only */
#define LDAP_OPT_X_SASL_SECPROPS		0x6106 /* write-only */
#define LDAP_OPT_X_SASL_SSF_MIN			0x6107
#define LDAP_OPT_X_SASL_SSF_MAX			0x6108
#define LDAP_OPT_X_SASL_MAXBUFSIZE		0x6109
#define LDAP_OPT_X_SASL_MECHLIST		0x610a /* read-only */
#define LDAP_OPT_X_SASL_NOCANON			0x610b
#define LDAP_OPT_X_SASL_USERNAME		0x610c /* read-only */
#define LDAP_OPT_X_SASL_GSS_CREDS		0x610d
#define LDAP_OPT_X_SASL_CBINDING		0x610e

/*
 * OpenLDAP per connection tcp-keepalive settings
 * (Linux only, ignored where unsupported)
 */
#define LDAP_OPT_X_KEEPALIVE_IDLE		0x6300
#define LDAP_OPT_X_KEEPALIVE_PROBES		0x6301
#define LDAP_OPT_X_KEEPALIVE_INTERVAL	0x6302

/* Private API Extensions -- reserved for application use */
#define LDAP_OPT_PRIVATE_EXTENSION_BASE 0x7000  /* Private API inclusive */

/*
 * ldap_get_option() and ldap_set_option() return values.
 * As later versions may return other values indicating
 * failure, current applications should only compare returned
 * value against LDAP_OPT_SUCCESS.
 */
#define LDAP_OPT_SUCCESS	0
#define	LDAP_OPT_ERROR		(-1)

/* option on/off values */
#define LDAP_OPT_ON		((void *) &ber_pvt_opt_on)
#define LDAP_OPT_OFF	((void *) 0)

typedef struct ldapapiinfo {
	int		ldapai_info_version;		/* version of LDAPAPIInfo */
#define LDAP_API_INFO_VERSION	(1)
	int		ldapai_api_version;			/* revision of API supported */
	int		ldapai_protocol_version;	/* highest LDAP version supported */
	char	**ldapai_extensions;		/* names of API extensions */
	char	*ldapai_vendor_name;		/* name of supplier */
	int		ldapai_vendor_version;		/* supplier-specific version * 100 */
} LDAPAPIInfo;

typedef struct ldap_apifeature_info {
	int		ldapaif_info_version;		/* version of LDAPAPIFeatureInfo */
#define LDAP_FEATURE_INFO_VERSION (1)	/* apifeature_info struct version */
	char*	ldapaif_name;				/* LDAP_API_FEATURE_* (less prefix) */
	int		ldapaif_version;			/* value of LDAP_API_FEATURE_... */
} LDAPAPIFeatureInfo;

/*
 * LDAP Control structure
 */
typedef struct ldapcontrol {
	char *			ldctl_oid;			/* numericoid of control */
	struct berval	ldctl_value;		/* encoded value of control */
	char			ldctl_iscritical;	/* criticality */
} LDAPControl;

/* LDAP Controls */
/*	standard track controls */
#define LDAP_CONTROL_MANAGEDSAIT	"2.16.840.1.113730.3.4.2"  /* RFC 3296 */
#define LDAP_CONTROL_PROXY_AUTHZ	"2.16.840.1.113730.3.4.18" /* RFC 4370 */
#define LDAP_CONTROL_SUBENTRIES		"1.3.6.1.4.1.4203.1.10.1"  /* RFC 3672 */

#define LDAP_CONTROL_VALUESRETURNFILTER "1.2.826.0.1.3344810.2.3"/* RFC 3876 */

#define LDAP_CONTROL_ASSERT				"1.3.6.1.1.12"			/* RFC 4528 */
#define LDAP_CONTROL_PRE_READ			"1.3.6.1.1.13.1"		/* RFC 4527 */
#define LDAP_CONTROL_POST_READ			"1.3.6.1.1.13.2"		/* RFC 4527 */

#define LDAP_CONTROL_SORTREQUEST    "1.2.840.113556.1.4.473" /* RFC 2891 */
#define LDAP_CONTROL_SORTRESPONSE	"1.2.840.113556.1.4.474" /* RFC 2891 */

/*	non-standard track controls */
#define LDAP_CONTROL_PAGEDRESULTS	"1.2.840.113556.1.4.319"   /* RFC 2696 */

#define LDAP_CONTROL_AUTHZID_REQUEST	"2.16.840.1.113730.3.4.16"   /* RFC 3829 */
#define LDAP_CONTROL_AUTHZID_RESPONSE   "2.16.840.1.113730.3.4.15"   /* RFC 3829 */

/* LDAP Content Synchronization Operation -- RFC 4533 */
#define LDAP_SYNC_OID			"1.3.6.1.4.1.4203.1.9.1"
#define LDAP_CONTROL_SYNC		LDAP_SYNC_OID ".1"
#define LDAP_CONTROL_SYNC_STATE	LDAP_SYNC_OID ".2"
#define LDAP_CONTROL_SYNC_DONE	LDAP_SYNC_OID ".3"
#define LDAP_SYNC_INFO			LDAP_SYNC_OID ".4"

#define LDAP_SYNC_NONE					0x00
#define LDAP_SYNC_REFRESH_ONLY			0x01
#define LDAP_SYNC_RESERVED				0x02
#define LDAP_SYNC_REFRESH_AND_PERSIST	0x03

#define LDAP_SYNC_REFRESH_PRESENTS		0
#define LDAP_SYNC_REFRESH_DELETES		1

#define LDAP_TAG_SYNC_NEW_COOKIE		((ber_tag_t) 0x80U)
#define LDAP_TAG_SYNC_REFRESH_DELETE	((ber_tag_t) 0xa1U)
#define LDAP_TAG_SYNC_REFRESH_PRESENT	((ber_tag_t) 0xa2U)
#define	LDAP_TAG_SYNC_ID_SET			((ber_tag_t) 0xa3U)

#define LDAP_TAG_SYNC_COOKIE			((ber_tag_t) 0x04U)
#define LDAP_TAG_REFRESHDELETES			((ber_tag_t) 0x01U)
#define LDAP_TAG_REFRESHDONE			((ber_tag_t) 0x01U)
#define LDAP_TAG_RELOAD_HINT			((ber_tag_t) 0x01U)

#define LDAP_SYNC_PRESENT				0
#define LDAP_SYNC_ADD					1
#define LDAP_SYNC_MODIFY				2
#define LDAP_SYNC_DELETE				3
#define LDAP_SYNC_NEW_COOKIE			4

/* LDAP Don't Use Copy Control (RFC 6171) */
#define LDAP_CONTROL_DONTUSECOPY		"1.3.6.1.1.22"

/* Password policy Controls *//* work in progress */
/* ITS#3458: released; disabled by default */
#define LDAP_CONTROL_PASSWORDPOLICYREQUEST	"1.3.6.1.4.1.42.2.27.8.5.1"
#define LDAP_CONTROL_PASSWORDPOLICYRESPONSE	"1.3.6.1.4.1.42.2.27.8.5.1"

/* various works in progress */
#define LDAP_CONTROL_NOOP				"1.3.6.1.4.1.4203.666.5.2"
#define LDAP_CONTROL_NO_SUBORDINATES	"1.3.6.1.4.1.4203.666.5.11"
#define LDAP_CONTROL_RELAX				"1.3.6.1.4.1.4203.666.5.12"
#define LDAP_CONTROL_MANAGEDIT			LDAP_CONTROL_RELAX
#define LDAP_CONTROL_SLURP				"1.3.6.1.4.1.4203.666.5.13"
#define LDAP_CONTROL_VALSORT			"1.3.6.1.4.1.4203.666.5.14"
#define	LDAP_CONTROL_X_DEREF			"1.3.6.1.4.1.4203.666.5.16"
#define	LDAP_CONTROL_X_WHATFAILED		"1.3.6.1.4.1.4203.666.5.17"

/* LDAP Chaining Behavior Control *//* work in progress */
/* <draft-sermersheim-ldap-chaining>;
 * see also LDAP_NO_REFERRALS_FOUND, LDAP_CANNOT_CHAIN */
#define LDAP_CONTROL_X_CHAINING_BEHAVIOR	"1.3.6.1.4.1.4203.666.11.3"

#define	LDAP_CHAINING_PREFERRED				0
#define	LDAP_CHAINING_REQUIRED				1
#define LDAP_REFERRALS_PREFERRED			2
#define LDAP_REFERRALS_REQUIRED				3

/* MS Active Directory controls (for compatibility) */
#define LDAP_CONTROL_X_LAZY_COMMIT			"1.2.840.113556.1.4.619"
#define LDAP_CONTROL_X_INCREMENTAL_VALUES	"1.2.840.113556.1.4.802"
#define LDAP_CONTROL_X_DOMAIN_SCOPE			"1.2.840.113556.1.4.1339"
#define LDAP_CONTROL_X_PERMISSIVE_MODIFY	"1.2.840.113556.1.4.1413"
#define LDAP_CONTROL_X_SEARCH_OPTIONS		"1.2.840.113556.1.4.1340"
#define LDAP_SEARCH_FLAG_DOMAIN_SCOPE 1 /* do not generate referrals */
#define LDAP_SEARCH_FLAG_PHANTOM_ROOT 2 /* search all subordinate NCs */
#define LDAP_CONTROL_X_TREE_DELETE		"1.2.840.113556.1.4.805"

/* MS Active Directory controls - not implemented in slapd(8) */
#define LDAP_CONTROL_X_SERVER_NOTIFICATION	"1.2.840.113556.1.4.528"
#define LDAP_CONTROL_X_EXTENDED_DN		"1.2.840.113556.1.4.529"
#define LDAP_CONTROL_X_SHOW_DELETED		"1.2.840.113556.1.4.417"
#define LDAP_CONTROL_X_DIRSYNC			"1.2.840.113556.1.4.841"

#define LDAP_CONTROL_X_DIRSYNC_OBJECT_SECURITY		0x00000001
#define LDAP_CONTROL_X_DIRSYNC_ANCESTORS_FIRST		0x00000800
#define LDAP_CONTROL_X_DIRSYNC_PUBLIC_DATA_ONLY		0x00002000
#define LDAP_CONTROL_X_DIRSYNC_INCREMENTAL_VALUES	0x80000000


/* <draft-wahl-ldap-session> */
#define LDAP_CONTROL_X_SESSION_TRACKING		"1.3.6.1.4.1.21008.108.63.1"
#define LDAP_CONTROL_X_SESSION_TRACKING_RADIUS_ACCT_SESSION_ID \
						LDAP_CONTROL_X_SESSION_TRACKING ".1"
#define LDAP_CONTROL_X_SESSION_TRACKING_RADIUS_ACCT_MULTI_SESSION_ID \
						LDAP_CONTROL_X_SESSION_TRACKING ".2"
#define LDAP_CONTROL_X_SESSION_TRACKING_USERNAME \
						LDAP_CONTROL_X_SESSION_TRACKING ".3"
/* various expired works */

/* LDAP Duplicated Entry Control Extension *//* not implemented in slapd(8) */
#define LDAP_CONTROL_DUPENT_REQUEST		"2.16.840.1.113719.1.27.101.1"
#define LDAP_CONTROL_DUPENT_RESPONSE	"2.16.840.1.113719.1.27.101.2"
#define LDAP_CONTROL_DUPENT_ENTRY		"2.16.840.1.113719.1.27.101.3"
#define LDAP_CONTROL_DUPENT	LDAP_CONTROL_DUPENT_REQUEST

/* LDAP Persistent Search Control *//* not implemented in slapd(8) */
#define LDAP_CONTROL_PERSIST_REQUEST				"2.16.840.1.113730.3.4.3"
#define LDAP_CONTROL_PERSIST_ENTRY_CHANGE_NOTICE	"2.16.840.1.113730.3.4.7"
#define LDAP_CONTROL_PERSIST_ENTRY_CHANGE_ADD		0x1
#define LDAP_CONTROL_PERSIST_ENTRY_CHANGE_DELETE	0x2
#define LDAP_CONTROL_PERSIST_ENTRY_CHANGE_MODIFY	0x4
#define LDAP_CONTROL_PERSIST_ENTRY_CHANGE_RENAME	0x8

/* LDAP VLV */
#define LDAP_CONTROL_VLVREQUEST    	"2.16.840.1.113730.3.4.9"
#define LDAP_CONTROL_VLVRESPONSE    "2.16.840.1.113730.3.4.10"

/* Sun's analogue to ppolicy */
#define LDAP_CONTROL_X_ACCOUNT_USABILITY "1.3.6.1.4.1.42.2.27.9.5.8"

#define LDAP_TAG_X_ACCOUNT_USABILITY_AVAILABLE	((ber_tag_t) 0x80U)	/* primitive + 0 */
#define LDAP_TAG_X_ACCOUNT_USABILITY_NOT_AVAILABLE	((ber_tag_t) 0xA1U)	/* constructed + 1 */

#define LDAP_TAG_X_ACCOUNT_USABILITY_INACTIVE	((ber_tag_t) 0x80U)	/* primitive + 0 */
#define LDAP_TAG_X_ACCOUNT_USABILITY_RESET	((ber_tag_t) 0x81U)	/* primitive + 1 */
#define LDAP_TAG_X_ACCOUNT_USABILITY_EXPIRED	((ber_tag_t) 0x82U)	/* primitive + 2 */
#define LDAP_TAG_X_ACCOUNT_USABILITY_REMAINING_GRACE	((ber_tag_t) 0x83U)	/* primitive + 3 */
#define LDAP_TAG_X_ACCOUNT_USABILITY_UNTIL_UNLOCK	((ber_tag_t) 0x84U)	/* primitive + 4 */

/* Netscape Password policy response controls */
/* <draft-vchu-ldap-pwd-policy> */
#define LDAP_CONTROL_X_PASSWORD_EXPIRED		"2.16.840.1.113730.3.4.4"
#define LDAP_CONTROL_X_PASSWORD_EXPIRING	"2.16.840.1.113730.3.4.5"

/* LDAP Unsolicited Notifications */
#define	LDAP_NOTICE_OF_DISCONNECTION	"1.3.6.1.4.1.1466.20036" /* RFC 4511 */
#define LDAP_NOTICE_DISCONNECT LDAP_NOTICE_OF_DISCONNECTION

/* LDAP Extended Operations */
#define LDAP_EXOP_START_TLS		"1.3.6.1.4.1.1466.20037"	/* RFC 4511 */

#define LDAP_EXOP_MODIFY_PASSWD	"1.3.6.1.4.1.4203.1.11.1"	/* RFC 3062 */
#define LDAP_TAG_EXOP_MODIFY_PASSWD_ID	((ber_tag_t) 0x80U)
#define LDAP_TAG_EXOP_MODIFY_PASSWD_OLD	((ber_tag_t) 0x81U)
#define LDAP_TAG_EXOP_MODIFY_PASSWD_NEW	((ber_tag_t) 0x82U)
#define LDAP_TAG_EXOP_MODIFY_PASSWD_GEN	((ber_tag_t) 0x80U)

#define LDAP_EXOP_CANCEL		"1.3.6.1.1.8"					/* RFC 3909 */
#define LDAP_EXOP_X_CANCEL		LDAP_EXOP_CANCEL

#define	LDAP_EXOP_REFRESH		"1.3.6.1.4.1.1466.101.119.1"	/* RFC 2589 */
#define	LDAP_TAG_EXOP_REFRESH_REQ_DN	((ber_tag_t) 0x80U)
#define	LDAP_TAG_EXOP_REFRESH_REQ_TTL	((ber_tag_t) 0x81U)
#define	LDAP_TAG_EXOP_REFRESH_RES_TTL	((ber_tag_t) 0x81U)

#define LDAP_EXOP_VERIFY_CREDENTIALS	"1.3.6.1.4.1.4203.666.6.5"
#define LDAP_EXOP_X_VERIFY_CREDENTIALS	LDAP_EXOP_VERIFY_CREDENTIALS

#define LDAP_TAG_EXOP_VERIFY_CREDENTIALS_COOKIE	 ((ber_tag_t) 0x80U)
#define LDAP_TAG_EXOP_VERIFY_CREDENTIALS_SCREDS	 ((ber_tag_t) 0x81U)
#define LDAP_TAG_EXOP_VERIFY_CREDENTIALS_CONTROLS ((ber_tag_t) 0xa2U) /* context specific + constructed + 2 */

#define LDAP_EXOP_WHO_AM_I		"1.3.6.1.4.1.4203.1.11.3"		/* RFC 4532 */
#define LDAP_EXOP_X_WHO_AM_I	LDAP_EXOP_WHO_AM_I

/* various works in progress */
#define LDAP_EXOP_TURN		"1.3.6.1.1.19"				/* RFC 4531 */
#define LDAP_EXOP_X_TURN	LDAP_EXOP_TURN

/* LDAP Distributed Procedures <draft-sermersheim-ldap-distproc> */
/* a work in progress */
#define LDAP_X_DISTPROC_BASE		"1.3.6.1.4.1.4203.666.11.6"
#define LDAP_EXOP_X_CHAINEDREQUEST	LDAP_X_DISTPROC_BASE ".1"
#define LDAP_FEATURE_X_CANCHAINOPS	LDAP_X_DISTPROC_BASE ".2"
#define LDAP_CONTROL_X_RETURNCONTREF	LDAP_X_DISTPROC_BASE ".3"
#define LDAP_URLEXT_X_LOCALREFOID	LDAP_X_DISTPROC_BASE ".4"
#define LDAP_URLEXT_X_REFTYPEOID	LDAP_X_DISTPROC_BASE ".5"
#define LDAP_URLEXT_X_SEARCHEDSUBTREEOID \
					LDAP_X_DISTPROC_BASE ".6"
#define LDAP_URLEXT_X_FAILEDNAMEOID	LDAP_X_DISTPROC_BASE ".7"
#define LDAP_URLEXT_X_LOCALREF		"x-localReference"
#define LDAP_URLEXT_X_REFTYPE		"x-referenceType"
#define LDAP_URLEXT_X_SEARCHEDSUBTREE	"x-searchedSubtree"
#define LDAP_URLEXT_X_FAILEDNAME	"x-failedName"

#define LDAP_TXN						"1.3.6.1.1.21" /* RFC 5805 */
#define LDAP_EXOP_TXN_START				LDAP_TXN ".1"
#define LDAP_CONTROL_TXN_SPEC			LDAP_TXN ".2"
#define LDAP_EXOP_TXN_END				LDAP_TXN ".3"
#define LDAP_EXOP_TXN_ABORTED_NOTICE	LDAP_TXN ".4"

/* LDAP Features */
#define LDAP_FEATURE_ALL_OP_ATTRS	"1.3.6.1.4.1.4203.1.5.1"	/* RFC 3673 */
#define LDAP_FEATURE_OBJECTCLASS_ATTRS \
	"1.3.6.1.4.1.4203.1.5.2" /*  @objectClass - new number to be assigned */
#define LDAP_FEATURE_ABSOLUTE_FILTERS "1.3.6.1.4.1.4203.1.5.3"  /* (&) (|) */
#define LDAP_FEATURE_LANGUAGE_TAG_OPTIONS "1.3.6.1.4.1.4203.1.5.4"
#define LDAP_FEATURE_LANGUAGE_RANGE_OPTIONS "1.3.6.1.4.1.4203.1.5.5"
#define LDAP_FEATURE_MODIFY_INCREMENT "1.3.6.1.1.14"

/* LDAP Experimental (works in progress) Features */
#define LDAP_FEATURE_SUBORDINATE_SCOPE \
	"1.3.6.1.4.1.4203.666.8.1" /* "children" */
#define LDAP_FEATURE_CHILDREN_SCOPE LDAP_FEATURE_SUBORDINATE_SCOPE

/*
 * specific LDAP instantiations of BER types we know about
 */

/* Overview of LBER tag construction
 *
 *	Bits
 *	______
 *	8 7 | CLASS
 *	0 0 = UNIVERSAL
 *	0 1 = APPLICATION
 *	1 0 = CONTEXT-SPECIFIC
 *	1 1 = PRIVATE
 *		_____
 *		| 6 | DATA-TYPE
 *		  0 = PRIMITIVE
 *		  1 = CONSTRUCTED
 *			___________
 *			| 5 ... 1 | TAG-NUMBER
 */

/* general stuff */
#define LDAP_TAG_MESSAGE	((ber_tag_t) 0x30U)	/* constructed + 16 */
#define LDAP_TAG_MSGID		((ber_tag_t) 0x02U)	/* integer */

#define LDAP_TAG_LDAPDN		((ber_tag_t) 0x04U)	/* octet string */
#define LDAP_TAG_LDAPCRED	((ber_tag_t) 0x04U)	/* octet string */

#define LDAP_TAG_CONTROLS	((ber_tag_t) 0xa0U)	/* context specific + constructed + 0 */
#define LDAP_TAG_REFERRAL	((ber_tag_t) 0xa3U)	/* context specific + constructed + 3 */

#define LDAP_TAG_NEWSUPERIOR	((ber_tag_t) 0x80U)	/* context-specific + primitive + 0 */

#define LDAP_TAG_EXOP_REQ_OID   ((ber_tag_t) 0x80U)	/* context specific + primitive */
#define LDAP_TAG_EXOP_REQ_VALUE ((ber_tag_t) 0x81U)	/* context specific + primitive */
#define LDAP_TAG_EXOP_RES_OID   ((ber_tag_t) 0x8aU)	/* context specific + primitive */
#define LDAP_TAG_EXOP_RES_VALUE ((ber_tag_t) 0x8bU)	/* context specific + primitive */

#define LDAP_TAG_IM_RES_OID   ((ber_tag_t) 0x80U)	/* context specific + primitive */
#define LDAP_TAG_IM_RES_VALUE ((ber_tag_t) 0x81U)	/* context specific + primitive */

#define LDAP_TAG_SASL_RES_CREDS	((ber_tag_t) 0x87U)	/* context specific + primitive */

/* LDAP Request Messages */
#define LDAP_REQ_BIND		((ber_tag_t) 0x60U)	/* application + constructed */
#define LDAP_REQ_UNBIND		((ber_tag_t) 0x42U)	/* application + primitive   */
#define LDAP_REQ_SEARCH		((ber_tag_t) 0x63U)	/* application + constructed */
#define LDAP_REQ_MODIFY		((ber_tag_t) 0x66U)	/* application + constructed */
#define LDAP_REQ_ADD		((ber_tag_t) 0x68U)	/* application + constructed */
#define LDAP_REQ_DELETE		((ber_tag_t) 0x4aU)	/* application + primitive   */
#define LDAP_REQ_MODDN		((ber_tag_t) 0x6cU)	/* application + constructed */
#define LDAP_REQ_MODRDN		LDAP_REQ_MODDN
#define LDAP_REQ_RENAME		LDAP_REQ_MODDN
#define LDAP_REQ_COMPARE	((ber_tag_t) 0x6eU)	/* application + constructed */
#define LDAP_REQ_ABANDON	((ber_tag_t) 0x50U)	/* application + primitive   */
#define LDAP_REQ_EXTENDED	((ber_tag_t) 0x77U)	/* application + constructed */

/* LDAP Response Messages */
#define LDAP_RES_BIND		((ber_tag_t) 0x61U)	/* application + constructed */
#define LDAP_RES_SEARCH_ENTRY	((ber_tag_t) 0x64U)	/* application + constructed */
#define LDAP_RES_SEARCH_REFERENCE	((ber_tag_t) 0x73U)	/* V3: application + constructed */
#define LDAP_RES_SEARCH_RESULT	((ber_tag_t) 0x65U)	/* application + constructed */
#define LDAP_RES_MODIFY		((ber_tag_t) 0x67U)	/* application + constructed */
#define LDAP_RES_ADD		((ber_tag_t) 0x69U)	/* application + constructed */
#define LDAP_RES_DELETE		((ber_tag_t) 0x6bU)	/* application + constructed */
#define LDAP_RES_MODDN		((ber_tag_t) 0x6dU)	/* application + constructed */
#define LDAP_RES_MODRDN		LDAP_RES_MODDN	/* application + constructed */
#define LDAP_RES_RENAME		LDAP_RES_MODDN	/* application + constructed */
#define LDAP_RES_COMPARE	((ber_tag_t) 0x6fU)	/* application + constructed */
#define LDAP_RES_EXTENDED	((ber_tag_t) 0x78U)	/* V3: application + constructed */
#define LDAP_RES_INTERMEDIATE	((ber_tag_t) 0x79U) /* V3+: application + constructed */

#define LDAP_RES_ANY			(-1)
#define LDAP_RES_UNSOLICITED	(0)


/* sasl methods */
#define LDAP_SASL_SIMPLE	((char*)0)
#define LDAP_SASL_NULL		("")


/* authentication methods available */
#define LDAP_AUTH_NONE   ((ber_tag_t) 0x00U) /* no authentication */
#define LDAP_AUTH_SIMPLE ((ber_tag_t) 0x80U) /* context specific + primitive */
#define LDAP_AUTH_SASL   ((ber_tag_t) 0xa3U) /* context specific + constructed */
#define LDAP_AUTH_KRBV4  ((ber_tag_t) 0xffU) /* means do both of the following */
#define LDAP_AUTH_KRBV41 ((ber_tag_t) 0x81U) /* context specific + primitive */
#define LDAP_AUTH_KRBV42 ((ber_tag_t) 0x82U) /* context specific + primitive */

/* used by the Windows API but not used on the wire */
#define LDAP_AUTH_NEGOTIATE ((ber_tag_t) 0x04FFU)

/* filter types */
#define LDAP_FILTER_AND	((ber_tag_t) 0xa0U)	/* context specific + constructed */
#define LDAP_FILTER_OR	((ber_tag_t) 0xa1U)	/* context specific + constructed */
#define LDAP_FILTER_NOT	((ber_tag_t) 0xa2U)	/* context specific + constructed */
#define LDAP_FILTER_EQUALITY ((ber_tag_t) 0xa3U) /* context specific + constructed */
#define LDAP_FILTER_SUBSTRINGS ((ber_tag_t) 0xa4U) /* context specific + constructed */
#define LDAP_FILTER_GE ((ber_tag_t) 0xa5U) /* context specific + constructed */
#define LDAP_FILTER_LE ((ber_tag_t) 0xa6U) /* context specific + constructed */
#define LDAP_FILTER_PRESENT ((ber_tag_t) 0x87U) /* context specific + primitive   */
#define LDAP_FILTER_APPROX ((ber_tag_t) 0xa8U)	/* context specific + constructed */
#define LDAP_FILTER_EXT	((ber_tag_t) 0xa9U)	/* context specific + constructed */

/* extended filter component types */
#define LDAP_FILTER_EXT_OID		((ber_tag_t) 0x81U)	/* context specific */
#define LDAP_FILTER_EXT_TYPE	((ber_tag_t) 0x82U)	/* context specific */
#define LDAP_FILTER_EXT_VALUE	((ber_tag_t) 0x83U)	/* context specific */
#define LDAP_FILTER_EXT_DNATTRS	((ber_tag_t) 0x84U)	/* context specific */

/* substring filter component types */
#define LDAP_SUBSTRING_INITIAL	((ber_tag_t) 0x80U)	/* context specific */
#define LDAP_SUBSTRING_ANY		((ber_tag_t) 0x81U)	/* context specific */
#define LDAP_SUBSTRING_FINAL	((ber_tag_t) 0x82U)	/* context specific */

/* search scopes */
#define LDAP_SCOPE_BASE			((ber_int_t) 0x0000)
#define LDAP_SCOPE_BASEOBJECT	LDAP_SCOPE_BASE
#define LDAP_SCOPE_ONELEVEL		((ber_int_t) 0x0001)
#define LDAP_SCOPE_ONE			LDAP_SCOPE_ONELEVEL
#define LDAP_SCOPE_SUBTREE		((ber_int_t) 0x0002)
#define LDAP_SCOPE_SUB			LDAP_SCOPE_SUBTREE
#define LDAP_SCOPE_SUBORDINATE	((ber_int_t) 0x0003) /* OpenLDAP extension */
#define LDAP_SCOPE_CHILDREN		LDAP_SCOPE_SUBORDINATE
#define LDAP_SCOPE_DEFAULT		((ber_int_t) -1)	 /* OpenLDAP extension */

/* substring filter component types */
#define LDAP_SUBSTRING_INITIAL	((ber_tag_t) 0x80U)	/* context specific */
#define LDAP_SUBSTRING_ANY		((ber_tag_t) 0x81U)	/* context specific */
#define LDAP_SUBSTRING_FINAL	((ber_tag_t) 0x82U)	/* context specific */

/*
 * LDAP Result Codes
 */
#define LDAP_SUCCESS				0x00

#define LDAP_RANGE(n,x,y)	(((x) <= (n)) && ((n) <= (y)))

#define LDAP_OPERATIONS_ERROR		0x01
#define LDAP_PROTOCOL_ERROR			0x02
#define LDAP_TIMELIMIT_EXCEEDED		0x03
#define LDAP_SIZELIMIT_EXCEEDED		0x04
#define LDAP_COMPARE_FALSE			0x05
#define LDAP_COMPARE_TRUE			0x06
#define LDAP_AUTH_METHOD_NOT_SUPPORTED	0x07
#define LDAP_STRONG_AUTH_NOT_SUPPORTED	LDAP_AUTH_METHOD_NOT_SUPPORTED
#define LDAP_STRONG_AUTH_REQUIRED	0x08
#define LDAP_STRONGER_AUTH_REQUIRED	LDAP_STRONG_AUTH_REQUIRED
#define LDAP_PARTIAL_RESULTS		0x09	/* LDAPv2+ (not LDAPv3) */

#define	LDAP_REFERRAL				0x0a /* LDAPv3 */
#define LDAP_ADMINLIMIT_EXCEEDED	0x0b /* LDAPv3 */
#define	LDAP_UNAVAILABLE_CRITICAL_EXTENSION	0x0c /* LDAPv3 */
#define LDAP_CONFIDENTIALITY_REQUIRED	0x0d /* LDAPv3 */
#define	LDAP_SASL_BIND_IN_PROGRESS	0x0e /* LDAPv3 */

#define LDAP_ATTR_ERROR(n)	LDAP_RANGE((n),0x10,0x15) /* 16-21 */

#define LDAP_NO_SUCH_ATTRIBUTE		0x10
#define LDAP_UNDEFINED_TYPE			0x11
#define LDAP_INAPPROPRIATE_MATCHING	0x12
#define LDAP_CONSTRAINT_VIOLATION	0x13
#define LDAP_TYPE_OR_VALUE_EXISTS	0x14
#define LDAP_INVALID_SYNTAX			0x15

#define LDAP_NAME_ERROR(n)	LDAP_RANGE((n),0x20,0x24) /* 32-34,36 */

#define LDAP_NO_SUCH_OBJECT			0x20
#define LDAP_ALIAS_PROBLEM			0x21
#define LDAP_INVALID_DN_SYNTAX		0x22
#define LDAP_IS_LEAF				0x23 /* not LDAPv3 */
#define LDAP_ALIAS_DEREF_PROBLEM	0x24

#define LDAP_SECURITY_ERROR(n)	LDAP_RANGE((n),0x2F,0x32) /* 47-50 */

#define LDAP_X_PROXY_AUTHZ_FAILURE	0x2F /* LDAPv3 proxy authorization */
#define LDAP_INAPPROPRIATE_AUTH		0x30
#define LDAP_INVALID_CREDENTIALS	0x31
#define LDAP_INSUFFICIENT_ACCESS	0x32

#define LDAP_SERVICE_ERROR(n)	LDAP_RANGE((n),0x33,0x36) /* 51-54 */

#define LDAP_BUSY					0x33
#define LDAP_UNAVAILABLE			0x34
#define LDAP_UNWILLING_TO_PERFORM	0x35
#define LDAP_LOOP_DETECT			0x36

#define LDAP_UPDATE_ERROR(n)	LDAP_RANGE((n),0x40,0x47) /* 64-69,71 */

#define LDAP_NAMING_VIOLATION		0x40
#define LDAP_OBJECT_CLASS_VIOLATION	0x41
#define LDAP_NOT_ALLOWED_ON_NONLEAF	0x42
#define LDAP_NOT_ALLOWED_ON_RDN		0x43
#define LDAP_ALREADY_EXISTS			0x44
#define LDAP_NO_OBJECT_CLASS_MODS	0x45
#define LDAP_RESULTS_TOO_LARGE		0x46 /* CLDAP */
#define LDAP_AFFECTS_MULTIPLE_DSAS	0x47

#define LDAP_VLV_ERROR				0x4C

#define LDAP_OTHER					0x50

/* LCUP operation codes (113-117) - not implemented */
#define LDAP_CUP_RESOURCES_EXHAUSTED	0x71
#define LDAP_CUP_SECURITY_VIOLATION		0x72
#define LDAP_CUP_INVALID_DATA			0x73
#define LDAP_CUP_UNSUPPORTED_SCHEME		0x74
#define LDAP_CUP_RELOAD_REQUIRED		0x75

/* Cancel operation codes (118-121) */
#define LDAP_CANCELLED				0x76
#define LDAP_NO_SUCH_OPERATION		0x77
#define LDAP_TOO_LATE				0x78
#define LDAP_CANNOT_CANCEL			0x79

/* Assertion control (122) */ 
#define LDAP_ASSERTION_FAILED		0x7A

/* Proxied Authorization Denied (123) */ 
#define LDAP_PROXIED_AUTHORIZATION_DENIED		0x7B

/* Experimental result codes */
#define LDAP_E_ERROR(n)	LDAP_RANGE((n),0x1000,0x3FFF)

/* LDAP Sync (4096) */
#define LDAP_SYNC_REFRESH_REQUIRED		0x1000


/* Private Use result codes */
#define LDAP_X_ERROR(n)	LDAP_RANGE((n),0x4000,0xFFFF)

#define LDAP_X_SYNC_REFRESH_REQUIRED	0x4100 /* defunct */
#define LDAP_X_ASSERTION_FAILED			0x410f /* defunct */

/* for the LDAP No-Op control */
#define LDAP_X_NO_OPERATION				0x410e

/* for the Chaining Behavior control (consecutive result codes requested;
 * see <draft-sermersheim-ldap-chaining> ) */
#ifdef LDAP_CONTROL_X_CHAINING_BEHAVIOR
#define	LDAP_X_NO_REFERRALS_FOUND		0x4110
#define LDAP_X_CANNOT_CHAIN			0x4111
#endif

/* for Distributed Procedures (see <draft-sermersheim-ldap-distproc>) */
#ifdef LDAP_X_DISTPROC_BASE
#define LDAP_X_INVALIDREFERENCE			0x4112
#endif

#define LDAP_TXN_SPECIFY_OKAY		0x4120
#define LDAP_TXN_ID_INVALID			0x4121

/* API Error Codes
 *
 * Based on draft-ietf-ldap-c-api-xx
 * but with new negative code values
 */
#define LDAP_API_ERROR(n)		((n)<0)
#define LDAP_API_RESULT(n)		((n)<=0)

#define LDAP_SERVER_DOWN				(-1)
#define LDAP_LOCAL_ERROR				(-2)
#define LDAP_ENCODING_ERROR				(-3)
#define LDAP_DECODING_ERROR				(-4)
#define LDAP_TIMEOUT					(-5)
#define LDAP_AUTH_UNKNOWN				(-6)
#define LDAP_FILTER_ERROR				(-7)
#define LDAP_USER_CANCELLED				(-8)
#define LDAP_PARAM_ERROR				(-9)
#define LDAP_NO_MEMORY					(-10)
#define LDAP_CONNECT_ERROR				(-11)
#define LDAP_NOT_SUPPORTED				(-12)
#define LDAP_CONTROL_NOT_FOUND			(-13)
#define LDAP_NO_RESULTS_RETURNED		(-14)
#define LDAP_MORE_RESULTS_TO_RETURN		(-15)	/* Obsolete */
#define LDAP_CLIENT_LOOP				(-16)
#define LDAP_REFERRAL_LIMIT_EXCEEDED	(-17)
#define	LDAP_X_CONNECTING			(-18)


/*
 * This structure represents both ldap messages and ldap responses.
 * These are really the same, except in the case of search responses,
 * where a response has multiple messages.
 */

typedef struct ldapmsg LDAPMessage;

/* for modifications */
typedef struct ldapmod {
	int		mod_op;

#define LDAP_MOD_OP			(0x0007)
#define LDAP_MOD_ADD		(0x0000)
#define LDAP_MOD_DELETE		(0x0001)
#define LDAP_MOD_REPLACE	(0x0002)
#define LDAP_MOD_INCREMENT	(0x0003) /* OpenLDAP extension */
#define LDAP_MOD_BVALUES	(0x0080)
/* IMPORTANT: do not use code 0x1000 (or above),
 * it is used internally by the backends!
 * (see ldap/servers/slapd/slap.h)
 */

	char		*mod_type;
	union mod_vals_u {
		char		**modv_strvals;
		struct berval	**modv_bvals;
	} mod_vals;
#define mod_values	mod_vals.modv_strvals
#define mod_bvalues	mod_vals.modv_bvals
} LDAPMod;

/*
 * structure representing an ldap session which can
 * encompass connections to multiple servers (in the
 * face of referrals).
 */
typedef struct ldap LDAP;

#define LDAP_DEREF_NEVER		0x00
#define LDAP_DEREF_SEARCHING	0x01
#define LDAP_DEREF_FINDING		0x02
#define LDAP_DEREF_ALWAYS		0x03

#define LDAP_NO_LIMIT			0

/* how many messages to retrieve results for */
#define LDAP_MSG_ONE			0x00
#define LDAP_MSG_ALL			0x01
#define LDAP_MSG_RECEIVED		0x02

/*
 * types for ldap URL handling
 */
typedef struct ldap_url_desc {
	struct ldap_url_desc *lud_next;
	char	*lud_scheme;
	char	*lud_host;
	int		lud_port;
	char	*lud_dn;
	char	**lud_attrs;
	int		lud_scope;
	char	*lud_filter;
	char	**lud_exts;
	int		lud_crit_exts;
} LDAPURLDesc;

#define LDAP_URL_SUCCESS		0x00	/* Success */
#define LDAP_URL_ERR_MEM		0x01	/* can't allocate memory space */
#define LDAP_URL_ERR_PARAM		0x02	/* parameter is bad */

#define LDAP_URL_ERR_BADSCHEME	0x03	/* URL doesn't begin with "ldap[si]://" */
#define LDAP_URL_ERR_BADENCLOSURE 0x04	/* URL is missing trailing ">" */
#define LDAP_URL_ERR_BADURL		0x05	/* URL is bad */
#define LDAP_URL_ERR_BADHOST	0x06	/* host port is bad */
#define LDAP_URL_ERR_BADATTRS	0x07	/* bad (or missing) attributes */
#define LDAP_URL_ERR_BADSCOPE	0x08	/* scope string is invalid (or missing) */
#define LDAP_URL_ERR_BADFILTER	0x09	/* bad or missing filter */
#define LDAP_URL_ERR_BADEXTS	0x0a	/* bad or missing extensions */

/*
 * LDAP sync (RFC4533) API
 */

typedef struct ldap_sync_t ldap_sync_t;

typedef enum {
	/* these are private - the client should never see them */
	LDAP_SYNC_CAPI_NONE		= -1,

	LDAP_SYNC_CAPI_PHASE_FLAG	= 0x10U,
	LDAP_SYNC_CAPI_IDSET_FLAG	= 0x20U,
	LDAP_SYNC_CAPI_DONE_FLAG	= 0x40U,

	/* these are passed to ls_search_entry() */
	LDAP_SYNC_CAPI_PRESENT		= LDAP_SYNC_PRESENT,
	LDAP_SYNC_CAPI_ADD		= LDAP_SYNC_ADD,
	LDAP_SYNC_CAPI_MODIFY		= LDAP_SYNC_MODIFY,
	LDAP_SYNC_CAPI_DELETE		= LDAP_SYNC_DELETE,

	/* these are passed to ls_intermediate() */
	LDAP_SYNC_CAPI_PRESENTS		= ( LDAP_SYNC_CAPI_PHASE_FLAG | LDAP_SYNC_CAPI_PRESENT ),
	LDAP_SYNC_CAPI_DELETES		= ( LDAP_SYNC_CAPI_PHASE_FLAG | LDAP_SYNC_CAPI_DELETE ),

	LDAP_SYNC_CAPI_PRESENTS_IDSET	= ( LDAP_SYNC_CAPI_PRESENTS | LDAP_SYNC_CAPI_IDSET_FLAG ),
	LDAP_SYNC_CAPI_DELETES_IDSET	= ( LDAP_SYNC_CAPI_DELETES | LDAP_SYNC_CAPI_IDSET_FLAG ),

	LDAP_SYNC_CAPI_DONE		= ( LDAP_SYNC_CAPI_DONE_FLAG | LDAP_SYNC_CAPI_PRESENTS )
} ldap_sync_refresh_t;

/*
 * Called when an entry is returned by ldap_result().
 * If phase is LDAP_SYNC_CAPI_ADD or LDAP_SYNC_CAPI_MODIFY,
 * the entry has been either added or modified, and thus
 * the complete view of the entry should be in the LDAPMessage.
 * If phase is LDAP_SYNC_CAPI_PRESENT or LDAP_SYNC_CAPI_DELETE,
 * only the DN should be in the LDAPMessage.
 */
typedef int (*ldap_sync_search_entry_f) LDAP_P((
	ldap_sync_t			*ls,
	LDAPMessage			*msg,
	struct berval			*entryUUID,
	ldap_sync_refresh_t		phase ));

/*
 * Called when a reference is returned; the client should know 
 * what to do with it.
 */
typedef int (*ldap_sync_search_reference_f) LDAP_P((
	ldap_sync_t			*ls,
	LDAPMessage			*msg ));

/*
 * Called when specific intermediate/final messages are returned.
 * If phase is LDAP_SYNC_CAPI_PRESENTS or LDAP_SYNC_CAPI_DELETES,
 * a "presents" or "deletes" phase begins.
 * If phase is LDAP_SYNC_CAPI_DONE, a special "presents" phase
 * with refreshDone set to "TRUE" has been returned, to indicate
 * that the refresh phase of a refreshAndPersist is complete.
 * In the above cases, syncUUIDs is NULL.
 *
 * If phase is LDAP_SYNC_CAPI_PRESENTS_IDSET or 
 * LDAP_SYNC_CAPI_DELETES_IDSET, syncUUIDs is an array of UUIDs
 * that are either present or have been deleted.
 */
typedef int (*ldap_sync_intermediate_f) LDAP_P((
	ldap_sync_t			*ls,
	LDAPMessage			*msg,
	BerVarray			syncUUIDs,
	ldap_sync_refresh_t		phase ));

/*
 * Called when a searchResultDone is returned.  In refreshAndPersist,
 * this can only occur if the search for any reason is being terminated
 * by the server.
 */
typedef int (*ldap_sync_search_result_f) LDAP_P((
	ldap_sync_t			*ls,
	LDAPMessage			*msg,
	int				refreshDeletes ));

/*
 * This structure contains all information about the persistent search;
 * the caller is responsible for connecting, setting version, binding, tls...
 */
struct ldap_sync_t {
	/* conf search params */
	char				*ls_base;
	int				ls_scope;
	char				*ls_filter;
	char				**ls_attrs;
	int				ls_timelimit;
	int				ls_sizelimit;

	/* poll timeout */
	int				ls_timeout;

	/* helpers - add as appropriate */
	ldap_sync_search_entry_f	ls_search_entry;
	ldap_sync_search_reference_f	ls_search_reference;
	ldap_sync_intermediate_f	ls_intermediate;
	ldap_sync_search_result_f	ls_search_result;

	/* set by the caller as appropriate */
	void				*ls_private;

	/* conn stuff */
	LDAP				*ls_ld;

	/* --- the parameters below are private - do not modify --- */

	/* FIXME: make the structure opaque, and provide an interface
	 * to modify the public values? */

	/* result stuff */
	int				ls_msgid;

	/* sync stuff */
	/* needed by refreshOnly */
	int				ls_reloadHint;

	/* opaque - need to pass between sessions, updated by the API */
	struct berval			ls_cookie;

	/* state variable - do not modify */
	ldap_sync_refresh_t		ls_refreshPhase;
};

/*
 * End of LDAP sync (RFC4533) API
 */

/*
 * Connection callbacks...
 */
struct ldap_conncb;
struct sockaddr;

/* Called after a connection is established */
typedef int (ldap_conn_add_f) LDAP_P(( LDAP *ld, Sockbuf *sb, LDAPURLDesc *srv, struct sockaddr *addr,
	struct ldap_conncb *ctx ));
/* Called before a connection is closed */
typedef void (ldap_conn_del_f) LDAP_P(( LDAP *ld, Sockbuf *sb, struct ldap_conncb *ctx ));

/* Callbacks are pushed on a stack. Last one pushed is first one executed. The
 * delete callback is called with a NULL Sockbuf just before freeing the LDAP handle.
 */
typedef struct ldap_conncb {
	ldap_conn_add_f *lc_add;
	ldap_conn_del_f *lc_del;
	void *lc_arg;
} ldap_conncb;

/*
 * The API draft spec says we should declare (or cause to be declared)
 * 'struct timeval'.   We don't.  See IETF LDAPext discussions.
 */
struct timeval;

/*
 * in options.c:
 */
LDAP_F( int )
ldap_get_option LDAP_P((
	LDAP *ld,
	int option,
	void *outvalue));

LDAP_F( int )
ldap_set_option LDAP_P((
	LDAP *ld,
	int option,
	LDAP_CONST void *invalue));

/* V3 REBIND Function Callback Prototype */
typedef int (LDAP_REBIND_PROC) LDAP_P((
	LDAP *ld, LDAP_CONST char *url,
	ber_tag_t request, ber_int_t msgid,
	void *params ));

LDAP_F( int )
ldap_set_rebind_proc LDAP_P((
	LDAP *ld,
	LDAP_REBIND_PROC *rebind_proc,
	void *params ));

/* V3 referral selection Function Callback Prototype */
typedef int (LDAP_NEXTREF_PROC) LDAP_P((
	LDAP *ld, char ***refsp, int *cntp,
	void *params ));

LDAP_F( int )
ldap_set_nextref_proc LDAP_P((
	LDAP *ld,
	LDAP_NEXTREF_PROC *nextref_proc,
	void *params ));

/* V3 URLLIST Function Callback Prototype */
typedef int (LDAP_URLLIST_PROC) LDAP_P((
	LDAP *ld, 
	LDAPURLDesc **urllist,
	LDAPURLDesc **url,
	void *params ));

LDAP_F( int )
ldap_set_urllist_proc LDAP_P((
	LDAP *ld,
	LDAP_URLLIST_PROC *urllist_proc,
	void *params ));

/*
 * in controls.c:
 */
#if LDAP_DEPRECATED	
LDAP_F( int )
ldap_create_control LDAP_P((	/* deprecated, use ldap_control_create */
	LDAP_CONST char *requestOID,
	BerElement *ber,
	int iscritical,
	LDAPControl **ctrlp ));

LDAP_F( LDAPControl * )
ldap_find_control LDAP_P((	/* deprecated, use ldap_control_find */
	LDAP_CONST char *oid,
	LDAPControl **ctrls ));
#endif

LDAP_F( int )
ldap_control_create LDAP_P((
	LDAP_CONST char *requestOID,
	int iscritical,
	struct berval *value,
	int dupval,
	LDAPControl **ctrlp ));

LDAP_F( LDAPControl * )
ldap_control_find LDAP_P((
	LDAP_CONST char *oid,
	LDAPControl **ctrls,
	LDAPControl ***nextctrlp ));

LDAP_F( void )
ldap_control_free LDAP_P((
	LDAPControl *ctrl ));

LDAP_F( void )
ldap_controls_free LDAP_P((
	LDAPControl **ctrls ));

LDAP_F( LDAPControl ** )
ldap_controls_dup LDAP_P((
	LDAPControl *LDAP_CONST *controls ));

LDAP_F( LDAPControl * )
ldap_control_dup LDAP_P((
	LDAP_CONST LDAPControl *c ));

/*
 * in dnssrv.c:
 */
LDAP_F( int )
ldap_domain2dn LDAP_P((
	LDAP_CONST char* domain,
	char** dn ));

LDAP_F( int )
ldap_dn2domain LDAP_P((
	LDAP_CONST char* dn,
	char** domain ));

LDAP_F( int )
ldap_domain2hostlist LDAP_P((
	LDAP_CONST char *domain,
	char** hostlist ));

/*
 * in extended.c:
 */
LDAP_F( int )
ldap_extended_operation LDAP_P((
	LDAP			*ld,
	LDAP_CONST char	*reqoid,
	struct berval	*reqdata,
	LDAPControl		**serverctrls,
	LDAPControl		**clientctrls,
	int				*msgidp ));

LDAP_F( int )
ldap_extended_operation_s LDAP_P((
	LDAP			*ld,
	LDAP_CONST char	*reqoid,
	struct berval	*reqdata,
	LDAPControl		**serverctrls,
	LDAPControl		**clientctrls,
	char			**retoidp,
	struct berval	**retdatap ));

LDAP_F( int )
ldap_parse_extended_result LDAP_P((
	LDAP			*ld,
	LDAPMessage		*res,
	char			**retoidp,
	struct berval	**retdatap,
	int				freeit ));

LDAP_F( int )
ldap_parse_intermediate LDAP_P((
	LDAP			*ld,
	LDAPMessage		*res,
	char			**retoidp,
	struct berval	**retdatap,
	LDAPControl		***serverctrls,
	int				freeit ));


/*
 * in abandon.c:
 */
LDAP_F( int )
ldap_abandon_ext LDAP_P((
	LDAP			*ld,
	int				msgid,
	LDAPControl		**serverctrls,
	LDAPControl		**clientctrls ));

#if LDAP_DEPRECATED	
LDAP_F( int )
ldap_abandon LDAP_P((	/* deprecated, use ldap_abandon_ext */
	LDAP *ld,
	int msgid ));
#endif

/*
 * in add.c:
 */
LDAP_F( int )
ldap_add_ext LDAP_P((
	LDAP			*ld,
	LDAP_CONST char	*dn,
	LDAPMod			**attrs,
	LDAPControl		**serverctrls,
	LDAPControl		**clientctrls,
	int 			*msgidp ));

LDAP_F( int )
ldap_add_ext_s LDAP_P((
	LDAP			*ld,
	LDAP_CONST char	*dn,
	LDAPMod			**attrs,
	LDAPControl		**serverctrls,
	LDAPControl		**clientctrls ));

#if LDAP_DEPRECATED
LDAP_F( int )
ldap_add LDAP_P((	/* deprecated, use ldap_add_ext */
	LDAP *ld,
	LDAP_CONST char *dn,
	LDAPMod **attrs ));

LDAP_F( int )
ldap_add_s LDAP_P((	/* deprecated, use ldap_add_ext_s */
	LDAP *ld,
	LDAP_CONST char *dn,
	LDAPMod **attrs ));
#endif


/*
 * in sasl.c:
 */
LDAP_F( int )
ldap_sasl_bind LDAP_P((
	LDAP			*ld,
	LDAP_CONST char	*dn,
	LDAP_CONST char	*mechanism,
	struct berval	*cred,
	LDAPControl		**serverctrls,
	LDAPControl		**clientctrls,
	int				*msgidp ));

/* Interaction flags (should be passed about in a control)
 *  Automatic (default): use defaults, prompt otherwise
 *  Interactive: prompt always
 *  Quiet: never prompt
 */
#define LDAP_SASL_AUTOMATIC		0U
#define LDAP_SASL_INTERACTIVE	1U
#define LDAP_SASL_QUIET			2U

/*
 * V3 SASL Interaction Function Callback Prototype
 *	when using Cyrus SASL, interact is pointer to sasl_interact_t
 *  should likely passed in a control (and provided controls)
 */
typedef int (LDAP_SASL_INTERACT_PROC) LDAP_P((
	LDAP *ld, unsigned flags, void* defaults, void *interact ));

LDAP_F( int )
ldap_sasl_interactive_bind LDAP_P((
	LDAP *ld,
	LDAP_CONST char *dn, /* usually NULL */
	LDAP_CONST char *saslMechanism,
	LDAPControl **serverControls,
	LDAPControl **clientControls,

	/* should be client controls */
	unsigned flags,
	LDAP_SASL_INTERACT_PROC *proc,
	void *defaults,
	
	/* as obtained from ldap_result() */
	LDAPMessage *result,

	/* returned during bind processing */
	const char **rmech,
	int *msgid ));

LDAP_F( int )
ldap_sasl_interactive_bind_s LDAP_P((
	LDAP *ld,
	LDAP_CONST char *dn, /* usually NULL */
	LDAP_CONST char *saslMechanism,
	LDAPControl **serverControls,
	LDAPControl **clientControls,

	/* should be client controls */
	unsigned flags,
	LDAP_SASL_INTERACT_PROC *proc,
	void *defaults ));

LDAP_F( int )
ldap_sasl_bind_s LDAP_P((
	LDAP			*ld,
	LDAP_CONST char	*dn,
	LDAP_CONST char	*mechanism,
	struct berval	*cred,
	LDAPControl		**serverctrls,
	LDAPControl		**clientctrls,
	struct berval	**servercredp ));

LDAP_F( int )
ldap_parse_sasl_bind_result LDAP_P((
	LDAP			*ld,
	LDAPMessage		*res,
	struct berval	**servercredp,
	int				freeit ));

#if LDAP_DEPRECATED
/*
 * in bind.c:
 *	(deprecated)
 */
LDAP_F( int )
ldap_bind LDAP_P((	/* deprecated, use ldap_sasl_bind */
	LDAP *ld,
	LDAP_CONST char *who,
	LDAP_CONST char *passwd,
	int authmethod ));

LDAP_F( int )
ldap_bind_s LDAP_P((	/* deprecated, use ldap_sasl_bind_s */
	LDAP *ld,
	LDAP_CONST char *who,
	LDAP_CONST char *cred,
	int authmethod ));

/*
 * in sbind.c:
 */
LDAP_F( int )
ldap_simple_bind LDAP_P(( /* deprecated, use ldap_sasl_bind */
	LDAP *ld,
	LDAP_CONST char *who,
	LDAP_CONST char *passwd ));

LDAP_F( int )
ldap_simple_bind_s LDAP_P(( /* deprecated, use ldap_sasl_bind_s */
	LDAP *ld,
	LDAP_CONST char *who,
	LDAP_CONST char *passwd ));

#endif


/*
 * in compare.c:
 */
LDAP_F( int )
ldap_compare_ext LDAP_P((
	LDAP			*ld,
	LDAP_CONST char	*dn,
	LDAP_CONST char	*attr,
	struct berval	*bvalue,
	LDAPControl		**serverctrls,
	LDAPControl		**clientctrls,
	int 			*msgidp ));

LDAP_F( int )
ldap_compare_ext_s LDAP_P((
	LDAP			*ld,
	LDAP_CONST char	*dn,
	LDAP_CONST char	*attr,
	struct berval	*bvalue,
	LDAPControl		**serverctrls,
	LDAPControl		**clientctrls ));

#if LDAP_DEPRECATED
LDAP_F( int )
ldap_compare LDAP_P((	/* deprecated, use ldap_compare_ext */
	LDAP *ld,
	LDAP_CONST char *dn,
	LDAP_CONST char *attr,
	LDAP_CONST char *value ));

LDAP_F( int )
ldap_compare_s LDAP_P((	/* deprecated, use ldap_compare_ext_s */
	LDAP *ld,
	LDAP_CONST char *dn,
	LDAP_CONST char *attr,
	LDAP_CONST char *value ));
#endif


/*
 * in delete.c:
 */
LDAP_F( int )
ldap_delete_ext LDAP_P((
	LDAP			*ld,
	LDAP_CONST char	*dn,
	LDAPControl		**serverctrls,
	LDAPControl		**clientctrls,
	int 			*msgidp ));

LDAP_F( int )
ldap_delete_ext_s LDAP_P((
	LDAP			*ld,
	LDAP_CONST char	*dn,
	LDAPControl		**serverctrls,
	LDAPControl		**clientctrls ));

#if LDAP_DEPRECATED
LDAP_F( int )
ldap_delete LDAP_P((	/* deprecated, use ldap_delete_ext */
	LDAP *ld,
	LDAP_CONST char *dn ));

LDAP_F( int )
ldap_delete_s LDAP_P((	/* deprecated, use ldap_delete_ext_s */
	LDAP *ld,
	LDAP_CONST char *dn ));
#endif


/*
 * in error.c:
 */
LDAP_F( int )
ldap_parse_result LDAP_P((
	LDAP			*ld,
	LDAPMessage		*res,
	int				*errcodep,
	char			**matcheddnp,
	char			**diagmsgp,
	char			***referralsp,
	LDAPControl		***serverctrls,
	int				freeit ));

LDAP_F( char * )
ldap_err2string LDAP_P((
	int err ));

#if LDAP_DEPRECATED
LDAP_F( int )
ldap_result2error LDAP_P((	/* deprecated, use ldap_parse_result */
	LDAP *ld,
	LDAPMessage *r,
	int freeit ));

LDAP_F( void )
ldap_perror LDAP_P((	/* deprecated, use ldap_err2string */
	LDAP *ld,
	LDAP_CONST char *s ));
#endif


/*
 * in modify.c:
 */
LDAP_F( int )
ldap_modify_ext LDAP_P((
	LDAP			*ld,
	LDAP_CONST char	*dn,
	LDAPMod			**mods,
	LDAPControl		**serverctrls,
	LDAPControl		**clientctrls,
	int 			*msgidp ));

LDAP_F( int )
ldap_modify_ext_s LDAP_P((
	LDAP			*ld,
	LDAP_CONST char	*dn,
	LDAPMod			**mods,
	LDAPControl		**serverctrls,
	LDAPControl		**clientctrls ));

#if LDAP_DEPRECATED
LDAP_F( int )
ldap_modify LDAP_P((	/* deprecated, use ldap_modify_ext */
	LDAP *ld,
	LDAP_CONST char *dn,
	LDAPMod **mods ));

LDAP_F( int )
ldap_modify_s LDAP_P((	/* deprecated, use ldap_modify_ext_s */
	LDAP *ld,
	LDAP_CONST char *dn,
	LDAPMod **mods ));
#endif


/*
 * in modrdn.c:
 */
LDAP_F( int )
ldap_rename LDAP_P((
	LDAP *ld,
	LDAP_CONST char *dn,
	LDAP_CONST char *newrdn,
	LDAP_CONST char *newSuperior,
	int deleteoldrdn,
	LDAPControl **sctrls,
	LDAPControl **cctrls,
	int *msgidp ));

LDAP_F( int )
ldap_rename_s LDAP_P((
	LDAP *ld,
	LDAP_CONST char *dn,
	LDAP_CONST char *newrdn,
	LDAP_CONST char *newSuperior,
	int deleteoldrdn,
	LDAPControl **sctrls,
	LDAPControl **cctrls ));

#if LDAP_DEPRECATED
LDAP_F( int )
ldap_rename2 LDAP_P((	/* deprecated, use ldap_rename */
	LDAP *ld,
	LDAP_CONST char *dn,
	LDAP_CONST char *newrdn,
	LDAP_CONST char *newSuperior,
	int deleteoldrdn ));

LDAP_F( int )
ldap_rename2_s LDAP_P((	/* deprecated, use ldap_rename_s */
	LDAP *ld,
	LDAP_CONST char *dn,
	LDAP_CONST char *newrdn,
	LDAP_CONST char *newSuperior,
	int deleteoldrdn ));

LDAP_F( int )
ldap_modrdn LDAP_P((	/* deprecated, use ldap_rename */
	LDAP *ld,
	LDAP_CONST char *dn,
	LDAP_CONST char *newrdn ));

LDAP_F( int )
ldap_modrdn_s LDAP_P((	/* deprecated, use ldap_rename_s */
	LDAP *ld,
	LDAP_CONST char *dn,
	LDAP_CONST char *newrdn ));

LDAP_F( int )
ldap_modrdn2 LDAP_P((	/* deprecated, use ldap_rename */
	LDAP *ld,
	LDAP_CONST char *dn,
	LDAP_CONST char *newrdn,
	int deleteoldrdn ));

LDAP_F( int )
ldap_modrdn2_s LDAP_P((	/* deprecated, use ldap_rename_s */
	LDAP *ld,
	LDAP_CONST char *dn,
	LDAP_CONST char *newrdn,
	int deleteoldrdn));
#endif


/*
 * in open.c:
 */
#if LDAP_DEPRECATED
LDAP_F( LDAP * )
ldap_init LDAP_P(( /* deprecated, use ldap_create or ldap_initialize */
	LDAP_CONST char *host,
	int port ));

LDAP_F( LDAP * )
ldap_open LDAP_P((	/* deprecated, use ldap_create or ldap_initialize */
	LDAP_CONST char *host,
	int port ));
#endif

LDAP_F( int )
ldap_create LDAP_P((
	LDAP **ldp ));

LDAP_F( int )
ldap_initialize LDAP_P((
	LDAP **ldp,
	LDAP_CONST char *url ));

LDAP_F( LDAP * )
ldap_dup LDAP_P((
	LDAP *old ));

LDAP_F( int )
ldap_connect( LDAP *ld );

/*
 * in tls.c
 */

LDAP_F( int )
ldap_tls_inplace LDAP_P((
	LDAP *ld ));

LDAP_F( int )
ldap_start_tls LDAP_P((
	LDAP *ld,
	LDAPControl **serverctrls,
	LDAPControl **clientctrls,
	int *msgidp ));

LDAP_F( int )
ldap_install_tls LDAP_P((
	LDAP *ld ));

LDAP_F( int )
ldap_start_tls_s LDAP_P((
	LDAP *ld,
	LDAPControl **serverctrls,
	LDAPControl **clientctrls ));

/*
 * in messages.c:
 */
LDAP_F( LDAPMessage * )
ldap_first_message LDAP_P((
	LDAP *ld,
	LDAPMessage *chain ));

LDAP_F( LDAPMessage * )
ldap_next_message LDAP_P((
	LDAP *ld,
	LDAPMessage *msg ));

LDAP_F( int )
ldap_count_messages LDAP_P((
	LDAP *ld,
	LDAPMessage *chain ));

/*
 * in references.c:
 */
LDAP_F( LDAPMessage * )
ldap_first_reference LDAP_P((
	LDAP *ld,
	LDAPMessage *chain ));

LDAP_F( LDAPMessage * )
ldap_next_reference LDAP_P((
	LDAP *ld,
	LDAPMessage *ref ));

LDAP_F( int )
ldap_count_references LDAP_P((
	LDAP *ld,
	LDAPMessage *chain ));

LDAP_F( int )
ldap_parse_reference LDAP_P((
	LDAP			*ld,
	LDAPMessage		*ref,
	char			***referralsp,
	LDAPControl		***serverctrls,
	int				freeit));


/*
 * in getentry.c:
 */
LDAP_F( LDAPMessage * )
ldap_first_entry LDAP_P((
	LDAP *ld,
	LDAPMessage *chain ));

LDAP_F( LDAPMessage * )
ldap_next_entry LDAP_P((
	LDAP *ld,
	LDAPMessage *entry ));

LDAP_F( int )
ldap_count_entries LDAP_P((
	LDAP *ld,
	LDAPMessage *chain ));

LDAP_F( int )
ldap_get_entry_controls LDAP_P((
	LDAP			*ld,
	LDAPMessage		*entry,
	LDAPControl		***serverctrls));


/*
 * in addentry.c
 */
LDAP_F( LDAPMessage * )
ldap_delete_result_entry LDAP_P((
	LDAPMessage **list,
	LDAPMessage *e ));

LDAP_F( void )
ldap_add_result_entry LDAP_P((
	LDAPMessage **list,
	LDAPMessage *e ));


/*
 * in getdn.c
 */
LDAP_F( char * )
ldap_get_dn LDAP_P((
	LDAP *ld,
	LDAPMessage *entry ));

typedef struct ldap_ava {
	struct berval la_attr;
	struct berval la_value;
	unsigned la_flags;
#define LDAP_AVA_NULL				0x0000U
#define LDAP_AVA_STRING				0x0001U
#define LDAP_AVA_BINARY				0x0002U
#define LDAP_AVA_NONPRINTABLE		0x0004U
#define LDAP_AVA_FREE_ATTR			0x0010U
#define LDAP_AVA_FREE_VALUE			0x0020U

	void *la_private;
} LDAPAVA;

typedef LDAPAVA** LDAPRDN;
typedef LDAPRDN* LDAPDN;

/* DN formats */
#define LDAP_DN_FORMAT_LDAP			0x0000U
#define LDAP_DN_FORMAT_LDAPV3		0x0010U
#define LDAP_DN_FORMAT_LDAPV2		0x0020U
#define LDAP_DN_FORMAT_DCE			0x0030U
#define LDAP_DN_FORMAT_UFN			0x0040U	/* dn2str only */
#define LDAP_DN_FORMAT_AD_CANONICAL	0x0050U	/* dn2str only */
#define LDAP_DN_FORMAT_LBER			0x00F0U /* for testing only */
#define LDAP_DN_FORMAT_MASK			0x00F0U

/* DN flags */
#define LDAP_DN_PRETTY				0x0100U
#define LDAP_DN_SKIP				0x0200U
#define LDAP_DN_P_NOLEADTRAILSPACES	0x1000U
#define LDAP_DN_P_NOSPACEAFTERRDN	0x2000U
#define LDAP_DN_PEDANTIC			0xF000U

LDAP_F( void ) ldap_rdnfree LDAP_P(( LDAPRDN rdn ));
LDAP_F( void ) ldap_dnfree LDAP_P(( LDAPDN dn ));

LDAP_F( int )
ldap_bv2dn LDAP_P(( 
	struct berval *bv, 
	LDAPDN *dn, 
	unsigned flags ));

LDAP_F( int )
ldap_str2dn LDAP_P((
	LDAP_CONST char *str,
	LDAPDN *dn,
	unsigned flags ));

LDAP_F( int )
ldap_dn2bv LDAP_P((
	LDAPDN dn,
	struct berval *bv,
	unsigned flags ));

LDAP_F( int )
ldap_dn2str LDAP_P((
	LDAPDN dn,
	char **str,
	unsigned flags ));

LDAP_F( int )
ldap_bv2rdn LDAP_P((
	struct berval *bv,
	LDAPRDN *rdn,
	char **next,
	unsigned flags ));

LDAP_F( int )
ldap_str2rdn LDAP_P((
	LDAP_CONST char *str,
	LDAPRDN *rdn,
	char **next,
	unsigned flags ));

LDAP_F( int )
ldap_rdn2bv LDAP_P((
	LDAPRDN rdn,
	struct berval *bv,
	unsigned flags ));

LDAP_F( int )
ldap_rdn2str LDAP_P((
	LDAPRDN rdn,
	char **str,
	unsigned flags ));

LDAP_F( int )
ldap_dn_normalize LDAP_P((
	LDAP_CONST char *in, unsigned iflags,
	char **out, unsigned oflags ));

LDAP_F( char * )
ldap_dn2ufn LDAP_P(( /* deprecated, use ldap_str2dn/dn2str */
	LDAP_CONST char *dn ));

LDAP_F( char ** )
ldap_explode_dn LDAP_P(( /* deprecated, ldap_str2dn */
	LDAP_CONST char *dn,
	int notypes ));

LDAP_F( char ** )
ldap_explode_rdn LDAP_P(( /* deprecated, ldap_str2rdn */
	LDAP_CONST char *rdn,
	int notypes ));

typedef int LDAPDN_rewrite_func
	LDAP_P(( LDAPDN dn, unsigned flags, void *ctx ));

LDAP_F( int )
ldap_X509dn2bv LDAP_P(( void *x509_name, struct berval *dn,
	LDAPDN_rewrite_func *func, unsigned flags ));

LDAP_F( char * )
ldap_dn2dcedn LDAP_P(( /* deprecated, ldap_str2dn/dn2str */
	LDAP_CONST char *dn ));

LDAP_F( char * )
ldap_dcedn2dn LDAP_P(( /* deprecated, ldap_str2dn/dn2str */
	LDAP_CONST char *dce ));

LDAP_F( char * )
ldap_dn2ad_canonical LDAP_P(( /* deprecated, ldap_str2dn/dn2str */
	LDAP_CONST char *dn ));

LDAP_F( int )
ldap_get_dn_ber LDAP_P((
	LDAP *ld, LDAPMessage *e, BerElement **berout, struct berval *dn ));

LDAP_F( int )
ldap_get_attribute_ber LDAP_P((
	LDAP *ld, LDAPMessage *e, BerElement *ber, struct berval *attr,
	struct berval **vals ));

/*
 * in getattr.c
 */
LDAP_F( char * )
ldap_first_attribute LDAP_P((
	LDAP *ld,
	LDAPMessage *entry,
	BerElement **ber ));

LDAP_F( char * )
ldap_next_attribute LDAP_P((
	LDAP *ld,
	LDAPMessage *entry,
	BerElement *ber ));


/*
 * in getvalues.c
 */
LDAP_F( struct berval ** )
ldap_get_values_len LDAP_P((
	LDAP *ld,
	LDAPMessage *entry,
	LDAP_CONST char *target ));

LDAP_F( int )
ldap_count_values_len LDAP_P((
	struct berval **vals ));

LDAP_F( void )
ldap_value_free_len LDAP_P((
	struct berval **vals ));

#if LDAP_DEPRECATED
LDAP_F( char ** )
ldap_get_values LDAP_P((	/* deprecated, use ldap_get_values_len */
	LDAP *ld,
	LDAPMessage *entry,
	LDAP_CONST char *target ));

LDAP_F( int )
ldap_count_values LDAP_P((	/* deprecated, use ldap_count_values_len */
	char **vals ));

LDAP_F( void )
ldap_value_free LDAP_P((	/* deprecated, use ldap_value_free_len */
	char **vals ));
#endif

/*
 * in result.c:
 */
LDAP_F( int )
ldap_result LDAP_P((
	LDAP *ld,
	int msgid,
	int all,
	struct timeval *timeout,
	LDAPMessage **result ));

LDAP_F( int )
ldap_msgtype LDAP_P((
	LDAPMessage *lm ));

LDAP_F( int )
ldap_msgid   LDAP_P((
	LDAPMessage *lm ));

LDAP_F( int )
ldap_msgfree LDAP_P((
	LDAPMessage *lm ));

LDAP_F( int )
ldap_msgdelete LDAP_P((
	LDAP *ld,
	int msgid ));


/*
 * in search.c:
 */
LDAP_F( int )
ldap_bv2escaped_filter_value LDAP_P(( 
	struct berval *in, 
	struct berval *out ));

LDAP_F( int )
ldap_search_ext LDAP_P((
	LDAP			*ld,
	LDAP_CONST char	*base,
	int				scope,
	LDAP_CONST char	*filter,
	char			**attrs,
	int				attrsonly,
	LDAPControl		**serverctrls,
	LDAPControl		**clientctrls,
	struct timeval	*timeout,
	int				sizelimit,
	int				*msgidp ));

LDAP_F( int )
ldap_search_ext_s LDAP_P((
	LDAP			*ld,
	LDAP_CONST char	*base,
	int				scope,
	LDAP_CONST char	*filter,
	char			**attrs,
	int				attrsonly,
	LDAPControl		**serverctrls,
	LDAPControl		**clientctrls,
	struct timeval	*timeout,
	int				sizelimit,
	LDAPMessage		**res ));

#if LDAP_DEPRECATED
LDAP_F( int )
ldap_search LDAP_P((	/* deprecated, use ldap_search_ext */
	LDAP *ld,
	LDAP_CONST char *base,
	int scope,
	LDAP_CONST char *filter,
	char **attrs,
	int attrsonly ));

LDAP_F( int )
ldap_search_s LDAP_P((	/* deprecated, use ldap_search_ext_s */
	LDAP *ld,
	LDAP_CONST char *base,
	int scope,
	LDAP_CONST char *filter,
	char **attrs,
	int attrsonly,
	LDAPMessage **res ));

LDAP_F( int )
ldap_search_st LDAP_P((	/* deprecated, use ldap_search_ext_s */
	LDAP *ld,
	LDAP_CONST char *base,
	int scope,
	LDAP_CONST char *filter,
    char **attrs,
	int attrsonly,
	struct timeval *timeout,
	LDAPMessage **res ));
#endif

/*
 * in unbind.c
 */
LDAP_F( int )
ldap_unbind_ext LDAP_P((
	LDAP			*ld,
	LDAPControl		**serverctrls,
	LDAPControl		**clientctrls));

LDAP_F( int )
ldap_unbind_ext_s LDAP_P((
	LDAP			*ld,
	LDAPControl		**serverctrls,
	LDAPControl		**clientctrls));

LDAP_F( int )
ldap_destroy LDAP_P((
	LDAP			*ld));

#if LDAP_DEPRECATED
LDAP_F( int )
ldap_unbind LDAP_P(( /* deprecated, use ldap_unbind_ext */
	LDAP *ld ));

LDAP_F( int )
ldap_unbind_s LDAP_P(( /* deprecated, use ldap_unbind_ext_s */
	LDAP *ld ));
#endif

/*
 * in filter.c
 */
LDAP_F( int )
ldap_put_vrFilter LDAP_P((
	BerElement *ber,
	const char *vrf ));

/*
 * in free.c
 */

LDAP_F( void * )
ldap_memalloc LDAP_P((
	ber_len_t s ));

LDAP_F( void * )
ldap_memrealloc LDAP_P((
	void* p,
	ber_len_t s ));

LDAP_F( void * )
ldap_memcalloc LDAP_P((
	ber_len_t n,
	ber_len_t s ));

LDAP_F( void )
ldap_memfree LDAP_P((
	void* p ));

LDAP_F( void )
ldap_memvfree LDAP_P((
	void** v ));

LDAP_F( char * )
ldap_strdup LDAP_P((
	LDAP_CONST char * ));

LDAP_F( void )
ldap_mods_free LDAP_P((
	LDAPMod **mods,
	int freemods ));


#if LDAP_DEPRECATED
/*
 * in sort.c (deprecated, use custom code instead)
 */
typedef int (LDAP_SORT_AD_CMP_PROC) LDAP_P(( /* deprecated */
	LDAP_CONST char *left,
	LDAP_CONST char *right ));

typedef int (LDAP_SORT_AV_CMP_PROC) LDAP_P(( /* deprecated */
	LDAP_CONST void *left,
	LDAP_CONST void *right ));

LDAP_F( int )	/* deprecated */
ldap_sort_entries LDAP_P(( LDAP *ld,
	LDAPMessage **chain,
	LDAP_CONST char *attr,
	LDAP_SORT_AD_CMP_PROC *cmp ));

LDAP_F( int )	/* deprecated */
ldap_sort_values LDAP_P((
	LDAP *ld,
	char **vals,
	LDAP_SORT_AV_CMP_PROC *cmp ));

LDAP_F( int ) /* deprecated */
ldap_sort_strcasecmp LDAP_P((
	LDAP_CONST void *a,
	LDAP_CONST void *b ));
#endif

/*
 * in url.c
 */
LDAP_F( int )
ldap_is_ldap_url LDAP_P((
	LDAP_CONST char *url ));

LDAP_F( int )
ldap_is_ldaps_url LDAP_P((
	LDAP_CONST char *url ));

LDAP_F( int )
ldap_is_ldapi_url LDAP_P((
	LDAP_CONST char *url ));

#ifdef LDAP_CONNECTIONLESS
LDAP_F( int )
ldap_is_ldapc_url LDAP_P((
	LDAP_CONST char *url ));
#endif

LDAP_F( int )
ldap_url_parse LDAP_P((
	LDAP_CONST char *url,
	LDAPURLDesc **ludpp ));

LDAP_F( char * )
ldap_url_desc2str LDAP_P((
	LDAPURLDesc *ludp ));

LDAP_F( void )
ldap_free_urldesc LDAP_P((
	LDAPURLDesc *ludp ));


/*
 * LDAP Cancel Extended Operation <draft-zeilenga-ldap-cancel-xx.txt>
 *  in cancel.c
 */
#define LDAP_API_FEATURE_CANCEL 1000

LDAP_F( int )
ldap_cancel LDAP_P(( LDAP *ld,
	int cancelid,
	LDAPControl		**sctrls,
	LDAPControl		**cctrls,
	int				*msgidp ));

LDAP_F( int )
ldap_cancel_s LDAP_P(( LDAP *ld,
	int cancelid,
	LDAPControl **sctrl,
	LDAPControl **cctrl ));

/*
 * LDAP Turn Extended Operation <draft-zeilenga-ldap-turn-xx.txt>
 *  in turn.c
 */
#define LDAP_API_FEATURE_TURN 1000

LDAP_F( int )
ldap_turn LDAP_P(( LDAP *ld,
	int mutual,
	LDAP_CONST char* identifier,
	LDAPControl		**sctrls,
	LDAPControl		**cctrls,
	int				*msgidp ));

LDAP_F( int )
ldap_turn_s LDAP_P(( LDAP *ld,
	int mutual,
	LDAP_CONST char* identifier,
	LDAPControl **sctrl,
	LDAPControl **cctrl ));

/*
 * LDAP Paged Results
 *	in pagectrl.c
 */
#define LDAP_API_FEATURE_PAGED_RESULTS 2000

LDAP_F( int )
ldap_create_page_control_value LDAP_P((
	LDAP *ld,
	ber_int_t pagesize,
	struct berval *cookie,
	struct berval *value ));

LDAP_F( int )
ldap_create_page_control LDAP_P((
	LDAP *ld,
	ber_int_t pagesize,
	struct berval *cookie,
	int iscritical,
	LDAPControl **ctrlp ));

#if LDAP_DEPRECATED
LDAP_F( int )
ldap_parse_page_control LDAP_P((
	/* deprecated, use ldap_parse_pageresponse_control */
	LDAP *ld,
	LDAPControl **ctrls,
	ber_int_t *count,
	struct berval **cookie ));
#endif

LDAP_F( int )
ldap_parse_pageresponse_control LDAP_P((
	LDAP *ld,
	LDAPControl *ctrl,
	ber_int_t *count,
	struct berval *cookie ));

/*
 * LDAP Server Side Sort
 *	in sortctrl.c
 */
#define LDAP_API_FEATURE_SERVER_SIDE_SORT 2000

/* structure for a sort-key */
typedef struct ldapsortkey {
	char *attributeType;
	char *orderingRule;
	int reverseOrder;
} LDAPSortKey;

LDAP_F( int )
ldap_create_sort_keylist LDAP_P((
	LDAPSortKey ***sortKeyList,
	char *keyString ));

LDAP_F( void )
ldap_free_sort_keylist LDAP_P((
	LDAPSortKey **sortkeylist ));

LDAP_F( int )
ldap_create_sort_control_value LDAP_P((
	LDAP *ld,
	LDAPSortKey **keyList,
	struct berval *value ));

LDAP_F( int )
ldap_create_sort_control LDAP_P((
	LDAP *ld,
	LDAPSortKey **keyList,
	int iscritical,
	LDAPControl **ctrlp ));

LDAP_F( int )
ldap_parse_sortresponse_control LDAP_P((
	LDAP *ld,
	LDAPControl *ctrl,
	ber_int_t *result,
	char **attribute ));

/*
 * LDAP Virtual List View
 *	in vlvctrl.c
 */
#define LDAP_API_FEATURE_VIRTUAL_LIST_VIEW 2000

/* structure for virtual list */
typedef struct ldapvlvinfo {
	ber_int_t ldvlv_version;
    ber_int_t ldvlv_before_count;
    ber_int_t ldvlv_after_count;
    ber_int_t ldvlv_offset;
    ber_int_t ldvlv_count;
    struct berval *	ldvlv_attrvalue;
    struct berval *	ldvlv_context;
    void *			ldvlv_extradata;
} LDAPVLVInfo;

LDAP_F( int )
ldap_create_vlv_control_value LDAP_P((
	LDAP *ld,
	LDAPVLVInfo *ldvlistp,
	struct berval *value));

LDAP_F( int )
ldap_create_vlv_control LDAP_P((
	LDAP *ld,
	LDAPVLVInfo *ldvlistp,
	LDAPControl **ctrlp ));

LDAP_F( int )
ldap_parse_vlvresponse_control LDAP_P((
	LDAP          *ld,
	LDAPControl   *ctrls,
	ber_int_t *target_posp,
	ber_int_t *list_countp,
	struct berval **contextp,
	int           *errcodep ));

/*
 * LDAP Verify Credentials
 */
#define LDAP_API_FEATURE_VERIFY_CREDENTIALS 1000

LDAP_F( int )
ldap_verify_credentials LDAP_P((
	LDAP		*ld,
	struct berval	*cookie,
	LDAP_CONST char	*dn,
	LDAP_CONST char	*mechanism,
	struct berval	*cred,
	LDAPControl	**ctrls,
	LDAPControl	**serverctrls,
	LDAPControl	**clientctrls,
	int		*msgidp ));

LDAP_F( int )
ldap_verify_credentials_s LDAP_P((
	LDAP		*ld,
	struct berval	*cookie,
	LDAP_CONST char	*dn,
	LDAP_CONST char	*mechanism,
	struct berval	*cred,
	LDAPControl	**vcictrls,
	LDAPControl	**serverctrls,
	LDAPControl	**clientctrls,
	int				*code,
	char			**diagmsgp,
	struct berval	**scookie,
	struct berval	**servercredp,
	LDAPControl	***vcoctrls));
	

LDAP_F( int )
ldap_parse_verify_credentials LDAP_P((
	LDAP		*ld,
	LDAPMessage	*res,
	int			*code,
	char			**diagmsgp,
	struct berval	**cookie,
	struct berval	**servercredp,
	LDAPControl	***vcctrls));

/* not yet implemented */
/* #define LDAP_API_FEATURE_VERIFY_CREDENTIALS_INTERACTIVE 1000 */
#ifdef LDAP_API_FEATURE_VERIFY_CREDENTIALS_INTERACTIVE
LDAP_F( int )
ldap_verify_credentials_interactive LDAP_P((
	LDAP *ld,
	LDAP_CONST char *dn, /* usually NULL */
	LDAP_CONST char *saslMechanism,
	LDAPControl **vcControls,
	LDAPControl **serverControls,
	LDAPControl **clientControls,

	/* should be client controls */
	unsigned flags,
	LDAP_SASL_INTERACT_PROC *proc,
	void *defaults,
	void *context,
	
	/* as obtained from ldap_result() */
	LDAPMessage *result,

	/* returned during bind processing */
	const char **rmech,
	int *msgid ));
#endif

/*
 * LDAP Who Am I?
 *	in whoami.c
 */
#define LDAP_API_FEATURE_WHOAMI 1000

LDAP_F( int )
ldap_parse_whoami LDAP_P((
	LDAP *ld,
	LDAPMessage *res,
	struct berval **authzid ));

LDAP_F( int )
ldap_whoami LDAP_P(( LDAP *ld,
	LDAPControl		**sctrls,
	LDAPControl		**cctrls,
	int				*msgidp ));

LDAP_F( int )
ldap_whoami_s LDAP_P((
	LDAP *ld,
	struct berval **authzid,
	LDAPControl **sctrls,
	LDAPControl **cctrls ));

/*
 * LDAP Password Modify
 *	in passwd.c
 */
#define LDAP_API_FEATURE_PASSWD_MODIFY 1000

LDAP_F( int )
ldap_parse_passwd LDAP_P((
	LDAP *ld,
	LDAPMessage *res,
	struct berval *newpasswd ));

LDAP_F( int )
ldap_passwd LDAP_P(( LDAP *ld,
	struct berval	*user,
	struct berval	*oldpw,
	struct berval	*newpw,
	LDAPControl		**sctrls,
	LDAPControl		**cctrls,
	int				*msgidp ));

LDAP_F( int )
ldap_passwd_s LDAP_P((
	LDAP *ld,
	struct berval	*user,
	struct berval	*oldpw,
	struct berval	*newpw,
	struct berval *newpasswd,
	LDAPControl **sctrls,
	LDAPControl **cctrls ));

#ifdef LDAP_CONTROL_PASSWORDPOLICYREQUEST
/*
 * LDAP Password Policy controls
 *	in ppolicy.c
 */
#define LDAP_API_FEATURE_PASSWORD_POLICY 1000

typedef enum passpolicyerror_enum {
       PP_passwordExpired = 0,
       PP_accountLocked = 1,
       PP_changeAfterReset = 2,
       PP_passwordModNotAllowed = 3,
       PP_mustSupplyOldPassword = 4,
       PP_insufficientPasswordQuality = 5,
       PP_passwordTooShort = 6,
       PP_passwordTooYoung = 7,
       PP_passwordInHistory = 8,
       PP_passwordTooLong = 9,
       PP_noError = 65535
} LDAPPasswordPolicyError;

LDAP_F( int )
ldap_create_passwordpolicy_control LDAP_P((
        LDAP *ld,
        LDAPControl **ctrlp ));

LDAP_F( int )
ldap_parse_passwordpolicy_control LDAP_P((
        LDAP *ld,
        LDAPControl *ctrl,
        ber_int_t *expirep,
        ber_int_t *gracep,
        LDAPPasswordPolicyError *errorp ));

LDAP_F( const char * )
ldap_passwordpolicy_err2txt LDAP_P(( LDAPPasswordPolicyError ));
#endif /* LDAP_CONTROL_PASSWORDPOLICYREQUEST */

LDAP_F( int )
ldap_parse_password_expiring_control LDAP_P((
	LDAP           *ld,
	LDAPControl    *ctrl,
	long           *secondsp ));

/*
 * LDAP Dynamic Directory Services Refresh -- RFC 2589
 *	in dds.c
 */
#define LDAP_API_FEATURE_REFRESH 1000

LDAP_F( int )
ldap_parse_refresh LDAP_P((
	LDAP *ld,
	LDAPMessage *res,
	ber_int_t *newttl ));

LDAP_F( int )
ldap_refresh LDAP_P(( LDAP *ld,
	struct berval	*dn,
	ber_int_t ttl,
	LDAPControl		**sctrls,
	LDAPControl		**cctrls,
	int				*msgidp ));

LDAP_F( int )
ldap_refresh_s LDAP_P((
	LDAP *ld,
	struct berval	*dn,
	ber_int_t ttl,
	ber_int_t *newttl,
	LDAPControl **sctrls,
	LDAPControl **cctrls ));

/*
 * LDAP Transactions
 */
LDAP_F( int )
ldap_txn_start LDAP_P(( LDAP *ld,
	LDAPControl		**sctrls,
	LDAPControl		**cctrls,
	int				*msgidp ));

LDAP_F( int )
ldap_txn_start_s LDAP_P(( LDAP *ld,
	LDAPControl **sctrl,
	LDAPControl **cctrl,
	struct berval **rettxnid ));

LDAP_F( int )
ldap_txn_end LDAP_P(( LDAP *ld,
	int	commit,
	struct berval	*txnid,
	LDAPControl		**sctrls,
	LDAPControl		**cctrls,
	int				*msgidp ));

LDAP_F( int )
ldap_txn_end_s LDAP_P(( LDAP *ld,
	int	commit,
	struct berval *txnid,
	LDAPControl **sctrl,
	LDAPControl **cctrl,
	int *retidp ));

/*
 * in ldap_sync.c
 */

/*
 * initialize the persistent search structure
 */
LDAP_F( ldap_sync_t * )
ldap_sync_initialize LDAP_P((
	ldap_sync_t	*ls ));

/*
 * destroy the persistent search structure
 */
LDAP_F( void )
ldap_sync_destroy LDAP_P((
	ldap_sync_t	*ls,
	int		freeit ));

/*
 * initialize a refreshOnly sync
 */
LDAP_F( int )
ldap_sync_init LDAP_P((
	ldap_sync_t	*ls,
	int		mode ));

/*
 * initialize a refreshOnly sync
 */
LDAP_F( int )
ldap_sync_init_refresh_only LDAP_P((
	ldap_sync_t	*ls ));

/*
 * initialize a refreshAndPersist sync
 */
LDAP_F( int )
ldap_sync_init_refresh_and_persist LDAP_P((
	ldap_sync_t	*ls ));

/*
 * poll for new responses
 */
LDAP_F( int )
ldap_sync_poll LDAP_P((
	ldap_sync_t	*ls ));

#ifdef LDAP_CONTROL_X_SESSION_TRACKING

/*
 * in stctrl.c
 */
LDAP_F( int )
ldap_create_session_tracking_value LDAP_P((
	LDAP		*ld,
	char		*sessionSourceIp,
	char		*sessionSourceName,
	char		*formatOID,
	struct berval	*sessionTrackingIdentifier,
	struct berval	*value ));

LDAP_F( int )
ldap_create_session_tracking_control LDAP_P((
	LDAP		*ld,
	char		*sessionSourceIp,
	char		*sessionSourceName,
	char		*formatOID,
	struct berval	*sessionTrackingIdentifier,
	LDAPControl	**ctrlp ));

LDAP_F( int )
ldap_parse_session_tracking_control LDAP_P((
	LDAP *ld,
	LDAPControl *ctrl,
	struct berval *ip,
	struct berval *name,
	struct berval *oid,
	struct berval *id ));

#endif /* LDAP_CONTROL_X_SESSION_TRACKING */

/*
 * in msctrl.c
 */
#ifdef LDAP_CONTROL_X_DIRSYNC
LDAP_F( int )
ldap_create_dirsync_value LDAP_P((
	LDAP		*ld,
	int		flags,
	int		maxAttrCount,
	struct berval	*cookie,
	struct berval	*value ));

LDAP_F( int )
ldap_create_dirsync_control LDAP_P((
	LDAP		*ld,
	int		flags,
	int		maxAttrCount,
	struct berval	*cookie,
	LDAPControl	**ctrlp ));

LDAP_F( int )
ldap_parse_dirsync_control LDAP_P((
	LDAP		*ld,
	LDAPControl	*ctrl,
	int		*continueFlag,
	struct berval	*cookie ));
#endif /* LDAP_CONTROL_X_DIRSYNC */

#ifdef LDAP_CONTROL_X_EXTENDED_DN
LDAP_F( int )
ldap_create_extended_dn_value LDAP_P((
	LDAP		*ld,
	int		flag,
	struct berval	*value ));

LDAP_F( int )
ldap_create_extended_dn_control LDAP_P((
	LDAP		*ld,
	int		flag,
	LDAPControl	**ctrlp ));
#endif /* LDAP_CONTROL_X_EXTENDED_DN */

#ifdef LDAP_CONTROL_X_SHOW_DELETED
LDAP_F( int )
ldap_create_show_deleted_control LDAP_P((
	LDAP		*ld,
	LDAPControl	**ctrlp ));
#endif /* LDAP_CONTROL_X_SHOW_DELETED */

#ifdef LDAP_CONTROL_X_SERVER_NOTIFICATION
LDAP_F( int )
ldap_create_server_notification_control LDAP_P((
	LDAP		*ld,
	LDAPControl	**ctrlp ));
#endif /* LDAP_CONTROL_X_SERVER_NOTIFICATION */

/*
 * in assertion.c
 */
LDAP_F (int)
ldap_create_assertion_control_value LDAP_P((
	LDAP		*ld,
	char		*assertion,
	struct berval	*value ));

LDAP_F( int )
ldap_create_assertion_control LDAP_P((
	LDAP		*ld,
	char		*filter,
	int		iscritical,
	LDAPControl	**ctrlp ));

/*
 * in deref.c
 */

typedef struct LDAPDerefSpec {
	char *derefAttr;
	char **attributes;
} LDAPDerefSpec;

typedef struct LDAPDerefVal {
	char *type;
	BerVarray vals;
	struct LDAPDerefVal *next;
} LDAPDerefVal;

typedef struct LDAPDerefRes {
	char *derefAttr;
	struct berval derefVal;
	LDAPDerefVal *attrVals;
	struct LDAPDerefRes *next;
} LDAPDerefRes;

LDAP_F( int )
ldap_create_deref_control_value LDAP_P((
	LDAP *ld,
	LDAPDerefSpec *ds,
	struct berval *value ));

LDAP_F( int )
ldap_create_deref_control LDAP_P((
	LDAP		*ld,
	LDAPDerefSpec	*ds,
	int		iscritical,
	LDAPControl	**ctrlp ));

LDAP_F( void )
ldap_derefresponse_free LDAP_P((
	LDAPDerefRes *dr ));

LDAP_F( int )
ldap_parse_derefresponse_control LDAP_P((
	LDAP *ld,
	LDAPControl *ctrl,
	LDAPDerefRes **drp ));

LDAP_F( int )
ldap_parse_deref_control LDAP_P((
	LDAP		*ld,
	LDAPControl	**ctrls,
	LDAPDerefRes	**drp ));

/*
 * in psearch.c
 */

LDAP_F( int )
ldap_create_persistentsearch_control_value LDAP_P((
	LDAP *ld,
	int changetypes,
	int changesonly,
	int return_echg_ctls,
	struct berval *value ));

LDAP_F( int )
ldap_create_persistentsearch_control LDAP_P((
	LDAP *ld,
	int changetypes,
	int changesonly,
	int return_echg_ctls,
	int isCritical,
	LDAPControl **ctrlp ));

LDAP_F( int )
ldap_parse_entrychange_control LDAP_P((
	LDAP *ld,
	LDAPControl *ctrl,
	int *chgtypep,
	struct berval *prevdnp,
	int *chgnumpresentp,
	long *chgnump ));

/* in account_usability.c */

LDAP_F( int )
ldap_create_accountusability_control LDAP_P((
	LDAP *ld,
	LDAPControl **ctrlp ));

typedef struct LDAPAccountUsabilityMoreInfo {
	ber_int_t inactive;
	ber_int_t reset;
	ber_int_t expired;
	ber_int_t remaining_grace;
	ber_int_t seconds_before_unlock;
} LDAPAccountUsabilityMoreInfo;

typedef union LDAPAccountUsability {
	ber_int_t seconds_remaining;
	LDAPAccountUsabilityMoreInfo more_info;
} LDAPAccountUsability;

LDAP_F( int )
ldap_parse_accountusability_control LDAP_P((
	LDAP           *ld,
	LDAPControl    *ctrl,
	int            *availablep,
	LDAPAccountUsability *usabilityp ));


/*
 * high level LDIF to LDAP structure support
 */
#define LDIF_DEFAULT_ADD  0x01 /* if changetype missing, assume LDAP_ADD */
#define LDIF_ENTRIES_ONLY 0x02 /* ignore changetypes other than add */
#define LDIF_NO_CONTROLS  0x04 /* ignore control specifications */
#define LDIF_MODS_ONLY    0x08 /* no changetypes, assume LDAP_MODIFY */
#define LDIF_NO_DN        0x10 /* dn is not present */

typedef struct ldifrecord {
	ber_tag_t lr_op; /* type of operation - LDAP_REQ_MODIFY, LDAP_REQ_ADD, etc. */
	struct berval lr_dn; /* DN of operation */
	LDAPControl **lr_ctrls; /* controls specified for operation */
	/* some ops such as LDAP_REQ_DELETE require only a DN */
	/* other ops require different data - the ldif_ops union
	   is used to specify the data for each type of operation */
	union ldif_ops_u {
		LDAPMod **lr_mods; /* list of mods for LDAP_REQ_MODIFY, LDAP_REQ_ADD */
#define lrop_mods ldif_ops.lr_mods
		struct ldif_op_rename_s {
			struct berval lr_newrdn; /* LDAP_REQ_MODDN, LDAP_REQ_MODRDN, LDAP_REQ_RENAME */
#define lrop_newrdn ldif_ops.ldif_op_rename.lr_newrdn
			struct berval lr_newsuperior; /* LDAP_REQ_MODDN, LDAP_REQ_MODRDN, LDAP_REQ_RENAME */
#define lrop_newsup ldif_ops.ldif_op_rename.lr_newsuperior
			int lr_deleteoldrdn; /* LDAP_REQ_MODDN, LDAP_REQ_MODRDN, LDAP_REQ_RENAME */
#define lrop_delold ldif_ops.ldif_op_rename.lr_deleteoldrdn
		} ldif_op_rename; /* rename/moddn/modrdn */
		/* the following are for future support */
		struct ldif_op_ext_s {
			struct berval lr_extop_oid; /* LDAP_REQ_EXTENDED */
#define lrop_extop_oid ldif_ops.ldif_op_ext.lr_extop_oid
			struct berval lr_extop_data; /* LDAP_REQ_EXTENDED */
#define lrop_extop_data ldif_ops.ldif_op_ext.lr_extop_data
		} ldif_op_ext; /* extended operation */
		struct ldif_op_cmp_s {
			struct berval lr_cmp_attr; /* LDAP_REQ_COMPARE */
#define lrop_cmp_attr ldif_ops.ldif_op_cmp.lr_cmp_attr
			struct berval lr_cmp_bvalue; /* LDAP_REQ_COMPARE */
#define lrop_cmp_bval ldif_ops.ldif_op_cmp.lr_cmp_bvalue
		} ldif_op_cmp; /* compare operation */
	} ldif_ops;
	/* PRIVATE STUFF - DO NOT TOUCH */
	/* for efficiency, the implementation allocates memory */
	/* in large blobs, and makes the above fields point to */
	/* locations inside those blobs - one consequence is that */
	/* you cannot simply free the above allocated fields, nor */
	/* assign them to be owned by another memory context which */
	/* might free them (unless providing your own mem ctx) */
	/* we use the fields below to keep track of those blobs */
	/* so we that we can free them later */
	void *lr_ctx; /* the memory context or NULL */
	int lr_lines;
	LDAPMod	*lr_lm;
	unsigned char *lr_mops;
	char *lr_freeval;
	struct berval *lr_vals;
	struct berval *lr_btype;
} LDIFRecord;

/* free internal fields - does not free the LDIFRecord */
LDAP_F( void )
ldap_ldif_record_done LDAP_P((
	LDIFRecord *lr ));

LDAP_F( int )
ldap_parse_ldif_record LDAP_P((
	struct berval *rbuf,
	unsigned long linenum,
	LDIFRecord *lr,
	const char *errstr,
	unsigned int flags ));

LDAP_END_DECL
#endif /* _LDAP_H */
