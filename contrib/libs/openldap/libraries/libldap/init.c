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

#include "portable.h"

#include <stdio.h>
#include <ac/stdlib.h>

#ifdef HAVE_GETEUID
#include <ac/unistd.h>
#endif

#include <ac/socket.h>
#include <ac/string.h>
#include <ac/ctype.h>
#include <ac/time.h>

#ifdef HAVE_LIMITS_H
#include <limits.h>
#endif

#include "ldap-int.h"
#include "ldap_defaults.h"
#include "lutil.h"

struct ldapoptions ldap_int_global_options =
	{ LDAP_UNINITIALIZED, LDAP_DEBUG_NONE
		LDAP_LDO_NULLARG
		LDAP_LDO_SOURCEIP_NULLARG
		LDAP_LDO_CONNECTIONLESS_NULLARG
		LDAP_LDO_TLS_NULLARG
		LDAP_LDO_SASL_NULLARG
		LDAP_LDO_MUTEX_NULLARG };

#define ATTR_NONE	0
#define ATTR_BOOL	1
#define ATTR_INT	2
#define ATTR_KV		3
#define ATTR_STRING	4
#define ATTR_OPTION	5

#define ATTR_SASL	6
#define ATTR_TLS	7

#define ATTR_OPT_TV	8
#define ATTR_OPT_INT	9

struct ol_keyvalue {
	const char *		key;
	int			value;
};

static const struct ol_keyvalue deref_kv[] = {
	{"never", LDAP_DEREF_NEVER},
	{"searching", LDAP_DEREF_SEARCHING},
	{"finding", LDAP_DEREF_FINDING},
	{"always", LDAP_DEREF_ALWAYS},
	{NULL, 0}
};

static const struct ol_attribute {
	int			useronly;
	int			type;
	const char *	name;
	const void *	data;
	size_t		offset;
} attrs[] = {
	{0, ATTR_OPT_TV,	"TIMEOUT",		NULL,	LDAP_OPT_TIMEOUT},
	{0, ATTR_OPT_TV,	"NETWORK_TIMEOUT",	NULL,	LDAP_OPT_NETWORK_TIMEOUT},
	{0, ATTR_OPT_INT,	"VERSION",		NULL,	LDAP_OPT_PROTOCOL_VERSION},
	{0, ATTR_KV,		"DEREF",	deref_kv, /* or &deref_kv[0] */
		offsetof(struct ldapoptions, ldo_deref)},
	{0, ATTR_INT,		"SIZELIMIT",	NULL,
		offsetof(struct ldapoptions, ldo_sizelimit)},
	{0, ATTR_INT,		"TIMELIMIT",	NULL,
		offsetof(struct ldapoptions, ldo_timelimit)},
	{1, ATTR_STRING,	"BINDDN",		NULL,
		offsetof(struct ldapoptions, ldo_defbinddn)},
	{0, ATTR_STRING,	"BASE",			NULL,
		offsetof(struct ldapoptions, ldo_defbase)},
	{0, ATTR_INT,		"PORT",			NULL,		/* deprecated */
		offsetof(struct ldapoptions, ldo_defport)},
	{0, ATTR_OPTION,	"HOST",			NULL,	LDAP_OPT_HOST_NAME}, /* deprecated */
	{0, ATTR_OPTION,	"URI",			NULL,	LDAP_OPT_URI}, /* replaces HOST/PORT */
	{0, ATTR_OPTION,	"SOCKET_BIND_ADDRESSES",	NULL,	LDAP_OPT_SOCKET_BIND_ADDRESSES},
	{0, ATTR_BOOL,		"REFERRALS",	NULL,	LDAP_BOOL_REFERRALS},
	{0, ATTR_OPT_INT,	"KEEPALIVE_IDLE",	NULL,	LDAP_OPT_X_KEEPALIVE_IDLE},
	{0, ATTR_OPT_INT,	"KEEPALIVE_PROBES",	NULL,	LDAP_OPT_X_KEEPALIVE_PROBES},
	{0, ATTR_OPT_INT,	"KEEPALIVE_INTERVAL",	NULL,	LDAP_OPT_X_KEEPALIVE_INTERVAL},

#if 0
	/* This should only be allowed via ldap_set_option(3) */
	{0, ATTR_BOOL,		"RESTART",		NULL,	LDAP_BOOL_RESTART},
#endif

#ifdef HAVE_CYRUS_SASL
	{0, ATTR_STRING,	"SASL_MECH",		NULL,
		offsetof(struct ldapoptions, ldo_def_sasl_mech)},
	{0, ATTR_STRING,	"SASL_REALM",		NULL,
		offsetof(struct ldapoptions, ldo_def_sasl_realm)},
	{1, ATTR_STRING,	"SASL_AUTHCID",		NULL,
		offsetof(struct ldapoptions, ldo_def_sasl_authcid)},
	{1, ATTR_STRING,	"SASL_AUTHZID",		NULL,
		offsetof(struct ldapoptions, ldo_def_sasl_authzid)},
	{0, ATTR_SASL,		"SASL_SECPROPS",	NULL,	LDAP_OPT_X_SASL_SECPROPS},
	{0, ATTR_BOOL,		"SASL_NOCANON",	NULL,	LDAP_BOOL_SASL_NOCANON},
	{0, ATTR_SASL,		"SASL_CBINDING",	NULL,	LDAP_OPT_X_SASL_CBINDING},
#endif

#ifdef HAVE_TLS
	{1, ATTR_TLS,	"TLS_CERT",			NULL,	LDAP_OPT_X_TLS_CERTFILE},
	{1, ATTR_TLS,	"TLS_KEY",			NULL,	LDAP_OPT_X_TLS_KEYFILE},
  	{0, ATTR_TLS,	"TLS_CACERT",		NULL,	LDAP_OPT_X_TLS_CACERTFILE},
  	{0, ATTR_TLS,	"TLS_CACERTDIR",	NULL,	LDAP_OPT_X_TLS_CACERTDIR},
  	{0, ATTR_TLS,	"TLS_REQCERT",		NULL,	LDAP_OPT_X_TLS_REQUIRE_CERT},
	{0, ATTR_TLS,	"TLS_REQSAN",		NULL,	LDAP_OPT_X_TLS_REQUIRE_SAN},
	{0, ATTR_TLS,	"TLS_RANDFILE",		NULL,	LDAP_OPT_X_TLS_RANDOM_FILE},
	{0, ATTR_TLS,	"TLS_CIPHER_SUITE",	NULL,	LDAP_OPT_X_TLS_CIPHER_SUITE},
	{0, ATTR_TLS,	"TLS_PROTOCOL_MIN",	NULL,	LDAP_OPT_X_TLS_PROTOCOL_MIN},
	{0, ATTR_TLS,	"TLS_PROTOCOL_MAX",	NULL,	LDAP_OPT_X_TLS_PROTOCOL_MAX},
	{0, ATTR_TLS,	"TLS_PEERKEY_HASH",	NULL,	LDAP_OPT_X_TLS_PEERKEY_HASH},
	{0, ATTR_TLS,	"TLS_ECNAME",		NULL,	LDAP_OPT_X_TLS_ECNAME},

#ifdef HAVE_OPENSSL
	{0, ATTR_TLS,	"TLS_CRLCHECK",		NULL,	LDAP_OPT_X_TLS_CRLCHECK},
#endif
#ifdef HAVE_GNUTLS
	{0, ATTR_TLS,	"TLS_CRLFILE",			NULL,	LDAP_OPT_X_TLS_CRLFILE},
#endif
        
#endif

	{0, ATTR_NONE,		NULL,		NULL,	0}
};

#define MAX_LDAP_ATTR_LEN  sizeof("SOCKET_BIND_ADDRESSES")
#define MAX_LDAP_ENV_PREFIX_LEN 8

static int
ldap_int_conf_option(
	struct ldapoptions *gopts,
	char *cmd, char *opt, int userconf )
{
	int i;

	for(i=0; attrs[i].type != ATTR_NONE; i++) {
		void *p;

		if( !userconf && attrs[i].useronly ) {
			continue;
		}

		if(strcasecmp(cmd, attrs[i].name) != 0) {
			continue;
		}

		switch(attrs[i].type) {
		case ATTR_BOOL:
			if((strcasecmp(opt, "on") == 0)
				|| (strcasecmp(opt, "yes") == 0)
				|| (strcasecmp(opt, "true") == 0))
			{
				LDAP_BOOL_SET(gopts, attrs[i].offset);

			} else {
				LDAP_BOOL_CLR(gopts, attrs[i].offset);
			}

			break;

		case ATTR_INT: {
			char *next;
			long l;
			p = &((char *) gopts)[attrs[i].offset];
			l = strtol( opt, &next, 10 );
			if ( next != opt && next[ 0 ] == '\0' ) {
				* (int*) p = l;
			}
			} break;

		case ATTR_KV: {
				const struct ol_keyvalue *kv;

				for(kv = attrs[i].data;
					kv->key != NULL;
					kv++) {

					if(strcasecmp(opt, kv->key) == 0) {
						p = &((char *) gopts)[attrs[i].offset];
						* (int*) p = kv->value;
						break;
					}
				}
			} break;

		case ATTR_STRING:
			p = &((char *) gopts)[attrs[i].offset];
			if (* (char**) p != NULL) LDAP_FREE(* (char**) p);
			* (char**) p = LDAP_STRDUP(opt);
			break;
		case ATTR_OPTION:
			ldap_set_option( NULL, attrs[i].offset, opt );
			break;
		case ATTR_SASL:
#ifdef HAVE_CYRUS_SASL
			ldap_int_sasl_config( gopts, attrs[i].offset, opt );
#endif
			break;
		case ATTR_TLS:
#ifdef HAVE_TLS
			ldap_pvt_tls_config( NULL, attrs[i].offset, opt );
#endif
			break;
		case ATTR_OPT_TV: {
			struct timeval tv;
			char *next;
			tv.tv_usec = 0;
			tv.tv_sec = strtol( opt, &next, 10 );
			if ( next != opt && next[ 0 ] == '\0' && tv.tv_sec > 0 ) {
				(void)ldap_set_option( NULL, attrs[i].offset, (const void *)&tv );
			}
			} break;
		case ATTR_OPT_INT: {
			long l;
			char *next;
			l = strtol( opt, &next, 10 );
			if ( next != opt && next[ 0 ] == '\0' && l > 0 && (long)((int)l) == l ) {
				int v = (int)l;
				(void)ldap_set_option( NULL, attrs[i].offset, (const void *)&v );
			}
			} break;
		}

		break;
	}

	if ( attrs[i].type == ATTR_NONE ) {
		Debug1( LDAP_DEBUG_TRACE, "ldap_pvt_tls_config: "
				"unknown option '%s'",
				cmd );
		return 1;
	}

	return 0;
}

int
ldap_pvt_conf_option(
	char *cmd, char *opt, int userconf )
{
	struct ldapoptions *gopts;
	int rc = LDAP_OPT_ERROR;

	/* Get pointer to global option structure */
	gopts = LDAP_INT_GLOBAL_OPT();
	if (NULL == gopts) {
		return LDAP_NO_MEMORY;
	}

	if ( gopts->ldo_valid != LDAP_INITIALIZED ) {
		ldap_int_initialize(gopts, NULL);
		if ( gopts->ldo_valid != LDAP_INITIALIZED )
			return LDAP_LOCAL_ERROR;
	}

	return ldap_int_conf_option( gopts, cmd, opt, userconf );
}

static void openldap_ldap_init_w_conf(
	const char *file, int userconf )
{
	char linebuf[ AC_LINE_MAX ];
	FILE *fp;
	int i;
	char *cmd, *opt;
	char *start, *end;
	struct ldapoptions *gopts;

	if ((gopts = LDAP_INT_GLOBAL_OPT()) == NULL) {
		return;			/* Could not allocate mem for global options */
	}

	if (file == NULL) {
		/* no file name */
		return;
	}

	Debug1(LDAP_DEBUG_TRACE, "ldap_init: trying %s\n", file );

	fp = fopen(file, "r");
	if(fp == NULL) {
		/* could not open file */
		return;
	}

	Debug1(LDAP_DEBUG_TRACE, "ldap_init: using %s\n", file );

	while((start = fgets(linebuf, sizeof(linebuf), fp)) != NULL) {
		/* skip lines starting with '#' */
		if(*start == '#') continue;

		/* trim leading white space */
		while((*start != '\0') && isspace((unsigned char) *start))
			start++;

		/* anything left? */
		if(*start == '\0') continue;

		/* trim trailing white space */
		end = &start[strlen(start)-1];
		while(isspace((unsigned char)*end)) end--;
		end[1] = '\0';

		/* anything left? */
		if(*start == '\0') continue;
		

		/* parse the command */
		cmd=start;
		while((*start != '\0') && !isspace((unsigned char)*start)) {
			start++;
		}
		if(*start == '\0') {
			/* command has no argument */
			continue;
		} 

		*start++ = '\0';

		/* we must have some whitespace to skip */
		while(isspace((unsigned char)*start)) start++;
		opt = start;

		ldap_int_conf_option( gopts, cmd, opt, userconf );
	}

	fclose(fp);
}

static void openldap_ldap_init_w_sysconf(const char *file)
{
	openldap_ldap_init_w_conf( file, 0 );
}

static void openldap_ldap_init_w_userconf(const char *file)
{
	char *home;
	char *path = NULL;

	if (file == NULL) {
		/* no file name */
		return;
	}

	home = getenv("HOME");

	if (home != NULL) {
		Debug1(LDAP_DEBUG_TRACE, "ldap_init: HOME env is %s\n",
		      home );
		path = LDAP_MALLOC(strlen(home) + strlen(file) + sizeof( LDAP_DIRSEP "."));
	} else {
		Debug0(LDAP_DEBUG_TRACE, "ldap_init: HOME env is NULL\n" );
	}

	if(home != NULL && path != NULL) {
		/* we assume UNIX path syntax is used... */

		/* try ~/file */
		sprintf(path, "%s" LDAP_DIRSEP "%s", home, file);
		openldap_ldap_init_w_conf(path, 1);

		/* try ~/.file */
		sprintf(path, "%s" LDAP_DIRSEP ".%s", home, file);
		openldap_ldap_init_w_conf(path, 1);
	}

	if(path != NULL) {
		LDAP_FREE(path);
	}

	/* try file */
	openldap_ldap_init_w_conf(file, 1);
}

static void openldap_ldap_init_w_env(
		struct ldapoptions *gopts,
		const char *prefix)
{
	char buf[MAX_LDAP_ATTR_LEN+MAX_LDAP_ENV_PREFIX_LEN];
	int len;
	int i;
	void *p;
	char *value;

	if (prefix == NULL) {
		prefix = LDAP_ENV_PREFIX;
	}

	strncpy(buf, prefix, MAX_LDAP_ENV_PREFIX_LEN);
	buf[MAX_LDAP_ENV_PREFIX_LEN] = '\0';
	len = strlen(buf);

	for(i=0; attrs[i].type != ATTR_NONE; i++) {
		strcpy(&buf[len], attrs[i].name);
		value = getenv(buf);

		if(value == NULL) {
			continue;
		}

		switch(attrs[i].type) {
		case ATTR_BOOL:
			if((strcasecmp(value, "on") == 0) 
				|| (strcasecmp(value, "yes") == 0)
				|| (strcasecmp(value, "true") == 0))
			{
				LDAP_BOOL_SET(gopts, attrs[i].offset);

			} else {
				LDAP_BOOL_CLR(gopts, attrs[i].offset);
			}
			break;

		case ATTR_INT:
			p = &((char *) gopts)[attrs[i].offset];
			* (int*) p = atoi(value);
			break;

		case ATTR_KV: {
				const struct ol_keyvalue *kv;

				for(kv = attrs[i].data;
					kv->key != NULL;
					kv++) {

					if(strcasecmp(value, kv->key) == 0) {
						p = &((char *) gopts)[attrs[i].offset];
						* (int*) p = kv->value;
						break;
					}
				}
			} break;

		case ATTR_STRING:
			p = &((char *) gopts)[attrs[i].offset];
			if (* (char**) p != NULL) LDAP_FREE(* (char**) p);
			if (*value == '\0') {
				* (char**) p = NULL;
			} else {
				* (char**) p = LDAP_STRDUP(value);
			}
			break;
		case ATTR_OPTION:
			ldap_set_option( NULL, attrs[i].offset, value );
			break;
		case ATTR_SASL:
#ifdef HAVE_CYRUS_SASL
		   	ldap_int_sasl_config( gopts, attrs[i].offset, value );
#endif			 	
		   	break;
		case ATTR_TLS:
#ifdef HAVE_TLS
		   	ldap_pvt_tls_config( NULL, attrs[i].offset, value );
#endif			 	
		   	break;
		case ATTR_OPT_TV: {
			struct timeval tv;
			char *next;
			tv.tv_usec = 0;
			tv.tv_sec = strtol( value, &next, 10 );
			if ( next != value && next[ 0 ] == '\0' && tv.tv_sec > 0 ) {
				(void)ldap_set_option( NULL, attrs[i].offset, (const void *)&tv );
			}
			} break;
		case ATTR_OPT_INT: {
			long l;
			char *next;
			l = strtol( value, &next, 10 );
			if ( next != value && next[ 0 ] == '\0' && l > 0 && (long)((int)l) == l ) {
				int v = (int)l;
				(void)ldap_set_option( NULL, attrs[i].offset, (const void *)&v );
			}
			} break;
		}
	}
}

#if defined(__GNUC__)
/* Declare this function as a destructor so that it will automatically be
 * invoked either at program exit (if libldap is a static library) or
 * at unload time (if libldap is a dynamic library).
 *
 * Sorry, don't know how to handle this for non-GCC environments.
 */
static void ldap_int_destroy_global_options(void)
	__attribute__ ((destructor));
#endif

static void
ldap_int_destroy_global_options(void)
{
	struct ldapoptions *gopts = LDAP_INT_GLOBAL_OPT();

	if ( gopts == NULL )
		return;

	gopts->ldo_valid = LDAP_UNINITIALIZED;

	if ( gopts->ldo_defludp ) {
		ldap_free_urllist( gopts->ldo_defludp );
		gopts->ldo_defludp = NULL;
	}

	if ( gopts->ldo_local_ip_addrs.local_ip_addrs ) {
		LDAP_FREE( gopts->ldo_local_ip_addrs.local_ip_addrs );
		gopts->ldo_local_ip_addrs.local_ip_addrs = NULL;
	}

#if defined(HAVE_WINSOCK) || defined(HAVE_WINSOCK2)
	WSACleanup( );
#endif

#if defined(HAVE_TLS) || defined(HAVE_CYRUS_SASL)
	if ( ldap_int_hostname ) {
		LDAP_FREE( ldap_int_hostname );
		ldap_int_hostname = NULL;
	}
#endif
#ifdef HAVE_CYRUS_SASL
	if ( gopts->ldo_def_sasl_authcid ) {
		LDAP_FREE( gopts->ldo_def_sasl_authcid );
		gopts->ldo_def_sasl_authcid = NULL;
	}
#endif
#ifdef HAVE_TLS
	ldap_int_tls_destroy( gopts );
#endif
}

/* 
 * Initialize the global options structure with default values.
 */
void ldap_int_initialize_global_options( struct ldapoptions *gopts, int *dbglvl )
{
	if (dbglvl)
	    gopts->ldo_debug = *dbglvl;
	else
		gopts->ldo_debug = 0;

	gopts->ldo_version   = LDAP_VERSION2;
	gopts->ldo_deref     = LDAP_DEREF_NEVER;
	gopts->ldo_timelimit = LDAP_NO_LIMIT;
	gopts->ldo_sizelimit = LDAP_NO_LIMIT;

	gopts->ldo_tm_api.tv_sec = -1;
	gopts->ldo_tm_net.tv_sec = -1;

	memset( &gopts->ldo_local_ip_addrs, 0,
		sizeof( gopts->ldo_local_ip_addrs ) );

	/* ldo_defludp will be freed by the termination handler
	 */
	ldap_url_parselist(&gopts->ldo_defludp, "ldap://localhost/");
	gopts->ldo_defport = LDAP_PORT;
#if !defined(__GNUC__) && !defined(PIC)
	/* Do this only for a static library, and only if we can't
	 * arrange for it to be executed as a library destructor
	 */
	atexit(ldap_int_destroy_global_options);
#endif

	gopts->ldo_refhoplimit = LDAP_DEFAULT_REFHOPLIMIT;
	gopts->ldo_rebind_proc = NULL;
	gopts->ldo_rebind_params = NULL;

	LDAP_BOOL_ZERO(gopts);

	LDAP_BOOL_SET(gopts, LDAP_BOOL_REFERRALS);

#ifdef LDAP_CONNECTIONLESS
	gopts->ldo_peer = NULL;
	gopts->ldo_cldapdn = NULL;
	gopts->ldo_is_udp = 0;
#endif

#ifdef HAVE_CYRUS_SASL
	gopts->ldo_def_sasl_mech = NULL;
	gopts->ldo_def_sasl_realm = NULL;
	gopts->ldo_def_sasl_authcid = NULL;
	gopts->ldo_def_sasl_authzid = NULL;

	memset( &gopts->ldo_sasl_secprops,
		'\0', sizeof(gopts->ldo_sasl_secprops) );

	gopts->ldo_sasl_secprops.max_ssf = INT_MAX;
	gopts->ldo_sasl_secprops.maxbufsize = SASL_MAX_BUFF_SIZE;
	gopts->ldo_sasl_secprops.security_flags =
		SASL_SEC_NOPLAINTEXT | SASL_SEC_NOANONYMOUS;
#endif

#ifdef HAVE_TLS
	gopts->ldo_tls_connect_cb = NULL;
	gopts->ldo_tls_connect_arg = NULL;
	gopts->ldo_tls_require_cert = LDAP_OPT_X_TLS_DEMAND;
	gopts->ldo_tls_require_san = LDAP_OPT_X_TLS_ALLOW;
#endif
	gopts->ldo_keepalive_probes = 0;
	gopts->ldo_keepalive_interval = 0;
	gopts->ldo_keepalive_idle = 0;

	gopts->ldo_tcp_user_timeout = 0;

#ifdef LDAP_R_COMPILE
	ldap_pvt_thread_mutex_init( &gopts->ldo_mutex );
#endif
	gopts->ldo_valid = LDAP_INITIALIZED;
   	return;
}

#if defined(HAVE_TLS) || defined(HAVE_CYRUS_SASL)
char * ldap_int_hostname = NULL;
#endif

#ifdef LDAP_R_COMPILE
int	ldap_int_stackguard;
#endif

void ldap_int_initialize( struct ldapoptions *gopts, int *dbglvl )
{
#ifdef LDAP_R_COMPILE
	static ldap_pvt_thread_mutex_t init_mutex;
	LDAP_PVT_MUTEX_FIRSTCREATE( init_mutex );

	LDAP_MUTEX_LOCK( &init_mutex );
#endif
	if ( gopts->ldo_valid == LDAP_INITIALIZED ) {
		/* someone else got here first */
		goto done;
	}

	ldap_int_error_init();

	ldap_int_utils_init();

#ifdef HAVE_WINSOCK2
{	WORD wVersionRequested;
	WSADATA wsaData;
 
	wVersionRequested = MAKEWORD( 2, 0 );
	if ( WSAStartup( wVersionRequested, &wsaData ) != 0 ) {
		/* Tell the user that we couldn't find a usable */
		/* WinSock DLL.                                  */
		goto done;
	}
 
	/* Confirm that the WinSock DLL supports 2.0.*/
	/* Note that if the DLL supports versions greater    */
	/* than 2.0 in addition to 2.0, it will still return */
	/* 2.0 in wVersion since that is the version we      */
	/* requested.                                        */
 
	if ( LOBYTE( wsaData.wVersion ) != 2 ||
		HIBYTE( wsaData.wVersion ) != 0 )
	{
	    /* Tell the user that we couldn't find a usable */
	    /* WinSock DLL.                                  */
	    WSACleanup( );
	    goto done;
	}
}	/* The WinSock DLL is acceptable. Proceed. */
#elif defined(HAVE_WINSOCK)
{	WSADATA wsaData;
	if ( WSAStartup( 0x0101, &wsaData ) != 0 ) {
	    goto done;
	}
}
#endif

#if defined(HAVE_TLS) || defined(HAVE_CYRUS_SASL)
	LDAP_MUTEX_LOCK( &ldap_int_hostname_mutex );
	{
		char	*name = ldap_int_hostname;

		ldap_int_hostname = ldap_pvt_get_fqdn( name );

		if ( name != NULL && name != ldap_int_hostname ) {
			LDAP_FREE( name );
		}
	}
	LDAP_MUTEX_UNLOCK( &ldap_int_hostname_mutex );
#endif

#ifndef HAVE_POLL
	if ( ldap_int_tblsize == 0 ) ldap_int_ip_init();
#endif

#ifdef HAVE_CYRUS_SASL
	if ( ldap_int_sasl_init() != 0 ) {
		goto done;
	}
#endif

	ldap_int_initialize_global_options(gopts, dbglvl);

	if( getenv("LDAPNOINIT") != NULL ) {
		goto done;
	}

#ifdef LDAP_R_COMPILE
	if( getenv("LDAPSTACKGUARD") != NULL ) {
		ldap_int_stackguard = 1;
	}
#endif

#ifdef HAVE_CYRUS_SASL
	{
		/* set authentication identity to current user name */
		char *user = getenv("USER");

		if( user == NULL ) user = getenv("USERNAME");
		if( user == NULL ) user = getenv("LOGNAME");

		if( user != NULL ) {
			gopts->ldo_def_sasl_authcid = LDAP_STRDUP( user );
		}
    }
#endif

	openldap_ldap_init_w_sysconf(LDAP_CONF_FILE);

#ifdef HAVE_GETEUID
	if ( geteuid() != getuid() )
		goto done;
#endif

	openldap_ldap_init_w_userconf(LDAP_USERRC_FILE);

	{
		char *altfile = getenv(LDAP_ENV_PREFIX "CONF");

		if( altfile != NULL ) {
			Debug2(LDAP_DEBUG_TRACE, "ldap_init: %s env is %s\n",
			      LDAP_ENV_PREFIX "CONF", altfile );
			openldap_ldap_init_w_sysconf( altfile );
		}
		else
			Debug1(LDAP_DEBUG_TRACE, "ldap_init: %s env is NULL\n",
			      LDAP_ENV_PREFIX "CONF" );
	}

	{
		char *altfile = getenv(LDAP_ENV_PREFIX "RC");

		if( altfile != NULL ) {
			Debug2(LDAP_DEBUG_TRACE, "ldap_init: %s env is %s\n",
			      LDAP_ENV_PREFIX "RC", altfile );
			openldap_ldap_init_w_userconf( altfile );
		}
		else
			Debug1(LDAP_DEBUG_TRACE, "ldap_init: %s env is NULL\n",
			      LDAP_ENV_PREFIX "RC" );
	}

	openldap_ldap_init_w_env(gopts, NULL);

done:;
#ifdef LDAP_R_COMPILE
	LDAP_MUTEX_UNLOCK( &init_mutex );
#endif
}
