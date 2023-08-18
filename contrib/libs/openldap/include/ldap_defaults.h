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
 * A copy of this license is available in file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */
/* Portions Copyright (c) 1994 Regents of the University of Michigan.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms are permitted
 * provided that this notice is preserved and that due credit is given
 * to the University of Michigan at Ann Arbor. The name of the University
 * may not be used to endorse or promote products derived from this
 * software without specific prior written permission. This software
 * is provided ``as is'' without express or implied warranty.
 */

/*
 * This file controls defaults for OpenLDAP package.
 * You probably do not need to edit the defaults provided by this file.
 */

#ifndef _LDAP_DEFAULTS_H
#define _LDAP_DEFAULTS_H


#include <ldap_config.h>

#define LDAP_CONF_FILE	 LDAP_SYSCONFDIR LDAP_DIRSEP "ldap.conf"
#define LDAP_USERRC_FILE "ldaprc"
#define LDAP_ENV_PREFIX "LDAP"

/* default ldapi:// socket */
#ifndef LDAPI_SOCK
#define LDAPI_SOCK LDAP_RUNDIR LDAP_DIRSEP "run" LDAP_DIRSEP "ldapi"
#endif

/*
 * SLAPD DEFINITIONS
 */
	/* location of the default slapd config file */
#ifndef SLAPD_DEFAULT_CONFIGFILE
#define SLAPD_DEFAULT_CONFIGFILE	LDAP_SYSCONFDIR LDAP_DIRSEP "slapd.conf"
#endif
#ifndef SLAPD_DEFAULT_CONFIGDIR
#define SLAPD_DEFAULT_CONFIGDIR		LDAP_SYSCONFDIR LDAP_DIRSEP "slapd.d"
#endif
#ifndef SLAPD_DEFAULT_DB_DIR
#define SLAPD_DEFAULT_DB_DIR		LDAP_RUNDIR LDAP_DIRSEP "openldap-data"
#endif
#define SLAPD_DEFAULT_DB_MODE		0600
	/* default max deref depth for aliases */
#define SLAPD_DEFAULT_MAXDEREFDEPTH	15
	/* default sizelimit on number of entries from a search */
#define SLAPD_DEFAULT_SIZELIMIT		500
	/* default timelimit to spend on a search */
#define SLAPD_DEFAULT_TIMELIMIT		3600

/* the following DNs must be normalized! */
	/* dn of the default subschema subentry */
#define SLAPD_SCHEMA_DN			"cn=Subschema"
	/* dn of the default "monitor" subentry */
#define SLAPD_MONITOR_DN		"cn=Monitor"

/*
 * LLOADD DEFINITIONS
 */
#define LLOADD_DEFAULT_CONFIGFILE	LDAP_SYSCONFDIR LDAP_DIRSEP "lloadd.conf"

#endif /* _LDAP_CONFIG_H */
