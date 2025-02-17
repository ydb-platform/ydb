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

#include <ac/ctype.h>
#include <ac/stdarg.h>
#include <ac/string.h>
#include <ac/time.h>

#include "ldap-int.h"

/*
 * ldap log 
 */

static int ldap_log_check( LDAP *ld, int loglvl )
{
	int errlvl;

	if(ld == NULL) {
		errlvl = ldap_debug;
	} else {
		errlvl = ld->ld_debug;
	}

	return errlvl & loglvl ? 1 : 0;
}

int ldap_log_printf( LDAP *ld, int loglvl, const char *fmt, ... )
{
	char buf[ 1024 ];
	va_list ap;

	if ( !ldap_log_check( ld, loglvl )) {
		return 0;
	}

	va_start( ap, fmt );

	buf[sizeof(buf) - 1] = '\0';
	vsnprintf( buf, sizeof(buf)-1, fmt, ap );

	va_end(ap);

	(*ber_pvt_log_print)( buf );
	return 1;
}
