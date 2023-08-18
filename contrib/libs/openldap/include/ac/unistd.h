/* Generic unistd.h */
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

#ifndef _AC_UNISTD_H
#define _AC_UNISTD_H

#ifdef HAVE_SYS_TYPES_H
#	include <sys/types.h>
#endif

#ifdef HAVE_UNISTD_H
#	include <unistd.h>
#endif

#ifdef HAVE_PROCESS_H
#	include <process.h>
#endif

/* note: callers of crypt(3) should include <ac/crypt.h> */

#if defined(HAVE_GETPASSPHRASE)
LDAP_LIBC_F(char*)(getpassphrase)();

#else
#define getpassphrase(p) lutil_getpass(p)
LDAP_LUTIL_F(char*)(lutil_getpass) LDAP_P((const char *getpass));
#endif

/* getopt() defines may be in separate include file */
#ifdef HAVE_GETOPT_H
#	include <getopt.h>

#elif !defined(HAVE_GETOPT)
	/* no getopt, assume we need getopt-compat.h */
#	error #include <getopt-compat.h>

#else
	/* assume we need to declare these externs */
	LDAP_LIBC_V (char *) optarg;
	LDAP_LIBC_V (int) optind, opterr, optopt;
#endif

/* use lutil file locking */
#define ldap_lockf(x)	lutil_lockf(x)
#define ldap_unlockf(x)	lutil_unlockf(x)
#include <lutil_lockf.h>

/*
 * Windows: although sleep() will be resolved by both MSVC and Mingw GCC
 * linkers, the function is not declared in header files. This is
 * because Windows' version of the function is called Sleep(), and it
 * is declared in windows.h
 */

#ifdef _WIN32
#define sleep(x)	Sleep((x) * 1000)
#endif

#endif /* _AC_UNISTD_H */
