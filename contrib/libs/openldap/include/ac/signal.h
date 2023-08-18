/* Generic signal.h */
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

#ifndef _AC_SIGNAL_H
#define _AC_SIGNAL_H

#include <signal.h>

#undef SIGNAL

#if defined( HAVE_SIGACTION )
#define SIGNAL lutil_sigaction
typedef void (*lutil_sig_t)(int);
LDAP_LUTIL_F(lutil_sig_t) lutil_sigaction( int sig, lutil_sig_t func );
#define SIGNAL_REINSTALL(sig,act)	(void)0
#elif defined( HAVE_SIGSET )
#define SIGNAL sigset
#define SIGNAL_REINSTALL sigset
#else
#define SIGNAL signal
#define SIGNAL_REINSTALL signal
#endif

#if !defined( LDAP_SIGUSR1 ) || !defined( LDAP_SIGUSR2 )
#undef LDAP_SIGUSR1
#undef LDAP_SIGUSR2

#	if defined(WINNT) || defined(_WINNT) || defined(_WIN32)
#		define LDAP_SIGUSR1	SIGILL
#		define LDAP_SIGUSR2	SIGTERM

#	elif !defined(HAVE_LINUX_THREADS)
#		define LDAP_SIGUSR1	SIGUSR1
#		define LDAP_SIGUSR2	SIGUSR2

#	else
		/*
		 * Some versions of LinuxThreads unfortunately uses the only
		 * two signals reserved for user applications.  This forces
		 * OpenLDAP to use other signals reserved for other uses.
		 */

#		if defined( SIGSTKFLT )
#			define LDAP_SIGUSR1	SIGSTKFLT
#		elif defined ( SIGSYS )
#			define LDAP_SIGUSR1	SIGSYS
#		endif

#		if defined( SIGUNUSED )
#			define LDAP_SIGUSR2	SIGUNUSED
#		elif defined ( SIGINFO )
#			define LDAP_SIGUSR2	SIGINFO
#		elif defined ( SIGEMT )
#			define LDAP_SIGUSR2	SIGEMT
#		endif
#	endif
#endif

#ifndef LDAP_SIGCHLD
#ifdef SIGCHLD
#define LDAP_SIGCHLD SIGCHLD
#elif SIGCLD
#define LDAP_SIGCHLD SIGCLD
#endif
#endif

#endif /* _AC_SIGNAL_H */
