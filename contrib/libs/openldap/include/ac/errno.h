/* Generic errno.h */
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

#ifndef _AC_ERRNO_H
#define _AC_ERRNO_H

#if defined( HAVE_ERRNO_H )
# include <errno.h>
#elif defined( HAVE_SYS_ERRNO_H )
# include <sys/errno.h>
#endif

#if defined( HAVE_SYS_ERRLIST ) && defined( DECL_SYS_ERRLIST )
	/* have sys_errlist but need declaration */
	LDAP_LIBC_V(int)      sys_nerr;
	LDAP_LIBC_V(char)    *sys_errlist[];
#endif

#endif /* _AC_ERRNO_H */
