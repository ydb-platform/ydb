/* Generic assert.h */
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

#ifndef _AC_ASSERT_H
#define _AC_ASSERT_H

#undef assert

#ifdef LDAP_DEBUG

#if defined( HAVE_ASSERT_H ) || defined( STDC_HEADERS )

#undef NDEBUG
#include <assert.h>

#else /* !(HAVE_ASSERT_H || STDC_HEADERS) */

#define LDAP_NEED_ASSERT 1

/*
 * no assert()... must be a very old compiler.
 * create a replacement and hope it works
 */

LBER_F (void) ber_pvt_assert LDAP_P(( const char *file, int line,
	const char *test ));

/* Can't use LDAP_STRING(test), that'd expand to "test" */
#if defined(__STDC__) || defined(__cplusplus)
#define assert(test) \
	((test) ? (void)0 : ber_pvt_assert( __FILE__, __LINE__, #test ) )
#else
#define assert(test) \
	((test) ? (void)0 : ber_pvt_assert( __FILE__, __LINE__, "test" ) )
#endif

#endif /* (HAVE_ASSERT_H || STDC_HEADERS) */

#else /* !LDAP_DEBUG */
/* no asserts */
#define assert(test) ((void)0)
#endif /* LDAP_DEBUG */

#endif /* _AC_ASSERT_H */
