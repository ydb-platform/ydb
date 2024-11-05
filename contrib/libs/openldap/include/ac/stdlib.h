/* Generic stdlib.h */
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

#ifndef _AC_STDLIB_H
#define _AC_STDLIB_H

#if defined( HAVE_CSRIMALLOC )
#include <stdio.h>
#define MALLOC_TRACE
#error #include <libmalloc.h>
#endif

#include <stdlib.h>

/* Ignore malloc.h if we have STDC_HEADERS */
#if defined(HAVE_MALLOC_H) && !defined(STDC_HEADERS)
#	include <malloc.h>
#endif

#ifndef EXIT_SUCCESS
#	define EXIT_SUCCESS 0
#	define EXIT_FAILURE 1
#endif

#ifdef HAVE_LIMITS_H
#include <limits.h>
#endif

#if defined(LINE_MAX) 
#	define AC_LINE_MAX LINE_MAX
#else
#	define AC_LINE_MAX 2048 /* POSIX MIN */
#endif

#endif /* _AC_STDLIB_H */
