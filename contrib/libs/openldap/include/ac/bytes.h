/* Generic bytes.h */
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

#ifndef _AC_BYTES_H
#define _AC_BYTES_H

/* cross compilers should define both AC_INT{2,4}_TYPE in CPPFLAGS */

#if !defined( AC_INT4_TYPE )
	/* use autoconf defines to provide sized typedefs */
#	if SIZEOF_LONG == 4
#		define AC_INT4_TYPE long
#	elif SIZEOF_INT == 4
#		define AC_INT4_TYPE int
#	elif SIZEOF_SHORT == 4
#		define AC_INT4_TYPE short
#	else
#	error "AC_INT4_TYPE?"
#	endif
#endif

typedef AC_INT4_TYPE ac_int4;
typedef signed AC_INT4_TYPE ac_sint4;
typedef unsigned AC_INT4_TYPE ac_uint4;

#if !defined( AC_INT2_TYPE )
#	if SIZEOF_SHORT == 2
#		define AC_INT2_TYPE short
#	elif SIZEOF_INT == 2
#		define AC_INT2_TYPE int
#	elif SIZEOF_LONG == 2
#		define AC_INT2_TYPE long
#	else
#	error "AC_INT2_TYPE?"
#	endif
#endif
 
#if defined( AC_INT2_TYPE )
typedef AC_INT2_TYPE ac_int2;
typedef signed AC_INT2_TYPE ac_sint2;
typedef unsigned AC_INT2_TYPE ac_uint2;
#endif

#ifndef BYTE_ORDER
/* cross compilers should define BYTE_ORDER in CPPFLAGS */

/*
 * Definitions for byte order, according to byte significance from low
 * address to high.
 */
#define LITTLE_ENDIAN   1234    /* LSB first: i386, vax */
#define BIG_ENDIAN  4321        /* MSB first: 68000, ibm, net */
#define PDP_ENDIAN  3412        /* LSB first in word, MSW first in long */

/* assume autoconf's AC_C_BIGENDIAN has been ran */
/* if it hasn't, we assume (maybe falsely) the order is LITTLE ENDIAN */
#	ifdef WORDS_BIGENDIAN
#		define BYTE_ORDER  BIG_ENDIAN
#	else
#		define BYTE_ORDER  LITTLE_ENDIAN
#	endif

#endif /* BYTE_ORDER */

#endif /* _AC_BYTES_H */
