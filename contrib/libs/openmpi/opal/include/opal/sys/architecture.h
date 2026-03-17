/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2011      Sandia National Laboratories. All rights reserved.
 * Copyright (c) 2014      Intel, Inc. All rights reserved
 * Copyright (c) 2016      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/*
 * List of supported architectures
 */

#ifndef OPAL_SYS_ARCHITECTURE_H
#define OPAL_SYS_ARCHITECTURE_H

/* Architectures */
#define OPAL_UNSUPPORTED    0000
#define OPAL_IA32           0010
#define OPAL_IA64           0020
#define OPAL_X86_64         0030
#define OPAL_POWERPC32      0050
#define OPAL_POWERPC64      0051
#define OPAL_SPARC          0060
#define OPAL_SPARCV9_32     0061
#define OPAL_SPARCV9_64     0062
#define OPAL_MIPS           0070
#define OPAL_ARM            0100
#define OPAL_ARM64          0101
#define OPAL_S390           0110
#define OPAL_S390X          0111
#define OPAL_BUILTIN_SYNC   0200
#define OPAL_BUILTIN_GCC    0202
#define OPAL_BUILTIN_NO     0203

/* Formats */
#define OPAL_DEFAULT        1000  /* standard for given architecture */
#define OPAL_DARWIN         1001  /* Darwin / OS X on PowerPC */
#define OPAL_PPC_LINUX      1002  /* Linux on PowerPC */
#define OPAL_AIX            1003  /* AIX on Power / PowerPC */

#endif /* #ifndef OPAL_SYS_ARCHITECTURE_H */
