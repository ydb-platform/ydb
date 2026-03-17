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
 * Copyright (c) 2014-2018 Intel, Inc.  All rights reserved.
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

#ifndef PMIX_SYS_ARCHITECTURE_H
#define PMIX_SYS_ARCHITECTURE_H

/* Architectures */
#define PMIX_UNSUPPORTED    0000
#define PMIX_IA32           0010
#define PMIX_IA64           0020
#define PMIX_X86_64         0030
#define PMIX_POWERPC32      0050
#define PMIX_POWERPC64      0051
#define PMIX_SPARC          0060
#define PMIX_SPARCV9_32     0061
#define PMIX_SPARCV9_64     0062
#define PMIX_MIPS           0070
#define PMIX_ARM            0100
#define PMIX_ARM64          0101
#define PMIX_S390           0110
#define PMIX_S390X          0111
#define PMIX_BUILTIN_SYNC   0200
#define PMIX_BUILTIN_GCC    0202
#define PMIX_BUILTIN_NO     0203
#define PMIX_BUILTIN_C11    0204

/* Formats */
#define PMIX_DEFAULT        1000  /* standard for given architecture */
#define PMIX_DARWIN         1001  /* Darwin / OS X on PowerPC */
#define PMIX_PPC_LINUX      1002  /* Linux on PowerPC */
#define PMIX_AIX            1003  /* AIX on Power / PowerPC */

#endif /* #ifndef PMIX_SYS_ARCHITECTURE_H */
