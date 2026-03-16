/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2014 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2016      Broadcom Limited. All rights reserved.
 * Copyright (c) 2016-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2018      Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file
 *
 * Cycle counter reading instructions.  Do not use directly - see the
 * timer interface instead
 */

#ifndef PMIX_SYS_TIMER_H
#define PMIX_SYS_TIMER_H 1

#include "pmix_config.h"

#include "src/atomics/sys/architecture.h"

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

/* do some quick #define cleanup in cases where we are doing
   testing... */
#ifdef PMIX_DISABLE_INLINE_ASM
#undef PMIX_C_GCC_INLINE_ASSEMBLY
#define PMIX_C_GCC_INLINE_ASSEMBLY 0
#endif

/* define PMIX_{GCC,DEC,XLC}_INLINE_ASSEMBLY based on the
   PMIX_{C,CXX}_{GCC,DEC,XLC}_INLINE_ASSEMBLY defines and whether we
   are in C or C++ */
#if defined(c_plusplus) || defined(__cplusplus)
#define PMIX_GCC_INLINE_ASSEMBLY PMIX_CXX_GCC_INLINE_ASSEMBLY
#else
#define PMIX_GCC_INLINE_ASSEMBLY PMIX_C_GCC_INLINE_ASSEMBLY
#endif

/**********************************************************************
 *
 * Load the appropriate architecture files and set some reasonable
 * default values for our support
 *
 *********************************************************************/

/* By default we suppose all timers are monotonic per node. */
#define PMIX_TIMER_MONOTONIC 1

BEGIN_C_DECLS

/* If you update this list, you probably also want to update
   pmix/mca/timer/linux/configure.m4.  Or not. */

#if defined(DOXYGEN)
/* don't include system-level gorp when generating doxygen files */
#elif PMIX_ASSEMBLY_ARCH == PMIX_X86_64
#include "src/atomics/sys/x86_64/timer.h"
#elif PMIX_ASSEMBLY_ARCH == PMIX_ARM
#include "src/atomics/sys/arm/timer.h"
#elif PMIX_ASSEMBLY_ARCH == PMIX_ARM64
#include "src/atomics/sys/arm64/timer.h"
#elif PMIX_ASSEMBLY_ARCH == PMIX_IA32
#include "src/atomics/sys/ia32/timer.h"
#elif PMIX_ASSEMBLY_ARCH == PMIX_IA64
#error #include "src/atomics/sys/ia64/timer.h"
#elif PMIX_ASSEMBLY_ARCH == PMIX_POWERPC32
#include "src/atomics/sys/powerpc/timer.h"
#elif PMIX_ASSEMBLY_ARCH == PMIX_POWERPC64
#include "src/atomics/sys/powerpc/timer.h"
#elif PMIX_ASSEMBLY_ARCH == PMIX_SPARCV9_32
#include "src/atomics/sys/sparcv9/timer.h"
#elif PMIX_ASSEMBLY_ARCH == PMIX_SPARCV9_64
#include "src/atomics/sys/sparcv9/timer.h"
#elif PMIX_ASSEMBLY_ARCH == PMIX_MIPS
#error #include "src/atomics/sys/mips/timer.h"
#endif

#ifndef DOXYGEN
#ifndef PMIX_HAVE_SYS_TIMER_GET_CYCLES
#define PMIX_HAVE_SYS_TIMER_GET_CYCLES 0

typedef long pmix_timer_t;
#endif
#endif

#ifndef PMIX_HAVE_SYS_TIMER_IS_MONOTONIC

#define PMIX_HAVE_SYS_TIMER_IS_MONOTONIC 1

static inline bool pmix_sys_timer_is_monotonic (void)
{
    return PMIX_TIMER_MONOTONIC;
}

#endif

END_C_DECLS

#endif /* PMIX_SYS_TIMER_H */
