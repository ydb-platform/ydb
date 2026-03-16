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

#ifndef OPAL_SYS_TIMER_H
#define OPAL_SYS_TIMER_H 1

#include "opal_config.h"

#include "opal/sys/architecture.h"

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

/* do some quick #define cleanup in cases where we are doing
   testing... */
#ifdef OPAL_DISABLE_INLINE_ASM
#undef OPAL_C_GCC_INLINE_ASSEMBLY
#define OPAL_C_GCC_INLINE_ASSEMBLY 0
#endif

/* define OPAL_{GCC,DEC,XLC}_INLINE_ASSEMBLY based on the
   OPAL_{C,CXX}_{GCC,DEC,XLC}_INLINE_ASSEMBLY defines and whether we
   are in C or C++ */
#if defined(c_plusplus) || defined(__cplusplus)
#define OPAL_GCC_INLINE_ASSEMBLY OPAL_CXX_GCC_INLINE_ASSEMBLY
#else
#define OPAL_GCC_INLINE_ASSEMBLY OPAL_C_GCC_INLINE_ASSEMBLY
#endif

/**********************************************************************
 *
 * Load the appropriate architecture files and set some reasonable
 * default values for our support
 *
 *********************************************************************/

/* By default we suppose all timers are monotonic per node. */
#define OPAL_TIMER_MONOTONIC 1

BEGIN_C_DECLS

/* If you update this list, you probably also want to update
   opal/mca/timer/linux/configure.m4.  Or not. */

#if defined(DOXYGEN)
/* don't include system-level gorp when generating doxygen files */
#elif OPAL_ASSEMBLY_ARCH == OPAL_X86_64
#include "opal/sys/x86_64/timer.h"
#elif OPAL_ASSEMBLY_ARCH == OPAL_ARM
#include "opal/sys/arm/timer.h"
#elif OPAL_ASSEMBLY_ARCH == OPAL_ARM64
#include "opal/sys/arm64/timer.h"
#elif OPAL_ASSEMBLY_ARCH == OPAL_IA32
#include "opal/sys/ia32/timer.h"
#elif OPAL_ASSEMBLY_ARCH == OPAL_IA64
#error #include "opal/sys/ia64/timer.h"
#elif OPAL_ASSEMBLY_ARCH == OPAL_POWERPC32
#include "opal/sys/powerpc/timer.h"
#elif OPAL_ASSEMBLY_ARCH == OPAL_POWERPC64
#include "opal/sys/powerpc/timer.h"
#elif OPAL_ASSEMBLY_ARCH == OPAL_SPARCV9_32
#include "opal/sys/sparcv9/timer.h"
#elif OPAL_ASSEMBLY_ARCH == OPAL_SPARCV9_64
#include "opal/sys/sparcv9/timer.h"
#elif OPAL_ASSEMBLY_ARCH == OPAL_MIPS
#error #include "opal/sys/mips/timer.h"
#endif

#ifndef DOXYGEN
#ifndef OPAL_HAVE_SYS_TIMER_GET_CYCLES
#define OPAL_HAVE_SYS_TIMER_GET_CYCLES 0

typedef long opal_timer_t;
#endif
#endif

#ifndef OPAL_HAVE_SYS_TIMER_IS_MONOTONIC

#define OPAL_HAVE_SYS_TIMER_IS_MONOTONIC 1

static inline bool opal_sys_timer_is_monotonic (void)
{
    return OPAL_TIMER_MONOTONIC;
}

#endif

END_C_DECLS

#endif /* OPAL_SYS_TIMER_H */
