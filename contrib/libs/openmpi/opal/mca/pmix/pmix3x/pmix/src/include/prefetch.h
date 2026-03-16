/*
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file
 *
 * Compiler-specific prefetch functions
 *
 * A small set of prefetch / prediction interfaces for using compiler
 * directives to improve memory prefetching and branch prediction
 */

#ifndef PMIX_PREFETCH_H
#define PMIX_PREFETCH_H

#include <src/include/pmix_config.h>

#if PMIX_C_HAVE_BUILTIN_EXPECT
#define PMIX_LIKELY(expression) __builtin_expect(!!(expression), 1)
#define PMIX_UNLIKELY(expression) __builtin_expect(!!(expression), 0)
#else
#define PMIX_LIKELY(expression) (expression)
#define PMIX_UNLIKELY(expression) (expression)
#endif

#if PMIX_C_HAVE_BUILTIN_PREFETCH
#define PMIX_PREFETCH(address,rw,locality) __builtin_prefetch(address,rw,locality)
#else
#define PMIX_PREFETCH(address,rw,locality)
#endif

#endif
