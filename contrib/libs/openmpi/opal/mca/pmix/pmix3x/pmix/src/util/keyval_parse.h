/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2016-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file */

#ifndef PMIX_UTIL_KEYVAL_PARSE_H
#define PMIX_UTIL_KEYVAL_PARSE_H

#include <src/include/pmix_config.h>

BEGIN_C_DECLS

PMIX_EXPORT extern int pmix_util_keyval_parse_lineno;

/**
 * Callback triggered for each key = value pair
 *
 * Callback triggered from pmix_util_keyval_parse for each key = value
 * pair.  Both key and value will be pointers into static buffers.
 * The buffers must not be free()ed and contents may be overwritten
 * immediately after the callback returns.
 */
typedef void (*pmix_keyval_parse_fn_t)(const char *key, const char *value);

/**
 * Parse \c filename, made up of key = value pairs.
 *
 * Parse \c filename, made up of key = value pairs.  For each line
 * that appears to contain a key = value pair, \c callback will be
 * called exactly once.  In a multithreaded context, calls to
 * pmix_util_keyval_parse() will serialize multiple calls.
 */
PMIX_EXPORT int pmix_util_keyval_parse(const char *filename,
                                       pmix_keyval_parse_fn_t callback);

PMIX_EXPORT int pmix_util_keyval_parse_init(void);

PMIX_EXPORT int pmix_util_keyval_parse_finalize(void);

PMIX_EXPORT int pmix_util_keyval_save_internal_envars(pmix_keyval_parse_fn_t callback);

END_C_DECLS

#endif
