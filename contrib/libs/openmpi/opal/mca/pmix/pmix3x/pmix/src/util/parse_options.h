/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2016      Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file:
 *
 */

#ifndef _PMIX_PARSE_OPTIONS_H_
#define _PMIX_PARSE_OPTIONS_H_

#include "pmix_config.h"

BEGIN_C_DECLS

PMIX_EXPORT void pmix_util_parse_range_options(char *input, char ***output);

PMIX_EXPORT void pmix_util_get_ranges(char *inp, char ***startpts, char ***endpts);

END_C_DECLS
#endif
