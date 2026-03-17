/* -*- C -*-
 *
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
 * Copyright (c) 2007-2011 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos National Security, Inc. All rights reserved.
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2019      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * Buffer management types.
 */

#ifndef PMIX_MCA_PREG_TYPES_H_
#define PMIX_MCA_PREG_TYPES_H_

#include <src/include/pmix_config.h>


#include "src/class/pmix_object.h"
#include "src/class/pmix_list.h"

BEGIN_C_DECLS

/* these classes are required by the regex code */
typedef struct {
    pmix_list_item_t super;
    int start;
    int cnt;
} pmix_regex_range_t;
PMIX_EXPORT PMIX_CLASS_DECLARATION(pmix_regex_range_t);

typedef struct {
    /* list object */
    pmix_list_item_t super;
    char *prefix;
    char *suffix;
    int num_digits;
    pmix_list_t ranges;
    bool skip;
} pmix_regex_value_t;
PMIX_EXPORT PMIX_CLASS_DECLARATION(pmix_regex_value_t);

END_C_DECLS

#endif /* PMIX_PREG_TYPES_H */
