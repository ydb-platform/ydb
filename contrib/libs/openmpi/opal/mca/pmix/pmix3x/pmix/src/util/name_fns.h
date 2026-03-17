/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2010      Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2014-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2018      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef _PMIX_NAME_FNS_H_
#define _PMIX_NAME_FNS_H_

#include "pmix_config.h"

#ifdef HAVE_STDINT_h
#include <stdint.h>
#endif

#include "pmix_common.h"

BEGIN_C_DECLS

/* useful define to print name args in output messages */
PMIX_EXPORT char* pmix_util_print_name_args(const pmix_proc_t *name);
#define PMIX_NAME_PRINT(n) \
    pmix_util_print_name_args(n)

PMIX_EXPORT char* pmix_util_print_rank(const pmix_rank_t vpid);
#define PMIX_RANK_PRINT(n) \
    pmix_util_print_rank(n)


END_C_DECLS
#endif
