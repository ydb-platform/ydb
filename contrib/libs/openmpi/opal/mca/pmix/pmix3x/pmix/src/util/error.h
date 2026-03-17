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
 * Copyright (c) 2015-2018 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PMIX_UTIL_ERROR_H
#define PMIX_UTIL_ERROR_H

#include <src/include/pmix_config.h>


#include <pmix_common.h>
#include "src/util/output.h"

 BEGIN_C_DECLS

/* internal error codes - never exposed outside of the library */
#define PMIX_ERR_NOT_AVAILABLE                          (PMIX_INTERNAL_ERR_BASE - 28)
#define PMIX_ERR_FATAL                                  (PMIX_INTERNAL_ERR_BASE - 29)
#define PMIX_ERR_VALUE_OUT_OF_BOUNDS                    (PMIX_INTERNAL_ERR_BASE - 30)
#define PMIX_ERR_PERM                                   (PMIX_INTERNAL_ERR_BASE - 31)
#define PMIX_ERR_NETWORK_NOT_PARSEABLE                  (PMIX_INTERNAL_ERR_BASE - 33)
#define PMIX_ERR_FILE_OPEN_FAILURE                      (PMIX_INTERNAL_ERR_BASE - 34)
#define PMIX_ERR_FILE_READ_FAILURE                      (PMIX_INTERNAL_ERR_BASE - 35)
#define PMIX_ERR_TAKE_NEXT_OPTION                       (PMIX_INTERNAL_ERR_BASE - 36)
#define PMIX_ERR_TEMP_UNAVAILABLE                       (PMIX_INTERNAL_ERR_BASE - 37)

#define PMIX_ERROR_LOG(r)                                           \
 do {                                                               \
    if (PMIX_ERR_SILENT != (r)) {                                   \
        pmix_output(0, "PMIX ERROR: %s in file %s at line %d",      \
                    PMIx_Error_string((r)), __FILE__, __LINE__);    \
    }                                                               \
} while (0)

 END_C_DECLS

#endif /* PMIX_UTIL_ERROR_H */
