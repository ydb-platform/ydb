/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2015      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2016      Intel, Inc. All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * This file is a simple set of wrappers around the selected PMIX PDL
 * component (it's a compile-time framework with, at most, a single
 * component; see pdl.h for details).
 */

#include <src/include/pmix_config.h>

#include "pmix_common.h"

#include "src/util/output.h"
#include "src/mca/pdl/base/base.h"


int pmix_pdl_open(const char *fname,
                 bool use_ext, bool private_namespace,
                 pmix_pdl_handle_t **handle, char **err_msg)
{
    *handle = NULL;

    if (NULL != pmix_pdl && NULL != pmix_pdl->open) {
        return pmix_pdl->open(fname, use_ext, private_namespace,
                             handle, err_msg);
    }

    return PMIX_ERR_NOT_SUPPORTED;
}

int pmix_pdl_lookup(pmix_pdl_handle_t *handle,
                   const char *symbol,
                   void **ptr, char **err_msg)
{
    if (NULL != pmix_pdl && NULL != pmix_pdl->lookup) {
        return pmix_pdl->lookup(handle, symbol, ptr, err_msg);
    }

    return PMIX_ERR_NOT_SUPPORTED;
}

int pmix_pdl_close(pmix_pdl_handle_t *handle)
{
    if (NULL != pmix_pdl && NULL != pmix_pdl->close) {
        return pmix_pdl->close(handle);
    }

    return PMIX_ERR_NOT_SUPPORTED;
}

int pmix_pdl_foreachfile(const char *search_path,
                        int (*cb_func)(const char *filename, void *context),
                        void *context)
{
    if (NULL != pmix_pdl && NULL != pmix_pdl->foreachfile) {
       return pmix_pdl->foreachfile(search_path, cb_func, context);
    }

    return PMIX_ERR_NOT_SUPPORTED;
}
