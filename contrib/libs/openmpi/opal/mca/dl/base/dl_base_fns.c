/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2015 Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * This file is a simple set of wrappers around the selected OPAL DL
 * component (it's a compile-time framework with, at most, a single
 * component; see dl.h for details).
 */

#include "opal_config.h"

#include "opal/include/opal/constants.h"

#include "opal/mca/dl/base/base.h"


int opal_dl_open(const char *fname,
                 bool use_ext, bool private_namespace,
                 opal_dl_handle_t **handle, char **err_msg)
{
    *handle = NULL;

    if (NULL != opal_dl && NULL != opal_dl->open) {
        return opal_dl->open(fname, use_ext, private_namespace,
                             handle, err_msg);
    }

    return OPAL_ERR_NOT_SUPPORTED;
}

int opal_dl_lookup(opal_dl_handle_t *handle,
                   const char *symbol,
                   void **ptr, char **err_msg)
{
    if (NULL != opal_dl && NULL != opal_dl->lookup) {
        return opal_dl->lookup(handle, symbol, ptr, err_msg);
    }

    return OPAL_ERR_NOT_SUPPORTED;
}

int opal_dl_close(opal_dl_handle_t *handle)
{
    if (NULL != opal_dl && NULL != opal_dl->close) {
        return opal_dl->close(handle);
    }

    return OPAL_ERR_NOT_SUPPORTED;
}

int opal_dl_foreachfile(const char *search_path,
                        int (*cb_func)(const char *filename, void *context),
                        void *context)
{
    if (NULL != opal_dl && NULL != opal_dl->foreachfile) {
        return opal_dl->foreachfile(search_path, cb_func, context);
    }

    return OPAL_ERR_NOT_SUPPORTED;
}
