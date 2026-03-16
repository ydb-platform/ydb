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
 * Copyright (c) 2007      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "pmix_config.h"
#include "pmix_common.h"

#include <string.h>
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif  /* HAVE_SYS_TIME_H */
#include <stdarg.h>

#include "src/util/argv.h"
#include "src/util/error.h"
#include "src/util/name_fns.h"
#include "src/util/show_help.h"
#include "src/common/pmix_iof.h"

#include "src/mca/plog/base/base.h"
#include "plog_stdfd.h"


/* Static API's */
static int init(void);
static void finalize(void);
static pmix_status_t mylog(const pmix_proc_t *source,
                           const pmix_info_t data[], size_t ndata,
                           const pmix_info_t directives[], size_t ndirs,
                           pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Module def */
pmix_plog_module_t pmix_plog_stdfd_module = {
    .name = "stdfd",
    .init = init,
    .finalize = finalize,
    .log = mylog
};


static int init(void)
{
    char *mychannels = "stdout,stderr";

    pmix_plog_stdfd_module.channels = pmix_argv_split(mychannels, ',');
    return PMIX_SUCCESS;
}

static void finalize(void)
{
    pmix_argv_free(pmix_plog_stdfd_module.channels);
}

static pmix_status_t mylog(const pmix_proc_t *source,
                           const pmix_info_t data[], size_t ndata,
                           const pmix_info_t directives[], size_t ndirs,
                           pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    size_t n;
    pmix_status_t rc;
    pmix_byte_object_t bo;
    pmix_iof_flags_t flags= {0};

    /* if there is no data, then we don't handle it */
    if (NULL == data || 0 == ndata) {
        return PMIX_ERR_NOT_AVAILABLE;
    }

    /* if we are not a gateway, then we don't handle this */
    if (!PMIX_PROC_IS_GATEWAY(pmix_globals.mypeer)) {
        return PMIX_ERR_TAKE_NEXT_OPTION;
    }

    /* check to see if there are any relevant directives */
    for (n=0; n < ndirs; n++) {
        if (0 == strncmp(directives[n].key, PMIX_LOG_TIMESTAMP, PMIX_MAX_KEYLEN)) {
            flags.timestamp = directives[n].value.data.time;
        } else if (0 == strncmp(directives[n].key, PMIX_LOG_XML_OUTPUT, PMIX_MAX_KEYLEN)) {
            flags.xml = PMIX_INFO_TRUE(&directives[n]);
        } else if (0 == strncmp(directives[n].key, PMIX_LOG_TAG_OUTPUT, PMIX_MAX_KEYLEN)) {
            flags.tag = PMIX_INFO_TRUE(&directives[n]);
        }
    }

    /* check to see if there are any stdfd entries */
    rc = PMIX_ERR_TAKE_NEXT_OPTION;
    for (n=0; n < ndata; n++) {
        if (0 == strncmp(data[n].key, PMIX_LOG_STDERR, PMIX_MAX_KEYLEN)) {
            bo.bytes = data[n].value.data.string;
            bo.size = strlen(bo.bytes);
            pmix_iof_write_output(source, PMIX_FWD_STDERR_CHANNEL, &bo, &flags);
            /* flag that we did this one */
            PMIX_INFO_OP_COMPLETED(&data[n]);
            rc = PMIX_SUCCESS;
        } else if (0 == strncmp(data[n].key, PMIX_LOG_STDOUT, PMIX_MAX_KEYLEN)) {
            bo.bytes = data[n].value.data.string;
            bo.size = strlen(bo.bytes);
            pmix_iof_write_output(source, PMIX_FWD_STDOUT_CHANNEL, &bo, &flags);
            /* flag that we did this one */
            PMIX_INFO_OP_COMPLETED(&data[n]);
            rc = PMIX_SUCCESS;
        }
    }

    return rc;
}
