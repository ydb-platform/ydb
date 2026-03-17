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
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include <sys/types.h>
#include <stdio.h>
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "orte/mca/errmgr/errmgr.h"
#include "orte/util/show_help.h"
#include "orte/runtime/orte_wait.h"
#include "orte/runtime/runtime_internals.h"

#include "orte/mca/ess/base/base.h"

int orte_ess_base_std_prolog(void)
{
    int ret;
    char *error = NULL;

    /* Initialize the ORTE data type support */
    if (ORTE_SUCCESS != (ret = orte_dt_init())) {
        error = "orte_dt_init";
        goto error;
    }

    if (!ORTE_PROC_IS_APP) {
        /*
         * Setup the waitpid/sigchld system
         */
        if (ORTE_SUCCESS != (ret = orte_wait_init())) {
            ORTE_ERROR_LOG(ret);
            error = "orte_wait_init";
            goto error;
        }
    }

    return ORTE_SUCCESS;

 error:
    orte_show_help("help-orte-runtime",
                   "orte_init:startup:internal-failure",
                   true, error, ORTE_ERROR_NAME(ret), ret);

    return ret;
}
