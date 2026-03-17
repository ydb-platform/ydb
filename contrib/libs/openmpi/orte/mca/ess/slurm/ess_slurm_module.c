/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2011 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013-2018 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#include "orte_config.h"
#include "orte/constants.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */
#include <string.h>
#include <ctype.h>


#include "opal/util/opal_environ.h"
#include "opal/util/output.h"
#include "opal/util/argv.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/dss/dss.h"

#include "orte/util/proc_info.h"
#include "orte/util/show_help.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/rml/rml.h"
#include "orte/util/name_fns.h"
#include "orte/runtime/orte_globals.h"

#include "orte/mca/ess/ess.h"
#include "orte/mca/ess/base/base.h"
#include "orte/mca/ess/slurm/ess_slurm.h"

static int slurm_set_name(void);

static int rte_init(void);
static int rte_finalize(void);

orte_ess_base_module_t orte_ess_slurm_module = {
    rte_init,
    rte_finalize,
    orte_ess_base_app_abort,
    NULL /* ft_event */
};

static int rte_init(void)
{
    int ret;
    char *error = NULL;

    /* run the prolog */
    if (ORTE_SUCCESS != (ret = orte_ess_base_std_prolog())) {
        error = "orte_ess_base_std_prolog";
        goto error;
    }

    /* Start by getting a unique name */
    slurm_set_name();

    /* if I am a daemon, complete my setup using the
     * default procedure
     */
    if (ORTE_PROC_IS_DAEMON) {
        if (ORTE_SUCCESS != (ret = orte_ess_base_orted_setup())) {
            ORTE_ERROR_LOG(ret);
            error = "orte_ess_base_orted_setup";
            goto error;
        }
        return ORTE_SUCCESS;
    }

    if (ORTE_PROC_IS_TOOL) {
        /* otherwise, if I am a tool proc, use that procedure */
        if (ORTE_SUCCESS != (ret = orte_ess_base_tool_setup(NULL))) {
            ORTE_ERROR_LOG(ret);
            error = "orte_ess_base_tool_setup";
            goto error;
        }
        return ORTE_SUCCESS;

    }

    /* no other options are supported! */
    error = "ess_error";
    ret = ORTE_ERROR;

error:
    if (ORTE_ERR_SILENT != ret && !orte_report_silent_errors) {
        orte_show_help("help-orte-runtime.txt",
                       "orte_init:startup:internal-failure",
                       true, error, ORTE_ERROR_NAME(ret), ret);
    }

    return ret;
}

static int rte_finalize(void)
{
    int ret;

    /* if I am a daemon, finalize using the default procedure */
    if (ORTE_PROC_IS_DAEMON) {
        if (ORTE_SUCCESS != (ret = orte_ess_base_orted_finalize())) {
            ORTE_ERROR_LOG(ret);
            return ret;
        }
    } else if (ORTE_PROC_IS_TOOL) {
        /* otherwise, if I am a tool proc, use that procedure */
        if (ORTE_SUCCESS != (ret = orte_ess_base_tool_finalize())) {
            ORTE_ERROR_LOG(ret);
        }
        return ret;
    } else {
        /* otherwise, I must be an application process
         * use the default procedure to finish
         */
        if (ORTE_SUCCESS != (ret = orte_ess_base_app_finalize())) {
            ORTE_ERROR_LOG(ret);
            return ret;
        }
    }

    return ORTE_SUCCESS;
}

static int slurm_set_name(void)
{
    int slurm_nodeid;
    int rc;
    orte_jobid_t jobid;
    orte_vpid_t vpid;
    char *tmp;

    OPAL_OUTPUT_VERBOSE((1, orte_ess_base_framework.framework_output,
                         "ess:slurm setting name"));

    if (NULL == orte_ess_base_jobid) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        return ORTE_ERR_NOT_FOUND;
    }
    if (ORTE_SUCCESS != (rc = orte_util_convert_string_to_jobid(&jobid, orte_ess_base_jobid))) {
        ORTE_ERROR_LOG(rc);
        return(rc);
    }

    if (NULL == orte_ess_base_vpid) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        return ORTE_ERR_NOT_FOUND;
    }
    if (ORTE_SUCCESS != (rc = orte_util_convert_string_to_vpid(&vpid, orte_ess_base_vpid))) {
        ORTE_ERROR_LOG(rc);
        return(rc);
    }

    ORTE_PROC_MY_NAME->jobid = jobid;

    /* fix up the vpid and make it the "real" vpid */
    slurm_nodeid = atoi(getenv("SLURM_NODEID"));
    ORTE_PROC_MY_NAME->vpid = vpid + slurm_nodeid;

    OPAL_OUTPUT_VERBOSE((1, orte_ess_base_framework.framework_output,
                         "ess:slurm set name to %s", ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    /* fix up the system info nodename to match exactly what slurm returned */
    if (NULL != orte_process_info.nodename) {
        free(orte_process_info.nodename);
    }
    if (NULL == (tmp = getenv("SLURMD_NODENAME"))) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        return ORTE_ERR_NOT_FOUND;
    }
    orte_process_info.nodename = strdup(tmp);


    OPAL_OUTPUT_VERBOSE((1, orte_ess_base_framework.framework_output,
                         "ess:slurm set nodename to %s",
                         (NULL == orte_process_info.nodename) ? "NULL" : orte_process_info.nodename));

    /* get the non-name common environmental variables */
    if (ORTE_SUCCESS != (rc = orte_ess_env_get())) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }

    return ORTE_SUCCESS;
}
