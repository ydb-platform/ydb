/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2013 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2007-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2007-2008 Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc.  All rights reserved.
 * Copyright (c) 2014-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file **/

#include "orte_config.h"
#include "orte/constants.h"

#include <sys/types.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "opal/util/error.h"
#include "opal/util/output.h"
#include "opal/util/proc.h"
#include "opal/util/timings.h"
#include "opal/runtime/opal.h"
#include "opal/threads/threads.h"

#include "orte/util/show_help.h"
#include "orte/mca/ess/base/base.h"
#include "orte/mca/ess/ess.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/schizo/base/base.h"
#include "orte/util/listener.h"
#include "orte/util/name_fns.h"
#include "orte/util/proc_info.h"
#include "orte/util/error_strings.h"
#include "orte/orted/pmix/pmix_server.h"

#include "orte/runtime/runtime.h"
#include "orte/runtime/orte_globals.h"
#include "orte/runtime/orte_locks.h"

/**
 * Static functions used to configure the interactions between the OPAL and
 * the runtime.
 */

static char*
_process_name_print_for_opal(const opal_process_name_t procname)
{
    orte_process_name_t* rte_name = (orte_process_name_t*)&procname;
    return ORTE_NAME_PRINT(rte_name);
}

static char*
_jobid_print_for_opal(const opal_jobid_t jobid)
{
    return ORTE_JOBID_PRINT(jobid);
}

static char*
_vpid_print_for_opal(const opal_vpid_t vpid)
{
    return ORTE_VPID_PRINT(vpid);
}

static int
_process_name_compare(const opal_process_name_t p1, const opal_process_name_t p2)
{
    return orte_util_compare_name_fields(ORTE_NS_CMP_ALL, &p1, &p2);
}

static int _convert_string_to_process_name(opal_process_name_t *name,
                                           const char* name_string)
{
    return orte_util_convert_string_to_process_name(name, name_string);
}

static int _convert_process_name_to_string(char** name_string,
                                          const opal_process_name_t *name)
{
    return orte_util_convert_process_name_to_string(name_string, name);
}

static int
_convert_string_to_jobid(opal_jobid_t *jobid, const char *jobid_string)
{
    return orte_util_convert_string_to_jobid(jobid, jobid_string);
}
/*
 * Whether we have completed orte_init or we are in orte_finalize
 */
int orte_initialized = 0;
bool orte_finalizing = false;
bool orte_debug_flag = false;
int orte_debug_verbosity = -1;
char *orte_prohibited_session_dirs = NULL;
bool orte_create_session_dirs = true;
opal_event_base_t *orte_event_base = {0};
bool orte_event_base_active = true;
bool orte_proc_is_bound = false;
int orte_progress_thread_debug = -1;
hwloc_cpuset_t orte_proc_applied_binding = NULL;

orte_process_name_t orte_name_wildcard = {ORTE_JOBID_WILDCARD, ORTE_VPID_WILDCARD};

orte_process_name_t orte_name_invalid = {ORTE_JOBID_INVALID, ORTE_VPID_INVALID};


#if OPAL_CC_USE_PRAGMA_IDENT
#pragma ident ORTE_IDENT_STRING
#elif OPAL_CC_USE_IDENT
#ident ORTE_IDENT_STRING
#endif
const char orte_version_string[] = ORTE_IDENT_STRING;

int orte_init(int* pargc, char*** pargv, orte_proc_type_t flags)
{
    int ret;
    char *error = NULL;
    OPAL_TIMING_ENV_INIT(tmng);

    if (0 < orte_initialized) {
        /* track number of times we have been called */
        orte_initialized++;
        return ORTE_SUCCESS;
    }
    orte_initialized++;

    /* Convince OPAL to use our naming scheme */
    opal_process_name_print = _process_name_print_for_opal;
    opal_vpid_print = _vpid_print_for_opal;
    opal_jobid_print = _jobid_print_for_opal;
    opal_compare_proc = _process_name_compare;
    opal_convert_string_to_process_name = _convert_string_to_process_name;
    opal_convert_process_name_to_string = _convert_process_name_to_string;
    opal_snprintf_jobid = orte_util_snprintf_jobid;
    opal_convert_string_to_jobid = _convert_string_to_jobid;

    OPAL_TIMING_ENV_NEXT(tmng, "initializations");

    /* initialize the opal layer */
    if (ORTE_SUCCESS != (ret = opal_init(pargc, pargv))) {
        error = "opal_init";
        goto error;
    }

    OPAL_TIMING_ENV_NEXT(tmng, "opal_init");

    /* ensure we know the type of proc for when we finalize */
    orte_process_info.proc_type = flags;

    /* setup the locks */
    if (ORTE_SUCCESS != (ret = orte_locks_init())) {
        error = "orte_locks_init";
        goto error;
    }

    OPAL_TIMING_ENV_NEXT(tmng, "orte_locks_init");

    /* Register all MCA Params */
    if (ORTE_SUCCESS != (ret = orte_register_params())) {
        error = "orte_register_params";
        goto error;
    }

    OPAL_TIMING_ENV_NEXT(tmng, "orte_register_params");

    /* setup the orte_show_help system */
    if (ORTE_SUCCESS != (ret = orte_show_help_init())) {
        error = "opal_output_init";
        goto error;
    }

    OPAL_TIMING_ENV_NEXT(tmng, "orte_show_help_init");

    /* register handler for errnum -> string conversion */
    opal_error_register("ORTE", ORTE_ERR_BASE, ORTE_ERR_MAX, orte_err2str);

    OPAL_TIMING_ENV_NEXT(tmng, "opal_error_register");

    /* Ensure the rest of the process info structure is initialized */
    if (ORTE_SUCCESS != (ret = orte_proc_info())) {
        error = "orte_proc_info";
        goto error;
    }

    OPAL_TIMING_ENV_NEXT(tmng, "orte_proc_info");

    /* we may have modified the local nodename according to
     * request to retain/strip the FQDN and prefix, so update
     * it here. The OPAL layer will strdup the hostname, so
     * we have to free it first to avoid a memory leak */
    if (NULL != opal_process_info.nodename) {
        free(opal_process_info.nodename);
    }
    /* opal_finalize_util will call free on this pointer so set from strdup */
    opal_process_info.nodename = strdup (orte_process_info.nodename);

    if (ORTE_PROC_IS_DAEMON || ORTE_PROC_IS_HNP) {
        /* let the pmix server register params */
        pmix_server_register_params();
        OPAL_TIMING_ENV_NEXT(tmng, "pmix_server_register_params");
    }

    /* open the SCHIZO framework as everyone needs it, and the
     * ess will use it to help select its component */
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&orte_schizo_base_framework, 0))) {
        ORTE_ERROR_LOG(ret);
        error = "orte_schizo_base_open";
        goto error;
    }

    OPAL_TIMING_ENV_NEXT(tmng, "framework_open(schizo)");

    if (ORTE_SUCCESS != (ret = orte_schizo_base_select())) {
        error = "orte_schizo_base_select";
        goto error;
    }

    OPAL_TIMING_ENV_NEXT(tmng, "orte_schizo_base_select");

    /* if we are an app, let SCHIZO help us determine our environment */
    if (ORTE_PROC_IS_APP) {
        (void)orte_schizo.check_launch_environment();
        OPAL_TIMING_ENV_NEXT(tmng, "orte_schizo.check_launch_environment");
    }

    /* open the ESS and select the correct module for this environment */
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&orte_ess_base_framework, 0))) {
        ORTE_ERROR_LOG(ret);
        error = "orte_ess_base_open";
        goto error;
    }

    OPAL_TIMING_ENV_NEXT(tmng, "framework_open(ess)");

    if (ORTE_SUCCESS != (ret = orte_ess_base_select())) {
        error = "orte_ess_base_select";
        goto error;
    }

    OPAL_TIMING_ENV_NEXT(tmng, "orte_ess_base_select");

    if (!ORTE_PROC_IS_APP) {
        /* ORTE tools "block" in their own loop over the event
         * base, so no progress thread is required - apps will
         * start their progress thread in ess_base_std_app.c
         * at the appropriate point
         */
        orte_event_base = opal_sync_event_base;
    }

    /* initialize the RTE for this environment */
    if (ORTE_SUCCESS != (ret = orte_ess.init())) {
        error = "orte_ess_init";
        goto error;
    }

    OPAL_TIMING_ENV_NEXT(tmng, "orte_ess.init");

    /* set the remaining opal_process_info fields. Note that
     * the OPAL layer will have initialized these to NULL, and
     * anyone between us would not have strdup'd the string, so
     * we cannot free it here */
    opal_process_info.job_session_dir  = orte_process_info.job_session_dir;
    opal_process_info.proc_session_dir = orte_process_info.proc_session_dir;
    opal_process_info.num_local_peers  = (int32_t)orte_process_info.num_local_peers;
    opal_process_info.my_local_rank    = (int32_t)orte_process_info.my_local_rank;
    opal_process_info.cpuset           = orte_process_info.cpuset;

    if (ORTE_PROC_IS_HNP || ORTE_PROC_IS_DAEMON) {
        /* start listening - will be ignored if no listeners
         * were registered */
        if (ORTE_SUCCESS != (ret = orte_start_listening())) {
            ORTE_ERROR_LOG(ret);
            error = "orte_start_listening";
            goto error;
        }
    }

    OPAL_TIMING_ENV_NEXT(tmng, "finalize");
    /* All done */
    return ORTE_SUCCESS;

 error:
    if (ORTE_ERR_SILENT != ret) {
        orte_show_help("help-orte-runtime",
                       "orte_init:startup:internal-failure",
                       true, error, ORTE_ERROR_NAME(ret), ret);
    }

    return ret;
}
