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
 * Copyright (c) 2008-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2013-2018 Intel, Inc.  All rights reserved.
 * Copyright (c) 2016-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2018      Mellanox Technologies, Inc.
 *                         All rights reserved.
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
#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif
#ifdef HAVE_IFADDRS_H
#include <ifaddrs.h>
#endif
#include <sys/mman.h>
#include <errno.h>
#include <fcntl.h>

#include "opal/util/opal_environ.h"
#include "opal/util/output.h"
#include "opal/util/argv.h"
#include "opal/runtime/opal_progress_threads.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/mca/hwloc/base/base.h"
#include "opal/util/printf.h"
#include "opal/util/proc.h"
#include "opal/mca/pmix/pmix.h"
#include "opal/mca/pmix/base/base.h"
#include "opal/util/timings.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/grpcomm/grpcomm.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/schizo/schizo.h"
#include "orte/util/proc_info.h"
#include "orte/util/show_help.h"
#include "orte/util/name_fns.h"
#include "orte/util/pre_condition_transports.h"
#include "orte/runtime/orte_globals.h"
#include "orte/runtime/orte_wait.h"

#include "orte/mca/ess/ess.h"
#include "orte/mca/ess/base/base.h"
#include "orte/mca/ess/pmi/ess_pmi.h"

static int rte_init(void);
static int rte_finalize(void);
static void rte_abort(int error_code, bool report);

orte_ess_base_module_t orte_ess_pmi_module = {
    rte_init,
    rte_finalize,
    rte_abort,
    NULL /* ft_event */
};

static bool added_transport_keys=false;
static bool added_num_procs = false;
static bool added_app_ctx = false;
static bool progress_thread_running = false;

/****    MODULE FUNCTIONS    ****/

static int rte_init(void)
{
    int ret;
    char *error = NULL;
    char *envar, *ev1, *ev2;
    uint64_t unique_key[2];
    char *string_key;
    opal_value_t *kv;
    char *val;
    int u32, *u32ptr;
    uint16_t u16, *u16ptr;
    char **peers=NULL, *mycpuset;
    opal_process_name_t wildcard_rank, pname;
    bool bool_val, *bool_ptr = &bool_val, tdir_mca_override = false;
    size_t i;

    OPAL_TIMING_ENV_INIT(rte_init);

    /* run the prolog */
    if (ORTE_SUCCESS != (ret = orte_ess_base_std_prolog())) {
        error = "orte_ess_base_std_prolog";
        goto error;
    }
    OPAL_TIMING_ENV_NEXT(rte_init, "orte_ess_base_std_prolog");

    /* get an async event base - we use the opal_async one so
     * we don't startup extra threads if not needed */
    orte_event_base = opal_progress_thread_init(NULL);
    progress_thread_running = true;
    OPAL_TIMING_ENV_NEXT(rte_init, "progress_thread_init");

    /* open and setup pmix */
    if (OPAL_SUCCESS != (ret = mca_base_framework_open(&opal_pmix_base_framework, 0))) {
        ORTE_ERROR_LOG(ret);
        /* we cannot run */
        error = "pmix init";
        goto error;
    }
    if (OPAL_SUCCESS != (ret = opal_pmix_base_select())) {
        /* we cannot run */
        error = "pmix init";
        goto error;
    }
    /* set the event base */
    opal_pmix_base_set_evbase(orte_event_base);
    OPAL_TIMING_ENV_NEXT(rte_init, "pmix_framework_open");

    /* initialize the selected module */
    if (!opal_pmix.initialized() && (OPAL_SUCCESS != (ret = opal_pmix.init(NULL)))) {
        /* we cannot run - this could be due to being direct launched
         * without the required PMI support being built. Try to detect
         * that scenario and warn the user */
        if (ORTE_SCHIZO_DIRECT_LAUNCHED == orte_schizo.check_launch_environment() &&
            NULL != (envar = getenv("ORTE_SCHIZO_DETECTION"))) {
            if (0 == strcmp(envar, "SLURM")) {
                /* yes to both - so emit a hopefully helpful
                 * error message and abort */
                orte_show_help_finalize();
                orte_show_help("help-ess-base.txt", "slurm-error", true);
                return ORTE_ERR_SILENT;
            } else if (0 == strcmp(envar, "ALPS")) {
                /* we were direct launched by ALPS */
                orte_show_help_finalize();
                orte_show_help("help-ess-base.txt", "alps-error", true);
                return ORTE_ERR_SILENT;
            }
        }
        error = "pmix init";
        goto error;
    }
    u32ptr = &u32;
    u16ptr = &u16;

    /****   THE FOLLOWING ARE REQUIRED VALUES   ***/
    /* pmix.init set our process name down in the OPAL layer,
     * so carry it forward here */
    ORTE_PROC_MY_NAME->jobid = OPAL_PROC_MY_NAME.jobid;
    ORTE_PROC_MY_NAME->vpid = OPAL_PROC_MY_NAME.vpid;

    /* setup a name for retrieving data associated with the job */
    wildcard_rank.jobid = ORTE_PROC_MY_NAME->jobid;
    wildcard_rank.vpid = ORTE_NAME_WILDCARD->vpid;

    /* setup a name for retrieving proc-specific data */
    pname.jobid = ORTE_PROC_MY_NAME->jobid;
    pname.vpid = 0;

    OPAL_TIMING_ENV_NEXT(rte_init, "pmix_init");
    
    /* get our local rank from PMI */
    OPAL_MODEX_RECV_VALUE(ret, OPAL_PMIX_LOCAL_RANK,
                          ORTE_PROC_MY_NAME, &u16ptr, OPAL_UINT16);
    if (OPAL_SUCCESS != ret) {
        error = "getting local rank";
        goto error;
    }
    orte_process_info.my_local_rank = u16;

    /* get our node rank from PMI */
    OPAL_MODEX_RECV_VALUE(ret, OPAL_PMIX_NODE_RANK,
                          ORTE_PROC_MY_NAME, &u16ptr, OPAL_UINT16);
    if (OPAL_SUCCESS != ret) {
        error = "getting node rank";
        goto error;
    }
    orte_process_info.my_node_rank = u16;

    /* get max procs for this application */
    OPAL_MODEX_RECV_VALUE(ret, OPAL_PMIX_MAX_PROCS,
                          &wildcard_rank, &u32ptr, OPAL_UINT32);
    if (OPAL_SUCCESS != ret) {
        error = "getting max procs";
        goto error;
    }
    orte_process_info.max_procs = u32;

    /* get job size */
    OPAL_MODEX_RECV_VALUE(ret, OPAL_PMIX_JOB_SIZE,
                          &wildcard_rank, &u32ptr, OPAL_UINT32);
    if (OPAL_SUCCESS != ret) {
        error = "getting job size";
        goto error;
    }
    orte_process_info.num_procs = u32;

    /* push into the environ for pickup in MPI layer for
     * MPI-3 required info key
     */
    if (NULL == getenv(OPAL_MCA_PREFIX"orte_ess_num_procs")) {
        asprintf(&ev1, OPAL_MCA_PREFIX"orte_ess_num_procs=%d", orte_process_info.num_procs);
        putenv(ev1);
        added_num_procs = true;
    }
    if (NULL == getenv("OMPI_APP_CTX_NUM_PROCS")) {
        asprintf(&ev2, "OMPI_APP_CTX_NUM_PROCS=%d", orte_process_info.num_procs);
        putenv(ev2);
        added_app_ctx = true;
    }


    /* get our app number from PMI - ok if not found */
    OPAL_MODEX_RECV_VALUE_OPTIONAL(ret, OPAL_PMIX_APPNUM,
                                   ORTE_PROC_MY_NAME, &u32ptr, OPAL_UINT32);
    if (OPAL_SUCCESS == ret) {
        orte_process_info.app_num = u32;
    } else {
        orte_process_info.app_num = 0;
    }

    /* get the number of local peers - required for wireup of
     * shared memory BTL */
    OPAL_MODEX_RECV_VALUE(ret, OPAL_PMIX_LOCAL_SIZE,
                          &wildcard_rank, &u32ptr, OPAL_UINT32);
    if (OPAL_SUCCESS == ret) {
        orte_process_info.num_local_peers = u32 - 1;  // want number besides ourselves
    } else {
        orte_process_info.num_local_peers = 0;
    }

    /* get number of nodes in the job */
    OPAL_MODEX_RECV_VALUE_OPTIONAL(ret, OPAL_PMIX_NUM_NODES,
                                   &wildcard_rank, &u32ptr, OPAL_UINT32);
    if (OPAL_SUCCESS == ret) {
        orte_process_info.num_nodes = u32;
    }
    OPAL_TIMING_ENV_NEXT(rte_init, "pmix_get_job_params");

    /* setup transport keys in case the MPI layer needs them -
     * we can use the jobfam and stepid as unique keys
     * because they are unique values assigned by the RM
     */
    if (NULL == getenv(OPAL_MCA_PREFIX"orte_precondition_transports")) {
        unique_key[0] = ORTE_JOB_FAMILY(ORTE_PROC_MY_NAME->jobid);
        unique_key[1] = ORTE_LOCAL_JOBID(ORTE_PROC_MY_NAME->jobid);
        if (NULL == (string_key = orte_pre_condition_transports_print(unique_key))) {
            ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
            return ORTE_ERR_OUT_OF_RESOURCE;
        }
        opal_output_verbose(2, orte_ess_base_framework.framework_output,
                            "%s transport key %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), string_key);
        asprintf(&envar, OPAL_MCA_PREFIX"orte_precondition_transports=%s", string_key);
        putenv(envar);
        added_transport_keys = true;
        /* cannot free the envar as that messes up our environ */
        free(string_key);
    }
    OPAL_TIMING_ENV_NEXT(rte_init, "orte_precondition_transport");

    /* retrieve temp directories info */
    OPAL_MODEX_RECV_VALUE_OPTIONAL(ret, OPAL_PMIX_TMPDIR, &wildcard_rank, &val, OPAL_STRING);
    if (OPAL_SUCCESS == ret && NULL != val) {
        /* We want to provide user with ability
         * to override RM settings at his own risk
         */
        if( NULL == orte_process_info.top_session_dir ){
            orte_process_info.top_session_dir = val;
        } else {
            /* keep the MCA setting */
            tdir_mca_override = true;
            free(val);
        }
        val = NULL;
    }

    if( !tdir_mca_override ){
        OPAL_MODEX_RECV_VALUE_OPTIONAL(ret, OPAL_PMIX_NSDIR, &wildcard_rank, &val, OPAL_STRING);
        if (OPAL_SUCCESS == ret && NULL != val) {
            /* We want to provide user with ability
             * to override RM settings at his own risk
             */
            if( NULL == orte_process_info.job_session_dir ){
                orte_process_info.job_session_dir = val;
            } else {
                /* keep the MCA setting */
                free(val);
                tdir_mca_override = true;
            }
            val = NULL;
        }
    }

    if( !tdir_mca_override ){
        OPAL_MODEX_RECV_VALUE_OPTIONAL(ret, OPAL_PMIX_PROCDIR, &wildcard_rank, &val, OPAL_STRING);
        if (OPAL_SUCCESS == ret && NULL != val) {
            /* We want to provide user with ability
             * to override RM settings at his own risk
             */
            if( NULL == orte_process_info.proc_session_dir ){
                orte_process_info.proc_session_dir = val;
            } else {
                /* keep the MCA setting */
                tdir_mca_override = true;
                free(val);
            }
            val = NULL;
        }
    }

    if( !tdir_mca_override ){
        OPAL_MODEX_RECV_VALUE_OPTIONAL(ret, OPAL_PMIX_TDIR_RMCLEAN, &wildcard_rank, &bool_ptr, OPAL_BOOL);
        if (OPAL_SUCCESS == ret ) {
            orte_process_info.rm_session_dirs = bool_val;
        }
    }
    OPAL_TIMING_ENV_NEXT(rte_init, "pmix_set_tdirs");

    /* get our local peers */
    if (0 < orte_process_info.num_local_peers) {
        /* if my local rank if too high, then that's an error */
        if (orte_process_info.num_local_peers < orte_process_info.my_local_rank) {
            ret = ORTE_ERR_BAD_PARAM;
            error = "num local peers";
            goto error;
        }
        /* retrieve the local peers */
        OPAL_MODEX_RECV_VALUE(ret, OPAL_PMIX_LOCAL_PEERS,
                              &wildcard_rank, &val, OPAL_STRING);
        if (OPAL_SUCCESS == ret && NULL != val) {
            peers = opal_argv_split(val, ',');
            free(val);
        } else {
            peers = NULL;
        }
    } else {
        peers = NULL;
    }

    /* set the locality */
    if (NULL != peers) {
        /* identify our location */
        val = NULL;
        OPAL_MODEX_RECV_VALUE_OPTIONAL(ret, OPAL_PMIX_LOCALITY_STRING,
                                       ORTE_PROC_MY_NAME, &val, OPAL_STRING);
        if (OPAL_SUCCESS == ret && NULL != val) {
            mycpuset = val;
        } else {
            mycpuset = NULL;
        }
        pname.jobid = ORTE_PROC_MY_NAME->jobid;
        for (i=0; NULL != peers[i]; i++) {
            pname.vpid = strtoul(peers[i], NULL, 10);
            if (pname.vpid == ORTE_PROC_MY_NAME->vpid) {
                /* we are fully local to ourselves */
                u16 = OPAL_PROC_ALL_LOCAL;
            } else {
                val = NULL;
                OPAL_MODEX_RECV_VALUE_OPTIONAL(ret, OPAL_PMIX_LOCALITY_STRING,
                                               &pname, &val, OPAL_STRING);
                if (OPAL_SUCCESS == ret && NULL != val) {
                    u16 = opal_hwloc_compute_relative_locality(mycpuset, val);
                    free(val);
                } else {
                    /* all we can say is that it shares our node */
                    u16 = OPAL_PROC_ON_CLUSTER | OPAL_PROC_ON_CU | OPAL_PROC_ON_NODE;
                }
            }
            kv = OBJ_NEW(opal_value_t);
            kv->key = strdup(OPAL_PMIX_LOCALITY);
            kv->type = OPAL_UINT16;
            OPAL_OUTPUT_VERBOSE((1, orte_ess_base_framework.framework_output,
                                 "%s ess:pmi:locality: proc %s locality %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_NAME_PRINT(&pname), opal_hwloc_base_print_locality(u16)));
            kv->data.uint16 = u16;
            ret = opal_pmix.store_local(&pname, kv);
            if (OPAL_SUCCESS != ret) {
                error = "local store of locality";
                opal_argv_free(peers);
                if (NULL != mycpuset) {
                    free(mycpuset);
                }
                goto error;
            }
            OBJ_RELEASE(kv);
        }
        opal_argv_free(peers);
        if (NULL != mycpuset) {
            free(mycpuset);
        }
    }
    OPAL_TIMING_ENV_NEXT(rte_init, "pmix_set_locality");

    /* now that we have all required info, complete the setup */
    if (ORTE_SUCCESS != (ret = orte_ess_base_app_setup(false))) {
        ORTE_ERROR_LOG(ret);
        error = "orte_ess_base_app_setup";
        goto error;
    }
    OPAL_TIMING_ENV_NEXT(rte_init, "ess_base_app_setup");

    /* setup process binding */
    if (ORTE_SUCCESS != (ret = orte_ess_base_proc_binding())) {
        error = "proc_binding";
        goto error;
    }
    OPAL_TIMING_ENV_NEXT(rte_init, "ess_base_proc_binding");

    /* this needs to be set to enable debugger use when direct launched */
    if (NULL == orte_process_info.my_daemon_uri) {
        orte_standalone_operation = true;
    }

    /* set max procs */
    if (orte_process_info.max_procs < orte_process_info.num_procs) {
        orte_process_info.max_procs = orte_process_info.num_procs;
    }

    /* push our hostname so others can find us, if they need to - the
     * native PMIx component will ignore this request as the hostname
     * is provided by the system */
    OPAL_MODEX_SEND_VALUE(ret, OPAL_PMIX_GLOBAL, OPAL_PMIX_HOSTNAME, orte_process_info.nodename, OPAL_STRING);
    if (ORTE_SUCCESS != ret) {
        error = "db store hostname";
        goto error;
    }

    /* if we are an ORTE app - and not an MPI app - then
     * we need to exchange our connection info here.
     * MPI_Init has its own modex, so we don't need to do
     * two of them. However, if we don't do a modex at all,
     * then processes have no way to communicate
     *
     * NOTE: only do this when the process originally launches.
     * Cannot do this on a restart as the rest of the processes
     * in the job won't be executing this step, so we would hang
     */
    if (ORTE_PROC_IS_NON_MPI && !orte_do_not_barrier) {
        /* need to commit the data before we fence */
        opal_pmix.commit();
        if (ORTE_SUCCESS != (ret = opal_pmix.fence(NULL, 0))) {
            error = "opal_pmix.fence() failed";
            goto error;
        }
    }
    OPAL_TIMING_ENV_NEXT(rte_init, "rte_init_done");
    
    return ORTE_SUCCESS;

 error:
    if (!progress_thread_running) {
        /* can't send the help message, so ensure it
         * comes out locally
         */
        orte_show_help_finalize();
    }
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

    /* remove the envars that we pushed into environ
     * so we leave that structure intact
     */
    if (added_transport_keys) {
        unsetenv(OPAL_MCA_PREFIX"orte_precondition_transports");
    }
    if (added_num_procs) {
        unsetenv(OPAL_MCA_PREFIX"orte_ess_num_procs");
    }
    if (added_app_ctx) {
        unsetenv("OMPI_APP_CTX_NUM_PROCS");
    }

    /* use the default app procedure to finish */
    if (ORTE_SUCCESS != (ret = orte_ess_base_app_finalize())) {
        ORTE_ERROR_LOG(ret);
        return ret;
    }

    /* release the event base */
    if (progress_thread_running) {
        opal_progress_thread_finalize(NULL);
        progress_thread_running = false;
    }
    return ORTE_SUCCESS;
}

static void rte_abort(int status, bool report)
{
    struct timespec tp = {0, 100000};

    OPAL_OUTPUT_VERBOSE((1, orte_ess_base_framework.framework_output,
                         "%s ess:pmi:abort: abort with status %d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         status));

    /* PMI doesn't like NULL messages, but our interface
     * doesn't provide one - so rig one up here
     */
    opal_pmix.abort(status, "N/A", NULL);

    /* provide a little delay for the PMIx thread to
     * get the info out */
    nanosleep(&tp, NULL);

    /* Now Exit */
    _exit(status);
}
