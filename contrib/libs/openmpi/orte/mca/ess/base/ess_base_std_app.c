/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2013-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2014-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2015      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2018      Mellanox Technologies, Inc.
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
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif

#include "opal/mca/event/event.h"
#include "opal/mca/pmix/base/base.h"
#include "opal/util/arch.h"
#include "opal/util/os_path.h"
#include "opal/util/output.h"
#include "opal/util/proc.h"
#include "opal/runtime/opal.h"

#include "orte/mca/rml/base/base.h"
#include "orte/mca/routed/base/base.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/dfs/base/base.h"
#include "orte/mca/grpcomm/base/base.h"
#include "orte/mca/oob/base/base.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/rml/base/rml_contact.h"
#include "orte/mca/odls/odls_types.h"
#include "orte/mca/filem/base/base.h"
#include "orte/mca/errmgr/base/base.h"
#include "orte/mca/state/base/base.h"
#include "orte/util/proc_info.h"
#include "orte/util/session_dir.h"
#include "orte/util/name_fns.h"
#include "orte/util/show_help.h"
#include "opal/util/timings.h"

#include "orte/runtime/orte_globals.h"
#include "orte/runtime/orte_wait.h"

#include "orte/mca/ess/base/base.h"

int orte_ess_base_app_setup(bool db_restrict_local)
{
    int ret;
    char *error = NULL;
    opal_list_t transports;

    OPAL_TIMING_ENV_INIT(ess_base_setup);
    /*
     * stdout/stderr buffering
     * If the user requested to override the default setting then do
     * as they wish.
     */
    if( orte_ess_base_std_buffering > -1 ) {
        if( 0 == orte_ess_base_std_buffering ) {
            setvbuf(stdout, NULL, _IONBF, 0);
            setvbuf(stderr, NULL, _IONBF, 0);
        }
        else if( 1 == orte_ess_base_std_buffering ) {
            setvbuf(stdout, NULL, _IOLBF, 0);
            setvbuf(stderr, NULL, _IOLBF, 0);
        }
        else if( 2 == orte_ess_base_std_buffering ) {
            setvbuf(stdout, NULL, _IOFBF, 0);
            setvbuf(stderr, NULL, _IOFBF, 0);
        }
    }

    /* if I am an MPI app, we will let the MPI layer define and
     * control the opal_proc_t structure. Otherwise, we need to
     * do so here */
    if (ORTE_PROC_NON_MPI) {
        orte_process_info.super.proc_name = *(opal_process_name_t*)ORTE_PROC_MY_NAME;
        orte_process_info.super.proc_hostname = orte_process_info.nodename;
        orte_process_info.super.proc_flags = OPAL_PROC_ALL_LOCAL;
        orte_process_info.super.proc_arch = opal_local_arch;
        opal_proc_local_set(&orte_process_info.super);
    }

    /* open and setup the state machine */
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&orte_state_base_framework, 0))) {
        ORTE_ERROR_LOG(ret);
        error = "orte_state_base_open";
        goto error;
    }
    if (ORTE_SUCCESS != (ret = orte_state_base_select())) {
        ORTE_ERROR_LOG(ret);
        error = "orte_state_base_select";
        goto error;
    }
    OPAL_TIMING_ENV_NEXT(ess_base_setup, "state_framework_open");

    /* open the errmgr */
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&orte_errmgr_base_framework, 0))) {
        ORTE_ERROR_LOG(ret);
        error = "orte_errmgr_base_open";
        goto error;
    }
    OPAL_TIMING_ENV_NEXT(ess_base_setup, "errmgr_framework_open");

    /* setup my session directory */
    if (orte_create_session_dirs) {
        OPAL_OUTPUT_VERBOSE((2, orte_ess_base_framework.framework_output,
                             "%s setting up session dir with\n\ttmpdir: %s\n\thost %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             (NULL == orte_process_info.tmpdir_base) ? "UNDEF" : orte_process_info.tmpdir_base,
                             orte_process_info.nodename));
        if (ORTE_SUCCESS != (ret = orte_session_dir(true, ORTE_PROC_MY_NAME))) {
            ORTE_ERROR_LOG(ret);
            error = "orte_session_dir";
            goto error;
        }
        /* Once the session directory location has been established, set
           the opal_output env file location to be in the
           proc-specific session directory. */
        opal_output_set_output_file_info(orte_process_info.proc_session_dir,
                                         "output-", NULL, NULL);
        /* register the directory for cleanup */
        if (NULL != opal_pmix.register_cleanup) {
            if (orte_standalone_operation) {
                if (OPAL_SUCCESS != (ret = opal_pmix.register_cleanup(orte_process_info.top_session_dir, true, false, true))) {
                    ORTE_ERROR_LOG(ret);
                    error = "register cleanup";
                    goto error;
                }
            } else {
                if (OPAL_SUCCESS != (ret = opal_pmix.register_cleanup(orte_process_info.job_session_dir, true, false, false))) {
                    ORTE_ERROR_LOG(ret);
                    error = "register cleanup";
                    goto error;
                }
            }
        }
    }
    OPAL_TIMING_ENV_NEXT(ess_base_setup, "create_session_dirs");

    /* Setup the communication infrastructure */
    /* Routed system */
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&orte_routed_base_framework, 0))) {
        ORTE_ERROR_LOG(ret);
        error = "orte_routed_base_open";
        goto error;
    }
    if (ORTE_SUCCESS != (ret = orte_routed_base_select())) {
        ORTE_ERROR_LOG(ret);
        error = "orte_routed_base_select";
        goto error;
    }
    OPAL_TIMING_ENV_NEXT(ess_base_setup, "routed_framework_open");

    /*
     * OOB Layer
     */
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&orte_oob_base_framework, 0))) {
        ORTE_ERROR_LOG(ret);
        error = "orte_oob_base_open";
        goto error;
    }
    if (ORTE_SUCCESS != (ret = orte_oob_base_select())) {
        ORTE_ERROR_LOG(ret);
        error = "orte_oob_base_select";
        goto error;
    }
    OPAL_TIMING_ENV_NEXT(ess_base_setup, "oob_framework_open");
    
    /* Runtime Messaging Layer */
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&orte_rml_base_framework, 0))) {
        ORTE_ERROR_LOG(ret);
        error = "orte_rml_base_open";
        goto error;
    }
    if (ORTE_SUCCESS != (ret = orte_rml_base_select())) {
        ORTE_ERROR_LOG(ret);
        error = "orte_rml_base_select";
        goto error;
    }
    OPAL_TIMING_ENV_NEXT(ess_base_setup, "rml_framework_open");
    
    /* if we have info on the HNP and local daemon, process it */
    if (NULL != orte_process_info.my_hnp_uri) {
        /* we have to set the HNP's name, even though we won't route messages directly
         * to it. This is required to ensure that we -do- send messages to the correct
         * HNP name
         */
        if (ORTE_SUCCESS != (ret = orte_rml_base_parse_uris(orte_process_info.my_hnp_uri,
                                                            ORTE_PROC_MY_HNP, NULL))) {
            ORTE_ERROR_LOG(ret);
            error = "orte_rml_parse_HNP";
            goto error;
        }
    }
    if (NULL != orte_process_info.my_daemon_uri) {
        opal_value_t val;

        /* extract the daemon's name so we can update the routing table */
        if (ORTE_SUCCESS != (ret = orte_rml_base_parse_uris(orte_process_info.my_daemon_uri,
                                                            ORTE_PROC_MY_DAEMON, NULL))) {
            ORTE_ERROR_LOG(ret);
            error = "orte_rml_parse_daemon";
            goto error;
        }
        /* Set the contact info in the database - this won't actually establish
         * the connection, but just tells us how to reach the daemon
         * if/when we attempt to send to it
         */
        OBJ_CONSTRUCT(&val, opal_value_t);
        val.key = OPAL_PMIX_PROC_URI;
        val.type = OPAL_STRING;
        val.data.string = orte_process_info.my_daemon_uri;
        if (OPAL_SUCCESS != (ret = opal_pmix.store_local(ORTE_PROC_MY_DAEMON, &val))) {
            ORTE_ERROR_LOG(ret);
            val.key = NULL;
            val.data.string = NULL;
            OBJ_DESTRUCT(&val);
            error = "store DAEMON URI";
            goto error;
        }
        val.key = NULL;
        val.data.string = NULL;
        OBJ_DESTRUCT(&val);
    }

    /* setup the errmgr */
    if (ORTE_SUCCESS != (ret = orte_errmgr_base_select())) {
        ORTE_ERROR_LOG(ret);
        error = "orte_errmgr_base_select";
        goto error;
    }
    OPAL_TIMING_ENV_NEXT(ess_base_setup, "errmgr_select");

    /* get a conduit for our use - we never route IO over fabric */
    OBJ_CONSTRUCT(&transports, opal_list_t);
    orte_set_attribute(&transports, ORTE_RML_TRANSPORT_TYPE,
                       ORTE_ATTR_LOCAL, orte_mgmt_transport, OPAL_STRING);
    if (ORTE_RML_CONDUIT_INVALID == (orte_mgmt_conduit = orte_rml.open_conduit(&transports))) {
        ret = ORTE_ERR_OPEN_CONDUIT_FAIL;
        error = "orte_rml_open_mgmt_conduit";
        goto error;
    }
    OPAL_LIST_DESTRUCT(&transports);

    OBJ_CONSTRUCT(&transports, opal_list_t);
    orte_set_attribute(&transports, ORTE_RML_TRANSPORT_TYPE,
                       ORTE_ATTR_LOCAL, orte_coll_transport, OPAL_STRING);
    if (ORTE_RML_CONDUIT_INVALID == (orte_coll_conduit = orte_rml.open_conduit(&transports))) {
        ret = ORTE_ERR_OPEN_CONDUIT_FAIL;
        error = "orte_rml_open_coll_conduit";
        goto error;
    }
    OPAL_LIST_DESTRUCT(&transports);
    OPAL_TIMING_ENV_NEXT(ess_base_setup, "rml_open_conduit");

    /*
     * Group communications
     */
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&orte_grpcomm_base_framework, 0))) {
        ORTE_ERROR_LOG(ret);
        error = "orte_grpcomm_base_open";
        goto error;
    }
    if (ORTE_SUCCESS != (ret = orte_grpcomm_base_select())) {
        ORTE_ERROR_LOG(ret);
        error = "orte_grpcomm_base_select";
        goto error;
    }
    OPAL_TIMING_ENV_NEXT(ess_base_setup, "grpcomm_framework_open");

    /* open the distributed file system */
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&orte_dfs_base_framework, 0))) {
        ORTE_ERROR_LOG(ret);
        error = "orte_dfs_base_open";
        goto error;
    }
    if (ORTE_SUCCESS != (ret = orte_dfs_base_select())) {
        ORTE_ERROR_LOG(ret);
        error = "orte_dfs_base_select";
        goto error;
    }
    OPAL_TIMING_ENV_NEXT(ess_base_setup, "dfs_framework_open");

    return ORTE_SUCCESS;
 error:
    orte_show_help("help-orte-runtime.txt",
                   "orte_init:startup:internal-failure",
                   true, error, ORTE_ERROR_NAME(ret), ret);
    return ret;
}

int orte_ess_base_app_finalize(void)
{
    /* release the conduits */
    orte_rml.close_conduit(orte_mgmt_conduit);
    orte_rml.close_conduit(orte_coll_conduit);

    /* close frameworks */
    (void) mca_base_framework_close(&orte_filem_base_framework);
    (void) mca_base_framework_close(&orte_errmgr_base_framework);

    /* now can close the rml and its friendly group comm */
    (void) mca_base_framework_close(&orte_grpcomm_base_framework);
    (void) mca_base_framework_close(&orte_dfs_base_framework);
    (void) mca_base_framework_close(&orte_routed_base_framework);

    (void) mca_base_framework_close(&orte_rml_base_framework);
    if (NULL != opal_pmix.finalize) {
        opal_pmix.finalize();
        (void) mca_base_framework_close(&opal_pmix_base_framework);
    }
    (void) mca_base_framework_close(&orte_oob_base_framework);
    (void) mca_base_framework_close(&orte_state_base_framework);

    if (NULL == opal_pmix.register_cleanup) {
        orte_session_dir_finalize(ORTE_PROC_MY_NAME);
    }
    /* cleanup the process info */
    orte_proc_info_finalize();

    return ORTE_SUCCESS;
}

/*
 * We do NOT call the regular C-library "abort" function, even
 * though that would have alerted us to the fact that this is
 * an abnormal termination, because it would automatically cause
 * a core file to be generated. On large systems, that can be
 * overwhelming (imagine a few thousand Gbyte-sized files hitting
                 * a shared file system simultaneously...ouch!).
 *
 * However, this causes a problem for OpenRTE as the system truly
 * needs to know that this actually IS an abnormal termination.
 * To get around the problem, we drop a marker in the proc-level
 * session dir. If session dir's were not allowed, then we just
 * ignore this question.
 *
 * In some cases, however, we DON'T want to create that alert. For
 * example, if an orted detects that the HNP has died, then there
 * is truly nobody to alert! In these cases, we pass report=false
 * to indicate that we don't want the marker dropped.
 */
void orte_ess_base_app_abort(int status, bool report)
{
    int fd;
    char *myfile;
    struct timespec tp = {0, 100000};

    /* Exit - do NOT do a normal finalize as this will very likely
     * hang the process. We are aborting due to an abnormal condition
     * that precludes normal cleanup
     *
     * We do need to do the following bits to make sure we leave a
     * clean environment. Taken from orte_finalize():
     * - Assume errmgr cleans up child processes before we exit.
     */

    /* If we were asked to report this termination, do so.
     * Since singletons don't start an HNP unless necessary, and
     * direct-launched procs don't have daemons at all, only send
     * the message if routing is enabled as this indicates we
     * have someone to send to
     */
    if (report && orte_routing_is_enabled && orte_create_session_dirs) {
        myfile = opal_os_path(false, orte_process_info.proc_session_dir, "aborted", NULL);
        fd = open(myfile, O_CREAT, S_IRUSR);
        close(fd);
        /* now introduce a short delay to allow any pending
         * messages (e.g., from a call to "show_help") to
         * have a chance to be sent */
        nanosleep(&tp, NULL);
    }
    /* - Clean out the global structures
     * (not really necessary, but good practice) */
    orte_proc_info_finalize();
    /* Now Exit */
    _exit(status);
}
