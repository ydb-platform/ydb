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
 * Copyright (c) 2009      Institut National de Recherche en Informatique
 *                         et Automatique. All rights reserved.
 * Copyright (c) 2011      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2013-2018 Intel, Inc.  All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
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

#include "opal/dss/dss.h"
#include "opal/mca/event/event.h"
#include "opal/runtime/opal.h"
#include "opal/mca/hwloc/base/base.h"
#include "opal/mca/pmix/base/base.h"
#include "opal/mca/pstat/base/base.h"
#include "opal/util/arch.h"
#include "opal/util/opal_environ.h"
#include "opal/util/os_path.h"
#include "opal/util/proc.h"

#include "orte/mca/rtc/base/base.h"
#include "orte/mca/rml/base/base.h"
#include "orte/mca/rml/base/rml_contact.h"
#include "orte/mca/routed/base/base.h"
#include "orte/mca/routed/routed.h"
#include "orte/mca/oob/base/base.h"
#include "orte/mca/dfs/base/base.h"
#include "orte/mca/grpcomm/grpcomm.h"
#include "orte/mca/grpcomm/base/base.h"
#include "orte/mca/iof/base/base.h"
#include "orte/mca/plm/base/base.h"
#include "orte/mca/odls/base/base.h"
#include "orte/mca/regx/base/base.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/rmaps/base/base.h"
#include "orte/mca/filem/base/base.h"
#include "orte/util/proc_info.h"
#include "orte/util/session_dir.h"
#include "orte/util/name_fns.h"
#include "orte/util/show_help.h"
#include "orte/mca/errmgr/base/base.h"
#include "orte/mca/state/base/base.h"
#include "orte/mca/state/state.h"
#include "orte/runtime/orte_wait.h"
#include "orte/runtime/orte_globals.h"
#include "orte/runtime/orte_quit.h"
#include "orte/orted/pmix/pmix_server.h"

#include "orte/mca/ess/base/base.h"

/* local globals */
static bool plm_in_use=false;
static bool signals_set=false;
static opal_event_t term_handler;
static opal_event_t int_handler;
static opal_event_t epipe_handler;
static char *log_path = NULL;
static void shutdown_signal(int fd, short flags, void *arg);
static void epipe_signal_callback(int fd, short flags, void *arg);
static void signal_forward_callback(int fd, short event, void *arg);
static opal_event_t *forward_signals_events = NULL;

static void setup_sighandler(int signal, opal_event_t *ev,
                             opal_event_cbfunc_t cbfunc)
{
    opal_event_signal_set(orte_event_base, ev, signal, cbfunc, ev);
    opal_event_set_priority(ev, ORTE_ERROR_PRI);
    opal_event_signal_add(ev, NULL);
}


int orte_ess_base_orted_setup(void)
{
    int ret = ORTE_ERROR;
    int fd;
    char log_file[PATH_MAX];
    char *jobidstring;
    char *error = NULL;
    orte_job_t *jdata;
    orte_proc_t *proc;
    orte_app_context_t *app;
    char *param;
    hwloc_obj_t obj;
    unsigned i, j;
    orte_topology_t *t;
    opal_list_t transports;
    orte_ess_base_signal_t *sig;
    int idx;

    /* my name is set, xfer it to the OPAL layer */
    orte_process_info.super.proc_name = *(opal_process_name_t*)ORTE_PROC_MY_NAME;
    orte_process_info.super.proc_hostname = strdup(orte_process_info.nodename);
    orte_process_info.super.proc_flags = OPAL_PROC_ALL_LOCAL;
    orte_process_info.super.proc_arch = opal_local_arch;
    opal_proc_local_set(&orte_process_info.super);

    plm_in_use = false;

    /* setup callback for SIGPIPE */
    setup_sighandler(SIGPIPE, &epipe_handler, epipe_signal_callback);
    /* Set signal handlers to catch kill signals so we can properly clean up
     * after ourselves.
     */
    setup_sighandler(SIGTERM, &term_handler, shutdown_signal);
    setup_sighandler(SIGINT, &int_handler, shutdown_signal);
    /** setup callbacks for signals we should forward */
    if (0 < (idx = opal_list_get_size(&orte_ess_base_signals))) {
        forward_signals_events = (opal_event_t*)malloc(sizeof(opal_event_t) * idx);
        if (NULL == forward_signals_events) {
            ret = ORTE_ERR_OUT_OF_RESOURCE;
            error = "unable to malloc";
            goto error;
        }
        idx = 0;
        OPAL_LIST_FOREACH(sig, &orte_ess_base_signals, orte_ess_base_signal_t) {
            setup_sighandler(sig->signal, forward_signals_events + idx, signal_forward_callback);
            ++idx;
        }
    }
    signals_set = true;


    /* get the local topology */
    if (NULL == opal_hwloc_topology) {
        if (OPAL_SUCCESS != (ret = opal_hwloc_base_get_topology())) {
            error = "topology discovery";
            goto error;
        }
    }
    /* generate the signature */
    orte_topo_signature = opal_hwloc_base_get_topo_signature(opal_hwloc_topology);
    /* remove the hostname from the topology. Unfortunately, hwloc
     * decided to add the source hostname to the "topology", thus
     * rendering it unusable as a pure topological description. So
     * we remove that information here.
     */
    obj = hwloc_get_root_obj(opal_hwloc_topology);
    for (i=0; i < obj->infos_count; i++) {
        if (NULL == obj->infos[i].name ||
            NULL == obj->infos[i].value) {
            continue;
        }
        if (0 == strncmp(obj->infos[i].name, "HostName", strlen("HostName"))) {
            free(obj->infos[i].name);
            free(obj->infos[i].value);
            /* left justify the array */
            for (j=i; j < obj->infos_count-1; j++) {
                obj->infos[j] = obj->infos[j+1];
            }
            obj->infos[obj->infos_count-1].name = NULL;
            obj->infos[obj->infos_count-1].value = NULL;
            obj->infos_count--;
            break;
        }
    }
    if (15 < opal_output_get_verbosity(orte_ess_base_framework.framework_output)) {
        opal_output(0, "%s Topology Info:", ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        opal_dss.dump(0, opal_hwloc_topology, OPAL_HWLOC_TOPO);
    }

    /* open and setup the opal_pstat framework so we can provide
     * process stats if requested
     */
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&opal_pstat_base_framework, 0))) {
        ORTE_ERROR_LOG(ret);
        error = "opal_pstat_base_open";
        goto error;
    }
    if (ORTE_SUCCESS != (ret = opal_pstat_base_select())) {
        ORTE_ERROR_LOG(ret);
        error = "opal_pstat_base_select";
        goto error;
    }

    /* define the HNP name */
    ORTE_PROC_MY_HNP->jobid = ORTE_PROC_MY_NAME->jobid;
    ORTE_PROC_MY_HNP->vpid = 0;

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
    /* open the errmgr */
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&orte_errmgr_base_framework, 0))) {
        ORTE_ERROR_LOG(ret);
        error = "orte_errmgr_base_open";
        goto error;
    }
    /* some environments allow remote launches - e.g., ssh - so
     * open and select something -only- if we are given
     * a specific module to use
     */
    (void) mca_base_var_env_name("plm", &param);
    plm_in_use = !!(getenv(param));
    free (param);
    if (plm_in_use)  {
        if (ORTE_SUCCESS != (ret = mca_base_framework_open(&orte_plm_base_framework, 0))) {
            ORTE_ERROR_LOG(ret);
            error = "orte_plm_base_open";
            goto error;
        }
        if (ORTE_SUCCESS != (ret = orte_plm_base_select())) {
            ORTE_ERROR_LOG(ret);
            error = "orte_plm_base_select";
            goto error;
        }
    }
    /* setup my session directory here as the OOB may need it */
    if (orte_create_session_dirs) {
        OPAL_OUTPUT_VERBOSE((2, orte_ess_base_framework.framework_output,
                             "%s setting up session dir with\n\ttmpdir: %s\n\thost %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             (NULL == orte_process_info.tmpdir_base) ? "UNDEF" : orte_process_info.tmpdir_base,
                             orte_process_info.nodename));

        /* take a pass thru the session directory code to fillin the
         * tmpdir names - don't create anything yet
         */
        if (ORTE_SUCCESS != (ret = orte_session_dir(false, ORTE_PROC_MY_NAME))) {
            ORTE_ERROR_LOG(ret);
            error = "orte_session_dir define";
            goto error;
        }
        /* clear the session directory just in case there are
         * stale directories laying around
         */
        orte_session_dir_cleanup(ORTE_JOBID_WILDCARD);
        /* now actually create the directory tree */
        if (ORTE_SUCCESS != (ret = orte_session_dir(true, ORTE_PROC_MY_NAME))) {
            ORTE_ERROR_LOG(ret);
            error = "orte_session_dir";
            goto error;
        }
        /* set the opal_output env file location to be in the
         * proc-specific session directory. */
        opal_output_set_output_file_info(orte_process_info.proc_session_dir,
                                         "output-", NULL, NULL);
        /* setup stdout/stderr */
        if (orte_debug_daemons_file_flag) {
            /* if we are debugging to a file, then send stdout/stderr to
             * the orted log file
             */
            /* get my jobid */
            if (ORTE_SUCCESS != (ret = orte_util_convert_jobid_to_string(&jobidstring,
                                                                         ORTE_PROC_MY_NAME->jobid))) {
                ORTE_ERROR_LOG(ret);
                error = "convert_jobid";
                goto error;
            }
            /* define a log file name in the session directory */
            snprintf(log_file, PATH_MAX, "output-orted-%s-%s.log",
                     jobidstring, orte_process_info.nodename);
            log_path = opal_os_path(false, orte_process_info.top_session_dir,
                                    log_file, NULL);

            fd = open(log_path, O_RDWR|O_CREAT|O_TRUNC, 0640);
            if (fd < 0) {
                /* couldn't open the file for some reason, so
                 * just connect everything to /dev/null
                 */
                fd = open("/dev/null", O_RDWR|O_CREAT|O_TRUNC, 0666);
            } else {
                dup2(fd, STDOUT_FILENO);
                dup2(fd, STDERR_FILENO);
                if(fd != STDOUT_FILENO && fd != STDERR_FILENO) {
                    close(fd);
                }
            }
        }
    }
    /* setup the global job and node arrays */
    orte_job_data = OBJ_NEW(opal_hash_table_t);
    if (ORTE_SUCCESS != (ret = opal_hash_table_init(orte_job_data, 128))) {
        ORTE_ERROR_LOG(ret);
        error = "setup job array";
        goto error;
    }
    orte_node_pool = OBJ_NEW(opal_pointer_array_t);
    if (ORTE_SUCCESS != (ret = opal_pointer_array_init(orte_node_pool,
                               ORTE_GLOBAL_ARRAY_BLOCK_SIZE,
                               ORTE_GLOBAL_ARRAY_MAX_SIZE,
                               ORTE_GLOBAL_ARRAY_BLOCK_SIZE))) {
        ORTE_ERROR_LOG(ret);
        error = "setup node array";
        goto error;
    }
    orte_node_topologies = OBJ_NEW(opal_pointer_array_t);
    if (ORTE_SUCCESS != (ret = opal_pointer_array_init(orte_node_topologies,
                               ORTE_GLOBAL_ARRAY_BLOCK_SIZE,
                               ORTE_GLOBAL_ARRAY_MAX_SIZE,
                               ORTE_GLOBAL_ARRAY_BLOCK_SIZE))) {
        ORTE_ERROR_LOG(ret);
        error = "setup node topologies array";
        goto error;
    }
    /* Setup the job data object for the daemons */
    /* create and store the job data object */
    jdata = OBJ_NEW(orte_job_t);
    jdata->jobid = ORTE_PROC_MY_NAME->jobid;
    opal_hash_table_set_value_uint32(orte_job_data, jdata->jobid, jdata);
    /* every job requires at least one app */
    app = OBJ_NEW(orte_app_context_t);
    opal_pointer_array_set_item(jdata->apps, 0, app);
    jdata->num_apps++;

    /* create and store a proc object for us */
    proc = OBJ_NEW(orte_proc_t);
    proc->name.jobid = ORTE_PROC_MY_NAME->jobid;
    proc->name.vpid = ORTE_PROC_MY_NAME->vpid;
    proc->pid = orte_process_info.pid;
    proc->state = ORTE_PROC_STATE_RUNNING;
    opal_pointer_array_set_item(jdata->procs, proc->name.vpid, proc);
    /* record that the daemon job is running */
    jdata->num_procs = 1;
    jdata->state = ORTE_JOB_STATE_RUNNING;
    /* obviously, we have "reported" */
    jdata->num_reported = 1;

    /* setup the PMIx framework - ensure it skips all non-PMIx components,
     * but do not override anything we were given */
    opal_setenv("OMPI_MCA_pmix", "^s1,s2,cray,isolated", false, &environ);
    if (OPAL_SUCCESS != (ret = mca_base_framework_open(&opal_pmix_base_framework, 0))) {
        ORTE_ERROR_LOG(ret);
        error = "orte_pmix_base_open";
        goto error;
    }
    if (ORTE_SUCCESS != (ret = opal_pmix_base_select())) {
        ORTE_ERROR_LOG(ret);
        error = "opal_pmix_base_select";
        goto error;
    }
    /* set the event base */
    opal_pmix_base_set_evbase(orte_event_base);
    /* setup the PMIx server - we need this here in case the
     * communications infrastructure wants to register
     * information */
    if (ORTE_SUCCESS != (ret = pmix_server_init())) {
        /* the server code already barked, so let's be quiet */
        ret = ORTE_ERR_SILENT;
        error = "pmix_server_init";
        goto error;
    }

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

    /* it is now safe to start the pmix server */
    pmix_server_start();

    if (NULL != orte_process_info.my_hnp_uri) {
        opal_value_t val;

        /* extract the HNP's name so we can update the routing table */
        if (ORTE_SUCCESS != (ret = orte_rml_base_parse_uris(orte_process_info.my_hnp_uri,
                                                            ORTE_PROC_MY_HNP, NULL))) {
            ORTE_ERROR_LOG(ret);
            error = "orte_rml_parse_HNP";
            goto error;
        }
        /* Set the contact info in the RML - this won't actually establish
         * the connection, but just tells the RML how to reach the HNP
         * if/when we attempt to send to it
         */
        OBJ_CONSTRUCT(&val, opal_value_t);
        val.key = OPAL_PMIX_PROC_URI;
        val.type = OPAL_STRING;
        val.data.string = orte_process_info.my_hnp_uri;
        if (OPAL_SUCCESS != (ret = opal_pmix.store_local(ORTE_PROC_MY_HNP, &val))) {
            ORTE_ERROR_LOG(ret);
            val.key = NULL;
            val.data.string = NULL;
            OBJ_DESTRUCT(&val);
            error = "store HNP URI";
            goto error;
        }
        val.key = NULL;
        val.data.string = NULL;
        OBJ_DESTRUCT(&val);
    }

    /* select the errmgr */
    if (ORTE_SUCCESS != (ret = orte_errmgr_base_select())) {
        ORTE_ERROR_LOG(ret);
        error = "orte_errmgr_base_select";
        goto error;
    }

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
    /* Open/select the odls */
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&orte_odls_base_framework, 0))) {
        ORTE_ERROR_LOG(ret);
        error = "orte_odls_base_open";
        goto error;
    }
    if (ORTE_SUCCESS != (ret = orte_odls_base_select())) {
        ORTE_ERROR_LOG(ret);
        error = "orte_odls_base_select";
        goto error;
    }
    /* Open/select the rtc */
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&orte_rtc_base_framework, 0))) {
        ORTE_ERROR_LOG(ret);
        error = "orte_rtc_base_open";
        goto error;
    }
    if (ORTE_SUCCESS != (ret = orte_rtc_base_select())) {
        ORTE_ERROR_LOG(ret);
        error = "orte_rtc_base_select";
        goto error;
    }
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&orte_rmaps_base_framework, 0))) {
        ORTE_ERROR_LOG(ret);
        error = "orte_rmaps_base_open";
        goto error;
    }
    if (ORTE_SUCCESS != (ret = orte_rmaps_base_select())) {
        ORTE_ERROR_LOG(ret);
        error = "orte_rmaps_base_select";
        goto error;
    }
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&orte_regx_base_framework, 0))) {
        ORTE_ERROR_LOG(ret);
        error = "orte_regx_base_open";
        goto error;
    }
    if (ORTE_SUCCESS != (ret = orte_regx_base_select())) {
        ORTE_ERROR_LOG(ret);
        error = "orte_regx_base_select";
        goto error;
    }


    /* if a topology file was given, then the rmaps framework open
     * will have reset our topology. Ensure we always get the right
     * one by setting our node topology afterwards
     */
    t = OBJ_NEW(orte_topology_t);
    t->topo = opal_hwloc_topology;
    /* generate the signature */
    orte_topo_signature = opal_hwloc_base_get_topo_signature(opal_hwloc_topology);
    t->sig = strdup(orte_topo_signature);
    opal_pointer_array_add(orte_node_topologies, t);
    if (15 < opal_output_get_verbosity(orte_ess_base_framework.framework_output)) {
        opal_output(0, "%s Topology Info:", ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        opal_dss.dump(0, opal_hwloc_topology, OPAL_HWLOC_TOPO);
    }

    /* if we were given the host list, then we need to setup
     * the daemon info so the RML can function properly
     * without requiring a wireup stage. This must be done
     * after we enable_comm as that function determines our
     * own port, which we need in order to construct the nidmap
     */
    if (NULL != orte_node_regex) {
        if (ORTE_SUCCESS != (ret = orte_regx.nidmap_parse(orte_node_regex))) {
            ORTE_ERROR_LOG(ret);
            error = "construct nidmap";
            goto error;
        }
        /* be sure to update the routing tree so any tree spawn operation
         * properly gets the number of children underneath us */
        orte_routed.update_routing_plan(NULL);
    }

    if (orte_static_ports || orte_fwd_mpirun_port) {
        if (NULL == orte_node_regex) {
            /* we didn't get the node info */
            error = "cannot construct daemon map for static ports - no node map info";
            goto error;
        }
        /* extract the node info from the environment and
         * build a nidmap from it - this will update the
         * routing plan as well
         */
        if (ORTE_SUCCESS != (ret = orte_regx.build_daemon_nidmap())) {
            ORTE_ERROR_LOG(ret);
            error = "construct daemon map from static ports";
            goto error;
        }
        /* be sure to update the routing tree so the initial "phone home"
         * to mpirun goes through the tree if static ports were enabled
         */
        orte_routed.update_routing_plan(NULL);
        /* routing can be enabled */
        orte_routed_base.routing_enabled = true;
    }

    /* Now provide a chance for the PLM
     * to perform any module-specific init functions. This
     * needs to occur AFTER the communications are setup
     * as it may involve starting a non-blocking recv
     * Do this only if a specific PLM was given to us - the
     * orted has no need of the proxy PLM at all
     */
    if (plm_in_use) {
        if (ORTE_SUCCESS != (ret = orte_plm.init())) {
            ORTE_ERROR_LOG(ret);
            error = "orte_plm_init";
            goto error;
        }
    }

    /* setup I/O forwarding system - must come after we init routes */
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&orte_iof_base_framework, 0))) {
        ORTE_ERROR_LOG(ret);
        error = "orte_iof_base_open";
        goto error;
    }
    if (ORTE_SUCCESS != (ret = orte_iof_base_select())) {
        ORTE_ERROR_LOG(ret);
        error = "orte_iof_base_select";
        goto error;
    }
    /* setup the FileM */
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&orte_filem_base_framework, 0))) {
        ORTE_ERROR_LOG(ret);
        error = "orte_filem_base_open";
        goto error;
    }
    if (ORTE_SUCCESS != (ret = orte_filem_base_select())) {
        ORTE_ERROR_LOG(ret);
        error = "orte_filem_base_select";
        goto error;
    }

    /* setup the DFS framework */
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&orte_dfs_base_framework, 0))) {
        ORTE_ERROR_LOG(ret);
        error = "orte_dfs_base_open";
        goto error;
    }
    if (ORTE_SUCCESS != (ret = orte_dfs_base_select())) {
        ORTE_ERROR_LOG(ret);
        error = "orte_dfs_select";
        goto error;
    }

    return ORTE_SUCCESS;

  error:
    orte_show_help("help-orte-runtime.txt",
                   "orte_init:startup:internal-failure",
                   true, error, ORTE_ERROR_NAME(ret), ret);
    /* remove our use of the session directory tree */
    orte_session_dir_finalize(ORTE_PROC_MY_NAME);
    /* ensure we scrub the session directory tree */
    orte_session_dir_cleanup(ORTE_JOBID_WILDCARD);
    return ORTE_ERR_SILENT;
}

int orte_ess_base_orted_finalize(void)
{
    orte_ess_base_signal_t *sig;
    unsigned int i;

    if (signals_set) {
        opal_event_del(&epipe_handler);
        opal_event_del(&term_handler);
        opal_event_del(&int_handler);
        /** Remove the USR signal handlers */
        i = 0;
        OPAL_LIST_FOREACH(sig, &orte_ess_base_signals, orte_ess_base_signal_t) {
            opal_event_signal_del(forward_signals_events + i);
            ++i;
        }
        free (forward_signals_events);
        forward_signals_events = NULL;
        signals_set = false;
    }

    /* cleanup */
    if (NULL != log_path) {
        unlink(log_path);
    }
    /* shutdown the pmix server */
    pmix_server_finalize();
    (void) mca_base_framework_close(&opal_pmix_base_framework);

    /* release the conduits */
    orte_rml.close_conduit(orte_mgmt_conduit);
    orte_rml.close_conduit(orte_coll_conduit);

    /* close frameworks */
    (void) mca_base_framework_close(&orte_filem_base_framework);
    (void) mca_base_framework_close(&orte_grpcomm_base_framework);
    (void) mca_base_framework_close(&orte_iof_base_framework);
    (void) mca_base_framework_close(&orte_errmgr_base_framework);
    (void) mca_base_framework_close(&orte_plm_base_framework);
    /* close the dfs so its threads can exit */
    (void) mca_base_framework_close(&orte_dfs_base_framework);
    /* make sure our local procs are dead */
    orte_odls.kill_local_procs(NULL);
    (void) mca_base_framework_close(&orte_rtc_base_framework);
    (void) mca_base_framework_close(&orte_odls_base_framework);
    (void) mca_base_framework_close(&orte_routed_base_framework);
    (void) mca_base_framework_close(&orte_rml_base_framework);
    (void) mca_base_framework_close(&orte_oob_base_framework);
    (void) mca_base_framework_close(&orte_state_base_framework);
    /* remove our use of the session directory tree */
    orte_session_dir_finalize(ORTE_PROC_MY_NAME);
    /* ensure we scrub the session directory tree */
    orte_session_dir_cleanup(ORTE_JOBID_WILDCARD);
    /* release the job hash table */
    OBJ_RELEASE(orte_job_data);
    return ORTE_SUCCESS;
}

static void shutdown_signal(int fd, short flags, void *arg)
{
    /* trigger the call to shutdown callback to protect
     * against race conditions - the trigger event will
     * check the one-time lock
     */
    ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
    ORTE_ACTIVATE_JOB_STATE(NULL, ORTE_JOB_STATE_FORCED_EXIT);
}

/**
 * Deal with sigpipe errors
 */
static void epipe_signal_callback(int fd, short flags, void *arg)
{
    /* for now, we just ignore them */
    return;
}

/* Pass user signals to the local application processes */
static void signal_forward_callback(int fd, short event, void *arg)
{
    opal_event_t *signal = (opal_event_t*)arg;
    int32_t signum, rc;
    opal_buffer_t *cmd;
    orte_daemon_cmd_flag_t command=ORTE_DAEMON_SIGNAL_LOCAL_PROCS;
    orte_jobid_t job = ORTE_JOBID_WILDCARD;

    signum = OPAL_EVENT_SIGNAL(signal);
    if (!orte_execute_quiet){
        fprintf(stderr, "%s: Forwarding signal %d to job\n",
                orte_basename, signum);
    }

    cmd = OBJ_NEW(opal_buffer_t);

    /* pack the command */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(cmd, &command, 1, ORTE_DAEMON_CMD))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(cmd);
        return;
    }

    /* pack the jobid */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(cmd, &job, 1, ORTE_JOBID))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(cmd);
        return;
    }

    /* pack the signal */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(cmd, &signum, 1, OPAL_INT32))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(cmd);
        return;
    }

    /* send it to ourselves */
    if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                          ORTE_PROC_MY_NAME, cmd,
                                          ORTE_RML_TAG_DAEMON,
                                          NULL, NULL))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(cmd);
    }

}
