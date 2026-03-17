/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
 * Copyright (c) 2010-2011 Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2011-2014 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2017 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2013-2018 Intel, Inc.  All rights reserved.
 * Copyright (c) 2017-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
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

#include "opal/hash_string.h"
#include "opal/class/opal_hash_table.h"
#include "opal/class/opal_list.h"
#include "opal/mca/event/event.h"
#include "opal/runtime/opal.h"

#include "opal/util/arch.h"
#include "opal/util/argv.h"
#include "opal/util/if.h"
#include "opal/util/os_path.h"
#include "opal/util/output.h"
#include "opal/util/opal_environ.h"
#include "opal/util/malloc.h"
#include "opal/util/basename.h"
#include "opal/util/fd.h"
#include "opal/mca/pmix/base/base.h"
#include "opal/mca/pstat/base/base.h"
#include "opal/mca/hwloc/base/base.h"

#include "orte/mca/oob/base/base.h"
#include "orte/mca/rml/base/base.h"
#include "orte/mca/rml/rml_types.h"
#include "orte/mca/routed/base/base.h"
#include "orte/mca/routed/routed.h"
#include "orte/mca/rtc/base/base.h"
#include "orte/mca/dfs/base/base.h"
#include "orte/mca/errmgr/base/base.h"
#include "orte/mca/grpcomm/base/base.h"
#include "orte/mca/iof/base/base.h"
#include "orte/mca/ras/base/base.h"
#include "orte/mca/regx/base/base.h"
#include "orte/mca/plm/base/base.h"
#include "orte/mca/plm/plm.h"
#include "orte/mca/odls/base/base.h"
#include "orte/mca/rmaps/base/base.h"
#include "orte/mca/filem/base/base.h"
#include "orte/mca/state/base/base.h"
#include "orte/mca/state/state.h"

#include "orte/orted/orted_submit.h"
#include "orte/orted/pmix/pmix_server.h"

#include "orte/util/show_help.h"
#include "orte/util/proc_info.h"
#include "orte/util/session_dir.h"
#include "orte/util/hnp_contact.h"
#include "orte/util/name_fns.h"
#include "orte/util/show_help.h"
#include "orte/util/comm/comm.h"

#include "orte/runtime/runtime.h"
#include "orte/runtime/orte_wait.h"
#include "orte/runtime/orte_globals.h"
#include "orte/runtime/orte_quit.h"
#include "orte/runtime/orte_locks.h"

#include "orte/mca/ess/ess.h"
#include "orte/mca/ess/base/base.h"
#include "orte/mca/ess/hnp/ess_hnp.h"

static int rte_init(void);
static int rte_finalize(void);
static void rte_abort(int status, bool report) __opal_attribute_noreturn__;

orte_ess_base_module_t orte_ess_hnp_module = {
    rte_init,
    rte_finalize,
    rte_abort,
    NULL /* ft_event */
};

/* local globals */
static bool signals_set=false;
static bool forcibly_die=false;
static opal_event_t term_handler;
static opal_event_t epipe_handler;
static int term_pipe[2];
static opal_event_t *forward_signals_events = NULL;

static void abort_signal_callback(int signal);
static void clean_abort(int fd, short flags, void *arg);
static void epipe_signal_callback(int fd, short flags, void *arg);
static void signal_forward_callback(int fd, short event, void *arg);

static void setup_sighandler(int signal, opal_event_t *ev,
                             opal_event_cbfunc_t cbfunc)
{
    opal_event_signal_set(orte_event_base, ev, signal, cbfunc, ev);
    opal_event_set_priority(ev, ORTE_ERROR_PRI);
    opal_event_signal_add(ev, NULL);
}

static int rte_init(void)
{
    int ret;
    char *error = NULL;
    char *contact_path;
    orte_job_t *jdata;
    orte_node_t *node;
    orte_proc_t *proc;
    orte_app_context_t *app;
    char **aliases, *aptr;
    char *coprocessors, **sns;
    uint32_t h;
    int idx;
    orte_topology_t *t;
    opal_list_t transports;
    orte_ess_base_signal_t *sig;
    opal_value_t val;

    /* run the prolog */
    if (ORTE_SUCCESS != (ret = orte_ess_base_std_prolog())) {
        error = "orte_ess_base_std_prolog";
        goto error;
    }

    /* setup callback for SIGPIPE */
    setup_sighandler(SIGPIPE, &epipe_handler, epipe_signal_callback);
    /** setup callbacks for abort signals - from this point
     * forward, we need to abort in a manner that allows us
     * to cleanup. However, we cannot directly use libevent
     * to trap these signals as otherwise we cannot respond
     * to them if we are stuck in an event! So instead use
     * the basic POSIX trap functions to handle the signal,
     * and then let that signal handler do some magic to
     * avoid the hang
     *
     * NOTE: posix traps don't allow us to do anything major
     * in them, so use a pipe tied to a libevent event to
     * reach a "safe" place where the termination event can
     * be created
     */
    pipe(term_pipe);
    /* setup an event to attempt normal termination on signal */
    opal_event_set(orte_event_base, &term_handler, term_pipe[0], OPAL_EV_READ, clean_abort, NULL);
    opal_event_set_priority(&term_handler, ORTE_ERROR_PRI);
    opal_event_add(&term_handler, NULL);

    /* Set both ends of this pipe to be close-on-exec so that no
       children inherit it */
    if (opal_fd_set_cloexec(term_pipe[0]) != OPAL_SUCCESS ||
        opal_fd_set_cloexec(term_pipe[1]) != OPAL_SUCCESS) {
        error = "unable to set the pipe to CLOEXEC";
        goto error;
    }

    /* point the signal trap to a function that will activate that event */
    signal(SIGTERM, abort_signal_callback);
    signal(SIGINT, abort_signal_callback);
    signal(SIGHUP, abort_signal_callback);

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

    /* if we are using xml for output, put an mpirun start tag */
    if (orte_xml_output) {
        fprintf(orte_xml_fp, "<mpirun>\n");
        fflush(orte_xml_fp);
    }

    /* open and setup the opal_pstat framework so we can provide
     * process stats if requested
     */
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&opal_pstat_base_framework, 0))) {
        error = "opal_pstat_base_open";
        goto error;
    }
    if (ORTE_SUCCESS != (ret = opal_pstat_base_select())) {
        error = "opal_pstat_base_select";
        goto error;
    }

    /* open and setup the state machine */
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&orte_state_base_framework, 0))) {
        error = "orte_state_base_open";
        goto error;
    }
    if (ORTE_SUCCESS != (ret = orte_state_base_select())) {
        error = "orte_state_base_select";
        goto error;
    }

    /* open the errmgr */
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&orte_errmgr_base_framework, 0))) {
        error = "orte_errmgr_base_open";
        goto error;
    }

    /* Since we are the HNP, then responsibility for
     * defining the name falls to the PLM component for our
     * respective environment - hence, we have to open the PLM
     * first and select that component.
     */
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&orte_plm_base_framework, 0))) {
        error = "orte_plm_base_open";
        goto error;
    }
    if (ORTE_SUCCESS != (ret = orte_plm_base_select())) {
        error = "orte_plm_base_select";
        if (ORTE_ERR_FATAL == ret) {
            /* we already output a show_help - so keep down the verbage */
            ret = ORTE_ERR_SILENT;
        }
        goto error;
    }
    /* if we were spawned by a singleton, our jobid was given to us */
    if (NULL != orte_ess_base_jobid) {
        if (ORTE_SUCCESS != (ret = orte_util_convert_string_to_jobid(&ORTE_PROC_MY_NAME->jobid, orte_ess_base_jobid))) {
            error = "convert_string_to_jobid";
            goto error;
        }
        ORTE_PROC_MY_NAME->vpid = 0;
    } else {
        if (ORTE_SUCCESS != (ret = orte_plm.set_hnp_name())) {
            error = "orte_plm_set_hnp_name";
            goto error;
        }
    }
    /* now that my name is set, xfer it to the OPAL layer */
    orte_process_info.super.proc_name = *(opal_process_name_t*)ORTE_PROC_MY_NAME;
    orte_process_info.super.proc_hostname = strdup(orte_process_info.nodename);
    orte_process_info.super.proc_flags = OPAL_PROC_ALL_LOCAL;
    orte_process_info.super.proc_arch = opal_local_arch;
    opal_proc_local_set(&orte_process_info.super);

    /* setup my session directory here as the OOB may need it */
    if (orte_create_session_dirs) {
        OPAL_OUTPUT_VERBOSE((2, orte_debug_output,
                             "%s setting up session dir with\n\ttmpdir: %s\n\thost %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             (NULL == orte_process_info.tmpdir_base) ? "UNDEF" : orte_process_info.tmpdir_base,
                             orte_process_info.nodename));
        /* take a pass thru the session directory code to fillin the
         * tmpdir names - don't create anything yet
         */
        if (ORTE_SUCCESS != (ret = orte_session_dir(false, ORTE_PROC_MY_NAME))) {
            error = "orte_session_dir define";
            goto error;
        }
        /* clear the session directory just in case there are
         * stale directories laying around
         */
        orte_session_dir_cleanup(ORTE_JOBID_WILDCARD);

        /* now actually create the directory tree */
        if (ORTE_SUCCESS != (ret = orte_session_dir(true, ORTE_PROC_MY_NAME))) {
            error = "orte_session_dir";
            goto error;
        }
    }

    /* setup the PMIx framework - ensure it skips all non-PMIx components, but
     * do not override anything we were given */
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
    /*
     * Routed system
     */
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&orte_routed_base_framework, 0))) {
        ORTE_ERROR_LOG(ret);
        error = "orte_rml_base_open";
        goto error;
    }
    if (ORTE_SUCCESS != (ret = orte_routed_base_select())) {
        ORTE_ERROR_LOG(ret);
        error = "orte_routed_base_select";
        goto error;
    }
    /*
     * OOB Layer
     */
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&orte_oob_base_framework, 0))) {
        error = "orte_oob_base_open";
        goto error;
    }
    if (ORTE_SUCCESS != (ret = orte_oob_base_select())) {
        error = "orte_oob_base_select";
        goto error;
    }

    /*
     * Runtime Messaging Layer
     */
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&orte_rml_base_framework, 0))) {
        error = "orte_rml_base_open";
        goto error;
    }
    if (ORTE_SUCCESS != (ret = orte_rml_base_select())) {
        error = "orte_rml_base_select";
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

    /* it is now safe to start the pmix server */
    pmix_server_start();

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


    /* setup the error manager */
    if (ORTE_SUCCESS != (ret = orte_errmgr_base_select())) {
        error = "orte_errmgr_base_select";
        goto error;
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
    /* mark that the daemons have reported as we are the
     * only ones in the system right now, and we definitely
     * are running!
     */
    jdata->state = ORTE_JOB_STATE_DAEMONS_REPORTED;

    /* every job requires at least one app */
    app = OBJ_NEW(orte_app_context_t);
    opal_pointer_array_set_item(jdata->apps, 0, app);
    jdata->num_apps++;
    /* create and store a node object where we are */
    node = OBJ_NEW(orte_node_t);
    node->name = strdup(orte_process_info.nodename);
    node->index = ORTE_PROC_MY_NAME->vpid;
    opal_pointer_array_set_item(orte_node_pool, 0, node);

    /* create and store a proc object for us */
    proc = OBJ_NEW(orte_proc_t);
    proc->name.jobid = ORTE_PROC_MY_NAME->jobid;
    proc->name.vpid = ORTE_PROC_MY_NAME->vpid;
    proc->pid = orte_process_info.pid;
    orte_oob_base_get_addr(&proc->rml_uri);
    orte_process_info.my_hnp_uri = strdup(proc->rml_uri);
    /* store it in the local PMIx repo for later retrieval */
    OBJ_CONSTRUCT(&val, opal_value_t);
    val.key = OPAL_PMIX_PROC_URI;
    val.type = OPAL_STRING;
    val.data.string = proc->rml_uri;
    if (OPAL_SUCCESS != (ret = opal_pmix.store_local(ORTE_PROC_MY_NAME, &val))) {
        ORTE_ERROR_LOG(ret);
        val.key = NULL;
        val.data.string = NULL;
        OBJ_DESTRUCT(&val);
        error = "store uri";
        goto error;
    }
    val.key = NULL;
    val.data.string = NULL;
    OBJ_DESTRUCT(&val);
    /* we are also officially a daemon, so better update that field too */
    orte_process_info.my_daemon_uri = strdup(proc->rml_uri);
    proc->state = ORTE_PROC_STATE_RUNNING;
    OBJ_RETAIN(node);  /* keep accounting straight */
    proc->node = node;
    opal_pointer_array_set_item(jdata->procs, proc->name.vpid, proc);
    /* record that the daemon (i.e., us) is on this node
     * NOTE: we do not add the proc object to the node's
     * proc array because we are not an application proc.
     * Instead, we record it in the daemon field of the
     * node object
     */
    OBJ_RETAIN(proc);   /* keep accounting straight */
    node->daemon = proc;
    ORTE_FLAG_SET(node, ORTE_NODE_FLAG_DAEMON_LAUNCHED);
    node->state = ORTE_NODE_STATE_UP;
    /* if we are to retain aliases, get ours */
    if (orte_retain_aliases) {
        aliases = NULL;
        opal_ifgetaliases(&aliases);
        if (0 < opal_argv_count(aliases)) {
            /* add our own local name to it */
            opal_argv_append_nosize(&aliases, orte_process_info.nodename);
            aptr = opal_argv_join(aliases, ',');
            orte_set_attribute(&node->attributes, ORTE_NODE_ALIAS, ORTE_ATTR_LOCAL, aptr, OPAL_STRING);
            free(aptr);
        }
        opal_argv_free(aliases);
    }
    /* record that the daemon job is running */
    jdata->num_procs = 1;
    jdata->state = ORTE_JOB_STATE_RUNNING;
    /* obviously, we have "reported" */
    jdata->num_reported = 1;

    /* Now provide a chance for the PLM
     * to perform any module-specific init functions. This
     * needs to occur AFTER the communications are setup
     * as it may involve starting a non-blocking recv
     */
    if (ORTE_SUCCESS != (ret = orte_plm.init())) {
        ORTE_ERROR_LOG(ret);
        error = "orte_plm_init";
        goto error;
    }
    /*
     * Setup the remaining resource
     * management and errmgr frameworks - application procs
     * and daemons do not open these frameworks as they only use
     * the hnp proxy support in the PLM framework.
     */
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&orte_ras_base_framework, 0))) {
        ORTE_ERROR_LOG(ret);
        error = "orte_ras_base_open";
        goto error;
    }
    if (ORTE_SUCCESS != (ret = orte_ras_base_select())) {
        ORTE_ERROR_LOG(ret);
        error = "orte_ras_base_find_available";
        goto error;
    }
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&orte_rmaps_base_framework, 0))) {
        ORTE_ERROR_LOG(ret);
        error = "orte_rmaps_base_open";
        goto error;
    }
    if (ORTE_SUCCESS != (ret = orte_rmaps_base_select())) {
        ORTE_ERROR_LOG(ret);
        error = "orte_rmaps_base_find_available";
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
    /* add it to the array of known topologies */
    t = OBJ_NEW(orte_topology_t);
    t->topo = opal_hwloc_topology;
    /* generate the signature */
    orte_topo_signature = opal_hwloc_base_get_topo_signature(opal_hwloc_topology);
    t->sig = strdup(orte_topo_signature);
    opal_pointer_array_add(orte_node_topologies, t);
    node->topology = t;
    if (15 < opal_output_get_verbosity(orte_ess_base_framework.framework_output)) {
        opal_output(0, "%s Topology Info:", ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        opal_dss.dump(0, opal_hwloc_topology, OPAL_HWLOC_TOPO);
    }


    /* init the hash table, if necessary */
    if (NULL == orte_coprocessors) {
        orte_coprocessors = OBJ_NEW(opal_hash_table_t);
        opal_hash_table_init(orte_coprocessors, orte_process_info.num_procs);
    }
    /* detect and add any coprocessors */
    coprocessors = opal_hwloc_base_find_coprocessors(opal_hwloc_topology);
    if (NULL != coprocessors) {
        /* separate the serial numbers of the coprocessors
         * on this host
         */
        sns = opal_argv_split(coprocessors, ',');
        for (idx=0; NULL != sns[idx]; idx++) {
            /* compute the hash */
            OPAL_HASH_STR(sns[idx], h);
            /* mark that this coprocessor is hosted by this node */
            opal_hash_table_set_value_uint32(orte_coprocessors, h, (void*)&(ORTE_PROC_MY_NAME->vpid));
        }
        opal_argv_free(sns);
        free(coprocessors);
        orte_coprocessors_detected = true;
    }
    /* see if I am on a coprocessor */
    coprocessors = opal_hwloc_base_check_on_coprocessor();
    if (NULL != coprocessors) {
        /* compute the hash */
        OPAL_HASH_STR(coprocessors, h);
        /* mark that I am on this coprocessor */
        opal_hash_table_set_value_uint32(orte_coprocessors, h, (void*)&(ORTE_PROC_MY_NAME->vpid));
        orte_set_attribute(&node->attributes, ORTE_NODE_SERIAL_NUMBER, ORTE_ATTR_LOCAL, coprocessors, OPAL_STRING);
        free(coprocessors);
        orte_coprocessors_detected = true;
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

    /* setup the orte_show_help system to recv remote output */
    orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD, ORTE_RML_TAG_SHOW_HELP,
                            ORTE_RML_PERSISTENT, orte_show_help_recv, NULL);

    if (orte_create_session_dirs) {
        /* set the opal_output hnp file location to be in the
         * proc-specific session directory. */
        opal_output_set_output_file_info(orte_process_info.proc_session_dir,
                                         "output-", NULL, NULL);
        /* save my contact info in a file for others to find */
        if( NULL == orte_process_info.jobfam_session_dir ){
            /* has to be set here! */
            ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
            goto error;
        }
        contact_path = opal_os_path(false, orte_process_info.jobfam_session_dir, "contact.txt", NULL);
        OPAL_OUTPUT_VERBOSE((2, orte_debug_output,
                             "%s writing contact file %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             contact_path));

        if (ORTE_SUCCESS != (ret = orte_write_hnp_contact_file(contact_path))) {
            OPAL_OUTPUT_VERBOSE((2, orte_debug_output,
                                 "%s writing contact file failed with error %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_ERROR_NAME(ret)));
        } else {
            OPAL_OUTPUT_VERBOSE((2, orte_debug_output,
                                 "%s wrote contact file",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        }
        free(contact_path);
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

    /* setup the dfs framework */
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

    /* setup to support debugging */
    orte_state.add_job_state(ORTE_JOB_STATE_READY_FOR_DEBUGGERS,
                             orte_debugger_init_after_spawn,
                             ORTE_SYS_PRI);
    orte_state.add_job_state(ORTE_JOB_STATE_DEBUGGER_DETACH,
                             orte_debugger_detached,
                             ORTE_SYS_PRI);

    /* if a tool has launched us and is requesting event reports,
     * then set its contact info into the comm system
     */
    if (orte_report_events) {
        if (ORTE_SUCCESS != (ret = orte_util_comm_connect_tool(orte_report_events_uri))) {
            error = "could not connect to tool";
            goto error;
        }
    }
    /* We actually do *not* want an HNP to voluntarily yield() the
       processor more than necessary.  Orterun already blocks when
       it is doing nothing, so it doesn't use any more CPU cycles than
       it should; but when it *is* doing something, we do not want it
       to be unnecessarily delayed because it voluntarily yielded the
       processor in the middle of its work.
       For example: when a message arrives at orterun, we want the
       OS to wake us up in a timely fashion (which most OS's
       seem good about doing) and then we want orterun to process
       the message as fast as possible.  If orterun yields and lets
       aggressive MPI applications get the processor back, it may be a
       long time before the OS schedules orterun to run again
       (particularly if there is no IO event to wake it up).  Hence,
       routed OOB messages (for example) may be significantly delayed
       before being delivered to MPI processes, which can be
       problematic in some scenarios (e.g., COMM_SPAWN, BTL's that
       require OOB messages for wireup, etc.). */
    opal_progress_set_yield_when_idle(false);
    return ORTE_SUCCESS;

  error:
    if (ORTE_ERR_SILENT != ret && !orte_report_silent_errors) {
        orte_show_help("help-orte-runtime.txt",
                       "orte_init:startup:internal-failure",
                       true, error, ORTE_ERROR_NAME(ret), ret);
    }
    /* remove my contact info file, if we have session directories */
    if (NULL != orte_process_info.jobfam_session_dir) {
        contact_path = opal_os_path(false, orte_process_info.jobfam_session_dir,
                                    "contact.txt", NULL);
        unlink(contact_path);
        free(contact_path);
    }
    /* remove our use of the session directory tree */
    orte_session_dir_finalize(ORTE_PROC_MY_NAME);
    /* ensure we scrub the session directory tree */
    orte_session_dir_cleanup(ORTE_JOBID_WILDCARD);
    return ORTE_ERR_SILENT;
}

static int rte_finalize(void)
{
    char *contact_path;
    orte_job_t *jdata;
    uint32_t key;
    orte_ess_base_signal_t *sig;
    unsigned int i;

    if (signals_set) {
        /* Remove the epipe handler */
        opal_event_signal_del(&epipe_handler);
        /* remove the term handler */
        opal_event_del(&term_handler);
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

    /* shutdown the pmix server */
    pmix_server_finalize();
    (void) mca_base_framework_close(&opal_pmix_base_framework);
    (void) mca_base_framework_close(&orte_dfs_base_framework);
    (void) mca_base_framework_close(&orte_filem_base_framework);
    /* output any lingering stdout/err data */
    fflush(stdout);
    fflush(stderr);

        /* release the conduits */
    orte_rml.close_conduit(orte_mgmt_conduit);
    orte_rml.close_conduit(orte_coll_conduit);

    (void) mca_base_framework_close(&orte_iof_base_framework);
    (void) mca_base_framework_close(&orte_rtc_base_framework);
    (void) mca_base_framework_close(&orte_odls_base_framework);
    (void) mca_base_framework_close(&orte_rmaps_base_framework);
    (void) mca_base_framework_close(&orte_ras_base_framework);
    (void) mca_base_framework_close(&orte_grpcomm_base_framework);
    (void) mca_base_framework_close(&orte_routed_base_framework);
    (void) mca_base_framework_close(&orte_plm_base_framework);
    (void) mca_base_framework_close(&orte_errmgr_base_framework);
    (void) mca_base_framework_close(&orte_state_base_framework);

    /* cleanup the pstat stuff */
    (void) mca_base_framework_close(&opal_pstat_base_framework);

    /* remove my contact info file, if we have session directories */
    if (NULL != orte_process_info.jobfam_session_dir) {
        contact_path = opal_os_path(false, orte_process_info.jobfam_session_dir,
                                    "contact.txt", NULL);
        unlink(contact_path);
        free(contact_path);
    }

    /* shutdown the messaging frameworks */
    (void) mca_base_framework_close(&orte_rml_base_framework);
    (void) mca_base_framework_close(&orte_oob_base_framework);

    /* remove our use of the session directory tree */
    orte_session_dir_finalize(ORTE_PROC_MY_NAME);
    /* ensure we scrub the session directory tree */
    orte_session_dir_cleanup(ORTE_JOBID_WILDCARD);

    /* close the xml output file, if open */
    if (orte_xml_output) {
        fprintf(orte_xml_fp, "</mpirun>\n");
        fflush(orte_xml_fp);
        if (stdout != orte_xml_fp) {
            fclose(orte_xml_fp);
        }
    }

    /* release the job hash table */
    OPAL_HASH_TABLE_FOREACH(key, uint32, jdata, orte_job_data) {
        if (NULL != jdata) {
            OBJ_RELEASE(jdata);
        }
    }
    OBJ_RELEASE(orte_job_data);

    if (NULL != orte_process_info.super.proc_hostname) {
        free(orte_process_info.super.proc_hostname);
    }
    if (orte_do_not_launch) {
        exit(0);
    }

{
    opal_pointer_array_t * array = orte_node_topologies;
    int i;
    if( array->number_free != array->size ) {
        OPAL_THREAD_LOCK(&array->lock);
        array->lowest_free = 0;
        array->number_free = array->size;
        for(i=0; i<array->size; i++) {
            if(NULL != array->addr[i]) {
                orte_topology_t * topo = (orte_topology_t *)array->addr[i];
                topo->topo = NULL;
                OBJ_RELEASE(topo);
            }
            array->addr[i] = NULL;
        }
        OPAL_THREAD_UNLOCK(&array->lock);
    }
}
    OBJ_RELEASE(orte_node_topologies);

{
    opal_pointer_array_t * array = orte_node_pool;
    int i;
    orte_node_t* node = (orte_node_t *)opal_pointer_array_get_item(orte_node_pool, 0);
    assert(NULL != node);
    OBJ_RELEASE(node->daemon);
    node->daemon = NULL;
    if( array->number_free != array->size ) {
        OPAL_THREAD_LOCK(&array->lock);
        array->lowest_free = 0;
        array->number_free = array->size;
        for(i=0; i<array->size; i++) {
            if(NULL != array->addr[i]) {
                node= (orte_node_t*)array->addr[i];
                OBJ_RELEASE(node);
            }
            array->addr[i] = NULL;
        }
        OPAL_THREAD_UNLOCK(&array->lock);
    }
}
    OBJ_RELEASE(orte_node_pool);

    free(orte_topo_signature);

    return ORTE_SUCCESS;
}

static void rte_abort(int status, bool report)
{
    /* do NOT do a normal finalize as this will very likely
     * hang the process. We are aborting due to an abnormal condition
     * that precludes normal cleanup
     *
     * We do need to do the following bits to make sure we leave a
     * clean environment. Taken from orte_finalize():
     * - Assume errmgr cleans up child processes before we exit.
     */

    /* ensure we scrub the session directory tree */
    orte_session_dir_cleanup(ORTE_JOBID_WILDCARD);
    /* - Clean out the global structures
     * (not really necessary, but good practice)
     */
    orte_proc_info_finalize();
    /* just exit */
    exit(status);
}

static void clean_abort(int fd, short flags, void *arg)
{
    /* if we have already ordered this once, don't keep
     * doing it to avoid race conditions
     */
    if (opal_atomic_trylock(&orte_abort_inprogress_lock)) { /* returns 1 if already locked */
        if (forcibly_die) {
            /* kill any local procs */
            orte_odls.kill_local_procs(NULL);
            /* whack any lingering session directory files from our jobs */
            orte_session_dir_cleanup(ORTE_JOBID_WILDCARD);
            /* cleanup our pmix server */
            opal_pmix.finalize();
            /* exit with a non-zero status */
            exit(ORTE_ERROR_DEFAULT_EXIT_CODE);
        }
        fprintf(stderr, "%s: abort is already in progress...hit ctrl-c again to forcibly terminate\n\n", orte_basename);
        forcibly_die = true;
        /* reset the event */
        opal_event_add(&term_handler, NULL);
        return;
    }
    /* ensure we exit with a non-zero status */
    ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);

    /* ensure that the forwarding of stdin stops */
    orte_job_term_ordered = true;
    /* tell us to be quiet - hey, the user killed us with a ctrl-c,
     * so need to tell them that!
     */
    orte_execute_quiet = true;
    /* We are in an event handler; the job completed procedure
       will delete the signal handler that is currently running
       (which is a Bad Thing), so we can't call it directly.
       Instead, we have to exit this handler and setup to call
       job_completed() after this. */
    orte_plm.terminate_orteds();;
}

static struct timeval current, last={0,0};
static bool first = true;

/*
 * Attempt to terminate the job and wait for callback indicating
 * the job has been aborted.
 */
static void abort_signal_callback(int fd)
{
    uint8_t foo = 1;
    char *msg = "Abort is in progress...hit ctrl-c again within 5 seconds to forcibly terminate\n\n";

    /* if this is the first time thru, just get
     * the current time
     */
    if (first) {
        first = false;
        gettimeofday(&current, NULL);
    } else {
        /* get the current time */
        gettimeofday(&current, NULL);
        /* if this is within 5 seconds of the
         * last time we were called, then just
         * exit - we are probably stuck
         */
        if ((current.tv_sec - last.tv_sec) < 5) {
            exit(1);
        }
        write(1, (void*)msg, strlen(msg));
    }
    /* save the time */
    last.tv_sec = current.tv_sec;
    /* tell the event lib to attempt to abnormally terminate */
    write(term_pipe[1], &foo, 1);
}

/**
 * Deal with sigpipe errors
 */
static int sigpipe_error_count=0;
static void epipe_signal_callback(int fd, short flags, void *arg)
{
    sigpipe_error_count++;

    if (10 < sigpipe_error_count) {
        /* time to abort */
        opal_output(0, "%s: SIGPIPE detected on fd %d - aborting", orte_basename, fd);
        clean_abort(0, 0, NULL);
    }

    return;
}

/**
 * Pass user signals to the remote application processes
 */
static void  signal_forward_callback(int fd, short event, void *arg)
{
    opal_event_t *signal = (opal_event_t*)arg;
    int signum, ret;

    signum = OPAL_EVENT_SIGNAL(signal);
    if (!orte_execute_quiet){
        fprintf(stderr, "%s: Forwarding signal %d to job\n",
                orte_basename, signum);
    }

    /** send the signal out to the processes, including any descendants */
    if (ORTE_SUCCESS != (ret = orte_plm.signal_job(ORTE_JOBID_WILDCARD, signum))) {
        fprintf(stderr, "Signal %d could not be sent to the job (returned %d)",
                signum, ret);
    }
}
