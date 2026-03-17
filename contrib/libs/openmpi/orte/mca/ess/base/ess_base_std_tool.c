/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2009 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2013-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2014      Hochschule Esslingen.  All rights reserved.
 *
 * Copyright (c) 2015      Cisco Systems, Inc.  All rights reserved.
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

#include "opal/mca/event/event.h"
#include "opal/mca/pmix/base/base.h"
#include "opal/runtime/opal.h"
#include "opal/runtime/opal_progress_threads.h"
#include "opal/util/arch.h"
#include "opal/util/opal_environ.h"
#include "opal/util/argv.h"
#include "opal/util/proc.h"

#include "orte/mca/iof/base/base.h"
#include "orte/mca/oob/base/base.h"
#include "orte/mca/plm/base/base.h"
#include "orte/mca/rml/base/base.h"
#include "orte/mca/rml/base/rml_contact.h"
#include "orte/mca/routed/base/base.h"
#include "orte/mca/errmgr/base/base.h"
#include "orte/mca/state/base/base.h"
#include "orte/util/proc_info.h"
#include "orte/util/session_dir.h"
#include "orte/util/show_help.h"

#include "orte/runtime/orte_globals.h"
#include "orte/runtime/orte_wait.h"

#include "orte/mca/ess/base/base.h"


static void infocb(int status,
                   opal_list_t *info,
                   void *cbdata,
                   opal_pmix_release_cbfunc_t release_fn,
                   void *release_cbdata)
{
    opal_value_t *kv;
    opal_pmix_lock_t *lock = (opal_pmix_lock_t*)cbdata;

    if (OPAL_SUCCESS != status) {
        ORTE_ERROR_LOG(status);
    } else {
        kv = (opal_value_t*)opal_list_get_first(info);
        if (NULL == kv) {
            ORTE_ERROR_LOG(ORTE_ERR_NOT_SUPPORTED);
        } else {
            if (0 == strcmp(kv->key, OPAL_PMIX_SERVER_URI)) {
                orte_process_info.my_hnp_uri = strdup(kv->data.string);
            } else {
                ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
            }
        }
    }
    if (NULL != release_fn) {
        release_fn(release_cbdata);
    }
    OPAL_PMIX_WAKEUP_THREAD(lock);
}

int orte_ess_base_tool_setup(opal_list_t *flags)
{
    int ret;
    char *error = NULL;
    opal_list_t transports;
    opal_list_t info;
    opal_value_t *kv, *knext, val;
    opal_pmix_query_t *q;
    opal_pmix_lock_t lock;
    opal_buffer_t *buf;

    /* we need an external progress thread to ensure that things run
     * async with the PMIx code */
    orte_event_base = opal_progress_thread_init("tool");

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
    if (NULL == opal_pmix.tool_init) {
        /* we no longer support non-pmix tools */
        orte_show_help("help-ess-base.txt",
                       "legacy-tool", true);
        ret = ORTE_ERR_SILENT;
        error = "opal_pmix.tool_init";
        goto error;
    }
    /* set the event base for the pmix component code */
    opal_pmix_base_set_evbase(orte_event_base);

    /* initialize */
    OBJ_CONSTRUCT(&info, opal_list_t);
    if (NULL != flags) {
        /* pass along any directives */
        OPAL_LIST_FOREACH_SAFE(kv, knext, flags, opal_value_t) {
            opal_list_remove_item(flags, &kv->super);
            opal_list_append(&info, &kv->super);
        }
    }
    if (OPAL_SUCCESS != (ret = opal_pmix.tool_init(&info))) {
        ORTE_ERROR_LOG(ret);
        error = "opal_pmix.init";
        OPAL_LIST_DESTRUCT(&info);
        goto error;
    }
    OPAL_LIST_DESTRUCT(&info);
    /* the PMIx server set our name - record it here */
    ORTE_PROC_MY_NAME->jobid = OPAL_PROC_MY_NAME.jobid;
    ORTE_PROC_MY_NAME->vpid = OPAL_PROC_MY_NAME.vpid;
    orte_process_info.super.proc_hostname = strdup(orte_process_info.nodename);
    orte_process_info.super.proc_flags = OPAL_PROC_ALL_LOCAL;
    orte_process_info.super.proc_arch = opal_local_arch;
    opal_proc_local_set(&orte_process_info.super);

    if (NULL != opal_pmix.query) {
        /* query the server for its URI so we can get any IO forwarded to us */
        OBJ_CONSTRUCT(&info, opal_list_t);
        q = OBJ_NEW(opal_pmix_query_t);
        opal_argv_append_nosize(&q->keys, OPAL_PMIX_SERVER_URI);
        opal_list_append(&info, &q->super);
        OPAL_PMIX_CONSTRUCT_LOCK(&lock);
        opal_pmix.query(&info, infocb, &lock);
        OPAL_PMIX_WAIT_THREAD(&lock);
        OPAL_PMIX_DESTRUCT_LOCK(&lock);
        OPAL_LIST_DESTRUCT(&info);
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
    /* open and setup the error manager */
    if (ORTE_SUCCESS != (ret = mca_base_framework_open(&orte_errmgr_base_framework, 0))) {
        ORTE_ERROR_LOG(ret);
        error = "orte_errmgr_base_open";
        goto error;
    }
    if (ORTE_SUCCESS != (ret = orte_errmgr_base_select())) {
        ORTE_ERROR_LOG(ret);
        error = "orte_errmgr_base_select";
        goto error;
    }
    /* Setup the communication infrastructure */
    /* Routed system */
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

    /* get a conduit for our use - we never route IO over fabric */
    OBJ_CONSTRUCT(&transports, opal_list_t);
    orte_set_attribute(&transports, ORTE_RML_TRANSPORT_TYPE,
                       ORTE_ATTR_LOCAL, orte_mgmt_transport, OPAL_STRING);
    orte_mgmt_conduit = orte_rml.open_conduit(&transports);
    OPAL_LIST_DESTRUCT(&transports);

    /* we -may- need to know the name of the head
     * of our session directory tree, particularly the
     * tmp base where any other session directories on
     * this node might be located
     */

    ret = orte_session_setup_base(ORTE_PROC_MY_NAME);
    if (ORTE_SUCCESS != ret ) {
        ORTE_ERROR_LOG(ret);
        error = "define session dir names";
        goto error;
    }

    /* setup I/O forwarding system - must come after we init routes */
    if (NULL != orte_process_info.my_hnp_uri && NULL == opal_pmix.server_iof_push) {
        /* extract the name */
        if (ORTE_SUCCESS != orte_rml_base_parse_uris(orte_process_info.my_hnp_uri, ORTE_PROC_MY_HNP, NULL)) {
            orte_show_help("help-orte-top.txt", "orte-top:hnp-uri-bad", true, orte_process_info.my_hnp_uri);
            exit(1);
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
        /* set the route to be direct */
        if (ORTE_SUCCESS != orte_routed.update_route(NULL, ORTE_PROC_MY_HNP, ORTE_PROC_MY_HNP)) {
            orte_show_help("help-orte-top.txt", "orte-top:hnp-uri-bad", true, orte_process_info.my_hnp_uri);
            orte_finalize();
            exit(1);
        }

        /* connect to the HNP so we can recv forwarded output */
        buf = OBJ_NEW(opal_buffer_t);
        ret = orte_rml.send_buffer_nb(orte_mgmt_conduit, ORTE_PROC_MY_HNP,
                                      buf, ORTE_RML_TAG_WARMUP_CONNECTION,
                                      orte_rml_send_callback, NULL);
        if (ORTE_SUCCESS != ret) {
            ORTE_ERROR_LOG(ret);
            error = "warmup connection";
            goto error;
        }

        /* set the target hnp as our lifeline so we will terminate if it exits */
        orte_routed.set_lifeline(NULL, ORTE_PROC_MY_HNP);

        /* setup the IOF */
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

    }

    return ORTE_SUCCESS;

 error:
    orte_show_help("help-orte-runtime.txt",
                   "orte_init:startup:internal-failure",
                   true, error, ORTE_ERROR_NAME(ret), ret);

    return ret;
}

int orte_ess_base_tool_finalize(void)
{
    orte_wait_finalize();

    orte_rml.close_conduit(orte_mgmt_conduit);

    /* if I am a tool, then all I will have done is
     * a very small subset of orte_init - ensure that
     * I only back those elements out
     */
    if (NULL != orte_process_info.my_hnp_uri && NULL == opal_pmix.server_iof_push) {
        (void) mca_base_framework_close(&orte_iof_base_framework);
    }
    (void) mca_base_framework_close(&orte_routed_base_framework);
    (void) mca_base_framework_close(&orte_rml_base_framework);
    (void) mca_base_framework_close(&orte_errmgr_base_framework);

    opal_pmix.finalize();
    (void) mca_base_framework_close(&opal_pmix_base_framework);

    return ORTE_SUCCESS;
}
