/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2007      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2010-2015 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2013-2019 Intel, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file **/

#include <src/include/pmix_config.h>

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include PMIX_EVENT_HEADER
#include "event2/thread.h"

#include <pmix_rename.h>

#include "src/util/output.h"
#include "src/util/show_help.h"
#include "src/mca/base/base.h"
#include "src/mca/base/pmix_mca_base_var.h"
#include "src/mca/bfrops/base/base.h"
#include "src/mca/gds/base/base.h"
#include "src/mca/pif/base/base.h"
#include "src/mca/pinstalldirs/base/base.h"
#include "src/mca/plog/base/base.h"
#include "src/mca/pnet/base/base.h"
#include "src/mca/psec/base/base.h"
#include "src/mca/preg/base/base.h"
#include "src/mca/ptl/base/base.h"

#include "src/client/pmix_client_ops.h"
#include "src/event/pmix_event.h"
#include "src/include/types.h"
#include "src/util/error.h"
#include "src/util/keyval_parse.h"

#include "src/runtime/pmix_rte.h"
#include "src/runtime/pmix_progress_threads.h"

const char pmix_version_string[] = PMIX_IDENT_STRING;

PMIX_EXPORT int pmix_initialized = 0;
PMIX_EXPORT bool pmix_init_called = false;
/* we have to export the pmix_globals object so
 * all plugins can access it. However, it is included
 * in the pmix_rename.h file for external protection */
PMIX_EXPORT pmix_globals_t pmix_globals = {
    .init_cntr = 0,
    .mypeer = NULL,
    .hostname = NULL,
    .nodeid = UINT32_MAX,
    .pindex = 0,
    .evbase = NULL,
    .external_evbase = false,
    .debug_output = -1,
    .connected = false,
    .commits_pending = false,
    .mygds = NULL
};


static void _notification_eviction_cbfunc(struct pmix_hotel_t *hotel,
                                          int room_num,
                                          void *occupant)
{
    pmix_notify_caddy_t *cache = (pmix_notify_caddy_t*)occupant;
    PMIX_RELEASE(cache);
}


int pmix_rte_init(pmix_proc_type_t type,
                  pmix_info_t info[], size_t ninfo,
                  pmix_ptl_cbfunc_t cbfunc)
{
    int ret, debug_level;
    char *error = NULL, *evar;
    size_t n;
    char hostname[PMIX_MAXHOSTNAMELEN];

    if( ++pmix_initialized != 1 ) {
        if( pmix_initialized < 1 ) {
            return PMIX_ERROR;
        }
        return PMIX_SUCCESS;
    }

    #if PMIX_NO_LIB_DESTRUCTOR
        if (pmix_init_called) {
            /* can't use show_help here */
            fprintf (stderr, "pmix_init: attempted to initialize after finalize without compiler "
                     "support for either __attribute__(destructor) or linker support for -fini -- process "
                     "will likely abort\n");
            return PMIX_ERR_NOT_SUPPORTED;
        }
    #endif

    pmix_init_called = true;

    /* initialize the output system */
    if (!pmix_output_init()) {
        return PMIX_ERROR;
    }

    /* initialize install dirs code */
    if (PMIX_SUCCESS != (ret = pmix_mca_base_framework_open(&pmix_pinstalldirs_base_framework, 0))) {
        fprintf(stderr, "pmix_pinstalldirs_base_open() failed -- process will likely abort (%s:%d, returned %d instead of PMIX_SUCCESS)\n",
                __FILE__, __LINE__, ret);
        return ret;
    }

    /* initialize the help system */
    pmix_show_help_init();

    /* keyval lex-based parser */
    if (PMIX_SUCCESS != (ret = pmix_util_keyval_parse_init())) {
        error = "pmix_util_keyval_parse_init";
        goto return_error;
    }

    /* Setup the parameter system */
    if (PMIX_SUCCESS != (ret = pmix_mca_base_var_init())) {
        error = "mca_base_var_init";
        goto return_error;
    }

    /* register params for pmix */
    if (PMIX_SUCCESS != (ret = pmix_register_params())) {
        error = "pmix_register_params";
        goto return_error;
    }

    /* initialize the mca */
    if (PMIX_SUCCESS != (ret = pmix_mca_base_open())) {
        error = "mca_base_open";
        goto return_error;
    }

    /* setup the globals structure */
    gethostname(hostname, PMIX_MAXHOSTNAMELEN);
    pmix_globals.hostname = strdup(hostname);
    memset(&pmix_globals.myid.nspace, 0, PMIX_MAX_NSLEN+1);
    pmix_globals.myid.rank = PMIX_RANK_INVALID;
    PMIX_CONSTRUCT(&pmix_globals.events, pmix_events_t);
    pmix_globals.event_window.tv_sec = pmix_event_caching_window;
    pmix_globals.event_window.tv_usec = 0;
    PMIX_CONSTRUCT(&pmix_globals.cached_events, pmix_list_t);
    /* construct the global notification ring buffer */
    PMIX_CONSTRUCT(&pmix_globals.notifications, pmix_hotel_t);
    ret = pmix_hotel_init(&pmix_globals.notifications, pmix_globals.max_events,
                          pmix_globals.evbase, pmix_globals.event_eviction_time,
                          _notification_eviction_cbfunc);
    if (PMIX_SUCCESS != ret) {
        error = "notification hotel init";
        goto return_error;
    }

    /* and setup the iof request tracking list */
    PMIX_CONSTRUCT(&pmix_globals.iof_requests, pmix_list_t);

    /* Setup client verbosities as all procs are allowed to
     * access client APIs */
    if (0 < pmix_client_globals.get_verbose) {
        /* set default output */
        pmix_client_globals.get_output = pmix_output_open(NULL);
        pmix_output_set_verbosity(pmix_client_globals.get_output,
                                  pmix_client_globals.get_verbose);
    }
    if (0 < pmix_client_globals.connect_verbose) {
        /* set default output */
        pmix_client_globals.connect_output = pmix_output_open(NULL);
        pmix_output_set_verbosity(pmix_client_globals.connect_output,
                                  pmix_client_globals.connect_verbose);
    }
    if (0 < pmix_client_globals.fence_verbose) {
        /* set default output */
        pmix_client_globals.fence_output = pmix_output_open(NULL);
        pmix_output_set_verbosity(pmix_client_globals.fence_output,
                                  pmix_client_globals.fence_verbose);
    }
    if (0 < pmix_client_globals.pub_verbose) {
        /* set default output */
        pmix_client_globals.pub_output = pmix_output_open(NULL);
        pmix_output_set_verbosity(pmix_client_globals.pub_output,
                                  pmix_client_globals.pub_verbose);
    }
    if (0 < pmix_client_globals.spawn_verbose) {
        /* set default output */
        pmix_client_globals.spawn_output = pmix_output_open(NULL);
        pmix_output_set_verbosity(pmix_client_globals.spawn_output,
                                  pmix_client_globals.spawn_verbose);
    }
    if (0 < pmix_client_globals.event_verbose) {
        /* set default output */
        pmix_client_globals.event_output = pmix_output_open(NULL);
        pmix_output_set_verbosity(pmix_client_globals.event_output,
                                  pmix_client_globals.event_verbose);
    }
    if (0 < pmix_client_globals.iof_verbose) {
        /* set default output */
        pmix_client_globals.iof_output = pmix_output_open(NULL);
        pmix_output_set_verbosity(pmix_client_globals.iof_output,
                                  pmix_client_globals.iof_verbose);
    }

    /* get our effective id's */
    pmix_globals.uid = geteuid();
    pmix_globals.gid = getegid();
    /* see if debug is requested */
    if (NULL != (evar = getenv("PMIX_DEBUG"))) {
        debug_level = strtol(evar, NULL, 10);
        pmix_globals.debug_output = pmix_output_open(NULL);
        pmix_output_set_verbosity(pmix_globals.debug_output, debug_level);
    }
    /* create our peer object */
    pmix_globals.mypeer = PMIX_NEW(pmix_peer_t);
    if (NULL == pmix_globals.mypeer) {
        ret = PMIX_ERR_NOMEM;
        goto return_error;
    }
    /* whatever our declared proc type, we are definitely v3.0 */
    pmix_globals.mypeer->proc_type = type | PMIX_PROC_V3;
    /* create an nspace object for ourselves - we will
     * fill in the nspace name later */
    pmix_globals.mypeer->nptr = PMIX_NEW(pmix_namespace_t);
    if (NULL == pmix_globals.mypeer->nptr) {
        PMIX_RELEASE(pmix_globals.mypeer);
        ret = PMIX_ERR_NOMEM;
        goto return_error;
    }

    /* scan incoming info for directives */
    if (NULL != info) {
        for (n=0; n < ninfo; n++) {
            if (PMIX_CHECK_KEY(&info[n], PMIX_EVENT_BASE)) {
                pmix_globals.evbase = (pmix_event_base_t*)info[n].value.data.ptr;
                pmix_globals.external_evbase = true;
            } else if (PMIX_CHECK_KEY(&info[n], PMIX_HOSTNAME)) {
                if (NULL != pmix_globals.hostname) {
                    free(pmix_globals.hostname);
                }
                pmix_globals.hostname = strdup(info[n].value.data.string);
            } else if (PMIX_CHECK_KEY(&info[n], PMIX_NODEID)) {
                PMIX_VALUE_GET_NUMBER(ret, &info[n].value, pmix_globals.nodeid, uint32_t);
                if (PMIX_SUCCESS != ret) {
                    goto return_error;
                }
            }
        }
    }

    /* the choice of modules to use when communicating with a peer
     * will be done by the individual init functions and at the
     * time of connection to that peer */

    /* open the bfrops and select the active plugins */
    if (PMIX_SUCCESS != (ret = pmix_mca_base_framework_open(&pmix_bfrops_base_framework, 0)) ) {
        error = "pmix_bfrops_base_open";
        goto return_error;
    }
    if (PMIX_SUCCESS != (ret = pmix_bfrop_base_select()) ) {
        error = "pmix_bfrops_base_select";
        goto return_error;
    }

    /* open the ptl and select the active plugins */
    if (PMIX_SUCCESS != (ret = pmix_mca_base_framework_open(&pmix_ptl_base_framework, 0)) ) {
        error = "pmix_ptl_base_open";
        goto return_error;
    }
    if (PMIX_SUCCESS != (ret = pmix_ptl_base_select()) ) {
        error = "pmix_ptl_base_select";
        goto return_error;
    }
    /* set the notification callback function */
    if (PMIX_SUCCESS != (ret = pmix_ptl_base_set_notification_cbfunc(cbfunc))) {
        error = "pmix_ptl_set_notification_cbfunc";
        goto return_error;
    }

    /* open the psec and select the active plugins */
    if (PMIX_SUCCESS != (ret = pmix_mca_base_framework_open(&pmix_psec_base_framework, 0))) {
        error = "pmix_psec_base_open";
        goto return_error;
    }
    if (PMIX_SUCCESS != (ret = pmix_psec_base_select())) {
        error = "pmix_psec_base_select";
        goto return_error;
    }

    /* open the gds and select the active plugins */
    if (PMIX_SUCCESS != (ret = pmix_mca_base_framework_open(&pmix_gds_base_framework, 0)) ) {
        error = "pmix_gds_base_open";
        goto return_error;
    }
    if (PMIX_SUCCESS != (ret = pmix_gds_base_select(info, ninfo)) ) {
        error = "pmix_gds_base_select";
        goto return_error;
    }

    /* initialize pif framework */
    if (PMIX_SUCCESS != (ret = pmix_mca_base_framework_open(&pmix_pif_base_framework, 0))) {
        error = "pmix_pif_base_open";
        return ret;
    }

    /* open the preg and select the active plugins */
    if (PMIX_SUCCESS != (ret = pmix_mca_base_framework_open(&pmix_preg_base_framework, 0)) ) {
        error = "pmix_preg_base_open";
        goto return_error;
    }
    if (PMIX_SUCCESS != (ret = pmix_preg_base_select()) ) {
        error = "pmix_preg_base_select";
        goto return_error;
    }

    /* open the plog and select the active plugins */
    if (PMIX_SUCCESS != (ret = pmix_mca_base_framework_open(&pmix_plog_base_framework, 0)) ) {
        error = "pmix_plog_base_open";
        goto return_error;
    }
    if (PMIX_SUCCESS != (ret = pmix_plog_base_select()) ) {
        error = "pmix_plog_base_select";
        goto return_error;
    }

    /* if an external event base wasn't provide, create one */
    if (!pmix_globals.external_evbase) {
        /* tell libevent that we need thread support */
        pmix_event_use_threads();

        /* create an event base and progress thread for us */
        if (NULL == (pmix_globals.evbase = pmix_progress_thread_init(NULL))) {
            error = "progress thread";
            ret = PMIX_ERROR;
            goto return_error;
        }
    }

    return PMIX_SUCCESS;

  return_error:
    if (PMIX_ERR_SILENT != ret) {
        pmix_show_help( "help-pmix-runtime.txt",
                        "pmix_init:startup:internal-failure", true,
                        error, ret );
    }
    return ret;
}
