/*
 * Copyright (c) 2012-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2013-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2012-2014 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2014      Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 */
#include "ompi_config.h"
#include "ompi/constants.h"

#include <string.h>
#include <stdio.h>
#include <ctype.h>

#include "opal/dss/dss.h"
#include "opal/util/argv.h"
#include "opal/util/proc.h"
#include "opal/util/opal_getcwd.h"
#include "opal/mca/pmix/pmix.h"
#include "opal/threads/threads.h"
#include "opal/class/opal_list.h"
#include "opal/dss/dss.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/ess/ess.h"
#include "orte/mca/grpcomm/base/base.h"
#include "orte/mca/odls/odls.h"
#include "orte/mca/plm/plm.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/rml/rml_types.h"
#include "orte/mca/rmaps/rmaps.h"
#include "orte/mca/rmaps/rmaps_types.h"
#include "orte/mca/rmaps/base/base.h"
#include "orte/mca/rml/base/rml_contact.h"
#include "orte/mca/state/state.h"
#include "orte/mca/routed/routed.h"
#include "orte/util/name_fns.h"
#include "orte/util/session_dir.h"
#include "orte/util/show_help.h"
#include "orte/runtime/orte_globals.h"
#include "orte/runtime/orte_wait.h"
#include "orte/runtime/orte_data_server.h"

#include "ompi/mca/rte/base/base.h"
#include "ompi/mca/rte/rte.h"
#include "ompi/debuggers/debuggers.h"
#include "ompi/proc/proc.h"
#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"

extern ompi_rte_component_t mca_rte_orte_component;

void ompi_rte_abort(int error_code, char *fmt, ...)
{
    va_list arglist;

    /* If there was a message, output it */
    va_start(arglist, fmt);
    if( NULL != fmt ) {
        char* buffer = NULL;
        vasprintf( &buffer, fmt, arglist );
        opal_output( 0, "%s", buffer );
        free( buffer );
    }
    va_end(arglist);

    /* if I am a daemon or the HNP... */
    if (ORTE_PROC_IS_HNP || ORTE_PROC_IS_DAEMON) {
        /* whack my local procs */
        orte_odls.kill_local_procs(NULL);
        /* whack any session directories */
        orte_session_dir_cleanup(ORTE_JOBID_WILDCARD);
    } else {
        /* cleanup my session directory */
        orte_session_dir_finalize(ORTE_PROC_MY_NAME);
    }

    /* if a critical connection failed, or a sensor limit was exceeded, exit without dropping a core */
    if (ORTE_ERR_CONNECTION_FAILED == error_code ||
        ORTE_ERR_SENSOR_LIMIT_EXCEEDED == error_code) {
        orte_ess.abort(error_code, false);
    } else {
        orte_ess.abort(error_code, true);
    }

    /*
     * We must exit in orte_ess.abort; all implementations of orte_ess.abort
     * contain __opal_attribute_noreturn__
     */
    /* No way to reach here, but put an exit() here a) just to cover
       for bugs, and b) to let the compiler know we're honoring the
       __opal_attribute_noreturn__. */
    exit(-1);
}

static size_t handler = SIZE_MAX;
static bool debugger_register_active = true;
static bool debugger_event_active = true;

static void _release_fn(int status,
                        const opal_process_name_t *source,
                        opal_list_t *info, opal_list_t *results,
                        opal_pmix_notification_complete_fn_t cbfunc,
                        void *cbdata)
{
    /* must let the notifier know we are done */
    if (NULL != cbfunc) {
        cbfunc(ORTE_SUCCESS, NULL, NULL, NULL, cbdata);
    }
    debugger_event_active = false;
}

static void _register_fn(int status,
                         size_t evhandler_ref,
                         void *cbdata)
{
    opal_list_t *codes = (opal_list_t*)cbdata;

    handler = evhandler_ref;
    OPAL_LIST_RELEASE(codes);
    debugger_register_active = false;
}

/*
 * Wait for a debugger if asked.  We support two ways of waiting for
 * attaching debuggers -- see big comment in
 * orte/tools/orterun/debuggers.c explaining the two scenarios.
 */
void ompi_rte_wait_for_debugger(void)
{
    int debugger;
    opal_list_t *codes, directives;
    opal_value_t *kv;
    char *evar;
    int time;

    /* See lengthy comment in orte/tools/orterun/debuggers.c about
       orte_in_parallel_debugger */
    debugger = orte_in_parallel_debugger;

    if (1 == MPIR_being_debugged) {
        debugger = 1;
    }

    if (!debugger && NULL == getenv("ORTE_TEST_DEBUGGER_ATTACH")) {
        /* if not, just return */
        return;
    }

    /* if we are being debugged, then we need to find
     * the correct plug-ins
     */
    ompi_debugger_setup_dlls();

    if (NULL != (evar = getenv("ORTE_TEST_DEBUGGER_SLEEP"))) {
        time = strtol(evar, NULL, 10);
        sleep(time);
        return;
    }

    if (orte_standalone_operation) {
        /* spin until debugger attaches and releases us */
        while (MPIR_debug_gate == 0) {
#if defined(HAVE_USLEEP)
            usleep(100000); /* microseconds */
#else
            sleep(1);       /* seconds */
#endif
        }
    } else {

        /* register an event handler for the ORTE_ERR_DEBUGGER_RELEASE event */
        codes = OBJ_NEW(opal_list_t);
        kv = OBJ_NEW(opal_value_t);
        kv->key = strdup("errorcode");
        kv->type = OPAL_INT;
        kv->data.integer = ORTE_ERR_DEBUGGER_RELEASE;
        opal_list_append(codes, &kv->super);

        OBJ_CONSTRUCT(&directives, opal_list_t);
        kv = OBJ_NEW(opal_value_t);
        kv->key = strdup(OPAL_PMIX_EVENT_HDLR_NAME);
        kv->type = OPAL_STRING;
        kv->data.string = strdup("MPI-DEBUGGER-ATTACH");
        opal_list_append(&directives, &kv->super);

        opal_pmix.register_evhandler(codes, &directives, _release_fn, _register_fn, codes);
        /* let the MPI progress engine run while we wait for registration to complete */
        OMPI_WAIT_FOR_COMPLETION(debugger_register_active);
        OPAL_LIST_DESTRUCT(&directives);

        /* let the MPI progress engine run while we wait for debugger release */
        OMPI_WAIT_FOR_COMPLETION(debugger_event_active);

        /* deregister the event handler */
        opal_pmix.deregister_evhandler(handler, NULL, NULL);
    }
}

bool ompi_rte_connect_accept_support(const char *port)
{
    char *ptr, *tmp;
    orte_process_name_t name;

    /* were we launched by mpirun, or are we calling
     * without a defined port? */
    if (NULL == orte_process_info.my_hnp_uri ||
        NULL == port || 0 == strlen(port)) {
        return true;
    }

    /* is the job family in the port different than my own? */
    tmp = strdup(port);  // protect input
    if (NULL == (ptr = strchr(tmp, ':'))) {
        /* this port didn't come from us! */
        orte_show_help("help-orterun.txt", "orterun:malformedport", true);
        free(tmp);
        return false;
    }
    *ptr = '\0';
    if (ORTE_SUCCESS != orte_util_convert_string_to_process_name(&name, tmp)) {
        free(tmp);
        orte_show_help("help-orterun.txt", "orterun:malformedport", true);
        return false;
    }
    free(tmp);
    if (ORTE_JOB_FAMILY(ORTE_PROC_MY_NAME->jobid) == ORTE_JOB_FAMILY(name.jobid)) {
        /* same job family, so our infrastructure is adequate */
        return true;
    }

    /* if the job family of the port is different than our own
     * and we were launched by mpirun, then we require ompi-server
     * support */
    if (NULL == orte_data_server_uri) {
        /* print a pretty help message */
        orte_show_help("help-orterun.txt", "orterun:server-unavailable", true);
        return false;
    }

    return true;
}
