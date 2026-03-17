/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2006 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2007 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2007-2012 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2008-2009 Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2011      IBM Corporation.  All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * These symbols are in a file by themselves to provide nice linker
 * semantics.  Since linkers generally pull in symbols by object
 * files, keeping these symbols as the only symbols in this file
 * prevents utility programs such as "ompi_info" from having to import
 * entire components just to query their version and parameters.
 */

#include "orte_config.h"
#include "orte/constants.h"

#include "opal/util/output.h"
#include "opal/mca/event/event.h"

#include "orte/util/show_help.h"
#include "orte/runtime/orte_globals.h"
#include "orte/util/name_fns.h"
#include "orte/util/proc_info.h"
#include "orte/util/threads.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/state/state.h"

#include "orte/mca/plm/plm.h"
#include "orte/mca/plm/base/base.h"
#include "orte/mca/plm/base/plm_private.h"
#include "orte/mca/plm/isolated/plm_isolated.h"

static int isolated_init(void);
static int isolated_launch(orte_job_t *jdata);
static int remote_spawn(void);
static int isolated_terminate_orteds(void);
static int isolated_finalize(void);

orte_plm_base_module_t orte_plm_isolated_module = {
    isolated_init,
    orte_plm_base_set_hnp_name,
    isolated_launch,
    remote_spawn,
    orte_plm_base_orted_terminate_job,
    isolated_terminate_orteds,
    orte_plm_base_orted_kill_local_procs,
    orte_plm_base_orted_signal_local_procs,
    isolated_finalize
};

static void launch_daemons(int fd, short args, void *cbdata);

/**
 * Init the module
 */
static int isolated_init(void)
{
    int rc;

    /* point to our launch command */
    if (ORTE_SUCCESS != (rc = orte_state.add_job_state(ORTE_JOB_STATE_LAUNCH_DAEMONS,
                                                       launch_daemons, ORTE_SYS_PRI))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }

    /* start the recvs */
    if (ORTE_SUCCESS != (rc = orte_plm_base_comm_start())) {
        ORTE_ERROR_LOG(rc);
    }

    return rc;
}

/*
 * launch a set of daemons from a remote daemon
 */
static int remote_spawn(void)
{
    /* unused function in this mode */
    return ORTE_SUCCESS;
}

static int isolated_launch(orte_job_t *jdata)
{
    if (ORTE_FLAG_TEST(jdata, ORTE_JOB_FLAG_RESTART)) {
        /* this is a restart situation - skip to the mapping stage */
        ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_MAP);
    } else {
        /* new job - set it up */
        ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_INIT);
    }
    return ORTE_SUCCESS;
}

static void launch_daemons(int fd, short args, void *cbdata)
{
    orte_state_caddy_t *state = (orte_state_caddy_t*)cbdata;

    ORTE_ACQUIRE_OBJECT(state);

    /* there are no daemons to launch, so just trigger the
     * daemon-launch-complete state
     */
    ORTE_ACTIVATE_JOB_STATE(state->jdata, ORTE_JOB_STATE_DAEMONS_REPORTED);
    OBJ_RELEASE(state);
}

static int isolated_terminate_orteds(void)
{
    int rc;

    /* send ourselves the halt command */
    if (ORTE_SUCCESS != (rc = orte_plm_base_orted_exit(ORTE_DAEMON_EXIT_CMD))) {
        ORTE_ERROR_LOG(rc);
    }
    return rc;
}

static int isolated_finalize(void)
{
    int rc;

    /* cleanup any pending recvs */
    if (ORTE_SUCCESS != (rc = orte_plm_base_comm_stop())) {
        ORTE_ERROR_LOG(rc);
    }

    return rc;
}
