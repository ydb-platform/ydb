/*
 * Copyright (c) 2011      Los Alamos National Security, LLC.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"

#include <sys/types.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */
#include <string.h>

#include "opal/util/output.h"

#include "orte/runtime/orte_quit.h"
#include "orte/mca/errmgr/errmgr.h"

#include "orte/mca/state/state.h"
#include "orte/mca/state/base/state_private.h"
#include "state_app.h"

/*
 * Module functions: Global
 */
static int init(void);
static int finalize(void);

/******************
 * APP module - just uses base functions after
 * initializing the proc state machine. Job state
 * machine is unused by application procs at this
 * time.
 ******************/
orte_state_base_module_t orte_state_app_module = {
    init,
    finalize,
    orte_state_base_activate_job_state,
    orte_state_base_add_job_state,
    orte_state_base_set_job_state_callback,
    orte_state_base_set_job_state_priority,
    orte_state_base_remove_job_state,
    orte_state_base_activate_proc_state,
    orte_state_base_add_proc_state,
    orte_state_base_set_proc_state_callback,
    orte_state_base_set_proc_state_priority,
    orte_state_base_remove_proc_state
};

static void force_quit(int fd, short args, void *cbdata)
{
    /* dont attempt to finalize as it could throw
     * us into an infinite loop on errors
     */
    exit(orte_exit_status);
}

/************************
 * API Definitions
 ************************/
static int init(void)
{
    int rc;

    OBJ_CONSTRUCT(&orte_job_states, opal_list_t);
    OBJ_CONSTRUCT(&orte_proc_states, opal_list_t);

    /* add a default error response */
    if (ORTE_SUCCESS != (rc = orte_state.add_job_state(ORTE_JOB_STATE_FORCED_EXIT,
                                                       force_quit, ORTE_ERROR_PRI))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }

    return ORTE_SUCCESS;
}

static int finalize(void)
{
    opal_list_item_t *item;

    /* cleanup the state machines */
    while (NULL != (item = opal_list_remove_first(&orte_proc_states))) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&orte_proc_states);

    while (NULL != (item = opal_list_remove_first(&orte_job_states))) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&orte_job_states);

    return ORTE_SUCCESS;
}
