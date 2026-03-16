/*
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2017      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file:
 */

#ifndef ORTE_MCA_STATE_PRIVATE_H
#define ORTE_MCA_STATE_PRIVATE_H

/*
 * includes
 */
#include "orte_config.h"
#include "orte/constants.h"
#include "orte/types.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */

#include "orte/mca/plm/plm_types.h"
#include "orte/runtime/orte_globals.h"

#include "orte/mca/state/state.h"

BEGIN_C_DECLS

extern bool orte_state_base_run_fdcheck;
/*
 * Base functions
 */
ORTE_DECLSPEC void orte_state_base_activate_job_state(orte_job_t *jdata,
                                                      orte_job_state_t state);

ORTE_DECLSPEC int orte_state_base_add_job_state(orte_job_state_t state,
                                                orte_state_cbfunc_t cbfunc,
                                                int priority);

ORTE_DECLSPEC int orte_state_base_set_job_state_callback(orte_job_state_t state,
                                                         orte_state_cbfunc_t cbfunc);

ORTE_DECLSPEC int orte_state_base_set_job_state_priority(orte_job_state_t state,
                                                         int priority);

ORTE_DECLSPEC int orte_state_base_remove_job_state(orte_job_state_t state);

ORTE_DECLSPEC void orte_util_print_job_state_machine(void);


ORTE_DECLSPEC void orte_state_base_activate_proc_state(orte_process_name_t *proc,
                                                       orte_proc_state_t state);

ORTE_DECLSPEC int orte_state_base_add_proc_state(orte_proc_state_t state,
                                                 orte_state_cbfunc_t cbfunc,
                                                 int priority);

ORTE_DECLSPEC int orte_state_base_set_proc_state_callback(orte_proc_state_t state,
                                                          orte_state_cbfunc_t cbfunc);

ORTE_DECLSPEC int orte_state_base_set_proc_state_priority(orte_proc_state_t state,
                                                          int priority);

ORTE_DECLSPEC int orte_state_base_remove_proc_state(orte_proc_state_t state);

ORTE_DECLSPEC void orte_util_print_proc_state_machine(void);

/* common state processing functions */
ORTE_DECLSPEC void orte_state_base_local_launch_complete(int fd, short argc, void *cbdata);
ORTE_DECLSPEC void orte_state_base_cleanup_job(int fd, short argc, void *cbdata);
ORTE_DECLSPEC void orte_state_base_report_progress(int fd, short argc, void *cbdata);
ORTE_DECLSPEC void orte_state_base_track_procs(int fd, short argc, void *cbdata);
ORTE_DECLSPEC void orte_state_base_check_all_complete(int fd, short args, void *cbdata);
ORTE_DECLSPEC void orte_state_base_check_fds(orte_job_t *jdata);
ORTE_DECLSPEC void orte_state_base_notify_data_server(orte_process_name_t *target);

END_C_DECLS
#endif
