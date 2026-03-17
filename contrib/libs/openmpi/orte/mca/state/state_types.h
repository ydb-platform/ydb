/*
 * Copyright (c) 2011      Los Alamos National Security, LLC.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/****    ORTE STATE MACHINE    ****/

/* States are treated as events so that the event
 * library can sequence them. Each state consists
 * of an event, a job or process state, a pointer
 * to the respective object, and a callback function
 * to be executed for that state. Events can be defined
 * at different priorities - e.g., SYS priority for
 * events associated with launching jobs, and ERR priority
 * for events associated with abnormal termination of
 * a process.
 *
 * The state machine consists of a list of state objects,
 * each defining a state-cbfunc pair. At startup, a default
 * list is created by the base functions which is then
 * potentially customized by selected components within
 * the various ORTE frameworks. For example, a PLM component
 * may need to insert states in the launch procedure, or may
 * want to redirect a particular state callback to a custom
 * function.
 *
 * For convenience, an ANY state can be defined along with a generic
 * callback function, with the corresponding state object
 * placed at the end of the state machine. Setting the
 * machine to a state that has not been explicitly defined
 * will cause this default action to be executed. Thus, you
 * don't have to explicitly define a state-cbfunc pair
 * for every job or process state.
 */

#ifndef _ORTE_STATE_TYPES_H_
#define _ORTE_STATE_TYPES_H_

#include "orte_config.h"

#include "opal/class/opal_list.h"
#include "opal/mca/event/event.h"

#include "orte/mca/plm/plm_types.h"
#include "orte/runtime/orte_globals.h"

BEGIN_C_DECLS

typedef void (*orte_state_cbfunc_t)(int fd, short args, void* cb);

typedef struct {
    opal_list_item_t super;
    orte_job_state_t job_state;
    orte_proc_state_t proc_state;
    orte_state_cbfunc_t cbfunc;
    int priority;
} orte_state_t;
ORTE_DECLSPEC OBJ_CLASS_DECLARATION(orte_state_t);

/* caddy for passing job and proc data to state event handlers */
typedef struct {
    opal_object_t super;
    opal_event_t ev;
    orte_job_t *jdata;
    orte_job_state_t job_state;
    orte_process_name_t name;
    orte_proc_state_t proc_state;
} orte_state_caddy_t;
ORTE_DECLSPEC OBJ_CLASS_DECLARATION(orte_state_caddy_t);

END_C_DECLS
#endif
