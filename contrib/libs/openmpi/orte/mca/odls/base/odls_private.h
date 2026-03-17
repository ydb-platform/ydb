/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2011      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2016-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file:
 */

#ifndef MCA_ODLS_PRIVATE_H
#define MCA_ODLS_PRIVATE_H

/*
 * includes
 */
#include "orte_config.h"
#include "orte/types.h"

#include "opal/class/opal_list.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/class/opal_bitmap.h"
#include "opal/dss/dss_types.h"

#include "orte/mca/iof/base/iof_base_setup.h"
#include "orte/mca/rml/rml_types.h"
#include "orte/runtime/orte_globals.h"
#include "orte/util/threads.h"
#include "orte/mca/odls/odls_types.h"

BEGIN_C_DECLS

/*
 * General ODLS types
 */

typedef struct {
    /** Verbose/debug output stream */
    int output;
    /** Time to allow process to forcibly die */
    int timeout_before_sigkill;
    /* list of ranks to be displayed on separate xterms */
    opal_list_t xterm_ranks;
    /* the xterm cmd to be used */
    char **xtermcmd;
    /* thread pool */
    int max_threads;
    int num_threads;
    int cutoff;
    opal_event_base_t **ev_bases;   // event base array for progress threads
    char** ev_threads;              // event progress thread names
    int next_base;                  // counter to load-level thread use
    bool signal_direct_children_only;
    orte_lock_t lock;
} orte_odls_globals_t;

ORTE_DECLSPEC extern orte_odls_globals_t orte_odls_globals;

/*
 * Default functions that are common to most environments - can
 * be overridden by specific environments if they need something
 * different (e.g., bproc)
 */
ORTE_DECLSPEC int
orte_odls_base_default_get_add_procs_data(opal_buffer_t *data,
                                          orte_jobid_t job);

ORTE_DECLSPEC int
orte_odls_base_default_construct_child_list(opal_buffer_t *data,
                                            orte_jobid_t *job);

ORTE_DECLSPEC void orte_odls_base_spawn_proc(int fd, short sd, void *cbdata);

/* define a function that will fork a local proc */
typedef int (*orte_odls_base_fork_local_proc_fn_t)(void *cd);

/* define an object for fork/exec the local proc */
typedef struct {
    opal_object_t super;
    opal_event_t ev;
    char *cmd;
    char *wdir;
    char **argv;
    char **env;
    orte_job_t *jdata;
    orte_app_context_t *app;
    orte_proc_t *child;
    bool index_argv;
    orte_iof_base_io_conf_t opts;
    orte_odls_base_fork_local_proc_fn_t fork_local;
} orte_odls_spawn_caddy_t;
OBJ_CLASS_DECLARATION(orte_odls_spawn_caddy_t);

/* define an object for starting local launch */
typedef struct {
    opal_object_t object;
    opal_event_t *ev;
    orte_jobid_t job;
    orte_odls_base_fork_local_proc_fn_t fork_local;
    int retries;
} orte_odls_launch_local_t;
OBJ_CLASS_DECLARATION(orte_odls_launch_local_t);

#define ORTE_ACTIVATE_LOCAL_LAUNCH(j, f)                                \
    do {                                                                \
        orte_odls_launch_local_t *ll;                                   \
        ll = OBJ_NEW(orte_odls_launch_local_t);                         \
        ll->job = (j);                                                  \
        ll->fork_local = (f);                                           \
        opal_event_set(orte_event_base, ll->ev, -1, OPAL_EV_WRITE,      \
                       orte_odls_base_default_launch_local, ll);        \
        opal_event_set_priority(ll->ev, ORTE_SYS_PRI);                  \
        opal_event_active(ll->ev, OPAL_EV_WRITE, 1);                    \
    } while(0);

ORTE_DECLSPEC void orte_odls_base_default_launch_local(int fd, short sd, void *cbdata);

ORTE_DECLSPEC void orte_odls_base_default_wait_local_proc(int fd, short sd, void *cbdata);

/* define a function type to signal a local proc */
typedef int (*orte_odls_base_signal_local_fn_t)(pid_t pid, int signum);

ORTE_DECLSPEC int
orte_odls_base_default_signal_local_procs(const orte_process_name_t *proc, int32_t signal,
                                          orte_odls_base_signal_local_fn_t signal_local);

/* define a function type for killing a local proc */
typedef int (*orte_odls_base_kill_local_fn_t)(pid_t pid, int signum);

/* define a function type to detect that a child died */
typedef bool (*orte_odls_base_child_died_fn_t)(orte_proc_t *child);

ORTE_DECLSPEC int
orte_odls_base_default_kill_local_procs(opal_pointer_array_t *procs,
                                        orte_odls_base_kill_local_fn_t kill_local);

ORTE_DECLSPEC int orte_odls_base_default_restart_proc(orte_proc_t *child,
                                                      orte_odls_base_fork_local_proc_fn_t fork_local);

/*
 * Preload binary/files functions
 */
ORTE_DECLSPEC int orte_odls_base_preload_files_app_context(orte_app_context_t* context);

/*
 * Obtain process stats on a child proc
 */
ORTE_DECLSPEC int orte_odls_base_get_proc_stats(opal_buffer_t *answer, orte_process_name_t *proc);

END_C_DECLS

#endif
