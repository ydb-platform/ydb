/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2017-2018 Intel, Inc. All rights reserved.
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

#ifndef MCA_PLM_PRIVATE_H
#define MCA_PLM_PRIVATE_H

/*
 * includes
 */
#include "orte_config.h"
#include "orte/types.h"

#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif  /* HAVE_SYS_TIME_H */

#include "opal/class/opal_list.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/dss/dss_types.h"

#include "opal/dss/dss_types.h"
#include "orte/mca/plm/plm_types.h"
#include "orte/mca/rml/rml_types.h"
#include "orte/mca/odls/odls_types.h"
#include "orte/runtime/orte_globals.h"


BEGIN_C_DECLS

ORTE_DECLSPEC extern mca_base_framework_t orte_plm_base_framework;

/* globals for use solely within PLM framework */
typedef struct {
    /* next jobid */
    uint16_t next_jobid;
    /* time when daemons started launch */
    struct timeval daemonlaunchstart;
    /* tree spawn cmd */
    opal_buffer_t tree_spawn_cmd;
    /* daemon nodes assigned at launch */
    bool daemon_nodes_assigned_at_launch;
    size_t node_regex_threshold;
} orte_plm_globals_t;
/**
 * Global instance of PLM framework data
 */
ORTE_DECLSPEC extern orte_plm_globals_t orte_plm_globals;


/**
 * Utility routine to set progress engine schedule
 */
ORTE_DECLSPEC int orte_plm_base_set_progress_sched(int sched);

/*
 * Launch support
 */
ORTE_DECLSPEC void orte_plm_base_daemon_callback(int status, orte_process_name_t* sender,
                                                 opal_buffer_t *buffer,
                                                 orte_rml_tag_t tag, void *cbdata);
ORTE_DECLSPEC void orte_plm_base_daemon_failed(int status, orte_process_name_t* sender,
                                               opal_buffer_t *buffer,
                                               orte_rml_tag_t tag, void *cbdata);
ORTE_DECLSPEC void orte_plm_base_daemon_topology(int status, orte_process_name_t* sender,
                                                 opal_buffer_t *buffer,
                                                 orte_rml_tag_t tag, void *cbdata);

ORTE_DECLSPEC int orte_plm_base_create_jobid(orte_job_t *jdata);
ORTE_DECLSPEC int orte_plm_base_set_hnp_name(void);
ORTE_DECLSPEC void orte_plm_base_reset_job(orte_job_t *jdata);
ORTE_DECLSPEC int orte_plm_base_setup_orted_cmd(int *argc, char ***argv);
ORTE_DECLSPEC void orte_plm_base_check_all_complete(int fd, short args, void *cbdata);
ORTE_DECLSPEC int orte_plm_base_setup_virtual_machine(orte_job_t *jdata);

/**
 * Utilities for plm components that use proxy daemons
 */
ORTE_DECLSPEC int orte_plm_base_orted_exit(orte_daemon_cmd_flag_t command);
ORTE_DECLSPEC int orte_plm_base_orted_terminate_job(orte_jobid_t jobid);
ORTE_DECLSPEC int orte_plm_base_orted_kill_local_procs(opal_pointer_array_t *procs);
ORTE_DECLSPEC int orte_plm_base_orted_signal_local_procs(orte_jobid_t job, int32_t signal);

/*
 * communications utilities
 */
ORTE_DECLSPEC int orte_plm_base_comm_start(void);
ORTE_DECLSPEC int orte_plm_base_comm_stop(void);
ORTE_DECLSPEC void orte_plm_base_recv(int status, orte_process_name_t* sender,
                                      opal_buffer_t* buffer, orte_rml_tag_t tag,
                                      void* cbdata);


/**
 * Construct basic ORTE Daemon command line arguments
 */
ORTE_DECLSPEC int orte_plm_base_orted_append_basic_args(int *argc, char ***argv,
                                                        char *ess_module,
                                                        int *proc_vpid_index);

/*
 * Proxy functions for use by daemons and application procs
 * needing dynamic operations
 */
ORTE_DECLSPEC int orte_plm_proxy_init(void);
ORTE_DECLSPEC int orte_plm_proxy_spawn(orte_job_t *jdata);
ORTE_DECLSPEC int orte_plm_proxy_finalize(void);

END_C_DECLS

#endif  /* MCA_PLS_PRIVATE_H */
