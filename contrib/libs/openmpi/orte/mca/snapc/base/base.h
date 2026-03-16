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
 * Copyright (c) 2007      Evergrid, Inc. All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC.  All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#ifndef ORTE_SNAPC_BASE_H
#define ORTE_SNAPC_BASE_H

#include "orte_config.h"
#include "orte/types.h"


#include "orte/mca/snapc/snapc.h"

/*
 * Global functions for MCA overall SNAPC
 */

BEGIN_C_DECLS

/*
 * MCA Framework
 */
ORTE_DECLSPEC extern mca_base_framework_t orte_snapc_base_framework;
/* select a component */
ORTE_DECLSPEC int orte_snapc_base_select(bool seed, bool app);

/*
 * Commands for command line tool and SnapC interaction
 */
typedef uint8_t orte_snapc_cmd_flag_t;
#define ORTE_SNAPC_CMD  OPAL_UINT8
#define ORTE_SNAPC_GLOBAL_INIT_CMD    1
#define ORTE_SNAPC_GLOBAL_TERM_CMD    2
#define ORTE_SNAPC_GLOBAL_UPDATE_CMD  3
#define ORTE_SNAPC_LOCAL_UPDATE_CMD   4
#define ORTE_SNAPC_LOCAL_FINISH_CMD   5

/**
 * There are 3 types of Coordinators, and any process may be once or more type.
 * e.g., orterun is can often be both a Global and Local coordinator if it
 *       launches processes locally.
 */
typedef uint32_t orte_snapc_coord_type_t;
#define ORTE_SNAPC_UNASSIGN_TYPE     0
#define ORTE_SNAPC_GLOBAL_COORD_TYPE 1
#define ORTE_SNAPC_LOCAL_COORD_TYPE  2
#define ORTE_SNAPC_APP_COORD_TYPE    4
ORTE_DECLSPEC extern orte_snapc_coord_type_t orte_snapc_coord_type;

#define ORTE_SNAPC_COORD_NAME_PRINT(ct) ( (ct == (ORTE_SNAPC_GLOBAL_COORD_TYPE | ORTE_SNAPC_LOCAL_COORD_TYPE) ) ? "Global-Local" : \
                                          (ct ==  ORTE_SNAPC_GLOBAL_COORD_TYPE) ? "Global" : \
                                          (ct ==  ORTE_SNAPC_LOCAL_COORD_TYPE)  ? "Local"  : \
                                          (ct ==  ORTE_SNAPC_APP_COORD_TYPE)    ? "App"    : \
                                          "Unknown")

/**
 * Global Snapshot Object Maintenance functions
 */
void orte_snapc_base_local_snapshot_construct(orte_snapc_base_local_snapshot_t *obj);
void orte_snapc_base_local_snapshot_destruct( orte_snapc_base_local_snapshot_t *obj);

void orte_snapc_base_global_snapshot_construct(orte_snapc_base_global_snapshot_t *obj);
void orte_snapc_base_global_snapshot_destruct( orte_snapc_base_global_snapshot_t *obj);

void orte_snapc_base_quiesce_construct(orte_snapc_base_quiesce_t *obj);
void orte_snapc_base_quiesce_destruct( orte_snapc_base_quiesce_t *obj);

void orte_snapc_base_request_op_construct(orte_snapc_base_request_op_t *op);
void orte_snapc_base_request_op_destruct(orte_snapc_base_request_op_t *op);

/**
 * 'None' component functions
 * These are to be used when no component is selected.
 * They just return success, and empty strings as necessary.
 */
ORTE_DECLSPEC int orte_snapc_base_none_open(void);
ORTE_DECLSPEC int orte_snapc_base_none_close(void);
ORTE_DECLSPEC int orte_snapc_base_none_query(mca_base_module_t **module, int *priority);

ORTE_DECLSPEC int orte_snapc_base_module_init(bool seed, bool app);
ORTE_DECLSPEC int orte_snapc_base_module_finalize(void);
ORTE_DECLSPEC int orte_snapc_base_none_setup_job(orte_jobid_t jobid);
ORTE_DECLSPEC int orte_snapc_base_none_release_job(orte_jobid_t jobid);
ORTE_DECLSPEC int orte_snapc_base_none_ft_event(int state);
ORTE_DECLSPEC int orte_snapc_base_none_start_ckpt(orte_snapc_base_quiesce_t *datum);
ORTE_DECLSPEC int orte_snapc_base_none_end_ckpt(orte_snapc_base_quiesce_t *datum);

ORTE_DECLSPEC extern orte_snapc_base_module_t orte_snapc;

/**
 * Globals
 */
ORTE_DECLSPEC extern bool   orte_snapc_base_store_only_one_seq;
ORTE_DECLSPEC extern size_t orte_snapc_base_snapshot_seq_number;
ORTE_DECLSPEC extern bool   orte_snapc_base_has_recovered;

/**
 * Some utility functions
 */
ORTE_DECLSPEC void orte_snapc_ckpt_state_notify(int state);
ORTE_DECLSPEC int orte_snapc_ckpt_state_str(char ** state_str, int state);

/*******************************
 * Global Coordinator functions
     *******************************/
/* Initial handshake with the orte_checkpoint command */
ORTE_DECLSPEC int orte_snapc_base_global_coord_ckpt_init_cmd(orte_process_name_t* peer,
                                                             opal_buffer_t* buffer,
                                                             opal_crs_base_ckpt_options_t *options,
                                                             orte_jobid_t *jobid);
ORTE_DECLSPEC int orte_snapc_base_global_coord_ckpt_update_cmd(orte_process_name_t* peer,
                                                               orte_sstore_base_handle_t handle,
                                                               int ckpt_status);

ORTE_DECLSPEC int orte_snapc_base_unpack_options(opal_buffer_t* buffer,
                                                 opal_crs_base_ckpt_options_t *options);
ORTE_DECLSPEC int orte_snapc_base_pack_options(opal_buffer_t* buffer,
                                               opal_crs_base_ckpt_options_t *options);

END_C_DECLS

#endif /* ORTE_SNAPC_BASE_H */
