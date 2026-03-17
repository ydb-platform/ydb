/*
 * Copyright (c) 2004-2009 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos National Security, LLC.
 *                         All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#ifndef ORTE_FILEM_BASE_H
#define ORTE_FILEM_BASE_H

#include "orte_config.h"

#include "orte/mca/rml/rml.h"

#include "orte/mca/filem/filem.h"

BEGIN_C_DECLS

/*
 * MCA framework
 */
ORTE_DECLSPEC extern mca_base_framework_t orte_filem_base_framework;
/*
 * Select an available component.
 */
ORTE_DECLSPEC int orte_filem_base_select(void);

/*
 * cmds for base receive
 */
typedef uint8_t orte_filem_cmd_flag_t;
#define ORTE_FILEM_CMD  OPAL_UINT8
#define ORTE_FILEM_GET_PROC_NODE_NAME_CMD  1
#define ORTE_FILEM_GET_REMOTE_PATH_CMD     2

/**
 * Globals
 */
ORTE_DECLSPEC extern orte_filem_base_module_t orte_filem;
ORTE_DECLSPEC extern bool orte_filem_base_is_active;

/**
 * 'None' component functions
 * These are to be used when no component is selected.
 * They just return success, and empty strings as necessary.
 */
int orte_filem_base_module_init(void);
int orte_filem_base_module_finalize(void);

int orte_filem_base_none_put(orte_filem_base_request_t *request);
int orte_filem_base_none_put_nb(orte_filem_base_request_t *request);
int orte_filem_base_none_get(orte_filem_base_request_t *request);
int orte_filem_base_none_get_nb(orte_filem_base_request_t *request);
int orte_filem_base_none_rm( orte_filem_base_request_t *request);
int orte_filem_base_none_rm_nb( orte_filem_base_request_t *request);
int orte_filem_base_none_wait( orte_filem_base_request_t *request);
int orte_filem_base_none_wait_all( opal_list_t *request_list);
int orte_filem_base_none_preposition_files(orte_job_t *jdata,
                                           orte_filem_completion_cbfunc_t cbfunc,
                                           void *cbdata);
int orte_filem_base_none_link_local_files(orte_job_t *jdata,
                                          orte_app_context_t *app);

/**
 * Some utility functions
 */
/* base comm functions */
ORTE_DECLSPEC int orte_filem_base_comm_start(void);
ORTE_DECLSPEC int orte_filem_base_comm_stop(void);
ORTE_DECLSPEC void orte_filem_base_recv(int status, orte_process_name_t* sender,
                                        opal_buffer_t* buffer, orte_rml_tag_t tag,
                                        void* cbdata);


END_C_DECLS

#endif /* ORTE_FILEM_BASE_H */
