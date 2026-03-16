/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef ORTE_ESS_TOOL_H
#define ORTE_ESS_TOOL_H

BEGIN_C_DECLS

/*
 * Module open / close
 */
int orte_ess_tool_component_open(void);
int orte_ess_tool_component_close(void);
int orte_ess_tool_component_query(mca_base_module_t **module, int *priority);

typedef struct {
    orte_ess_base_component_t super;
    bool async;
    bool system_server_first;
    bool system_server_only;
    bool do_not_connect;
    int wait_to_connect;
    int num_retries;
    int pid;
} orte_ess_tool_component_t;

ORTE_MODULE_DECLSPEC extern orte_ess_tool_component_t mca_ess_tool_component;

END_C_DECLS

#endif /* ORTE_ESS_TOOL_H */
