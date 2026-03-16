/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2011-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC.  All rights reserved.
 * Copyright (c) 2013-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file:
 */

#ifndef MCA_ESS_BASE_H
#define MCA_ESS_BASE_H

#include "orte_config.h"
#include "orte/types.h"

#include "orte/mca/mca.h"
#include "opal/dss/dss_types.h"

#include "orte/mca/ess/ess.h"

BEGIN_C_DECLS

/*
 * MCA Framework
 */
ORTE_DECLSPEC extern mca_base_framework_t orte_ess_base_framework;
/**
 * Select a ess module
 */
ORTE_DECLSPEC int orte_ess_base_select(void);

/*
 * stdout/stderr buffering control parameter
 */
ORTE_DECLSPEC extern int orte_ess_base_std_buffering;

ORTE_DECLSPEC extern int orte_ess_base_num_procs;
ORTE_DECLSPEC extern char *orte_ess_base_jobid;
ORTE_DECLSPEC extern char *orte_ess_base_vpid;
ORTE_DECLSPEC extern opal_list_t orte_ess_base_signals;

/*
 * Internal helper functions used by components
 */
ORTE_DECLSPEC int orte_ess_env_get(void);

ORTE_DECLSPEC int orte_ess_base_std_prolog(void);

ORTE_DECLSPEC int orte_ess_base_app_setup(bool db_restrict_local);
ORTE_DECLSPEC int orte_ess_base_app_finalize(void);
ORTE_DECLSPEC void orte_ess_base_app_abort(int status, bool report);

ORTE_DECLSPEC int orte_ess_base_tool_setup(opal_list_t *flags);
ORTE_DECLSPEC int orte_ess_base_tool_finalize(void);

ORTE_DECLSPEC int orte_ess_base_orted_setup(void);
ORTE_DECLSPEC int orte_ess_base_orted_finalize(void);

/* Detect whether or not this proc is bound - if not,
 * see if it should bind itself
 */
ORTE_DECLSPEC int orte_ess_base_proc_binding(void);

/*
 * Put functions
 */
ORTE_DECLSPEC int orte_ess_env_put(orte_std_cntr_t num_procs,
                                   orte_std_cntr_t num_local_procs,
                                   char ***env);

typedef struct {
    opal_list_item_t super;
    char *signame;
    int signal;
} orte_ess_base_signal_t;
OBJ_CLASS_DECLARATION(orte_ess_base_signal_t);

END_C_DECLS

#endif
