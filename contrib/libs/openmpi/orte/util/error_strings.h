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
 * Copyright (c) 2008      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, LLC. All rights reserved
 * Copyright (c) 2014      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file:
 *
 */

#ifndef _ORTE_ERROR_STRINGS_H_
#define _ORTE_ERROR_STRINGS_H_

#include "orte_config.h"

#include "orte/runtime/orte_globals.h"
#include "orte/mca/plm/plm_types.h"

BEGIN_C_DECLS

ORTE_DECLSPEC int orte_err2str(int errnum, const char **errmsg);


ORTE_DECLSPEC const char *orte_job_state_to_str(orte_job_state_t state);

ORTE_DECLSPEC const char *orte_app_ctx_state_to_str(orte_app_state_t state);

ORTE_DECLSPEC const char *orte_proc_state_to_str(orte_proc_state_t state);

ORTE_DECLSPEC const char *orte_node_state_to_str(orte_node_state_t state);

END_C_DECLS
#endif
