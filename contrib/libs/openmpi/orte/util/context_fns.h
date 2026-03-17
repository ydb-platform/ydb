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
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file:
 *
 */

#ifndef _ORTE_CONTEXT_FNS_H_
#define _ORTE_CONTEXT_FNS_H_

#include "orte_config.h"

#include "orte/runtime/orte_globals.h"

BEGIN_C_DECLS

ORTE_DECLSPEC int orte_util_check_context_app(orte_app_context_t *context,
                                              char **env);

ORTE_DECLSPEC int orte_util_check_context_cwd(orte_app_context_t *context,
                                              bool want_chdir);

END_C_DECLS
#endif
