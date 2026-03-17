/*
 * Copyright (c) 2010      Cisco Systems, Inc. All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2016      Intel, Inc. All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 */

#ifndef ORTE_QUIT_H
#define ORTE_QUIT_H

#include "orte_config.h"

#include "orte/runtime/orte_globals.h"

BEGIN_C_DECLS

ORTE_DECLSPEC void orte_quit(int fd, short args, void *cbdata);

ORTE_DECLSPEC int orte_print_aborted_job(orte_job_t *job,
                                         orte_app_context_t *approc,
                                         orte_proc_t *proc,
                                         orte_node_t *node);

END_C_DECLS

#endif /* ORTE_CR_H */
