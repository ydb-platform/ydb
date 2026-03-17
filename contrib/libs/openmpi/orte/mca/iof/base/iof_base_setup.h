/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2016-2017 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#ifndef IOF_BASE_SETUP_H_
#define IOF_BASE_SETUP_H_

#include "orte_config.h"
#include "orte/types.h"

#include "orte/mca/iof/base/base.h"

struct orte_iof_base_io_conf_t {
    int usepty;
    bool connect_stdin;

    /* private - callers should not modify these fields */
    int p_stdin[2];
    int p_stdout[2];
    int p_stderr[2];
#if OPAL_PMIX_V1
    int p_internal[2];
#endif
};
typedef struct orte_iof_base_io_conf_t orte_iof_base_io_conf_t;


/**
 * Do pre-fork IOF setup tasks
 *
 * Do all stdio forwarding that must be done before fork() is called.
 * This might include creating pipes or ptys or similar work.
 */
ORTE_DECLSPEC int orte_iof_base_setup_prefork(orte_iof_base_io_conf_t *opts);

ORTE_DECLSPEC int orte_iof_base_setup_child(orte_iof_base_io_conf_t *opts,
                                            char ***env);

ORTE_DECLSPEC int orte_iof_base_setup_parent(const orte_process_name_t* name,
                                             orte_iof_base_io_conf_t *opts);

/* setup output files */
ORTE_DECLSPEC int orte_iof_base_setup_output_files(const orte_process_name_t* dst_name,
                                                   orte_job_t *jobdat,
                                                   orte_iof_proc_t *proct);

#endif
