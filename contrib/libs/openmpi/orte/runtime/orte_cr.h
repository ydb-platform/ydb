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
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * Checkpoint/Restart Functionality for the ORTE layer
 */

#ifndef ORTE_CR_H
#define ORTE_CR_H

#include "orte_config.h"

BEGIN_C_DECLS

    /*
     * Initialization called in orte_init()
     */
    ORTE_DECLSPEC int orte_cr_init(void);

    /*
     * Finalization called in orte_finalize()
     */
    ORTE_DECLSPEC int orte_cr_finalize(void);

    /*
     * Interlayer Coodination Callback
     */
    ORTE_DECLSPEC int orte_cr_coord(int state);

    /*
     * Init/Finalize functions for ORTE Entry Point
     */
    ORTE_DECLSPEC int orte_cr_entry_point_init(void);
    ORTE_DECLSPEC int orte_cr_entry_point_finalize(void);

    ORTE_DECLSPEC extern bool orte_cr_flush_restart_files;

END_C_DECLS

#endif /* ORTE_CR_H */
