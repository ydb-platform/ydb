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
 * Checkpoint/Restart Functionality for the OMPI layer
 */

#ifndef OMPI_CR_H
#define OMPI_CR_H

#include "ompi_config.h"
#if OPAL_ENABLE_FT_CR == 1
#include "orte/runtime/orte_cr.h"
#endif

BEGIN_C_DECLS

    /*
     * Initialization called in ompi_init()
     */
    OMPI_DECLSPEC int ompi_cr_init(void);

    /*
     * Finalization called in ompi_finalize()
     */
    OMPI_DECLSPEC int ompi_cr_finalize(void);

    /*
     * Interlayer Coodination Callback
     */
    OMPI_DECLSPEC int ompi_cr_coord(int state);

    /*
     * A general output handle to use for FT related messages
     */
    OMPI_DECLSPEC extern int ompi_cr_output;

#if OPAL_ENABLE_CRDEBUG == 1
    OMPI_DECLSPEC extern int    MPIR_checkpointable;
    OMPI_DECLSPEC extern char * MPIR_controller_hostname;
    OMPI_DECLSPEC extern char * MPIR_checkpoint_command;
    OMPI_DECLSPEC extern char * MPIR_restart_command;
    OMPI_DECLSPEC extern char * MPIR_checkpoint_listing_command;
#endif

END_C_DECLS

#endif /* OMPI_CR_H */
