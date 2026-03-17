/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2010-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "opal/mca/event/event.h"
#include "opal/mca/btl/base/base.h"
#include "ompi/mca/bml/bml.h"
#include "bml_r2.h"
#include "mpi.h"

static int mca_bml_r2_component_register(void);

mca_bml_base_component_2_0_0_t mca_bml_r2_component = {

    /* First, the mca_base_component_t struct containing meta
       information about the component itself */

    .bml_version = {
        MCA_BML_BASE_VERSION_2_0_0,

        .mca_component_name = "r2",
        MCA_BASE_MAKE_VERSION(component, OMPI_MAJOR_VERSION, OMPI_MINOR_VERSION,
                              OMPI_RELEASE_VERSION),
        .mca_open_component = mca_bml_r2_component_open,
        .mca_close_component = mca_bml_r2_component_close,
        .mca_register_component_params = mca_bml_r2_component_register,
    },
    .bml_data = {
        /* The component is checkpoint ready */
        MCA_BASE_METADATA_PARAM_CHECKPOINT
    },
    .bml_init = mca_bml_r2_component_init,
};

static int mca_bml_r2_component_register(void)
{
    mca_bml_r2.show_unreach_errors = true;
    (void) mca_base_component_var_register(&mca_bml_r2_component.bml_version,
                                           "show_unreach_errors",
                                           "Show error message when procs are unreachable",
                                           MCA_BASE_VAR_TYPE_BOOL, NULL, 0,0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &mca_bml_r2.show_unreach_errors);

    return OMPI_SUCCESS;
}

int mca_bml_r2_component_open(void)
{
    return OMPI_SUCCESS;
}


int mca_bml_r2_component_close(void)
{
    return OMPI_SUCCESS;
}


mca_bml_base_module_t* mca_bml_r2_component_init( int* priority,
                                                  bool enable_progress_threads,
                                                  bool enable_mpi_threads )
{
    /* initialize BTLs */

    if(OMPI_SUCCESS != mca_btl_base_select(enable_progress_threads,enable_mpi_threads))
        return NULL;

    *priority = 100;
    mca_bml_r2.btls_added = false;
    return &mca_bml_r2.super;
}
