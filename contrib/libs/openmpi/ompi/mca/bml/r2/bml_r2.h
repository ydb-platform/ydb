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
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * BML Management Layer (BML)
 *
 */

#include "ompi_config.h"
#include "ompi/mca/mca.h"
#include "opal/mca/btl/btl.h"

#ifndef MCA_BML_R2_H
#define MCA_BML_R2_H

#include "ompi/types.h"
#include "ompi/mca/bml/bml.h"

BEGIN_C_DECLS

/**
 * BML module interface functions and attributes.
 */
struct mca_bml_r2_module_t {
    mca_bml_base_module_t super;
    size_t num_btl_modules;
    mca_btl_base_module_t** btl_modules;
    size_t num_btl_progress;
    mca_btl_base_component_progress_fn_t * btl_progress;
    bool btls_added;
    bool show_unreach_errors;
};

typedef struct mca_bml_r2_module_t mca_bml_r2_module_t;

OMPI_DECLSPEC extern mca_bml_base_component_2_0_0_t mca_bml_r2_component;
extern mca_bml_r2_module_t mca_bml_r2;

int mca_bml_r2_component_open(void);
int mca_bml_r2_component_close(void);

mca_bml_base_module_t* mca_bml_r2_component_init( int* priority,
                                                  bool enable_progress_threads,
                                                  bool enable_mpi_threads );

int mca_bml_r2_progress(void);

int mca_bml_r2_component_fini(void);

int mca_bml_r2_ft_event(int status);

int mca_bml_r2_finalize( void );

END_C_DECLS

#endif /* OMPI_MCA_BML_R2_H */
