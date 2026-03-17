/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_MTL_BASE_H
#define MCA_MTL_BASE_H

#include "ompi_config.h"

#include "ompi/mca/mca.h"
#include "ompi/mca/mtl/mtl.h"


/*
 * Global functions for the MTL
 */

BEGIN_C_DECLS

OMPI_DECLSPEC extern mca_mtl_base_component_t* ompi_mtl_base_selected_component;

OMPI_DECLSPEC int ompi_mtl_base_select(bool enable_progress_threads,
                                       bool enable_mpi_threads,
                                       int *priority);

OMPI_DECLSPEC extern mca_base_framework_t ompi_mtl_base_framework;

END_C_DECLS
#endif /* MCA_MTL_BASE_H */
