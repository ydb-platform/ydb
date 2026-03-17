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
 * Copyright (c) 2006      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2008-2009 Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * This framework is for the selection and assignment of "op" modules
 * to intrinsic MPI_Op objects.  This framework is not used for
 * user-defined MPI_Op objects.
 *
 * The main idea is to let intrinsic MPI_Ops be able to utilize
 * functions from multiple op modules, based on the (datatype,
 * operation) tuple.  Hence, it is possible for specialized hardware
 * to be utilized for datatypes and operations that are supported.
 */

#ifndef MCA_OP_BASE_H
#define MCA_OP_BASE_H

#include "ompi_config.h"
#include "opal/mca/base/base.h"
#include "ompi/mca/op/op.h"

BEGIN_C_DECLS

typedef struct ompi_op_base_selected_module_t {
    opal_list_item_t super;
    ompi_op_base_component_t *op_component;
    ompi_op_base_module_t *op_module;
} ompi_op_base_selected_module_t;


/**
 * Find all available op components.
 */
OMPI_DECLSPEC int ompi_op_base_find_available(bool enable_progress_threads,
                                              bool enable_mpi_threads);

/**
 * Select an available component for a new intrinsice MPI_Op (this
 * function is *not* used for user-defined MPI_Ops!).
 *
 * @param op MPI_Op that the component will be selected for.
 *
 * @return OMPI_SUCCESS Upon success.
 * @return OMPI_ERROR Upon failure.
 *
 * Note that the types of the parameters have "struct" in them (e.g.,
 * ompi_op_t" vs. a plain "ompi_op_t") to avoid an include file loop.
 * All similar types (e.g., "struct ompi_op_t *", "ompi_op_t *", and
 * "MPI_Op") are all typedef'ed to be the same, so the fact that we
 * use struct here in the prototype is ok.
 *
 * This function is invoked when a new MPI_Op is created and
 * op components need to be selected for it.
 */
int ompi_op_base_op_select(struct ompi_op_t *op);

/**
 * Finalize all op modules on a specific (intrinsic) MPI_Op.
 *
 * @param op The op that is being destroyed.
 *
 * @retval OMPI_SUCCESS Always.
 *
 * Note that the type of the parameter is only a "struct ompi_op_t"
 * (vs. a plain "ompi_op_t") to avoid an include file loop.  The types
 * "struct ompi_op_t *", "ompi_op_t *", and "MPI_Op" are all
 * typedef'ed to be the same, so the fact that we use struct here in
 * the prototype is ok.
 *
 * This function is invoked near the beginning of the destruction of
 * an op.  It finalizes the op modules associated with the MPI_Op
 * (e.g., allowing the component to clean up and free any resources
 * allocated for that MPI_Op) by calling the destructor on each
 * object.
 */
OMPI_DECLSPEC int ompi_op_base_op_unselect(struct ompi_op_t *op);

OMPI_DECLSPEC extern mca_base_framework_t ompi_op_base_framework;

END_C_DECLS
#endif /* MCA_OP_BASE_H */
