/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2013 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2008 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2012-2013 Inria.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include "ompi_config.h"
#include <stdio.h>

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/mca/topo/base/base.h"
#include "ompi/memchecker.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Cart_create = PMPI_Cart_create
#endif
#define MPI_Cart_create PMPI_Cart_create
#endif

static const char FUNC_NAME[] = "MPI_Cart_create";


int MPI_Cart_create(MPI_Comm old_comm, int ndims, const int dims[],
                    const int periods[], int reorder, MPI_Comm *comm_cart)
{
    mca_topo_base_module_t* topo;
    int err;

    MEMCHECKER(
        memchecker_comm(old_comm);
    );

    /* check the arguments */
    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (ompi_comm_invalid(old_comm)) {
            return OMPI_ERRHANDLER_INVOKE (MPI_COMM_WORLD, MPI_ERR_COMM,
                                          FUNC_NAME);
        } else if (OMPI_COMM_IS_INTER(old_comm)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COMM,
                                          FUNC_NAME);
        }
        if (ndims < 0) {
            return OMPI_ERRHANDLER_INVOKE (old_comm, MPI_ERR_ARG,
                                          FUNC_NAME);
        } else if (ndims >= 1 &&
                   (NULL == dims || NULL == periods || NULL == comm_cart)) {
            return OMPI_ERRHANDLER_INVOKE (old_comm, MPI_ERR_ARG,
                                          FUNC_NAME);
        }

        /* check if the number of processes on the grid are correct */
        {
           int i, count_nodes = 1;
           const int *p = dims;
           int parent_procs = ompi_comm_size(old_comm);

           for (i=0; i < ndims; i++, p++) {
               count_nodes *= *p;
           }

           if (parent_procs < count_nodes) {
               return OMPI_ERRHANDLER_INVOKE (old_comm, MPI_ERR_ARG,
                                              FUNC_NAME);
           }
        }
    }

    /*
     * everything seems to be alright with the communicator, we can go
     * ahead and select a topology module for this purpose and create
     * the new graph communicator
     */
    if (OMPI_SUCCESS != (err = mca_topo_base_comm_select(old_comm,
                                                         NULL,
                                                         &topo,
                                                         OMPI_COMM_CART))) {
        return err;
    }

    /* Now let that topology module rearrange procs/ranks if it wants to */
    err = topo->topo.cart.cart_create(topo, old_comm,
                                      ndims, dims, periods,
                                      (0 == reorder) ? false : true, comm_cart);
    OPAL_CR_EXIT_LIBRARY();

    if (MPI_SUCCESS != err) {
        OBJ_RELEASE(topo);
        return OMPI_ERRHANDLER_INVOKE(old_comm, err, FUNC_NAME);
    }

    /* All done */
    return MPI_SUCCESS;
}
