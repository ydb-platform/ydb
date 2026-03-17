/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2013 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2009      Sun Microsystems, Inc. All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/memchecker.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Type_create_darray = PMPI_Type_create_darray
#endif
#define MPI_Type_create_darray PMPI_Type_create_darray
#endif

static const char FUNC_NAME[] = "MPI_Type_create_darray";

int MPI_Type_create_darray(int size,
                           int rank,
                           int ndims,
                           const int gsize_array[],
                           const int distrib_array[],
                           const int darg_array[],
                           const int psize_array[],
                           int order,
                           MPI_Datatype oldtype,
                           MPI_Datatype *newtype)

{
    int i, rc;

    MEMCHECKER(
        memchecker_datatype(oldtype);
    );

    if (MPI_PARAM_CHECK) {
        int prod_psize = 1;
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if( (rank < 0) || (size < 0) || (rank >= size) ) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG, FUNC_NAME);
        } else if( ndims < 0 ) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COUNT, FUNC_NAME);
        } else if( (ndims > 0) && ((NULL == gsize_array) || (NULL == distrib_array) ||
                                   (NULL == darg_array) || (NULL == psize_array))) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG, FUNC_NAME);
        } else if (NULL == newtype) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_TYPE, FUNC_NAME);
        } else if( !(OPAL_DATATYPE_FLAG_DATA & oldtype->super.flags) ) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_TYPE, FUNC_NAME);
        } else if( (MPI_ORDER_C != order) && (MPI_ORDER_FORTRAN != order) ) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG, FUNC_NAME);
        }
        if( ndims > 0 ) {
            for( i = 0; i < ndims; i++ ) {
                if( (MPI_DISTRIBUTE_BLOCK != distrib_array[i]) &&
                    (MPI_DISTRIBUTE_CYCLIC != distrib_array[i]) &&
                    (MPI_DISTRIBUTE_NONE != distrib_array[i]) ) {
                    return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG, FUNC_NAME);
                } else if( (gsize_array[i] < 1) || (psize_array[i] < 0) ||
                           ((darg_array[i] < 0) && (MPI_DISTRIBUTE_DFLT_DARG != darg_array[i]) ) ) {
                    return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG, FUNC_NAME);
                } else if( (MPI_DISTRIBUTE_DFLT_DARG != darg_array[i]) &&
                           (MPI_DISTRIBUTE_BLOCK == distrib_array[i]) &&
                           ((darg_array[i] * psize_array[i]) < gsize_array[i]) ) {
                    return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG, FUNC_NAME);
                } else if( 1 > psize_array[i] )
                    return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG, FUNC_NAME);
                prod_psize *= psize_array[i];
            }
            if( prod_psize != size )
                return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG, FUNC_NAME);
        }
    }

    OPAL_CR_ENTER_LIBRARY();

    rc = ompi_datatype_create_darray( size, rank, ndims,
                                      gsize_array, distrib_array, darg_array, psize_array,
                                      order, oldtype, newtype );
    if( OMPI_SUCCESS == rc ) {
        const int* a_i[8] = {&size, &rank, &ndims, gsize_array, distrib_array, darg_array,
                             psize_array, &order};

        ompi_datatype_set_args( *newtype, 4 * ndims + 4, a_i, 0, NULL, 1, &oldtype,
                                MPI_COMBINER_DARRAY );
    }

    OPAL_CR_EXIT_LIBRARY();

    OMPI_ERRHANDLER_RETURN(rc, MPI_COMM_WORLD, rc, FUNC_NAME);
}
