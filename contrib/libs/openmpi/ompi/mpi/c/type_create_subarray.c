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
 * Copyright (c) 2009      Sun Microsystems, Inc. All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC.  All rights
 *                         reserved.
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
#pragma weak MPI_Type_create_subarray = PMPI_Type_create_subarray
#endif
#define MPI_Type_create_subarray PMPI_Type_create_subarray
#endif

static const char FUNC_NAME[] = "MPI_Type_create_subarray";


int MPI_Type_create_subarray(int ndims,
                             const int size_array[],
                             const int subsize_array[],
                             const int start_array[],
                             int order,
                             MPI_Datatype oldtype,
                             MPI_Datatype *newtype)
{
    int32_t i, rc;

    MEMCHECKER(
        memchecker_datatype(oldtype);
        );

    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if( ndims < 0 ) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COUNT, FUNC_NAME);
        } else if( (ndims > 0) && ((NULL == size_array) || (NULL == subsize_array) || (NULL == start_array)) ) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG, FUNC_NAME);
        } else if( (NULL == oldtype) || (MPI_DATATYPE_NULL == oldtype) || (NULL == newtype) ) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_TYPE, FUNC_NAME);
        } else if( (MPI_ORDER_C != order) && (MPI_ORDER_FORTRAN != order) ) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG, FUNC_NAME);
        }
        for( i = 0; i < ndims; i++ ) {
            if( (subsize_array[i] < 1) || (subsize_array[i] > size_array[i]) ) {
                return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG, FUNC_NAME);
            } else if( (start_array[i] < 0) || (start_array[i] > (size_array[i] - subsize_array[i])) ) {
                return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG, FUNC_NAME);
            }
        }
    }

    OPAL_CR_ENTER_LIBRARY();

    rc = ompi_datatype_create_subarray( ndims, size_array, subsize_array, start_array,
                                        order, oldtype, newtype);
    if( OMPI_SUCCESS == rc ) {
        const int* a_i[5] = {&ndims, size_array, subsize_array, start_array, &order};

        ompi_datatype_set_args( *newtype, 3 * ndims + 2, a_i, 0, NULL, 1, &oldtype,
                                MPI_COMBINER_SUBARRAY );
    }

    OPAL_CR_EXIT_LIBRARY();

    OMPI_ERRHANDLER_RETURN(rc, MPI_COMM_WORLD, rc, FUNC_NAME);
}
