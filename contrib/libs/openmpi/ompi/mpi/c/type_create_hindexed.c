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
#pragma weak MPI_Type_create_hindexed = PMPI_Type_create_hindexed
#endif
#define MPI_Type_create_hindexed PMPI_Type_create_hindexed
#endif

static const char FUNC_NAME[] = "MPI_Type_create_hindexed";


int MPI_Type_create_hindexed(int count,
                             const int array_of_blocklengths[],
                             const MPI_Aint array_of_displacements[],
                             MPI_Datatype oldtype,
                             MPI_Datatype *newtype)
{
    int rc, i;

    MEMCHECKER(
        memchecker_datatype(oldtype);
        );

    if( MPI_PARAM_CHECK ) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if( count < 0 ) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COUNT,
                                          FUNC_NAME);
        } else if ((count > 0) && (NULL == array_of_blocklengths ||
                                   NULL == array_of_displacements)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG,
                                          FUNC_NAME);
        } else if (MPI_DATATYPE_NULL == oldtype || NULL == oldtype ||
                   NULL == newtype) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_TYPE,
                                          FUNC_NAME);
        }
        for ( i = 0; i < count; i++ ) {
            if (array_of_blocklengths[i] < 0) {
                return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG,
                                              FUNC_NAME );
            }
        }
    }

    OPAL_CR_ENTER_LIBRARY();

    rc = ompi_datatype_create_hindexed( count, array_of_blocklengths, array_of_displacements,
                                        oldtype, newtype );
    if( rc != MPI_SUCCESS ) {
        ompi_datatype_destroy( newtype );
        OMPI_ERRHANDLER_RETURN( rc, MPI_COMM_WORLD, rc, FUNC_NAME );
    }
    /* data description */
    {
        const int* a_i[2] = {&count, array_of_blocklengths};

        ompi_datatype_set_args( *newtype, count + 1, a_i, count, array_of_displacements,
                                1, &oldtype, MPI_COMBINER_HINDEXED );
    }

    OPAL_CR_EXIT_LIBRARY();
    return MPI_SUCCESS;
}
