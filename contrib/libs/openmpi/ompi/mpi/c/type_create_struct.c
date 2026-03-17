/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
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
#pragma weak MPI_Type_create_struct = PMPI_Type_create_struct
#endif
#define MPI_Type_create_struct PMPI_Type_create_struct
#endif

static const char FUNC_NAME[] = "MPI_Type_create_struct";


int MPI_Type_create_struct(int count,
                           const int array_of_blocklengths[],
                           const MPI_Aint array_of_displacements[],
                           const MPI_Datatype array_of_types[],
                           MPI_Datatype *newtype)
{
    int i, rc;

    if ( count > 0 ) {
        for ( i = 0; i < count; i++ ) {
            MEMCHECKER(
                memchecker_datatype(array_of_types[i]);
                );
        }
    }

    if( MPI_PARAM_CHECK ) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if( count < 0 ) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COUNT,
                                          FUNC_NAME);
        } else if( (count > 0) && (NULL == array_of_blocklengths ||
                                   NULL == array_of_displacements ||
                                   NULL == array_of_types) ) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG, FUNC_NAME);
        } else if (NULL == newtype) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_TYPE, FUNC_NAME);
        }
        for ( i = 0; i < count; i++ ){
            if (NULL == array_of_types[i] ||
                MPI_DATATYPE_NULL == array_of_types[i]) {
                return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_TYPE,
                                              FUNC_NAME);
            } else if (array_of_blocklengths[i] < 0) {
                return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG,
                                              FUNC_NAME);
            }
        }
    }

    OPAL_CR_ENTER_LIBRARY();

    rc = ompi_datatype_create_struct( count, array_of_blocklengths, array_of_displacements,
                                      array_of_types, newtype );
    if( rc != MPI_SUCCESS ) {
        ompi_datatype_destroy( newtype );
        OMPI_ERRHANDLER_RETURN( rc, MPI_COMM_WORLD,
                                rc, FUNC_NAME );
    }

    {
        const int* a_i[2] = {&count, array_of_blocklengths};

        ompi_datatype_set_args( *newtype, count + 1, a_i, count, array_of_displacements,
                                count, array_of_types, MPI_COMBINER_STRUCT );
    }

    OPAL_CR_EXIT_LIBRARY();
    return MPI_SUCCESS;
}
