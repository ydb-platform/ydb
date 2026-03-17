/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
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
#include "ompi/datatype/ompi_datatype_internal.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Type_match_size = PMPI_Type_match_size
#endif
#define MPI_Type_match_size PMPI_Type_match_size
#endif

static const char FUNC_NAME[] = "MPI_Type_match_size";


int MPI_Type_match_size(int typeclass, int size, MPI_Datatype *type)
{
    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
    }

    OPAL_CR_ENTER_LIBRARY();

    switch( typeclass ) {
    case MPI_TYPECLASS_REAL:
        *type = (MPI_Datatype)ompi_datatype_match_size( size, OMPI_DATATYPE_FLAG_DATA_FLOAT, OMPI_DATATYPE_FLAG_DATA_FORTRAN );
        break;
    case MPI_TYPECLASS_INTEGER:
        *type = (MPI_Datatype)ompi_datatype_match_size( size, OMPI_DATATYPE_FLAG_DATA_INT, OMPI_DATATYPE_FLAG_DATA_FORTRAN );
        break;
    case MPI_TYPECLASS_COMPLEX:
        *type = (MPI_Datatype)ompi_datatype_match_size( size, OMPI_DATATYPE_FLAG_DATA_COMPLEX, OMPI_DATATYPE_FLAG_DATA_FORTRAN );
        break;
    default:
        *type = &ompi_mpi_datatype_null.dt;
    }

    OPAL_CR_EXIT_LIBRARY();
    if( *type != &ompi_mpi_datatype_null.dt ) {
        return MPI_SUCCESS;
    }

    return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG, FUNC_NAME);
}
