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
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2019      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include <stdio.h>

/* This implementation has been removed from the MPI 3.0 standard.
 * Open MPI v4.0.x is keeping the implementation in the library, but
 * removing the prototypes from the headers, unless the user configures
 * with --enable-mpi1-compatibility.
 */

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Address = PMPI_Address
#endif
/* undef before defining, to prevent possible redefinition when
 * using _Static_assert to error on usage of removed functions.
 */
#undef MPI_Address
#define MPI_Address PMPI_Address
#endif

static const char FUNC_NAME[] = "MPI_Address";


int MPI_Address(void *location, MPI_Aint *address)
{

    OPAL_CR_NOOP_PROGRESS();

    if( MPI_PARAM_CHECK ) {
      OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
      if (NULL == location || NULL == address) {
        return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG, FUNC_NAME);
      }
    }

    *address = (MPI_Aint)location;
    return MPI_SUCCESS;
}
