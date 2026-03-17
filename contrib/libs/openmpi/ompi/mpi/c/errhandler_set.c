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

/* This implementation has been removed from the MPI 3.0 standard.
 * Open MPI v4.0.x is keeping the implementation in the library, but
 * removing the prototypes from the headers, unless the user configures
 * with --enable-mpi1-compatibility.
 */

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/memchecker.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Errhandler_set = PMPI_Errhandler_set
#endif
/* undef before defining, to prevent possible redefinition when
 * using _Static_assert to error on usage of removed functions.
 */
#undef MPI_Errhandler_set
#define MPI_Errhandler_set PMPI_Errhandler_set
#endif

static const char FUNC_NAME[] = "MPI_Errhandler_set";


int MPI_Errhandler_set(MPI_Comm comm, MPI_Errhandler errhandler)
{
    MEMCHECKER(
        memchecker_comm(comm);
    );

    OPAL_CR_NOOP_PROGRESS();

  if (MPI_PARAM_CHECK) {
    OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
  }

  /* This is a deprecated -- just turn around and call the real
     function */

  return PMPI_Comm_set_errhandler(comm, errhandler);
}
