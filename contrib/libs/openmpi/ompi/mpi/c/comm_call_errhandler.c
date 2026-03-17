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
#include "ompi/memchecker.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Comm_call_errhandler = PMPI_Comm_call_errhandler
#endif
#define MPI_Comm_call_errhandler PMPI_Comm_call_errhandler
#endif


static const char FUNC_NAME[] = "MPI_Comm_call_errhandler";


int MPI_Comm_call_errhandler(MPI_Comm comm, int errorcode)
{
    MEMCHECKER(
        memchecker_comm(comm);
    );

    OPAL_CR_NOOP_PROGRESS();

  /* Error checking */

  if (MPI_PARAM_CHECK) {
    OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
    if (ompi_comm_invalid(comm)) {
      return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COMM,
                                    FUNC_NAME);
    }
  }

  /* Invoke the errhandler */

  OMPI_ERRHANDLER_INVOKE(comm, errorcode, FUNC_NAME);

  /* See MPI-2 8.5 why this function has to return MPI_SUCCESS */
  return MPI_SUCCESS;
}

