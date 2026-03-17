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

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Errhandler_free = PMPI_Errhandler_free
#endif
#define MPI_Errhandler_free PMPI_Errhandler_free
#endif

static const char FUNC_NAME[] = "MPI_Errhandler_free";


int MPI_Errhandler_free(MPI_Errhandler *errhandler)
{

    OPAL_CR_NOOP_PROGRESS();

  /* Error checking */

  if (MPI_PARAM_CHECK) {
    OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
    /* Raise an MPI exception if we got NULL or if we got an intrinsic
       *and* the reference count is 1 (meaning that this FREE would
       actually free the underlying intrinsic object).  This is ugly
       but necessary -- see below. */
    if (NULL == errhandler ||
        (ompi_errhandler_is_intrinsic(*errhandler) &&
         1 == (*errhandler)->super.obj_reference_count)) {
      return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG,
                                   "MPI_Errhandler_free");
    }
  }

  /* Return the errhandler.  According to MPI-2 errata, any errhandler
     obtained by MPI_*_GET_ERRHANDLER or MPI_ERRHANDLER_GET must also
     be freed by MPI_ERRHANDLER_FREE (including intrinsic error
     handlers).  For example, this is valid:

     int main() {
         MPI_Errhandler errhdl;
         MPI_Init(NULL, NULL);
         MPI_Comm_get_errhandler(MPI_COMM_WORLD, &errhdl);
         MPI_Errhandler_free(&errhdl);
         MPI_Finalize();
         return 0;
     }

     So decrease the refcount here. */

  OBJ_RELEASE(*errhandler);
  *errhandler = MPI_ERRHANDLER_NULL;

  /* All done */

  return MPI_SUCCESS;
}
