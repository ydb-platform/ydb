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
 * Copyright (c) 2013      Los Alamos National Security, LLC. All rights
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
#pragma weak MPI_Type_get_true_extent_x = PMPI_Type_get_true_extent_x
#endif
#define MPI_Type_get_true_extent_x PMPI_Type_get_true_extent_x
#endif

static const char FUNC_NAME[] = "MPI_Type_get_true_extent_x";

int MPI_Type_get_true_extent_x(MPI_Datatype datatype,
                               MPI_Count *true_lb,
                               MPI_Count *true_extent)
{
   MPI_Aint atrue_lb, atrue_extent;
   int rc;

   MEMCHECKER(
      memchecker_datatype(datatype);
   );

   if( MPI_PARAM_CHECK ) {
      OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
      if (NULL == datatype || MPI_DATATYPE_NULL == datatype) {
        return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_TYPE,
                                      FUNC_NAME );
      } else if (NULL == true_lb || NULL == true_extent) {
        return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG,
                                      FUNC_NAME );
      }
   }

   OPAL_CR_ENTER_LIBRARY();

   rc = ompi_datatype_get_true_extent( datatype, &atrue_lb, &atrue_extent );
   if (OMPI_SUCCESS == rc) {
      *true_lb = (MPI_Count) atrue_lb;
      *true_extent = (MPI_Count) atrue_extent;
   }

   OMPI_ERRHANDLER_RETURN(rc, MPI_COMM_WORLD, rc, FUNC_NAME );
}
