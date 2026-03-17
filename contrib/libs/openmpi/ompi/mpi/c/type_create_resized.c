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

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/memchecker.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Type_create_resized = PMPI_Type_create_resized
#endif
#define MPI_Type_create_resized PMPI_Type_create_resized
#endif

static const char FUNC_NAME[] = "MPI_Type_create_resized";


int MPI_Type_create_resized(MPI_Datatype oldtype,
                            MPI_Aint lb,
                            MPI_Aint extent,
                            MPI_Datatype *newtype)
{
   int rc;

   MEMCHECKER(
      memchecker_datatype(oldtype);
   );

   if( MPI_PARAM_CHECK ) {
      OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
      if (NULL == oldtype || MPI_DATATYPE_NULL == oldtype ||
          NULL == newtype) {
        return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_TYPE,
                                      FUNC_NAME );
      }
   }

   OPAL_CR_ENTER_LIBRARY();

   rc = ompi_datatype_create_resized( oldtype, lb, extent, newtype );
   if( rc != MPI_SUCCESS ) {
      ompi_datatype_destroy( newtype );
      OMPI_ERRHANDLER_RETURN( rc, MPI_COMM_WORLD, rc, FUNC_NAME );
   }

   {
      MPI_Aint a_a[2];
      a_a[0] = lb;
      a_a[1] = extent;
      ompi_datatype_set_args( *newtype, 0, NULL, 2, a_a, 1, &oldtype, MPI_COMBINER_RESIZED );
   }

   OPAL_CR_EXIT_LIBRARY();
   return MPI_SUCCESS;
}


