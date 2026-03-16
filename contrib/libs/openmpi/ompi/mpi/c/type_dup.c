/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
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
#include "ompi/attribute/attribute.h"
#include "ompi/memchecker.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Type_dup = PMPI_Type_dup
#endif
#define MPI_Type_dup PMPI_Type_dup
#endif

static const char FUNC_NAME[] = "MPI_Type_dup";


int MPI_Type_dup (MPI_Datatype type,
                  MPI_Datatype *newtype)
{
   MEMCHECKER(
      memchecker_datatype(type);
   );

   if( MPI_PARAM_CHECK ) {
      OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
      if (NULL == type || MPI_DATATYPE_NULL == type ||
          NULL == newtype) {
        return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_TYPE,
                                      FUNC_NAME );
      }
   }

   OPAL_CR_ENTER_LIBRARY();

   if (OMPI_SUCCESS != ompi_datatype_duplicate( type, newtype)) {
       ompi_datatype_destroy( newtype );
       OMPI_ERRHANDLER_RETURN (MPI_ERR_INTERN, MPI_COMM_WORLD,
                               MPI_ERR_INTERN, FUNC_NAME );
   }

   ompi_datatype_set_args( *newtype, 0, NULL, 0, NULL, 1, &type, MPI_COMBINER_DUP );

   /* Copy all the old attributes, if there were any.  This is done
      here (vs. ompi_datatype_duplicate()) because MPI_TYPE_DUP is the
      only MPI function that copies attributes.  All other MPI
      functions that take an old type and generate a newtype do not
      copy attributes.  Really. */
   if (NULL != type->d_keyhash) {
       ompi_attr_hash_init(&(*newtype)->d_keyhash);
       if (OMPI_SUCCESS != ompi_attr_copy_all(TYPE_ATTR,
                                              type, *newtype,
                                              type->d_keyhash,
                                              (*newtype)->d_keyhash)) {
           ompi_datatype_destroy(newtype);
           OMPI_ERRHANDLER_RETURN( MPI_ERR_INTERN, MPI_COMM_WORLD,
                                   MPI_ERR_INTERN, FUNC_NAME );
       }
   }

   OPAL_CR_EXIT_LIBRARY();

   return MPI_SUCCESS;
}
