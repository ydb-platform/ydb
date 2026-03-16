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
 * Copyright (c) 2008      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <string.h>

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/memchecker.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Type_get_name = PMPI_Type_get_name
#endif
#define MPI_Type_get_name PMPI_Type_get_name
#endif

static const char FUNC_NAME[] = "MPI_Type_get_name";


int MPI_Type_get_name(MPI_Datatype type, char *type_name, int *resultlen)
{

    MEMCHECKER(
        memchecker_datatype(type);
    );

    OPAL_CR_NOOP_PROGRESS();

   if ( MPI_PARAM_CHECK ) {
      OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
      if (NULL == type || MPI_DATATYPE_NULL == type) {
        return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_TYPE,
                                      FUNC_NAME );
      } else if (NULL == type_name || NULL == resultlen) {
        return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG,
                                      FUNC_NAME );
      }
   }

    /* Note that MPI-2.1 requires:
       - terminating the string with a \0
       - name[*resultlen] == '\0'
       - and therefore (*resultlen) cannot be > (MPI_MAX_OBJECT_NAME-1)

       The Fortran API version will pad to the right if necessary.

       Note that type->name is guaranteed to be \0-terminated and
       able to completely fit into MPI_MAX_OBJECT_NAME bytes (i.e.,
       name+\0). */
   *resultlen = (int)strlen(type->name);
   strncpy(type_name, type->name, MPI_MAX_OBJECT_NAME);
   return MPI_SUCCESS;
}
