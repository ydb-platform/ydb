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
 * Copyright (c) 2006-2012 Cisco Systems, Inc.  All rights reserved.
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
#include "ompi/errhandler/errhandler.h"
#include "ompi/mpi/fortran/base/fint_2_int.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Errhandler_c2f = PMPI_Errhandler_c2f
#endif
#define MPI_Errhandler_c2f PMPI_Errhandler_c2f
#endif

static const char FUNC_NAME[] = "MPI_Errhandler_c2f";


MPI_Fint MPI_Errhandler_c2f(MPI_Errhandler errhandler)
{

    OPAL_CR_NOOP_PROGRESS();

  /* Error checking */

  if (MPI_PARAM_CHECK) {
    OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

	/* mapping an invalid handle to a null handle */
    if (NULL == errhandler) {
        return OMPI_INT_2_FINT(-1);
    }
  }


  return OMPI_INT_2_FINT(errhandler->eh_f_to_c_index);
}
