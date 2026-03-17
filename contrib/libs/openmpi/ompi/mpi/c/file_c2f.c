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
#include "ompi/file/file.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_File_c2f = PMPI_File_c2f
#endif
#define MPI_File_c2f PMPI_File_c2f
#endif

static const char FUNC_NAME[] = "MPI_File_c2f";


MPI_Fint MPI_File_c2f(MPI_File file)
{

    OPAL_CR_NOOP_PROGRESS();

    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

        /* Note that ompi_file_invalid() explicitly checks for
           MPI_FILE_NULL, but MPI_FILE_C2F is supposed to treat
           MPI_FILE_NULL as a valid file (and therefore return a valid
           Fortran handle for it).  Hence, this function should not
           return an error if MPI_FILE_NULL is passed in.

           See a big comment in ompi/communicator/communicator.h about
           this. */
        if (ompi_file_invalid(file) && MPI_FILE_NULL != file) {
            return OMPI_INT_2_FINT(-1);
        }
    }

    return OMPI_INT_2_FINT(file->f_f_to_c_index);
}
