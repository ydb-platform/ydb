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
#include "ompi/errhandler/errhandler.h"
#include "ompi/file/file.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_File_close = PMPI_File_close
#endif
#define MPI_File_close PMPI_File_close
#endif

static const char FUNC_NAME[] = "MPI_File_close";


int MPI_File_close(MPI_File *fh)
{
    int rc;

    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

        /* Note that MPI-2:9.7 (p265) says that errors in
           MPI_FILE_CLOSE should invoke the default error handler on
           MPI_FILE_NULL */

        if (NULL == fh || ompi_file_invalid(*fh)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_FILE_NULL, MPI_ERR_FILE,
                                          FUNC_NAME);
        }
    }

    OPAL_CR_ENTER_LIBRARY();

    /* Release the MPI_File; the destructor releases the component,
       zeroes out fiels, etc. */

    rc = ompi_file_close(fh);
    OMPI_ERRHANDLER_RETURN(rc, *fh, rc, FUNC_NAME);
}
