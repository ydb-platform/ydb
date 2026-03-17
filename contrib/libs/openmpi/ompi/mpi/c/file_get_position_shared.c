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
 * Copyright (c) 2008      Sun Microsystems, Inc.  All rights reserved.
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
#pragma weak MPI_File_get_position_shared = PMPI_File_get_position_shared
#endif
#define MPI_File_get_position_shared PMPI_File_get_position_shared
#endif

static const char FUNC_NAME[] = "MPI_File_get_position_shared";


int MPI_File_get_position_shared(MPI_File fh, MPI_Offset *offset)
{
    int rc;

    if (MPI_PARAM_CHECK) {
        rc = MPI_SUCCESS;
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (ompi_file_invalid(fh)) {
            rc = MPI_ERR_FILE;
            fh = MPI_FILE_NULL;
        } else if (NULL == offset) {
            rc = MPI_ERR_ARG;
        }
        OMPI_ERRHANDLER_CHECK(rc, fh, rc, FUNC_NAME);
    }

    OPAL_CR_ENTER_LIBRARY();

    /* Call the back-end io component function */

    switch (fh->f_io_version) {
    case MCA_IO_BASE_V_2_0_0:
        rc = fh->f_io_selected_module.v2_0_0.
            io_module_file_get_position_shared(fh, offset);
        break;

    default:
        rc = MPI_ERR_INTERN;
        break;
    }

    /* All done */

    OMPI_ERRHANDLER_RETURN(rc, fh, rc, FUNC_NAME);
}
