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
 * Copyright (c) 2008      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2015      University of Houston.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation.  All rights reserved.
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
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/file/file.h"
#include "ompi/mca/io/io.h"
#include "ompi/mca/io/base/io_base_request.h"
#include "ompi/memchecker.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_File_iread_at_all = PMPI_File_iread_at_all
#endif
#define MPI_File_iread_at_all PMPI_File_iread_at_all
#endif

static const char FUNC_NAME[] = "MPI_File_iread_at_all";


int MPI_File_iread_at_all(MPI_File fh, MPI_Offset offset, void *buf,
                          int count, MPI_Datatype datatype, MPI_Request *request)
{
    int rc;

    MEMCHECKER(
        memchecker_datatype(datatype);
    );

    if (MPI_PARAM_CHECK) {
        rc = MPI_SUCCESS;
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (ompi_file_invalid(fh)) {
            fh = MPI_FILE_NULL;
            rc = MPI_ERR_FILE;
        } else if (count < 0) {
            rc = MPI_ERR_COUNT;
        } else if (NULL == request) {
            rc = MPI_ERR_REQUEST;
        } else {
           OMPI_CHECK_DATATYPE_FOR_RECV(rc, datatype, count);
        }
        OMPI_ERRHANDLER_CHECK(rc, fh, rc, FUNC_NAME);
    }

    OPAL_CR_ENTER_LIBRARY();

    /* Call the back-end io component function */
    switch (fh->f_io_version) {
    case MCA_IO_BASE_V_2_0_0:
        if( OPAL_UNLIKELY(NULL == fh->f_io_selected_module.v2_0_0.io_module_file_iread_at_all) ) {
            rc = MPI_ERR_UNSUPPORTED_OPERATION;
        }
        else {
            rc = fh->f_io_selected_module.v2_0_0.
                io_module_file_iread_at_all(fh, offset, buf, count, datatype,
                                            request);
        }
        break;

    default:
        rc = MPI_ERR_INTERN;
        break;
    }

    /* All done */

    OMPI_ERRHANDLER_RETURN(rc, fh, rc, FUNC_NAME);
}
