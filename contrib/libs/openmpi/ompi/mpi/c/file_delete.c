/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
 * Copyright (c) 2013      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
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
#include "ompi/info/info.h"
#include "ompi/file/file.h"
#include "ompi/mca/io/io.h"
#include "ompi/mca/io/base/base.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_File_delete = PMPI_File_delete
#endif
#define MPI_File_delete PMPI_File_delete
#endif

static const char FUNC_NAME[] = "MPI_File_delete";


int MPI_File_delete(const char *filename, MPI_Info info)
{
    int rc;

    if (MPI_PARAM_CHECK) {
        rc = MPI_SUCCESS;
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (NULL == info || ompi_info_is_freed(info)) {
            rc = MPI_ERR_INFO;
        } else if (NULL == filename) {
            rc = MPI_ERR_ARG;
        }
        OMPI_ERRHANDLER_CHECK(rc, MPI_FILE_NULL, rc, FUNC_NAME);
    }

    /* Note that MPI-2:9.7 (p265 in the ps; 261 in the pdf) says that
       errors in MPI_FILE_OPEN (before the file handle is created)
       should invoke the default error handler on MPI_FILE_NULL.
       Hence, if we get a file handle out of ompi_file_open(), invoke
       the error handler on that.  If not, invoke the error handler on
       MPI_FILE_NULL. */

    /* The io framework is only initialized lazily.  If it hasn't
       already been initialized, do so now (note that MPI_FILE_OPEN
       and MPI_FILE_DELETE are the only two places that it will be
       initialized). We might want to add a check to see if the
       framework is open instead of just incrementing the open count. */

    if (OMPI_SUCCESS != (rc = mca_base_framework_open(&ompi_io_base_framework, 0))) {
        return OMPI_ERRHANDLER_INVOKE(MPI_FILE_NULL, rc, FUNC_NAME);
    }

    OPAL_CR_ENTER_LIBRARY();

    /* Since there is no MPI_File handle associated with this
       function, the MCA has to do a selection and perform the
       action */
    rc = mca_io_base_delete(filename, &(info->super));
    OMPI_ERRHANDLER_RETURN(rc, MPI_FILE_NULL, rc, FUNC_NAME);
}
