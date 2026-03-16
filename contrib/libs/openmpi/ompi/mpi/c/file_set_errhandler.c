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
 * Copyright (c) 2008      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
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
#pragma weak MPI_File_set_errhandler = PMPI_File_set_errhandler
#endif
#define MPI_File_set_errhandler PMPI_File_set_errhandler
#endif

static const char FUNC_NAME[] = "MPI_File_set_errhandler";


int MPI_File_set_errhandler( MPI_File file, MPI_Errhandler errhandler)
{
    MPI_Errhandler tmp;

    OPAL_CR_NOOP_PROGRESS();

    /* Error checking */

    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

        /* Note that MPI-2:9.7 (p265 in the ps; p261 in the pdf)
           explicitly says that you are allowed to set the error
           handler on MPI_FILE_NULL */

        if (NULL == file) {
            return OMPI_ERRHANDLER_INVOKE(MPI_FILE_NULL, MPI_ERR_FILE,
                                          FUNC_NAME);
        } else if (NULL == errhandler ||
                   MPI_ERRHANDLER_NULL == errhandler ||
                   (OMPI_ERRHANDLER_TYPE_FILE != errhandler->eh_mpi_object_type &&
		    OMPI_ERRHANDLER_TYPE_PREDEFINED != errhandler->eh_mpi_object_type) ) {
            return OMPI_ERRHANDLER_INVOKE(file, MPI_ERR_ARG, FUNC_NAME);
        }
    }

    /* Prepare the new error handler */
    OBJ_RETAIN(errhandler);

    OPAL_THREAD_LOCK(&file->f_lock);
    /* Ditch the old errhandler, and decrement its refcount. */
    tmp = file->error_handler;
    file->error_handler = errhandler;
    OBJ_RELEASE(tmp);
    OPAL_THREAD_UNLOCK(&file->f_lock);

    /* All done */
    return MPI_SUCCESS;
}
