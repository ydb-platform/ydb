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
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include <stdio.h>

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/mca/io/base/base.h"
#include "ompi/file/file.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Register_datarep = PMPI_Register_datarep
#endif
#define MPI_Register_datarep PMPI_Register_datarep
#endif

static const char FUNC_NAME[] = "MPI_Register_datarep";


int MPI_Register_datarep(const char *datarep,
			 MPI_Datarep_conversion_function *read_conversion_fn,
			 MPI_Datarep_conversion_function *write_conversion_fn,
			 MPI_Datarep_extent_function *dtype_file_extent_fn,
			 void *extra_state)
{
    int rc;

    if (MPI_PARAM_CHECK) {
        rc = MPI_SUCCESS;
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (NULL == datarep) {
            rc = MPI_ERR_ARG;
        }
        OMPI_ERRHANDLER_CHECK(rc, MPI_FILE_NULL, rc, FUNC_NAME);
    }

    /* The io framework is only initialized lazily.  If it hasn't
       already been initialized, do so now (note that MPI_FILE_OPEN
       and MPI_FILE_DELETE are the only two places that it will be
       initialized). */

    if (OMPI_SUCCESS != (rc = mca_base_framework_open(&ompi_io_base_framework, 0))) {
        return OMPI_ERRHANDLER_INVOKE(MPI_FILE_NULL, rc, FUNC_NAME);
    }

    OPAL_CR_ENTER_LIBRARY();

    /* Call the back-end io component function */
    rc = mca_io_base_register_datarep(datarep, read_conversion_fn,
                                      write_conversion_fn,
                                      dtype_file_extent_fn,
                                      extra_state);


    /* All done */

    OMPI_ERRHANDLER_RETURN(rc, MPI_FILE_NULL, rc, FUNC_NAME);
}
