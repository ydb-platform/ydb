/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006      University of Houston. All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
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
#include "ompi/errhandler/errcode.h"
#include "ompi/communicator/communicator.h"
#include "ompi/attribute/attribute.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Add_error_class = PMPI_Add_error_class
#endif
#define MPI_Add_error_class PMPI_Add_error_class
#endif

static const char FUNC_NAME[] = "MPI_Add_error_class";


int MPI_Add_error_class(int *errorclass)
{
    int err_class;
    int rc;

    OPAL_CR_NOOP_PROGRESS();

    if ( MPI_PARAM_CHECK ) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

        if (NULL == errorclass) {
            return OMPI_ERRHANDLER_INVOKE (MPI_COMM_WORLD,
                                           MPI_ERR_ARG, FUNC_NAME);
        }
    }

    err_class = ompi_mpi_errclass_add();
    if ( 0 > err_class ) {
        return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_INTERN,
                                      FUNC_NAME);
    }


    /*
    ** Update the attribute value. See the comments
    ** in attribute/attribute.c and attribute/attribute_predefined.c
    ** why we have to call the fortran attr_set function
    */
    rc  = ompi_attr_set_fint (COMM_ATTR,
                              MPI_COMM_WORLD,
                              &MPI_COMM_WORLD->c_keyhash,
                              MPI_LASTUSEDCODE,
                              ompi_mpi_errcode_lastused,
                              true);
    if ( MPI_SUCCESS != rc ) {
	return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, rc, FUNC_NAME);
    }

    *errorclass = err_class;
    return MPI_SUCCESS;
}

