/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart,
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
#include <stdio.h>

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/mpi/fortran/base/fint_2_int.h"
#include "ompi/request/request.h"
#include "ompi/memchecker.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Request_c2f = PMPI_Request_c2f
#endif
#define MPI_Request_c2f PMPI_Request_c2f
#endif

static const char FUNC_NAME[] = "MPI_Request_c2f";


MPI_Fint MPI_Request_c2f(MPI_Request request)
{
    MEMCHECKER(
        memchecker_request(&request);
    );

    OPAL_CR_NOOP_PROGRESS();

    if ( MPI_PARAM_CHECK ) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

        if (NULL == request) {
            return OMPI_INT_2_FINT(-1);
        }
    }

    /* We only put requests in the f2c table when this function is
       invoked.  This is because putting requests in the table
       involves locking and unlocking the table, which would incur a
       performance penalty (in the critical performance path) for C
       applications.  In this way, at least only Fortran applications
       are penalized.  :-\

       Modifying this one function neatly fixes up all the Fortran
       bindings because they all call MPI_Request_c2f in order to
       transmorgify the C MPI_Request that they got back into a
       fortran integer.
    */

    if (MPI_UNDEFINED == request->req_f_to_c_index) {
        request->req_f_to_c_index =
            opal_pointer_array_add(&ompi_request_f_to_c_table, request);
    }

    return OMPI_INT_2_FINT(request->req_f_to_c_index) ;
}
