/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2014 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2008 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include "ompi_config.h"

#include "opal/util/show_help.h"
#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/runtime/mpiruntime.h"
#include "ompi/memchecker.h"
#include "ompi/communicator/communicator.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Abort = PMPI_Abort
#endif
#define MPI_Abort PMPI_Abort
#endif

static const char FUNC_NAME[] = "MPI_Abort";


int MPI_Abort(MPI_Comm comm, int errorcode)
{
    MEMCHECKER(
        memchecker_comm(comm);
    );

    OPAL_CR_ABORT_LIBRARY();

    /* Don't even bother checking comm and errorcode values for
       errors */

    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
    }

    opal_show_help("help-mpi-api.txt", "mpi-abort", true,
                   ompi_comm_rank(comm),
                   ('\0' != comm->c_name[0]) ? comm->c_name : "<Unknown>",
                   errorcode);
    return ompi_mpi_abort(comm, errorcode);
}
