/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
 * Copyright (c) 2006-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2008      University of Houston, Inc.  All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2015      Intel, Inc. All rights reserved.
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

#include "opal/util/show_help.h"

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/runtime/mpiruntime.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/info/info.h"
#include "ompi/dpm/dpm.h"
#include "ompi/memchecker.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Comm_accept = PMPI_Comm_accept
#endif
#define MPI_Comm_accept PMPI_Comm_accept
#endif

static const char FUNC_NAME[] = "MPI_Comm_accept";


int MPI_Comm_accept(const char *port_name, MPI_Info info, int root,
                    MPI_Comm comm, MPI_Comm *newcomm)
{
    int rank, rc;
    bool send_first=false; /* we receive first */
    ompi_communicator_t *newcomp=MPI_COMM_NULL;

    MEMCHECKER(
        memchecker_comm(comm);
    );

    if ( MPI_PARAM_CHECK ) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

        if (ompi_comm_invalid (comm)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COMM,
                                          FUNC_NAME);
        }
        if ( OMPI_COMM_IS_INTER(comm)) {
            return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_COMM,
                                          FUNC_NAME);
        }
        if ( (0 > root) || (ompi_comm_size(comm) <= root) ) {
            return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_ARG,
                                          FUNC_NAME);
        }
        if ( NULL == newcomm ) {
            return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_ARG,
                                          FUNC_NAME);
        }
        if (NULL == info || ompi_info_is_freed(info)) {
          return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_INFO,
                                        FUNC_NAME);
        }
    }

    rank = ompi_comm_rank ( comm );
    if ( MPI_PARAM_CHECK ) {
        if ( rank == root ) {
            if ( NULL == port_name )
                return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_ARG,
                                              FUNC_NAME);
        }
    }

    if (!ompi_mpi_dynamics_is_enabled(FUNC_NAME)) {
        return OMPI_ERRHANDLER_INVOKE(comm, OMPI_ERR_NOT_SUPPORTED, FUNC_NAME);
    }

    /* parse info object. no prefedined values for this function in MPI-2
     * so lets ignore it for the moment.
     * if ( rank == root && MPI_INFO_NULL != info ) {
     * }
     */
    OPAL_CR_ENTER_LIBRARY();

    if ( rank == root ) {
	rc = ompi_dpm_connect_accept (comm, root, port_name, send_first,
				      &newcomp);
    }
    else {
	rc = ompi_dpm_connect_accept (comm, root, NULL, send_first,
				      &newcomp);
    }

    OPAL_CR_EXIT_LIBRARY();

    if (OPAL_ERR_NOT_SUPPORTED == rc) {
        opal_show_help("help-mpi-api.txt",
                       "MPI function not supported",
                       true,
                       FUNC_NAME,
                       "Underlying runtime environment does not support accept/connect functionality");
    }

    *newcomm = newcomp;
    OMPI_ERRHANDLER_RETURN(rc, comm, rc, FUNC_NAME );
}
