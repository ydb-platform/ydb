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
 * Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
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

#include "ompi/info/info.h"
#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/runtime/mpiruntime.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/dpm/dpm.h"
#include "ompi/memchecker.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Comm_spawn = PMPI_Comm_spawn
#endif
#define MPI_Comm_spawn PMPI_Comm_spawn
#endif

static const char FUNC_NAME[] = "MPI_Comm_spawn";


int MPI_Comm_spawn(const char *command, char *argv[], int maxprocs, MPI_Info info,
		   int root, MPI_Comm comm, MPI_Comm *intercomm,
		   int array_of_errcodes[])
{
    int rank, rc=OMPI_SUCCESS, i, flag;
    bool send_first = false; /* we wait to be contacted */
    ompi_communicator_t *newcomp=NULL;
    char port_name[MPI_MAX_PORT_NAME];
    bool non_mpi = false;

    MEMCHECKER(
        memchecker_comm(comm);
    );

    if ( MPI_PARAM_CHECK ) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

        if ( ompi_comm_invalid (comm)) {
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
        if ( NULL == intercomm ) {
            return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_ARG,
                                          FUNC_NAME);
        }
    }

    rank = ompi_comm_rank ( comm );
    if ( MPI_PARAM_CHECK ) {
        if ( rank == root ) {
            if ( NULL == command ) {
                return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_ARG,
                                              FUNC_NAME);
            }
            if ( 0 > maxprocs ) {
                return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_ARG,
                                              FUNC_NAME);
            }
            if (NULL == info || ompi_info_is_freed(info)) {
                return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_INFO,
                                              FUNC_NAME);
            }
        }
    }

    if (!ompi_mpi_dynamics_is_enabled(FUNC_NAME)) {
        return OMPI_ERRHANDLER_INVOKE(comm, OMPI_ERR_NOT_SUPPORTED, FUNC_NAME);
    }

    /* initialize the port name to avoid problems */
    memset(port_name, 0, MPI_MAX_PORT_NAME);

    /* See if the info key "ompi_non_mpi" was set to true */
    if (rank == root) {
        ompi_info_get_bool(info, "ompi_non_mpi", &non_mpi, &flag);
    }

    OPAL_CR_ENTER_LIBRARY();

    if ( rank == root ) {
        if (!non_mpi) {
            /* Open a port. The port_name is passed as an environment
               variable to the children. */
            if (OMPI_SUCCESS != (rc = ompi_dpm_open_port (port_name))) {
                goto error;
            }
        } else if (1 < ompi_comm_size(comm)) {
            /* we do not support non_mpi spawns on comms this size */
            rc = OMPI_ERR_NOT_SUPPORTED;
            goto error;
        }
        if (OMPI_SUCCESS != (rc = ompi_dpm_spawn (1, &command, &argv, &maxprocs,
                                                  &info, port_name))) {
            goto error;
        }
    }

    if (non_mpi) {
        newcomp = MPI_COMM_NULL;
    } else {
        rc = ompi_dpm_connect_accept (comm, root, port_name, send_first, &newcomp);
    }

error:
    if (OPAL_ERR_NOT_SUPPORTED == rc) {
        opal_show_help("help-mpi-api.txt",
                       "MPI function not supported",
                       true,
                       FUNC_NAME,
                       "Underlying runtime environment does not support spawn functionality");
    }

    /* close the port */
    if (rank == root && !non_mpi) {
        ompi_dpm_close_port(port_name);
    }

    OPAL_CR_EXIT_LIBRARY();

    /* set error codes */
    if (MPI_ERRCODES_IGNORE != array_of_errcodes) {
        for ( i=0; i < maxprocs; i++ ) {
            array_of_errcodes[i]=rc;
        }
    }

    *intercomm = newcomp;
    OMPI_ERRHANDLER_RETURN (rc, comm, rc, FUNC_NAME);
}
