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
 * Copyright (c) 2004-2008 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2007 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC. All rights
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
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/memchecker.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Comm_create_group = PMPI_Comm_create_group
#endif
#define MPI_Comm_create_group PMPI_Comm_create_group
#endif

static const char FUNC_NAME[] = "MPI_Comm_create_group";


int MPI_Comm_create_group (MPI_Comm comm, MPI_Group group, int tag, MPI_Comm *newcomm) {
    int rc;

    MEMCHECKER(
        memchecker_comm(comm);
    );

    if ( MPI_PARAM_CHECK ) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

        if (ompi_comm_invalid (comm))
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COMM,
                                          FUNC_NAME);

        if (tag < 0 || tag > mca_pml.pml_max_tag)
            return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_TAG,
                                          FUNC_NAME);

        if ( NULL == group )
            return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_GROUP,
                                          FUNC_NAME);

        if ( NULL == newcomm )
            return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_ARG,
                                          FUNC_NAME);
    }

    if (MPI_GROUP_NULL == group || MPI_UNDEFINED == ompi_group_rank (group)) {
        *newcomm = MPI_COMM_NULL;
        return MPI_SUCCESS;
    }

    OPAL_CR_ENTER_LIBRARY();

    rc = ompi_comm_create_group ((ompi_communicator_t *) comm, (ompi_group_t *) group,
                                 tag, (ompi_communicator_t **) newcomm);
    OMPI_ERRHANDLER_RETURN (rc, comm, rc, FUNC_NAME);
}
