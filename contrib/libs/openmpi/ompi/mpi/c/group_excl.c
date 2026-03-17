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
 * Copyright (c) 2006      University of Houston. All rights reserved.
 * Copyright (c) 2006-2009 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos Nat Security, LLC. All rights reserved.
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
#include "ompi/group/group.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/communicator/communicator.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Group_excl = PMPI_Group_excl
#endif
#define MPI_Group_excl PMPI_Group_excl
#endif

static const char FUNC_NAME[] = "MPI_Group_excl";


int MPI_Group_excl(MPI_Group group, int n, const int ranks[],
                   MPI_Group *new_group)
{
    ompi_group_t *group_pointer = (ompi_group_t *)group;
    int i, err, group_size;

    group_size = ompi_group_size ( group_pointer);
    if( MPI_PARAM_CHECK ) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

        /* verify that group is valid group */
        if ( (MPI_GROUP_NULL == group)  || (NULL == group) ||
             (NULL == new_group) ) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_GROUP,
                                          FUNC_NAME);
        } else if (NULL == ranks && n > 0) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG,
                                          FUNC_NAME);
        }

        /* check that new group is no larger than old group */
        if ( n > group_size) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_GROUP,
                                          FUNC_NAME);
        }

        /* check to see if procs are within range */
        for( i=0 ; i  < n ; i++ ) {
            if( ( 0 > ranks[i] ) || (ranks[i] >= group_size)){
                return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_RANK,
                                              FUNC_NAME );
            }
        }

    }  /* end if( MPI_PARAM_CHECK ) */

    if ( n == group_size ) {
        *new_group = MPI_GROUP_EMPTY;
        OBJ_RETAIN(MPI_GROUP_EMPTY);
        return MPI_SUCCESS;
    }

    OPAL_CR_ENTER_LIBRARY();

    err = ompi_group_excl ( group, n, ranks, new_group );
    OMPI_ERRHANDLER_RETURN(err, MPI_COMM_WORLD, err, FUNC_NAME );
}
