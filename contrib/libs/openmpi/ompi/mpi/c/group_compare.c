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
 * Copyright (c) 2006      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2009      University of Houston. All rights reserved.
 * Copyright (c) 2012      Oak Ridge National Labs.  All rights reserved.
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
#include "ompi/proc/proc.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Group_compare = PMPI_Group_compare
#endif
#define MPI_Group_compare PMPI_Group_compare
#endif

static const char FUNC_NAME[] = "MPI_Group_compare";


int MPI_Group_compare(MPI_Group group1, MPI_Group group2, int *result) {
    int return_value = MPI_SUCCESS;

    /* check for errors */
    if( MPI_PARAM_CHECK ) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

        if( ( MPI_GROUP_NULL == group1 ) || ( MPI_GROUP_NULL == group2 ) ||
                (NULL == group1) || (NULL==group2) ){
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_GROUP,
                                          FUNC_NAME);
        } else if (NULL == result) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG,
                                          FUNC_NAME);
        }
    }

    OPAL_CR_NOOP_PROGRESS();

    return_value = ompi_group_compare((ompi_group_t *)group1, (ompi_group_t *)group2, result);

    return return_value;
}
