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
#include "ompi/group/group.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Group_free = PMPI_Group_free
#endif
#define MPI_Group_free PMPI_Group_free
#endif

static const char FUNC_NAME[] = "MPI_Group_free";


int MPI_Group_free(MPI_Group *group)
{
    int ret;

    /* check to make sure we don't free GROUP_NULL.  Note that we *do*
       allow freeing GROUP_EMPTY after much debate in the OMPI core
       group.  The final thread about this, and the decision to
       support freeing GROUP_EMPTY can be found here:

       http://www.open-mpi.org/community/lists/devel/2007/12/2750.php

       The short version: other MPI's allow it (LAM/MPI, CT6, MPICH2)
       probably mainly because the Intel MPI test suite expects it to
       happen and there's now several years worth of expected behavior
       to allow this behavior.  Rather than have to explain every time
       why OMPI is the only one who completely adheres to the standard
       / fails the intel tests, it seemed easier to just let this one
       slide.  It's not really that important, after all! */
    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

        if ((NULL == group) ||
            (MPI_GROUP_NULL == *group) || (NULL == *group) ) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_GROUP,
                                          FUNC_NAME);
        }

    }

    OPAL_CR_ENTER_LIBRARY();

    ret = ompi_group_free ( group);
    OMPI_ERRHANDLER_CHECK(ret, MPI_COMM_WORLD, ret, FUNC_NAME);

    OPAL_CR_EXIT_LIBRARY();
    return MPI_SUCCESS;
}
