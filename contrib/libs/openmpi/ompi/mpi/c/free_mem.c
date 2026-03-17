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
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
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
#include <stdlib.h>

#include "ompi/mpi/c/bindings.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "opal/mca/mpool/mpool.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Free_mem = PMPI_Free_mem
#endif
#define MPI_Free_mem PMPI_Free_mem
#endif

static const char FUNC_NAME[] = "MPI_Free_mem";


int MPI_Free_mem(void *baseptr)
{
    OPAL_CR_ENTER_LIBRARY();

    /* Per these threads:

         http://www.open-mpi.org/community/lists/devel/2007/07/1977.php
         http://www.open-mpi.org/community/lists/devel/2007/07/1979.php

       If you call MPI_ALLOC_MEM with a size of 0, you get NULL
       back.  So don't consider a NULL==baseptr an error. */
    if (NULL != baseptr && OMPI_SUCCESS != mca_mpool_base_free(baseptr)) {
        OPAL_CR_EXIT_LIBRARY();
        return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_NO_MEM, FUNC_NAME);
    }

    OPAL_CR_EXIT_LIBRARY();
    return MPI_SUCCESS;
}

