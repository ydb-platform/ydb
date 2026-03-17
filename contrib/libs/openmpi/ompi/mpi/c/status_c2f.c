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
 * Copyright (c) 2010-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC.  All rights
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
#include "ompi/mpi/fortran/base/fint_2_int.h"
#include "ompi/mpi/fortran/base/constants.h"
#include "ompi/memchecker.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Status_c2f = PMPI_Status_c2f
#endif
#define MPI_Status_c2f PMPI_Status_c2f
#endif

static const char FUNC_NAME[] = "MPI_Status_c2f";


int MPI_Status_c2f(const MPI_Status *c_status, MPI_Fint *f_status)
{
    const int *c_ints;
    int i;
    MEMCHECKER(
        if(c_status != MPI_STATUSES_IGNORE) {
            /*
             * Before checking the complete status, we need to reset the definedness
             * of the MPI_ERROR-field (single-completion calls wait/test).
             */
            opal_memchecker_base_mem_defined((void*)&c_status->MPI_ERROR, sizeof(int));
            memchecker_status(c_status);
        }
    );

    OPAL_CR_NOOP_PROGRESS();

    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

        /* MPI-2:4.12.5 says that if you pass in
           MPI_STATUS[ES]_IGNORE, it's erroneous */

        if (NULL == c_status || MPI_STATUS_IGNORE == c_status ||
            MPI_STATUSES_IGNORE == c_status || NULL == f_status) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD,
                                          MPI_ERR_IN_STATUS, FUNC_NAME);
        }
    }

    /* Note that MPI-2.2 16.3.5 states that even the hidden data in a
       status must be converted (!).  This is somewhat problematic
       because the Fortran data is all INTEGERS while the C MPI_Status
       contains a size_t.  That being said, note 2 things:

       1. The _ucount and _canceled members are never accessed from
          Fortran.
       2. configure calculated a value of MPI_STATUS_SIZE to ensure
          that the Fortran status is the Right size to hold the C
          MPI_Status (including the size_t member).

       So for the purposes of this function, just copy over all the
       data as if they were int's.  This works because all OMPI
       Fortran MPI API functions that take a status as an IN argument
       first call MPI_Status_f2c on it before using it (in which case
       we'll do the exact opposite copy, thereby rebuilding the size_t
       value properly before it is accessed in C).

       Note that if sizeof(int) > sizeof(INTEGER), we're potentially
       hosed anyway (i.e., even the public values in the status could
       get truncated).  But if sizeof(int) == sizeof(INTEGER) or
       sizeof(int) < sizeof(INTEGER), everything should be kosher. */
    c_ints = (const int*)c_status;
    for( i = 0; i < (int)(sizeof(MPI_Status) / sizeof(int)); i++ )
        f_status[i] = OMPI_INT_2_FINT(c_ints[i]);

    return MPI_SUCCESS;
}
