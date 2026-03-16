/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2010 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
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
#include <limits.h>

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/memchecker.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Get_count = PMPI_Get_count
#endif
#define MPI_Get_count PMPI_Get_count
#endif

static const char FUNC_NAME[] = "MPI_Get_count";


int MPI_Get_count(const MPI_Status *status, MPI_Datatype datatype, int *count)
{
    size_t size = 0, internal_count;
    int rc      = MPI_SUCCESS;

    OPAL_CR_NOOP_PROGRESS();

    MEMCHECKER(
               if (status != MPI_STATUSES_IGNORE) {
                   /*
                    * Before checking the complete status, we need to reset the definedness
                    * of the MPI_ERROR-field (single-completion calls wait/test).
                    */
                   opal_memchecker_base_mem_defined((void*)&status->MPI_ERROR, sizeof(int));
                   memchecker_status(status);
                   memchecker_datatype(datatype);
               }
               );

    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        OMPI_CHECK_DATATYPE_FOR_RECV(rc, datatype, 1);

        OMPI_ERRHANDLER_CHECK(rc, MPI_COMM_WORLD, rc, FUNC_NAME);
    }

    if( ompi_datatype_type_size( datatype, &size ) == MPI_SUCCESS ) {
        if( size == 0 ) {
            *count = 0;
        } else {
            internal_count = status->_ucount / size; /* count the number of complete datatypes */
            if( (internal_count * size) != status->_ucount ||
                internal_count > ((size_t) INT_MAX) ) {
                *count = MPI_UNDEFINED;
            } else {
                *count = (int)internal_count;
            }
        }
    }
    return MPI_SUCCESS;
}
