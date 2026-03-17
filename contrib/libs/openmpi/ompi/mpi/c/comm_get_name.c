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
 * Copyright (c) 2006-2008 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <string.h>

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/totalview.h"
#include "opal/threads/mutex.h"
#include "ompi/memchecker.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Comm_get_name = PMPI_Comm_get_name
#endif
#define MPI_Comm_get_name PMPI_Comm_get_name
#endif

static const char FUNC_NAME[] = "MPI_Comm_get_name";


int MPI_Comm_get_name(MPI_Comm comm, char *name, int *length)
{
    MEMCHECKER(
        memchecker_comm(comm);
    );

    OPAL_CR_NOOP_PROGRESS();

    if ( MPI_PARAM_CHECK ) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

        if ( ompi_comm_invalid ( comm ) )
            return OMPI_ERRHANDLER_INVOKE ( MPI_COMM_WORLD, MPI_ERR_COMM,
                                            FUNC_NAME);

        if ( NULL == name || NULL == length )
            return OMPI_ERRHANDLER_INVOKE ( comm, MPI_ERR_ARG,
                                            FUNC_NAME);
    }
    OPAL_THREAD_LOCK(&(comm->c_lock));
    /* Note that MPI-2.1 requires:
       - terminating the string with a \0
       - name[*resultlen] == '\0'
       - and therefore (*resultlen) cannot be > (MPI_MAX_OBJECT_NAME-1)

       The Fortran API version will pad to the right if necessary.

       Note that comm->c_name is guaranteed to be \0-terminated and
       able to completely fit into MPI_MAX_OBJECT_NAME bytes (i.e.,
       name+\0). */
    if ( comm->c_flags & OMPI_COMM_NAMEISSET ) {
        strncpy(name, comm->c_name, MPI_MAX_OBJECT_NAME);
        *length = (int) strlen(comm->c_name);
    } else {
        name[0] = '\0';
        *length = 0;
    }
    OPAL_THREAD_UNLOCK(&(comm->c_lock));

    return MPI_SUCCESS;
}
