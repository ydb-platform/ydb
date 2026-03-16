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
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <stdio.h>

#include "opal_stdint.h"

#include "ompi/mpi/c/bindings.h"


int MPI_Bcast(void *buffer, int count, MPI_Datatype datatype,
              int root, MPI_Comm comm)
{
    char typename[MPI_MAX_OBJECT_NAME], commname[MPI_MAX_OBJECT_NAME];
    int len;
    int rank;

    PMPI_Comm_rank(MPI_COMM_WORLD, &rank);
    PMPI_Type_get_name(datatype, typename, &len);
    PMPI_Comm_get_name(comm, commname, &len);

    fprintf(stderr, "MPI_BCAST[%d]: buffer %0" PRIxPTR " count %d datatype %s root %d comm %s\n",
           rank, (uintptr_t) buffer, count, typename, root, commname);
    fflush(stderr);

    return PMPI_Bcast(buffer, count, datatype, root, comm);
}
