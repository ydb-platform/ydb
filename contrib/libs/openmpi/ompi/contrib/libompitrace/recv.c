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

int MPI_Recv(void *buf, int count, MPI_Datatype type, int source,
             int tag, MPI_Comm comm, MPI_Status *status)
{
    char typename[MPI_MAX_OBJECT_NAME], commname[MPI_MAX_OBJECT_NAME];
    int len;
    int rank;

    PMPI_Comm_rank(MPI_COMM_WORLD, &rank);
    PMPI_Type_get_name(type, typename, &len);
    PMPI_Comm_get_name(comm, commname, &len);

    fprintf(stderr, "MPI_RECV[%d]: buf %0" PRIxPTR " count %d datatype %s source %d tag %d comm %s\n",
           rank, (uintptr_t) buf, count, typename, source, tag, commname);
    fflush(stderr);

    return PMPI_Recv(buf, count, type, source, tag, comm, status);
}
