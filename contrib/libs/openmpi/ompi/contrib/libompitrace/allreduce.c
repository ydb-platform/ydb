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
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <stdio.h>

#include "opal_stdint.h"
#include "ompi/op/op.h"
#include "ompi/mpi/c/bindings.h"

int MPI_Allreduce(const void *sendbuf, void *recvbuf, int count,
                  MPI_Datatype datatype, MPI_Op op, MPI_Comm comm)
{
    char typename[MPI_MAX_OBJECT_NAME], commname[MPI_MAX_OBJECT_NAME];
    int len;
    int rank;

    PMPI_Comm_rank(MPI_COMM_WORLD, &rank);
    PMPI_Type_get_name(datatype, typename, &len);
    PMPI_Comm_get_name(comm, commname, &len);

    fprintf(stderr, "MPI_ALLREDUCE[%d]: sendbuf %0" PRIxPTR " recvbuf %0" PRIxPTR " count %d datatype %s op %s comm %s\n",
           rank, (uintptr_t)sendbuf, (uintptr_t)recvbuf, count, typename, op->o_name, commname);
    fflush(stderr);

    return PMPI_Allreduce(sendbuf, recvbuf, count, datatype, op, comm);
}
