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
 * Copyright (c) 2007-2009 Cisco Systems, Inc.  All rights reserved.
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

#include "ompi/mpi/c/bindings.h"

int MPI_Allgather(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                  void *recvbuf, int recvcount, MPI_Datatype recvtype,
                  MPI_Comm comm)
{
    char sendtypename[MPI_MAX_OBJECT_NAME], recvtypename[MPI_MAX_OBJECT_NAME];
    char commname[MPI_MAX_OBJECT_NAME];
    int len;
    int rank;

    PMPI_Comm_rank(MPI_COMM_WORLD, &rank);
    PMPI_Type_get_name(sendtype, sendtypename, &len);
    PMPI_Type_get_name(recvtype, recvtypename, &len);
    PMPI_Comm_get_name(comm, commname, &len);

    fprintf(stderr, "MPI_ALLGATHER[%d]: sendbuf %0" PRIxPTR " sendcount %d sendtype %s\n\trecvbuf %0" PRIxPTR " recvcount %d recvtype %s comm %s\n",
           rank, (uintptr_t) sendbuf, sendcount, sendtypename, (uintptr_t) recvbuf, recvcount, recvtypename, commname);
    fflush(stderr);

    return PMPI_Allgather(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, comm);
}

