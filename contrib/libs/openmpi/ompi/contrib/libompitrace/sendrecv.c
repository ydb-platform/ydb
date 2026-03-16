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
#include <string.h>

#include "opal_stdint.h"

#include "ompi/mpi/c/bindings.h"

int MPI_Sendrecv(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                 int dest, int sendtag, void *recvbuf, int recvcount,
                 MPI_Datatype recvtype, int source, int recvtag,
                 MPI_Comm comm,  MPI_Status *status)
{
    char sendtypename[MPI_MAX_OBJECT_NAME], recvtypename[MPI_MAX_OBJECT_NAME];
    char commname[MPI_MAX_OBJECT_NAME];
    int len;
    int rank;
    int size;

    PMPI_Comm_rank(MPI_COMM_WORLD, &rank);
    PMPI_Type_get_name(sendtype, sendtypename, &len);
    PMPI_Type_get_name(sendtype, recvtypename, &len);
    PMPI_Comm_get_name(comm, commname, &len);
    PMPI_Type_size(recvtype, &size);

    fprintf(stderr, "MPI_SENDRECV[%d]: sendbuf %0" PRIxPTR " sendcount %d sendtype %s dest %d sendtag %d\n\t"
           "recvbuf %0" PRIxPTR " recvcount %d recvtype %s source %d recvtag %d comm %s\n",
            rank, (uintptr_t) sendbuf, sendcount, sendtypename, dest, sendtag,
            (uintptr_t) recvbuf, recvcount, recvtypename, source, recvtag, commname);
    fflush(stderr);

    memset(recvbuf, 0, recvcount*size);

    return PMPI_Sendrecv(sendbuf, sendcount, sendtype, dest, sendtag,
                         recvbuf, recvcount, recvtype, source, recvtag,
                         comm, status);
}
