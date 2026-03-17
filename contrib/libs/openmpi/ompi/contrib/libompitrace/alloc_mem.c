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
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <stdio.h>

#include "ompi/mpi/c/bindings.h"

int MPI_Alloc_mem(MPI_Aint size, MPI_Info info, void *baseptr)
{

    int rank;

    PMPI_Comm_rank(MPI_COMM_WORLD, &rank);

    fprintf(stderr, "MPI_Alloc_mem[%d]: size %0ld\n", rank, (long)size);
    fflush(stderr);

    return PMPI_Alloc_mem(size, info, baseptr);
}

