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
 * Copyright (c) 2006      University of Houston. All rights reserved.
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

int MPI_Add_error_code(int errorclass, int *errorcode)
{
    int rank;

    PMPI_Comm_rank(MPI_COMM_WORLD, &rank);

    fprintf(stderr, "MPI_ADD_ERROR_CODE[%d]: errorclass %d errcode %0" PRIxPTR "\n", rank, errorclass, (uintptr_t)errorcode);
    fflush(stderr);

    return PMPI_Add_error_code(errorclass, errorcode);
}
