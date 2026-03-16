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
 * Copyright (c) 2009      Sun Microsystmes, Inc.  All rights reserved.
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

int MPI_Accumulate(const void *origin_addr, int origin_count, MPI_Datatype origin_datatype,
                   int target_rank, MPI_Aint target_disp, int target_count,
                   MPI_Datatype target_datatype, MPI_Op op, MPI_Win win)
{

    char typename[MPI_MAX_OBJECT_NAME], target_dt[MPI_MAX_OBJECT_NAME];
    char winname[MPI_MAX_OBJECT_NAME];
    int len;
    int rank;

    PMPI_Comm_rank(MPI_COMM_WORLD, &rank);
    PMPI_Type_get_name(origin_datatype, typename, &len);
    PMPI_Type_get_name(target_datatype, target_dt, &len);
    PMPI_Win_get_name(win, winname, &len);

    fprintf(stderr, "MPI_ACCUMULATE[%d]: origin_addr %0" PRIxPTR " origin_count %d origin_datatype %s\n"
            "\ttarget_rank %d target_disp %" PRIdPTR " target_count %d target_datatype %s op %s win %s\n",
            rank, (uintptr_t)origin_addr, origin_count, typename, target_rank, (intptr_t) target_disp,
            target_count, target_dt, op->o_name, winname);
    fflush(stderr);

    return PMPI_Accumulate(origin_addr, origin_count, origin_datatype,
                           target_rank, target_disp, target_count,
                           target_datatype, op, win);
}
