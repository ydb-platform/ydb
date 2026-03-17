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
 * Copyright (c) 2006-2012 Cisco Systems, Inc.  All rights reserved.
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

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/group/group.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/communicator/communicator.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Group_range_incl = PMPI_Group_range_incl
#endif
#define MPI_Group_range_incl PMPI_Group_range_incl
#endif

static const char FUNC_NAME[] = "MPI_Group_range_incl";


int MPI_Group_range_incl(MPI_Group group, int n_triplets, int ranges[][3],
                         MPI_Group *new_group)
{
    int err, i,indx;
    int group_size;
    int * elements_int_list;

    /* can't act on NULL group */
    if( MPI_PARAM_CHECK ) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

        if ( (MPI_GROUP_NULL == group) || (NULL == group) ||
             (NULL == new_group) ) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_GROUP,
                                          FUNC_NAME);
        }

        group_size = ompi_group_size ( group);
        elements_int_list = (int *) malloc(sizeof(int) * (group_size+1));
        if (NULL == elements_int_list) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_OTHER, FUNC_NAME);
        }
        for (i = 0; i < group_size; i++) {
            elements_int_list[i] = -1;
        }

        for ( i=0; i < n_triplets; i++) {
            if ((0 > ranges[i][0]) || (ranges[i][0] > group_size)) {
                goto error_rank;
            }
            if ((0 > ranges[i][1]) || (ranges[i][1] > group_size)) {
                goto error_rank;
            }
            if (ranges[i][2] == 0) {
                goto error_rank;
            }

            if ((ranges[i][0] < ranges[i][1])) {
                if (ranges[i][2] < 0) {
                    goto error_rank;
                }
                /* positive stride */
                for (indx = ranges[i][0]; indx <= ranges[i][1]; indx += ranges[i][2]) {
                    /* make sure rank has not already been selected */
                    if (elements_int_list[indx] != -1) {
                        goto error_rank;
                    }
                    elements_int_list[indx] = i;
                }
            } else if (ranges[i][0] > ranges[i][1]) {
                if (ranges[i][2] > 0) {
                    goto error_rank;
                }
                /* negative stride */
                for (indx = ranges[i][0]; indx >= ranges[i][1]; indx += ranges[i][2]) {
                    /* make sure rank has not already been selected */
                    if (elements_int_list[indx] != -1) {
                        goto error_rank;
                    }
                    elements_int_list[indx] = i;
                }
            } else {
                /* first_rank == last_rank */
                indx = ranges[i][0];
                if (elements_int_list[indx] != -1) {
                    goto error_rank;
                }
                elements_int_list[indx] = i;
            }
        }

        free ( elements_int_list);
    }

    OPAL_CR_ENTER_LIBRARY();

    err = ompi_group_range_incl ( group, n_triplets, ranges, new_group );
    OMPI_ERRHANDLER_RETURN(err, MPI_COMM_WORLD, err, FUNC_NAME );

error_rank:
    free(elements_int_list);
    return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_RANK, FUNC_NAME);
}
