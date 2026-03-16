/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2007 University of Houston. All rights reserved.
 * Copyright (c) 2007      Cisco Systems, Inc. All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "ompi/group/group.h"
#include "ompi/constants.h"
#include "mpi.h"

static int check_stride(const int[],int);

int ompi_group_calc_strided ( int n , const int *ranks ) {
    if(-1 == check_stride(ranks,n)) {
        return -1;
    }
    else {
        return (sizeof(int)*3);
    }
}

/* from parent group to child group*/
int ompi_group_translate_ranks_strided (ompi_group_t *parent_group,
                                        int n_ranks, const int *ranks1,
                                        ompi_group_t *child_group,
                                        int *ranks2)
{
    int s,o,l,i;
    s = child_group->sparse_data.grp_strided.grp_strided_stride;
    o = child_group->sparse_data.grp_strided.grp_strided_offset;
    l = child_group->sparse_data.grp_strided.grp_strided_last_element;
    for (i = 0; i < n_ranks; i++) {
        if ( MPI_PROC_NULL == ranks1[i]) {
            ranks2[i] = MPI_PROC_NULL;
        }
        else {
            ranks2[i] = MPI_UNDEFINED;

            if ( (ranks1[i]-o) >= 0  && (ranks1[i]-o)%s == 0 && ranks1[i] <= l) {
                ranks2[i] = (ranks1[i] - o)/s;
            }
        }
    }
    return OMPI_SUCCESS;
}

/* from child group to parent group*/
int ompi_group_translate_ranks_strided_reverse (ompi_group_t *child_group,
                                                int n_ranks, const int *ranks1,
                                                ompi_group_t *parent_group,
                                                int *ranks2)
{
    int s,o,i;
    s = child_group->sparse_data.grp_strided.grp_strided_stride;
    o = child_group->sparse_data.grp_strided.grp_strided_offset;
    for (i = 0; i < n_ranks; i++) {
        if ( MPI_PROC_NULL == ranks1[i]) {
            ranks2[i] = MPI_PROC_NULL;
        }
        else {
            ranks2[i] =s*ranks1[i] + o;
        }
    }
    return OMPI_SUCCESS;
}

static int check_stride(const int incl[],int incllen) {
    int s,i;
    if (incllen > 1) {
        s = incl[1] - incl[0];
    }
    else {
        s = 1;
    }
    if( s < 0 ) {
        return -1;
    }
    for(i=0 ; i < incllen-1 ; i++) {
    	if(incl[i+1] - incl[i] != s) {
            return -1;
        }
    }
    return s;
}

int ompi_group_incl_strided(ompi_group_t* group, int n, const int *ranks,
                            ompi_group_t **new_group)
{
    /* local variables */
    int my_group_rank,stride;
    ompi_group_t *group_pointer, *new_group_pointer;

    group_pointer = (ompi_group_t *)group;

    if ( 0 == n ) {
        *new_group = MPI_GROUP_EMPTY;
        OBJ_RETAIN(MPI_GROUP_EMPTY);
        return OMPI_SUCCESS;
    }

    stride = check_stride(ranks,n);
    new_group_pointer = ompi_group_allocate_strided();
    if( NULL == new_group_pointer ) {
        return MPI_ERR_GROUP;
    }
    new_group_pointer -> grp_parent_group_ptr = group_pointer;

    OBJ_RETAIN(new_group_pointer -> grp_parent_group_ptr);
    ompi_group_increment_proc_count(new_group_pointer -> grp_parent_group_ptr);

    new_group_pointer -> sparse_data.grp_strided.grp_strided_stride = stride;
    new_group_pointer -> sparse_data.grp_strided.grp_strided_offset = ranks[0];
    new_group_pointer -> sparse_data.grp_strided.grp_strided_last_element = ranks[n-1];
    new_group_pointer -> grp_proc_count = n;

    ompi_group_increment_proc_count(new_group_pointer);
    my_group_rank = group_pointer->grp_my_rank;
    ompi_group_translate_ranks (new_group_pointer->grp_parent_group_ptr,1,&my_group_rank,
                                new_group_pointer,&new_group_pointer->grp_my_rank);

    *new_group = (MPI_Group)new_group_pointer;

    return OMPI_SUCCESS;
}
