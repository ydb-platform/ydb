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

int ompi_group_calc_sporadic ( int n , const int *ranks)
{
    int i,l=0;
    for (i=0 ; i<n ; i++) {
        if(ranks[i] == ranks[i-1]+1) {
            if(l==0) {
                l++;
            }
        }
        else {
            l++;
        }
    }
    return sizeof(struct ompi_group_sporadic_list_t ) * l;
}

/* from parent group to child group*/
int ompi_group_translate_ranks_sporadic ( ompi_group_t *parent_group,
                                          int n_ranks, const int *ranks1,
                                          ompi_group_t *child_group,
                                          int *ranks2)
{
    int i,count,j;
    for (j=0 ; j<n_ranks ; j++) {
        if (MPI_PROC_NULL == ranks1[j]) {
            ranks2[j] = MPI_PROC_NULL;
        }
        else {
            /*
             * if the rank is in the current range of the sporadic list, we calculate
             * the rank in the child by adding the length of all ranges that we passed
             * and the position in the current range
             */
            ranks2[j] = MPI_UNDEFINED;
            count = 0;
            for(i=0 ; i <child_group->sparse_data.grp_sporadic.grp_sporadic_list_len ; i++) {
                if( child_group->sparse_data.grp_sporadic.grp_sporadic_list[i].rank_first
                    <= ranks1[j] && ranks1[j] <=
                    child_group->sparse_data.grp_sporadic.grp_sporadic_list[i].rank_first +
                    child_group->sparse_data.grp_sporadic.grp_sporadic_list[i].length -1 ) {

                    ranks2[j] = ranks1[j] - child_group->
                        sparse_data.grp_sporadic.grp_sporadic_list[i].rank_first + count;
                    break;
                }
                else {
                    count = count + child_group->sparse_data.grp_sporadic.grp_sporadic_list[i].length;
                }
            }
        }
    }
    return OMPI_SUCCESS;
}
/* from child group to parent group*/
int ompi_group_translate_ranks_sporadic_reverse ( ompi_group_t *child_group,
                                                  int n_ranks, const int *ranks1,
                                                  ompi_group_t *parent_group,
                                                  int *ranks2)
{
    int i,j,count;

    for (j=0 ; j<n_ranks ; j++) {
        if (MPI_PROC_NULL == ranks1[j]) {
            ranks2[j] = MPI_PROC_NULL;
        }
        else {
            count = 0;
            /*
             * if the rank of the child is in the current range, the rank of the parent will be
             * the position in the current range of the sporadic list
             */
            for (i=0 ; i<child_group->sparse_data.grp_sporadic.grp_sporadic_list_len ; i++) {
                if ( ranks1[j] > ( count +
                                   child_group->sparse_data.grp_sporadic.grp_sporadic_list[i].length
                                   - 1) ) {
                    count = count + child_group->sparse_data.grp_sporadic.grp_sporadic_list[i].length;
                }
                else {
                    ranks2[j] = child_group->sparse_data.grp_sporadic.grp_sporadic_list[i].rank_first
                        + (ranks1[j] - count);
                    break;
                }
            }
        }
    }
    return OMPI_SUCCESS;
}

int ompi_group_incl_spor(ompi_group_t* group, int n, const int *ranks,
                         ompi_group_t **new_group)
{
    /* local variables */
    int my_group_rank,l,i,j,proc_count;
    ompi_group_t *group_pointer, *new_group_pointer;

    group_pointer = (ompi_group_t *)group;

    if (0 == n) {
        *new_group = MPI_GROUP_EMPTY;
        OBJ_RETAIN(MPI_GROUP_EMPTY);
        return OMPI_SUCCESS;
    }

    l=0;
    j=0;
    proc_count = 0;

    for(i=0 ; i<n ; i++){
        if(ranks[i] == ranks[i-1]+1) {
            if(l==0) {
                l++;
            }
        }
        else {
            l++;
        }
    }

    new_group_pointer = ompi_group_allocate_sporadic(l);
    if( NULL == new_group_pointer ) {
        return MPI_ERR_GROUP;
    }

    new_group_pointer ->
        sparse_data.grp_sporadic.grp_sporadic_list[j].rank_first = ranks[0];
    new_group_pointer ->
        sparse_data.grp_sporadic.grp_sporadic_list[j].length = 1;

    for(i=1 ; i<n ; i++){
        if(ranks[i] == ranks[i-1]+1) {
            new_group_pointer -> sparse_data.grp_sporadic.grp_sporadic_list[j].length ++;
        }
        else {
            j++;
            new_group_pointer ->
                sparse_data.grp_sporadic.grp_sporadic_list[j].rank_first = ranks[i];
            new_group_pointer ->
                sparse_data.grp_sporadic.grp_sporadic_list[j].length = 1;
        }
    }

    new_group_pointer->sparse_data.grp_sporadic.grp_sporadic_list_len = j+1;
    new_group_pointer -> grp_parent_group_ptr = group_pointer;

    OBJ_RETAIN(new_group_pointer -> grp_parent_group_ptr);
    ompi_group_increment_proc_count(new_group_pointer -> grp_parent_group_ptr);

    for(i=0 ; i<new_group_pointer->sparse_data.grp_sporadic.grp_sporadic_list_len ; i++) {
        proc_count = proc_count + new_group_pointer ->
            sparse_data.grp_sporadic.grp_sporadic_list[i].length;
    }
    new_group_pointer->grp_proc_count = proc_count;

    ompi_group_increment_proc_count(new_group_pointer);
    my_group_rank=group_pointer->grp_my_rank;

    ompi_group_translate_ranks (group_pointer,1,&my_group_rank,
                                new_group_pointer,&new_group_pointer->grp_my_rank);

    *new_group = (MPI_Group)new_group_pointer;

    return OMPI_SUCCESS;
}
