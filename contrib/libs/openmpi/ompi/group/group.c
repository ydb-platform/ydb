/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2013 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2007 University of Houston. All rights reserved.
 * Copyright (c) 2007      Cisco Systems, Inc. All rights reserved.
 * Copyright (c) 2012      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2012-2013 Inria.  All rights reserved.
 * Copyright (c) 2013-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2015-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "ompi/group/group.h"
#include "ompi/constants.h"
#include "ompi/proc/proc.h"
#include "ompi/runtime/params.h"
#include "mpi.h"

int ompi_group_free ( ompi_group_t **group )
{
    ompi_group_t *l_group;

    l_group = (ompi_group_t *) *group;
    OBJ_RELEASE(l_group);

    *group = MPI_GROUP_NULL;
    return OMPI_SUCCESS;
}

int ompi_group_translate_ranks ( ompi_group_t *group1,
                                 int n_ranks, const int *ranks1,
                                 ompi_group_t *group2,
                                 int *ranks2)
{
    if ( MPI_GROUP_EMPTY == group1 || MPI_GROUP_EMPTY == group2 ) {
        for (int proc = 0; proc < n_ranks ; ++proc) {
            ranks2[proc] = MPI_UNDEFINED;
        }
        return MPI_SUCCESS;
    }

#if OMPI_GROUP_SPARSE
    /*
     * If we are translating from a parent to a child that uses the sparse format
     * or vice versa, we use the translate ranks function corresponding to the
     * format used. Generally, all these functions require less time than the
     * original method that loops over the processes of both groups till we
     * find a match.
     */
    if( group1->grp_parent_group_ptr == group2 ) { /* from child to parent */
        if(OMPI_GROUP_IS_SPORADIC(group1)) {
            return ompi_group_translate_ranks_sporadic_reverse
                (group1,n_ranks,ranks1,group2,ranks2);
        }
        else if(OMPI_GROUP_IS_STRIDED(group1)) {
            return ompi_group_translate_ranks_strided_reverse
                (group1,n_ranks,ranks1,group2,ranks2);
        }
        else if(OMPI_GROUP_IS_BITMAP(group1)) {
            return ompi_group_translate_ranks_bmap_reverse
                (group1,n_ranks,ranks1,group2,ranks2);
        }

        /* unknown sparse group type */
        assert (0);
    }

    if( group2->grp_parent_group_ptr == group1 ) { /* from parent to child*/
        if(OMPI_GROUP_IS_SPORADIC(group2)) {
            return ompi_group_translate_ranks_sporadic
                (group1,n_ranks,ranks1,group2,ranks2);
        }
        else if(OMPI_GROUP_IS_STRIDED(group2)) {
            return ompi_group_translate_ranks_strided
                (group1,n_ranks,ranks1,group2,ranks2);
        }
        else if(OMPI_GROUP_IS_BITMAP(group2)) {
            return ompi_group_translate_ranks_bmap
                (group1,n_ranks,ranks1,group2,ranks2);
        }

        /* unknown sparse group type */
        assert (0);
    }
#endif

    /* loop over all ranks */
    for (int proc = 0; proc < n_ranks; ++proc) {
        struct ompi_proc_t *proc1_pointer, *proc2_pointer;
        int rank = ranks1[proc];

        if ( MPI_PROC_NULL == rank) {
            ranks2[proc] = MPI_PROC_NULL;
            continue;
        }

        proc1_pointer = ompi_group_get_proc_ptr_raw (group1, rank);
        /* initialize to no "match" */
        ranks2[proc] = MPI_UNDEFINED;
        for (int proc2 = 0; proc2 < group2->grp_proc_count; ++proc2) {
            proc2_pointer = ompi_group_get_proc_ptr_raw (group2, proc2);
            if ( proc1_pointer == proc2_pointer) {
                ranks2[proc] = proc2;
                break;
            }
        }  /* end proc2 loop */
    } /* end proc loop */

    return MPI_SUCCESS;
}

int ompi_group_dump (ompi_group_t* group)
{
    int i;
    int new_rank;

    i=0;
    printf("Group Proc Count: %d\n",group->grp_proc_count);
    printf("Group My Rank: %d\n",group->grp_my_rank);
    if (OMPI_GROUP_IS_SPORADIC(group)) {
        ompi_group_translate_ranks( group,1,&group->grp_my_rank,
                                    group->grp_parent_group_ptr,
                                    &new_rank);
        printf("Rank in the parent group: %d\n",new_rank);
        printf("The Sporadic List Length: %d\n",
               group->sparse_data.grp_sporadic.grp_sporadic_list_len);
        printf("Rank First       Length\n");
        for(i=0 ; i<group->sparse_data.grp_sporadic.grp_sporadic_list_len ; i++) {
            printf("%d               %d\n",
                   group->sparse_data.grp_sporadic.grp_sporadic_list[i].rank_first,
                   group->sparse_data.grp_sporadic.grp_sporadic_list[i].length);
        }
    }
    else if (OMPI_GROUP_IS_STRIDED(group)) {
        ompi_group_translate_ranks( group,1,&group->grp_my_rank,
                                    group->grp_parent_group_ptr,
                                    &new_rank);
        printf("Rank in the parent group: %d\n",new_rank);
        printf("The Offset is: %d\n",group->sparse_data.grp_strided.grp_strided_offset);
        printf("The Stride is: %d\n",group->sparse_data.grp_strided.grp_strided_stride);
        printf("The Last Element is: %d\n",
               group->sparse_data.grp_strided.grp_strided_last_element);
    }
    else if (OMPI_GROUP_IS_BITMAP(group)) {
        ompi_group_translate_ranks( group,1,&group->grp_my_rank,
                                    group->grp_parent_group_ptr,
                                    &new_rank);
        printf("Rank in the parent group: %d\n",new_rank);
        printf("The length of the bitmap array is: %d\n",
               group->sparse_data.grp_bitmap.grp_bitmap_array_len);
        for (i=0 ; i<group->sparse_data.grp_bitmap.grp_bitmap_array_len ; i++) {
            printf("%d\t",group->sparse_data.grp_bitmap.grp_bitmap_array[i]);
        }
    }
    printf("*********************************************************\n");
    return OMPI_SUCCESS;
}

int ompi_group_minloc ( int list[] , int length )
{
    int i,index,min;
    min = list[0];
    index = 0;

    for (i=0 ; i<length ; i++) {
        if (min > list[i] && list[i] != -1) {
            min = list[i];
            index = i;
        }
    }
    return index;
}

int ompi_group_incl(ompi_group_t* group, int n, const int *ranks, ompi_group_t **new_group)
{
    int method,result;

    method = 0;
#if OMPI_GROUP_SPARSE
    if (ompi_use_sparse_group_storage) {
        int len [4];

        len[0] = ompi_group_calc_plist    ( n ,ranks );
        len[1] = ompi_group_calc_strided  ( n ,ranks );
        len[2] = ompi_group_calc_sporadic ( n ,ranks );
        len[3] = ompi_group_calc_bmap     ( n , group->grp_proc_count ,ranks );

        /* determin minimum length */
        method = ompi_group_minloc ( len, 4 );
    }
#endif

    switch (method)
        {
        case 0:
            result = ompi_group_incl_plist(group, n, ranks, new_group);
            break;
        case 1:
            result = ompi_group_incl_strided(group, n, ranks, new_group);
            break;
        case 2:
            result = ompi_group_incl_spor(group, n, ranks, new_group);
            break;
        default:
            result = ompi_group_incl_bmap(group, n, ranks, new_group);
            break;
        }

    return result;
}

int ompi_group_excl(ompi_group_t* group, int n, const int *ranks, ompi_group_t **new_group)
{
    int i, j, k, result;
    int *ranks_included=NULL;

    /* determine the list of included processes for the excl-method */
    k = 0;
    if (0 < (group->grp_proc_count - n)) {
        ranks_included = (int *)malloc( (group->grp_proc_count-n)*(sizeof(int)));

        for (i=0 ; i<group->grp_proc_count ; i++) {
            for(j=0 ; j<n ; j++) {
                if(ranks[j] == i) {
                    break;
                }
            }
            if (j==n) {
                ranks_included[k] = i;
                k++;
            }
        }
    }

    result = ompi_group_incl(group, k, ranks_included, new_group);

    if (NULL != ranks_included) {
        free(ranks_included);
    }

    return result;
}

int ompi_group_range_incl(ompi_group_t* group, int n_triplets, int ranges[][3],
                          ompi_group_t **new_group)
{
    int j,k;
    int *ranks_included=NULL;
    int index,first_rank,last_rank,stride;
    int count,result;

    count = 0;
    /* determine the number of included processes for the range-incl-method */
    k = 0;
    for(j=0 ; j<n_triplets ; j++) {

        first_rank = ranges[j][0];
        last_rank = ranges[j][1];
        stride = ranges[j][2];

        if (first_rank < last_rank) {
            /* positive stride */
            index = first_rank;
            while (index <= last_rank) {
                count ++;
                k++;
                index += stride;
            }                   /* end while loop */
        }
        else if (first_rank > last_rank) {
            /* negative stride */
            index = first_rank;
            while (index >= last_rank) {
                count ++;
                k++;
                index += stride;
            }                   /* end while loop */

        } else {                /* first_rank == last_rank */
            index = first_rank;
            count ++;
            k++;
        }
    }
    if (0 != count) {
        ranks_included = (int *)malloc( (count)*(sizeof(int)));
    }
    /* determine the list of included processes for the range-incl-method */
    k = 0;
    for(j=0 ; j<n_triplets ; j++) {

        first_rank = ranges[j][0];
        last_rank = ranges[j][1];
        stride = ranges[j][2];

        if (first_rank < last_rank) {
            /* positive stride */
            index = first_rank;
            while (index <= last_rank) {
                ranks_included[k] = index;
                k++;
                index += stride;
            }                   /* end while loop */
        }
        else if (first_rank > last_rank) {
            /* negative stride */
            index = first_rank;
            while (index >= last_rank) {
                ranks_included[k] = index;
                k++;
                index += stride;
            }                   /* end while loop */

        } else {                /* first_rank == last_rank */
            index = first_rank;
            ranks_included[k] = index;
            k++;
        }
    }

    result = ompi_group_incl(group, k, ranks_included, new_group);

    if (NULL != ranks_included) {
        free(ranks_included);
    }
    return result;
}

int ompi_group_range_excl(ompi_group_t* group, int n_triplets, int ranges[][3],
                          ompi_group_t **new_group)
{

    int j,k,i;
    int *ranks_included=NULL, *ranks_excluded=NULL;
    int index,first_rank,last_rank,stride,count,result;

    count = 0;

    /* determine the number of excluded processes for the range-excl-method */
    k = 0;
    for(j=0 ; j<n_triplets ; j++) {
        first_rank = ranges[j][0];
        last_rank = ranges[j][1];
        stride = ranges[j][2];

        if (first_rank < last_rank) {
            /* positive stride */
            index = first_rank;
            while (index <= last_rank) {
                count ++;
                index += stride;
            }                   /* end while loop */
        }
        else if (first_rank > last_rank) {
            /* negative stride */
            index = first_rank;
            while (index >= last_rank) {
                count ++;
                index += stride;
            }                   /* end while loop */

        } else {                /* first_rank == last_rank */
            index = first_rank;
            count ++;
        }
    }
    if (0 != count) {
        ranks_excluded = (int *)malloc( (count)*(sizeof(int)));
    }
    /* determine the list of included processes for the range-excl-method */
    k = 0;
    i = 0;
    for(j=0 ; j<n_triplets ; j++) {
        first_rank = ranges[j][0];
        last_rank = ranges[j][1];
        stride = ranges[j][2];

        if (first_rank < last_rank) {
            /* positive stride */
            index = first_rank;
            while (index <= last_rank) {
                ranks_excluded[i] = index;
                i++;
                index += stride;
            }                   /* end while loop */
        }
        else if (first_rank > last_rank) {
            /* negative stride */
            index = first_rank;
            while (index >= last_rank) {
                ranks_excluded[i] = index;
                i++;
                index += stride;
            }                   /* end while loop */

        } else {                /* first_rank == last_rank */
            index = first_rank;
            ranks_excluded[i] = index;
            i++;
        }
    }
    if (0 != (group->grp_proc_count - count)) {
        ranks_included = (int *)malloc( (group->grp_proc_count - count)*(sizeof(int)));
    }
    for (j=0 ; j<group->grp_proc_count ; j++) {
        for(index=0 ; index<i ; index++) {
            if(ranks_excluded[index] == j) break;
        }
        if (index == i) {
            ranks_included[k] = j;
            k++;
        }
    }
    if (NULL != ranks_excluded) {
        free(ranks_excluded);
    }

    result = ompi_group_incl(group, k, ranks_included, new_group);

    if (NULL != ranks_included) {
        free(ranks_included);
    }

    return result;
}

int ompi_group_intersection(ompi_group_t* group1,ompi_group_t* group2,
                            ompi_group_t **new_group)
{
    int proc1,proc2,k, result;
    int *ranks_included=NULL;
    ompi_group_t *group1_pointer, *group2_pointer;
    ompi_proc_t *proc1_pointer, *proc2_pointer;

    group1_pointer=(ompi_group_t *)group1;
    group2_pointer=(ompi_group_t *)group2;

    k = 0;
    /* allocate the max required memory */
    if (0 < group1_pointer->grp_proc_count) {
        ranks_included = (int *)malloc(group1_pointer->grp_proc_count*(sizeof(int)));
        if (NULL == ranks_included) {
            return MPI_ERR_NO_MEM;
        }
    }
    /* determine the list of included processes for the incl-method */
    k = 0;
    for (proc1 = 0; proc1 < group1_pointer->grp_proc_count; proc1++) {
        proc1_pointer = ompi_group_peer_lookup (group1_pointer , proc1);

        /* check to see if this proc is in group2 */

        for (proc2 = 0; proc2 < group2_pointer->grp_proc_count; proc2++) {
            proc2_pointer = ompi_group_peer_lookup (group2_pointer ,proc2);

            if( proc1_pointer == proc2_pointer ) {
                ranks_included[k] = proc1;
                k++;
                break;
            }
        }  /* end proc2 loop */
    }  /* end proc1 loop */

    result = ompi_group_incl(group1, k, ranks_included, new_group);

    if (NULL != ranks_included) {
        free(ranks_included);
    }

    return result;
}

int ompi_group_compare(ompi_group_t *group1,
                       ompi_group_t *group2,
                       int *result)
{
    int return_value = OMPI_SUCCESS;
    int proc1, proc2, match;
    bool similar, identical;
    ompi_group_t *group1_pointer, *group2_pointer;
    ompi_proc_t *proc1_pointer, *proc2_pointer;

    /* check for same groups */
    if( group1 == group2 ) {
        *result=MPI_IDENT;
        return return_value;
    }

    /* check to see if either is MPI_GROUP_NULL or MPI_GROUP_EMPTY */
    if( ( MPI_GROUP_EMPTY == group1 ) || ( MPI_GROUP_EMPTY == group2 ) ) {
        *result=MPI_UNEQUAL;
        return return_value;
    }

    /* get group pointers */
    group1_pointer = (ompi_group_t *)group1;
    group2_pointer = (ompi_group_t *)group2;

    /* compare sizes */
    if( group1_pointer->grp_proc_count != group2_pointer->grp_proc_count ) {
        /* if not same size - return */
        *result=MPI_UNEQUAL;
        return return_value;
    }

    /* check for similarity */
    /* loop over group1 processes */
    similar=true;
    identical=true;
    for(proc1=0 ; proc1 < group1_pointer->grp_proc_count ; proc1++ ) {
        proc1_pointer= ompi_group_peer_lookup(group1_pointer,proc1);
        /* loop over group2 processes to find "match" */
        match=-1;
        for(proc2=0 ; proc2 < group2_pointer->grp_proc_count ; proc2++ ) {
            proc2_pointer=ompi_group_peer_lookup(group2_pointer,proc2);
            if( proc1_pointer == proc2_pointer ) {
                if(proc1 != proc2 ) {
                    identical=false;
                }
                match=proc2;
                break;
            }
        } /* end proc2 loop */
        if( match== -1 ) {
            similar=false;
            identical=false;
            break;
        }
    } /* end proc1 loop */

    /* set comparison result */
    if( identical ) {
        *result=MPI_IDENT;
    } else if( similar ) {
        *result=MPI_SIMILAR;
    } else {
        *result=MPI_UNEQUAL;
    }

    return return_value;
}

bool ompi_group_have_remote_peers (ompi_group_t *group)
{
    for (int i = 0 ; i < group->grp_proc_count ; ++i) {
        ompi_proc_t *proc = NULL;
#if OMPI_GROUP_SPARSE
        proc = ompi_group_peer_lookup (group, i);
#else
        proc = ompi_group_get_proc_ptr_raw (group, i);
        if (ompi_proc_is_sentinel (proc)) {
            /* the proc must be stored in the group or cached in the proc
             * hash table if the process resides in the local node
             * (see ompi_proc_complete_init) */
            return true;
        }
#endif
        if (!OPAL_PROC_ON_LOCAL_NODE(proc->super.proc_flags)) {
            return true;
        }
    }

    return false;
}
