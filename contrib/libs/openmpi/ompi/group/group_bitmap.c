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

static bool check_ranks (int, const int *);

int ompi_group_calc_bmap ( int n, int orig_size , const int *ranks) {
    if (check_ranks(n,ranks)) {
        return ompi_group_div_ceil(orig_size,BSIZE);
    }
    else {
        return -1;
    }
}

/* from parent group to child group*/
int ompi_group_translate_ranks_bmap ( ompi_group_t *parent_group,
                                      int n_ranks, const int *ranks1,
                                      ompi_group_t *child_group,
                                      int *ranks2)
{
    int i,count,j,k,m;
    unsigned char tmp, tmp1;
    for (j=0 ; j<n_ranks ; j++) {
        if ( MPI_PROC_NULL == ranks1[j]) {
            ranks2[j] = MPI_PROC_NULL;
        }
        else {
            ranks2[j] = MPI_UNDEFINED;
            m = ranks1[j];
            count = 0;
            tmp = ( 1 << (m % BSIZE) );
            /* check if the bit that correponds to the parent rank is set in the bitmap */
            if ( tmp == (child_group->sparse_data.grp_bitmap.grp_bitmap_array[(int)(m/BSIZE)]
                         & (1 << (m % BSIZE)))) {
                /*
                 * add up how many bits are set, till we get to the bit of parent
                 * rank that we want. The rank in the child will be the sum of the bits
                 * that are set on the way till we get to the correponding bit
                 */
                for (i=0 ; i<=(int)(m/BSIZE) ; i++) {
                    for (k=0 ; k<BSIZE ; k++) {
                        tmp1 = ( 1 << k);
                        if ( tmp1 == ( child_group->sparse_data.grp_bitmap.grp_bitmap_array[i]
                                       & (1 << k) ) ) {
                            count++;
                        }
                        if( i==(int)(m/BSIZE) &&  k==m % BSIZE ) {
                            ranks2[j] = count-1;
                            i = (int)(m/BSIZE) + 1;
                            break;
                        }
                    }
                }
            }
        }
    }
    return OMPI_SUCCESS;
}
/* from child group to parent group */
int ompi_group_translate_ranks_bmap_reverse ( ompi_group_t *child_group,
                                              int n_ranks, const int *ranks1,
                                              ompi_group_t *parent_group,
                                              int *ranks2)
{
    int i,j,count,m,k;
    unsigned char tmp;
    for (j=0 ; j<n_ranks ; j++) {
        if ( MPI_PROC_NULL == ranks1[j]) {
            ranks2[j] = MPI_PROC_NULL;
        }
        else {
            m = ranks1[j];
            count = 0;
            /*
             * Go through all the bits set in the bitmap up to the child rank.
             * The parent rank will be the sum of all bits passed (set and unset)
             */
            for (i=0 ; i<child_group->sparse_data.grp_bitmap.grp_bitmap_array_len ; i++) {
                for (k=0 ; k<BSIZE ; k++) {
                    tmp = ( 1 << k);
                    if ( tmp == ( child_group->sparse_data.grp_bitmap.grp_bitmap_array[i]
                                  & (1 << k) ) ) {
                        count++;
                    }
                    if( m == count-1 ) {
                        ranks2[j] = i*BSIZE + k;
                        i = child_group->sparse_data.grp_bitmap.grp_bitmap_array_len + 1;
                        break;
                    }
                }
            }
        }
    }
    return OMPI_SUCCESS;
}

int ompi_group_div_ceil (int num, int den)
{
    if (0 == num%den) {
        return num/den;
    }
    else {
        return (int)(num/den) + 1;
    }
}
/*
 * This functions is to check that all ranks in the included list of ranks
 * are monotonically increasing. If not, the bitmap format can not be used
 * since we won't be able to translate the ranks corrently since the algorithms
 * assume that the ranks are in order in the bitmap list.
 */
static bool check_ranks (int n, const int *ranks) {
    int i;
    for (i=1 ; i < n ; i++) {
        if ( ranks[i-1] > ranks [i] ) {
            return false;
        }
    }
    return true;
}

int ompi_group_incl_bmap(ompi_group_t* group, int n, const int *ranks,
                         ompi_group_t **new_group)
{
    /* local variables */
    int my_group_rank,i,bit_set;
    ompi_group_t *group_pointer, *new_group_pointer;

    group_pointer = (ompi_group_t *)group;

    if ( 0 == n ) {
        *new_group = MPI_GROUP_EMPTY;
        OBJ_RETAIN(MPI_GROUP_EMPTY);
        return OMPI_SUCCESS;
    }

    new_group_pointer = ompi_group_allocate_bmap(group->grp_proc_count, n);
    if( NULL == new_group_pointer ) {
        return MPI_ERR_GROUP;
    }
    /* Initialize the bit array to zeros */
    for (i=0 ; i<new_group_pointer->sparse_data.grp_bitmap.grp_bitmap_array_len ; i++) {
        new_group_pointer->
            sparse_data.grp_bitmap.grp_bitmap_array[i] = 0;
    }

    /* set the bits */
    for (i=0 ; i<n ; i++) {
        bit_set = ranks[i] % BSIZE;
        new_group_pointer->
            sparse_data.grp_bitmap.grp_bitmap_array[(int)(ranks[i]/BSIZE)] |= (1 << bit_set);
    }

    new_group_pointer -> grp_parent_group_ptr = group_pointer;

    OBJ_RETAIN(new_group_pointer -> grp_parent_group_ptr);
    ompi_group_increment_proc_count(new_group_pointer -> grp_parent_group_ptr);

    ompi_group_increment_proc_count(new_group_pointer);
    my_group_rank=group_pointer->grp_my_rank;

    ompi_group_translate_ranks (group_pointer,1,&my_group_rank,
                                new_group_pointer,&new_group_pointer->grp_my_rank);

    *new_group = (MPI_Group)new_group_pointer;

    return OMPI_SUCCESS;
}
