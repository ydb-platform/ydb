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
 * Copyright (c) 2013-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2016      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "opal/class/opal_bitmap.h"
#include "ompi/group/group.h"
#include "ompi/constants.h"
#include "ompi/proc/proc.h"
#include "mpi.h"

#include <math.h>

static int ompi_group_dense_overlap (ompi_group_t *group1, ompi_group_t *group2, opal_bitmap_t *bitmap)
{
    ompi_proc_t *proc1_pointer, *proc2_pointer;
    int rc, overlap_count;

    overlap_count = 0;

    for (int proc1 = 0 ; proc1 < group1->grp_proc_count ; ++proc1) {
        proc1_pointer = ompi_group_get_proc_ptr_raw (group1, proc1);

        /* check to see if this proc is in group2 */
        for (int proc2 = 0 ; proc2 < group2->grp_proc_count ; ++proc2) {
            proc2_pointer = ompi_group_get_proc_ptr_raw (group2, proc2);
            if( proc1_pointer == proc2_pointer ) {
                rc = opal_bitmap_set_bit (bitmap, proc2);
                if (OPAL_SUCCESS != rc) {
                    return rc;
                }
                ++overlap_count;

                break;
            }
        }  /* end proc1 loop */
    }  /* end proc loop */

    return overlap_count;
}

static struct ompi_proc_t *ompi_group_dense_lookup_raw (ompi_group_t *group, const int peer_id)
{
    if (OPAL_UNLIKELY(ompi_proc_is_sentinel (group->grp_proc_pointers[peer_id]))) {
        ompi_proc_t *proc =
            (ompi_proc_t *) ompi_proc_lookup (ompi_proc_sentinel_to_name ((uintptr_t) group->grp_proc_pointers[peer_id]));
        if (NULL != proc) {
            /* replace sentinel value with an actual ompi_proc_t */
            group->grp_proc_pointers[peer_id] = proc;
            /* retain the proc */
            OBJ_RETAIN(group->grp_proc_pointers[peer_id]);
        }
    }

    return group->grp_proc_pointers[peer_id];
}

ompi_proc_t *ompi_group_get_proc_ptr_raw (ompi_group_t *group, int rank)
{
#if OMPI_GROUP_SPARSE
    do {
        if (OMPI_GROUP_IS_DENSE(group)) {
            return ompi_group_dense_lookup_raw (group, rank);
        }
        int ranks1 = rank;
        ompi_group_translate_ranks (group, 1, &ranks1, group->grp_parent_group_ptr, &rank);
        group = group->grp_parent_group_ptr;
    } while (1);
#else
    return ompi_group_dense_lookup_raw (group, rank);
#endif
}

int ompi_group_calc_plist ( int n , const int *ranks ) {
    return sizeof(char *) * n ;
}

int ompi_group_incl_plist(ompi_group_t* group, int n, const int *ranks,
                          ompi_group_t **new_group)
{
    /* local variables */
    int my_group_rank;
    ompi_group_t *group_pointer, *new_group_pointer;

    group_pointer = (ompi_group_t *)group;

    if ( 0 == n ) {
        *new_group = MPI_GROUP_EMPTY;
        OBJ_RETAIN(MPI_GROUP_EMPTY);
        return OMPI_SUCCESS;
    }

    /* get new group struct */
    new_group_pointer=ompi_group_allocate(n);
    if( NULL == new_group_pointer ) {
        return MPI_ERR_GROUP;
    }

    /* put group elements in the list */
    for (int proc = 0; proc < n; proc++) {
        new_group_pointer->grp_proc_pointers[proc] =
            ompi_group_get_proc_ptr_raw (group_pointer, ranks[proc]);
    }                           /* end proc loop */

    /* increment proc reference counters */
    ompi_group_increment_proc_count(new_group_pointer);

    /* find my rank */
    my_group_rank=group_pointer->grp_my_rank;
    if (MPI_UNDEFINED != my_group_rank) {
        ompi_set_group_rank(new_group_pointer, ompi_proc_local_proc);
    } else {
        new_group_pointer->grp_my_rank = MPI_UNDEFINED;
    }

    *new_group = (MPI_Group)new_group_pointer;

    return OMPI_SUCCESS;
}

/*
 * Group Union has to use the dense format since we don't support
 * two parent groups in the group structure and maintain functions
 */
int ompi_group_union (ompi_group_t* group1, ompi_group_t* group2,
                      ompi_group_t **new_group)
{
    /* local variables */
    int new_group_size, cnt, rc, overlap_count;
    ompi_group_t *new_group_pointer;
    ompi_proc_t *proc2_pointer;
    opal_bitmap_t bitmap;

    /*
     * form union
     */

    /* get new group size */
    OBJ_CONSTRUCT(&bitmap, opal_bitmap_t);
    rc = opal_bitmap_init (&bitmap, 32);
    if (OPAL_SUCCESS != rc) {
        return rc;
    }

    /* check group2 elements to see if they need to be included in the list */
    overlap_count = ompi_group_dense_overlap (group1, group2, &bitmap);
    if (0 > overlap_count) {
        OBJ_DESTRUCT(&bitmap);
        return overlap_count;
    }

    new_group_size = group1->grp_proc_count + group2->grp_proc_count - overlap_count;
    if ( 0 == new_group_size ) {
        *new_group = MPI_GROUP_EMPTY;
        OBJ_RETAIN(MPI_GROUP_EMPTY);
        OBJ_DESTRUCT(&bitmap);
        return MPI_SUCCESS;
    }

    /* get new group struct */
    new_group_pointer = ompi_group_allocate(new_group_size);
    if (NULL == new_group_pointer) {
        OBJ_DESTRUCT(&bitmap);
        return MPI_ERR_GROUP;
    }

    /* fill in the new group list */

    /* put group1 elements in the list */
    for (int proc1 = 0; proc1 < group1->grp_proc_count; ++proc1) {
        new_group_pointer->grp_proc_pointers[proc1] =
            ompi_group_get_proc_ptr_raw (group1, proc1);
    }
    cnt = group1->grp_proc_count;

    /* check group2 elements to see if they need to be included in the list */
    for (int proc2 = 0; proc2 < group2->grp_proc_count; ++proc2) {
        if (opal_bitmap_is_set_bit (&bitmap, proc2)) {
            continue;
        }

        proc2_pointer = ompi_group_get_proc_ptr_raw (group2, proc2);
        new_group_pointer->grp_proc_pointers[cnt++] = proc2_pointer;
    }                           /* end proc loop */

    OBJ_DESTRUCT(&bitmap);

    /* increment proc reference counters */
    ompi_group_increment_proc_count(new_group_pointer);

    /* find my rank */
    if (MPI_UNDEFINED != group1->grp_my_rank || MPI_UNDEFINED != group2->grp_my_rank) {
        ompi_set_group_rank(new_group_pointer, ompi_proc_local_proc);
    } else {
        new_group_pointer->grp_my_rank = MPI_UNDEFINED;
    }

    *new_group = (MPI_Group) new_group_pointer;

    return OMPI_SUCCESS;
}

/*
 * Group Difference has to use the dense format since we don't support
 * two parent groups in the group structure and maintain functions
 */
int ompi_group_difference(ompi_group_t* group1, ompi_group_t* group2,
                          ompi_group_t **new_group) {

    /* local varibles */
    int new_group_size, overlap_count, rc;
    ompi_group_t *new_group_pointer;
    ompi_proc_t *proc1_pointer;
    opal_bitmap_t bitmap;

    /*
     * form union
     */

    /* get new group size */
    OBJ_CONSTRUCT(&bitmap, opal_bitmap_t);
    rc = opal_bitmap_init (&bitmap, 32);
    if (OPAL_SUCCESS != rc) {
        return rc;
    }

    /* check group2 elements to see if they need to be included in the list */
    overlap_count = ompi_group_dense_overlap (group2, group1, &bitmap);
    if (0 > overlap_count) {
        OBJ_DESTRUCT(&bitmap);
        return overlap_count;
    }

    new_group_size = group1->grp_proc_count - overlap_count;
    if ( 0 == new_group_size ) {
        *new_group = MPI_GROUP_EMPTY;
        OBJ_RETAIN(MPI_GROUP_EMPTY);
        OBJ_DESTRUCT(&bitmap);
        return MPI_SUCCESS;
    }

    /* allocate a new ompi_group_t structure */
    new_group_pointer = ompi_group_allocate(new_group_size);
    if( NULL == new_group_pointer ) {
        OBJ_DESTRUCT(&bitmap);
        return MPI_ERR_GROUP;
    }

    /* fill in group list */
    /* loop over group1 members */
    for (int proc1 = 0, cnt = 0 ; proc1 < group1->grp_proc_count ; ++proc1) {
        if (opal_bitmap_is_set_bit (&bitmap, proc1)) {
            continue;
        }

        proc1_pointer = ompi_group_get_proc_ptr_raw (group1, proc1);
        new_group_pointer->grp_proc_pointers[cnt++] = proc1_pointer;
    }  /* end proc loop */

    OBJ_DESTRUCT(&bitmap);

    /* increment proc reference counters */
    ompi_group_increment_proc_count(new_group_pointer);

    /* find my rank */
    if (MPI_UNDEFINED == group1->grp_my_rank || MPI_UNDEFINED != group2->grp_my_rank) {
        new_group_pointer->grp_my_rank = MPI_UNDEFINED;
    } else {
        ompi_set_group_rank(new_group_pointer, ompi_proc_local_proc);
    }

    *new_group = (MPI_Group)new_group_pointer;

    return OMPI_SUCCESS;
}
