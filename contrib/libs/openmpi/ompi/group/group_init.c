/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2007 University of Houston. All rights reserved.
 * Copyright (c) 2007-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2009      Sun Microsystems, Inc. All rights reserved.
 * Copyright (c) 2012      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
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
#include "mpi.h"

/* define class information */
static void ompi_group_construct(ompi_group_t *);
static void ompi_group_destruct(ompi_group_t *);

OBJ_CLASS_INSTANCE(ompi_group_t,
                   opal_object_t,
                   ompi_group_construct,
                   ompi_group_destruct);

/*
 * Table for Fortran <-> C group handle conversion
 */
opal_pointer_array_t ompi_group_f_to_c_table = {{0}};

/*
 * Predefined group objects
 */
ompi_predefined_group_t ompi_mpi_group_empty = {{{0}}};
ompi_predefined_group_t ompi_mpi_group_null = {{{0}}};
ompi_predefined_group_t *ompi_mpi_group_empty_addr = &ompi_mpi_group_empty;
ompi_predefined_group_t *ompi_mpi_group_null_addr = &ompi_mpi_group_null;


/*
 * Allocate a new group structure
 */
ompi_group_t *ompi_group_allocate(int group_size)
{
    /* local variables */
    ompi_proc_t **procs = calloc (group_size, sizeof (ompi_proc_t *));
    ompi_group_t *new_group;

    if (NULL == procs) {
        return NULL;
    }

    new_group = ompi_group_allocate_plist_w_procs (procs, group_size);
    if (NULL == new_group) {
        free (procs);
    }

    return new_group;
}

ompi_group_t *ompi_group_allocate_plist_w_procs (ompi_proc_t **procs, int group_size)
{
    /* local variables */
    ompi_group_t * new_group = NULL;

    assert (group_size >= 0);

    /* create new group group element */
    new_group = OBJ_NEW(ompi_group_t);

    if (NULL == new_group) {
        return NULL;
    }

    if (0 > new_group->grp_f_to_c_index) {
        OBJ_RELEASE (new_group);
        return NULL;
    }

    /*
     * Allocate array of (ompi_proc_t *)'s, one for each
     * process in the group.
     */
    new_group->grp_proc_pointers = procs;

    /* set the group size */
    new_group->grp_proc_count = group_size;

    /* initialize our rank to MPI_UNDEFINED */
    new_group->grp_my_rank = MPI_UNDEFINED;
    OMPI_GROUP_SET_DENSE(new_group);

    ompi_group_increment_proc_count (new_group);

    return new_group;
}

ompi_group_t *ompi_group_allocate_sporadic(int group_size)
{
    /* local variables */
    ompi_group_t *new_group = NULL;

    assert (group_size >= 0);

    /* create new group group element */
    new_group = OBJ_NEW(ompi_group_t);
    if( NULL == new_group) {
        goto error_exit;
    }
    if (0 > new_group->grp_f_to_c_index) {
        OBJ_RELEASE(new_group);
        new_group = NULL;
        goto error_exit;
    }
    /* allocate array of (grp_sporadic_list )'s */
    if (0 < group_size) {
        new_group->sparse_data.grp_sporadic.grp_sporadic_list =
            (struct ompi_group_sporadic_list_t *)malloc
            (sizeof(struct ompi_group_sporadic_list_t ) * group_size);

        /* non-empty group */
        if ( NULL == new_group->sparse_data.grp_sporadic.grp_sporadic_list) {
            /* sporadic list allocation failed */
            OBJ_RELEASE (new_group);
            new_group = NULL;
            goto error_exit;
        }
    }

    /* set the group size */
    new_group->grp_proc_count = group_size; /* actually it's the number of
                                               elements in the sporadic list*/

    /* initialize our rank to MPI_UNDEFINED */
    new_group->grp_my_rank       = MPI_UNDEFINED;
    new_group->grp_proc_pointers = NULL;
    OMPI_GROUP_SET_SPORADIC(new_group);

 error_exit:
    return new_group;
}

ompi_group_t *ompi_group_allocate_strided(void)
{
    ompi_group_t *new_group = NULL;

    /* create new group group element */
    new_group = OBJ_NEW(ompi_group_t);
    if( NULL == new_group ) {
        goto error_exit;
    }
    if (0 > new_group->grp_f_to_c_index) {
        OBJ_RELEASE(new_group);
        new_group = NULL;
        goto error_exit;
    }
    /* initialize our rank to MPI_UNDEFINED */
    new_group->grp_my_rank    = MPI_UNDEFINED;
    new_group->grp_proc_pointers     = NULL;
    OMPI_GROUP_SET_STRIDED(new_group);
    new_group->sparse_data.grp_strided.grp_strided_stride         = -1;
    new_group->sparse_data.grp_strided.grp_strided_offset         = -1;
    new_group->sparse_data.grp_strided.grp_strided_last_element   = -1;
 error_exit:
    /* return */
    return new_group;
}
ompi_group_t *ompi_group_allocate_bmap(int orig_group_size , int group_size)
{
    ompi_group_t *new_group = NULL;

    assert (group_size >= 0);

    /* create new group group element */
    new_group = OBJ_NEW(ompi_group_t);
    if( NULL == new_group) {
        goto error_exit;
    }
    if (0 > new_group->grp_f_to_c_index) {
        OBJ_RELEASE(new_group);
        new_group = NULL;
        goto error_exit;
    }
    /* allocate the unsigned char list */
    new_group->sparse_data.grp_bitmap.grp_bitmap_array = (unsigned char *)malloc
        (sizeof(unsigned char) * ompi_group_div_ceil(orig_group_size,BSIZE));

    new_group->sparse_data.grp_bitmap.grp_bitmap_array_len =
        ompi_group_div_ceil(orig_group_size,BSIZE);

    new_group->grp_proc_count = group_size;

    /* initialize our rank to MPI_UNDEFINED */
    new_group->grp_my_rank    = MPI_UNDEFINED;
    new_group->grp_proc_pointers     = NULL;
    OMPI_GROUP_SET_BITMAP(new_group);

 error_exit:
    /* return */
    return new_group;
}

/*
 * increment the reference count of the proc structures
 */
void ompi_group_increment_proc_count(ompi_group_t *group)
{
    ompi_proc_t * proc_pointer;
    for (int proc = 0 ; proc < group->grp_proc_count ; ++proc) {
	proc_pointer = ompi_group_peer_lookup_existing (group, proc);
	if (proc_pointer) {
	    OBJ_RETAIN(proc_pointer);
	}
    }
}

/*
 * decrement the reference count of the proc structures
 */

void ompi_group_decrement_proc_count(ompi_group_t *group)
{
    ompi_proc_t * proc_pointer;
    for (int proc = 0 ; proc < group->grp_proc_count ; ++proc) {
	proc_pointer = ompi_group_peer_lookup_existing (group, proc);
	if (proc_pointer) {
	    OBJ_RELEASE(proc_pointer);
	}
    }
}

/*
 * group constructor
 */
static void ompi_group_construct(ompi_group_t *new_group)
{
    int ret_val;

    /* Note that we do *NOT* increase the refcount on all the included
       procs here because that is handled at a different level (e.g.,
       the proc counts are not decreased during the desstructor,
       either). */

    /* assign entry in fortran <-> c translation array */
    ret_val = opal_pointer_array_add(&ompi_group_f_to_c_table, new_group);
    new_group->grp_f_to_c_index = ret_val;
    new_group->grp_flags = 0;

    /* default the sparse values for groups */
    new_group->grp_parent_group_ptr = NULL;
}


/*
 * group destructor
 */
static void ompi_group_destruct(ompi_group_t *group)
{
    /* Note that we do *NOT* decrease the refcount on all the included
       procs here because that is handled at a different level (e.g.,
       the proc counts are not increased during the constructor,
       either). */

#if OMPI_GROUP_SPARSE
    if (OMPI_GROUP_IS_DENSE(group))
	/* sparse groups do not increment proc reference counters */
#endif
	ompi_group_decrement_proc_count (group);

    /* release thegrp_proc_pointers memory */
    if (NULL != group->grp_proc_pointers) {
        free(group->grp_proc_pointers);
    }

    if (OMPI_GROUP_IS_SPORADIC(group)) {
        if (NULL != group->sparse_data.grp_sporadic.grp_sporadic_list) {
            free(group->sparse_data.grp_sporadic.grp_sporadic_list);
        }
    }

    if (OMPI_GROUP_IS_BITMAP(group)) {
        if (NULL != group->sparse_data.grp_bitmap.grp_bitmap_array) {
            free(group->sparse_data.grp_bitmap.grp_bitmap_array);
        }
    }

    if (NULL != group->grp_parent_group_ptr){
        OBJ_RELEASE(group->grp_parent_group_ptr);
    }

    /* reset the ompi_group_f_to_c_table entry - make sure that the
     * entry is in the table */
    if (NULL != opal_pointer_array_get_item(&ompi_group_f_to_c_table,
                                            group->grp_f_to_c_index)) {
        opal_pointer_array_set_item(&ompi_group_f_to_c_table,
                                    group->grp_f_to_c_index, NULL);
    }
}


/*
 * Initialize OMPI group infrastructure
 */
int ompi_group_init(void)
{
    /* initialize ompi_group_f_to_c_table */
    OBJ_CONSTRUCT( &ompi_group_f_to_c_table, opal_pointer_array_t);
    if( OPAL_SUCCESS != opal_pointer_array_init(&ompi_group_f_to_c_table, 4,
                                                OMPI_FORTRAN_HANDLE_MAX, 16) ) {
        return OMPI_ERROR;
    }

    /* add MPI_GROUP_NULL to table */
    OBJ_CONSTRUCT(&ompi_mpi_group_null, ompi_group_t);
    ompi_mpi_group_null.group.grp_proc_count        = 0;
    ompi_mpi_group_null.group.grp_my_rank           = MPI_PROC_NULL;
    ompi_mpi_group_null.group.grp_proc_pointers     = NULL;
    ompi_mpi_group_null.group.grp_flags            |= OMPI_GROUP_DENSE;
    ompi_mpi_group_null.group.grp_flags            |= OMPI_GROUP_INTRINSIC;

    /* add MPI_GROUP_EMPTY to table */
    OBJ_CONSTRUCT(&ompi_mpi_group_empty, ompi_group_t);
    ompi_mpi_group_empty.group.grp_proc_count        = 0;
    ompi_mpi_group_empty.group.grp_my_rank           = MPI_UNDEFINED;
    ompi_mpi_group_empty.group.grp_proc_pointers     = NULL;
    ompi_mpi_group_empty.group.grp_flags            |= OMPI_GROUP_DENSE;
    ompi_mpi_group_empty.group.grp_flags            |= OMPI_GROUP_INTRINSIC;

    return OMPI_SUCCESS;
}


/*
 * Clean up group infrastructure
 */
int ompi_group_finalize(void)
{
    ompi_mpi_group_null.group.grp_flags = 0;
    OBJ_DESTRUCT(&ompi_mpi_group_null);

    ompi_mpi_group_null.group.grp_flags = 0;
    OBJ_DESTRUCT(&ompi_mpi_group_empty);

    OBJ_DESTRUCT(&ompi_group_f_to_c_table);

    return OMPI_SUCCESS;
}
