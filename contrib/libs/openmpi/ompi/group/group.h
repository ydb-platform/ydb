/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2007 University of Houston. All rights reserved.
 * Copyright (c) 2007-2017 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2009      Sun Microsystems, Inc. All rights reserved.
 * Copyright (c) 2012      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2013-2017 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2016      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file:
 *
 * Infrastructure for MPI group support.
 */
#ifndef OMPI_GROUP_H
#define OMPI_GROUP_H

#include "ompi_config.h"
#include "ompi/proc/proc.h"
#include "mpi.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/util/output.h"

BEGIN_C_DECLS

#define BSIZE ((int)sizeof(unsigned char)*8)

struct ompi_group_sporadic_list_t
{
  int rank_first;
  int length;
};

struct ompi_group_sporadic_data_t
{
    struct ompi_group_sporadic_list_t  *grp_sporadic_list;
                                            /** list to hold the sporadic struct */
    int                        grp_sporadic_list_len;/** length of the structure*/
};
struct ompi_group_strided_data_t
{
    int grp_strided_offset;         /** offset to start from when including or excluding */
    int grp_strided_stride;         /** stride for including or excluding */
    int grp_strided_last_element;       /** the last element to be included for */
};
struct ompi_group_bitmap_data_t
{
    unsigned char *grp_bitmap_array;     /* the bit map array for sparse groups of type BMAP */
    int            grp_bitmap_array_len; /* length of the bit array */
};

/**
 * Group structure
 * Currently we have four formats for storing the process pointers that are members
 * of the group.
 * PList: a dense format that stores all the process pointers of the group.
 * Sporadic: a sparse format that stores the ranges of the ranks from the parent group,
 *           that are included in the current group.
 * Strided: a sparse format that stores three integers that describe a red-black pattern
 *          that the current group is formed from its parent group.
 * Bitmap: a sparse format that maintains a bitmap of the included processes from the
 *         parent group. For each process that is included from the parent group
 *         its corresponding rank is set in the bitmap array.
 */
struct ompi_group_t {
    opal_object_t super;    /**< base class */
    int grp_proc_count;     /**< number of processes in group */
    int grp_my_rank;        /**< rank in group */
    int grp_f_to_c_index;   /**< index in Fortran <-> C translation array */
    struct ompi_proc_t **grp_proc_pointers;
                            /**< list of pointers to ompi_proc_t structures
                                 for each process in the group */
    uint32_t grp_flags;     /**< flags, e.g. freed, cannot be freed etc.*/
    /** pointer to the original group when using sparse storage */
    struct ompi_group_t *grp_parent_group_ptr;
    union
    {
        struct ompi_group_sporadic_data_t grp_sporadic;
        struct ompi_group_strided_data_t  grp_strided;
        struct ompi_group_bitmap_data_t   grp_bitmap;
    } sparse_data;
};

typedef struct ompi_group_t ompi_group_t;
OMPI_DECLSPEC OBJ_CLASS_DECLARATION(ompi_group_t);

/**
 * Padded struct to maintain back compatibiltiy.
 * See ompi/communicator/communicator.h comments with struct ompi_communicator_t
 * for full explanation why we chose the following padding construct for predefines.
 */
#define PREDEFINED_GROUP_PAD 256

struct ompi_predefined_group_t {
    struct ompi_group_t group;
    char padding[PREDEFINED_GROUP_PAD - sizeof(ompi_group_t)];
};

typedef struct ompi_predefined_group_t ompi_predefined_group_t;

/*
 * The following include pulls in shared typedefs with debugger plugins.
 * For more information on why we do this see the Notice to developers
 * comment at the top of the ompi_msgq_dll.c file.
 */
#include "group_dbg.h"

#define OMPI_GROUP_IS_INTRINSIC(_group) ((_group)->grp_flags&OMPI_GROUP_INTRINSIC)
#define OMPI_GROUP_IS_DENSE(_group) ((_group)->grp_flags & OMPI_GROUP_DENSE)
#define OMPI_GROUP_IS_SPORADIC(_group) ((_group)->grp_flags & OMPI_GROUP_SPORADIC)
#define OMPI_GROUP_IS_STRIDED(_group) ((_group)->grp_flags & OMPI_GROUP_STRIDED)
#define OMPI_GROUP_IS_BITMAP(_group) ((_group)->grp_flags & OMPI_GROUP_BITMAP)

#define OMPI_GROUP_SET_INTRINSIC(_group) ( (_group)->grp_flags |= OMPI_GROUP_INTRINSIC)
#define OMPI_GROUP_SET_DENSE(_group) ( (_group)->grp_flags |= OMPI_GROUP_DENSE)
#define OMPI_GROUP_SET_SPORADIC(_group) ( (_group)->grp_flags |= OMPI_GROUP_SPORADIC)
#define OMPI_GROUP_SET_STRIDED(_group) ( (_group)->grp_flags |= OMPI_GROUP_STRIDED)
#define OMPI_GROUP_SET_BITMAP(_group) ( (_group)->grp_flags |= OMPI_GROUP_BITMAP)

/**
 * Table for Fortran <-> C group handle conversion
 */
OMPI_DECLSPEC extern struct opal_pointer_array_t ompi_group_f_to_c_table;
OMPI_DECLSPEC extern struct ompi_predefined_group_t ompi_mpi_group_null;
OMPI_DECLSPEC extern struct ompi_predefined_group_t *ompi_mpi_group_null_addr;


/*
 * function prototypes
 */

/**
 * Allocate a new group structure.
 *
 * @param group_size Number of MPI processes in the group
 *
 * @return Pointer to new group structure
 */
OMPI_DECLSPEC ompi_group_t *ompi_group_allocate(int group_size);
ompi_group_t *ompi_group_allocate_plist_w_procs (ompi_proc_t **procs, int group_size);
ompi_group_t *ompi_group_allocate_sporadic(int group_size);
ompi_group_t *ompi_group_allocate_strided(void);
ompi_group_t *ompi_group_allocate_bmap(int orig_group_size, int group_size);

/**
 * Increment the reference count of the proc structures.
 *
 * @param group Pointer to ompi_group_t structute (IN)
 *
 */
OMPI_DECLSPEC void ompi_group_increment_proc_count(ompi_group_t *group);

/**
 * Decrement the reference count of the proc structures.
 *
 * @param group Pointer to ompi_group_t structute (IN)
 *
 */
OMPI_DECLSPEC void ompi_group_decrement_proc_count(ompi_group_t *group);


/**
 * Initialize OMPI group infrastructure.
 *
 * @return Error code
 */
int ompi_group_init(void);


/**
 * Clean up OMPI group infrastructure.
 *
 * @return Error code
 */
int ompi_group_finalize(void);


/**
 * Get group size.
 *
 * @param group Pointer to ompi_group_t structute (IN)
 *
 * @return Group size
 */
static inline int ompi_group_size(ompi_group_t *group)
{
    return group->grp_proc_count;
}


/**
 * Get group rank
 *
 * @param group Pointer to ompi_group_t structure (IN)
 *
 * @return Group rank
 */
static inline int ompi_group_rank(ompi_group_t *group)
{
    return group->grp_my_rank;
}



/**
 * Set group rank in the input group structure
 *
 * @param group Group Pointer to ompi_group_t structure (IN)
 * @param proc_pointer Pointer to ompi_proc_t structure for process.
 *                     MPI_PROC_NULL may be used to indicate proc not
 *                     in group
 *
 * @return Error code
 */
void ompi_set_group_rank(ompi_group_t *group, struct ompi_proc_t *proc_pointer);

/**
 * Abstracting MPI_Group_translate_ranks to an ompi function for internal use
 */
OMPI_DECLSPEC int ompi_group_translate_ranks ( ompi_group_t *group1,
                                               int n_ranks, const int *ranks1,
                                               ompi_group_t *group2,
                                               int *ranks2);

/**
 * Abstracting MPI_Group_compare to an ompi function for internal use
 */
OMPI_DECLSPEC int ompi_group_compare(ompi_group_t *group1,
                                     ompi_group_t *group2,
                                     int *result);

/**
 * Abstracting MPI_Group_free, since it is required by some internal functions...
 */
int ompi_group_free (ompi_group_t **group);

/**
 * Functions to handle process pointers for sparse group formats
 */
int ompi_group_translate_ranks_sporadic ( ompi_group_t *group1,
                                 int n_ranks, const int *ranks1,
                                 ompi_group_t *group2,
                                 int *ranks2);
int ompi_group_translate_ranks_sporadic_reverse ( ompi_group_t *group1,
                                 int n_ranks, const int *ranks1,
                                 ompi_group_t *group2,
                                 int *ranks2);
int ompi_group_translate_ranks_strided ( ompi_group_t *group1,
                                 int n_ranks, const int *ranks1,
                                 ompi_group_t *group2,
                                 int *ranks2);
int ompi_group_translate_ranks_strided_reverse ( ompi_group_t *group1,
                                 int n_ranks, const int *ranks1,
                                 ompi_group_t *group2,
                                 int *ranks2);
int ompi_group_translate_ranks_bmap ( ompi_group_t *group1,
                                 int n_ranks, const int *ranks1,
                                 ompi_group_t *group2,
                                 int *ranks2);
int ompi_group_translate_ranks_bmap_reverse ( ompi_group_t *group1,
                                 int n_ranks, const int *ranks1,
                                 ompi_group_t *group2,
                                 int *ranks2);

/**
 *  Prototypes for the group back-end functions. Argument lists
 are similar to the according  C MPI functions.
 */
int ompi_group_incl(ompi_group_t* group, int n, const int *ranks,
                    ompi_group_t **new_group);
int ompi_group_excl(ompi_group_t* group, int n, const int *ranks,
                    ompi_group_t **new_group);
int ompi_group_range_incl(ompi_group_t* group, int n_triplets,
                          int ranges[][3],ompi_group_t **new_group);
int ompi_group_range_excl(ompi_group_t* group, int n_triplets,
                          int ranges[][3],ompi_group_t **new_group);
int ompi_group_union (ompi_group_t* group1, ompi_group_t* group2,
                      ompi_group_t **new_group);
int ompi_group_intersection(ompi_group_t* group1,ompi_group_t* group2,
                            ompi_group_t **new_group);
int ompi_group_difference(ompi_group_t* group1, ompi_group_t* group2,
                          ompi_group_t **new_group);


/**
 *  Include Functions to handle Sparse storage formats
 */
int ompi_group_incl_plist(ompi_group_t* group, int n, const int *ranks,
                          ompi_group_t **new_group);
int ompi_group_incl_spor(ompi_group_t* group, int n, const int *ranks,
                         ompi_group_t **new_group);
int ompi_group_incl_strided(ompi_group_t* group, int n, const int *ranks,
                            ompi_group_t **new_group);
int ompi_group_incl_bmap(ompi_group_t* group, int n, const int *ranks,
                         ompi_group_t **new_group);

/**
 *  Functions to calculate storage spaces
 */
int ompi_group_calc_plist ( int n, const int *ranks );
int ompi_group_calc_strided ( int n, const int *ranks );
int ompi_group_calc_sporadic ( int n, const int *ranks );
int ompi_group_calc_bmap ( int n, int orig_size , const int *ranks );

/**
 * Function to return the minimum value in an array
 */
int ompi_group_minloc (int list[], int length);

/**
 * @brief Helper function for retreiving the proc of a group member in a dense group
 *
 * This function exists to handle the translation of sentinel group members to real
 * ompi_proc_t's. If a sentinel value is found and allocate is true then this function
 * looks for an existing ompi_proc_t using ompi_proc_for_name which will allocate a
 * ompi_proc_t if one does not exist. If allocate is false then sentinel values translate
 * to NULL.
 */
static inline struct ompi_proc_t *ompi_group_dense_lookup (ompi_group_t *group, const int peer_id, const bool allocate)
{
    ompi_proc_t *proc;

#if OPAL_ENABLE_DEBUG
    if (peer_id >= group->grp_proc_count) {
        opal_output(0, "ompi_group_dense_lookup: invalid peer index (%d)", peer_id);
        return (struct ompi_proc_t *) NULL;
    }
#endif

    proc = group->grp_proc_pointers[peer_id];

    if (OPAL_UNLIKELY(ompi_proc_is_sentinel (proc))) {
        if (!allocate) {
            return NULL;
        }

        /* replace sentinel value with an actual ompi_proc_t */
        ompi_proc_t *real_proc =
            (ompi_proc_t *) ompi_proc_for_name (ompi_proc_sentinel_to_name ((uintptr_t) proc));

        if (opal_atomic_compare_exchange_strong_ptr (group->grp_proc_pointers + peer_id, &proc, real_proc)) {
            OBJ_RETAIN(real_proc);
        }

        proc = real_proc;
    }

    return proc;
}

/*
 * This is the function that iterates through the sparse groups to the dense group
 * to reach the process pointer
 */
static inline ompi_proc_t *ompi_group_get_proc_ptr (ompi_group_t *group, int rank, const bool allocate)
{
#if OMPI_GROUP_SPARSE
    do {
        if (OMPI_GROUP_IS_DENSE(group)) {
            return ompi_group_dense_lookup (group, rank, allocate);
        }
        int ranks1 = rank;
        ompi_group_translate_ranks (group, 1, &ranks1, group->grp_parent_group_ptr, &rank);
        group = group->grp_parent_group_ptr;
    } while (1);
#else
    return ompi_group_dense_lookup (group, rank, allocate);
#endif
}

/**
 * @brief Get the raw proc pointer from the group
 *
 * This function will either return a ompi_proc_t if one exists (either stored in the group
 * or cached in the proc hash table) or a sentinel value representing the proc. This
 * differs from ompi_group_get_proc_ptr() which returns the ompi_proc_t or NULL.
 */
ompi_proc_t *ompi_group_get_proc_ptr_raw (ompi_group_t *group, int rank);

static inline opal_process_name_t ompi_group_get_proc_name (ompi_group_t *group, int rank)
{
    ompi_proc_t *proc = ompi_group_get_proc_ptr_raw (group, rank);
    if (ompi_proc_is_sentinel (proc)) {
        return ompi_proc_sentinel_to_name ((intptr_t) proc);
    }

    return proc->super.proc_name;
}

/**
 * Inline function to check if sparse groups are enabled and return the direct access
 * to the proc pointer, otherwise the lookup function
 */
static inline struct ompi_proc_t* ompi_group_peer_lookup(ompi_group_t *group, int peer_id)
{
    return ompi_group_get_proc_ptr (group, peer_id, true);
}

static inline struct ompi_proc_t *ompi_group_peer_lookup_existing (ompi_group_t *group, int peer_id)
{
    return ompi_group_get_proc_ptr (group, peer_id, false);
}

bool ompi_group_have_remote_peers (ompi_group_t *group);

/**
 *  Function to print the group info
 */
int ompi_group_dump (ompi_group_t* group);

/**
 * Ceil Function so not to include the math.h lib
 */
int ompi_group_div_ceil (int num, int den);

END_C_DECLS
#endif /* OMPI_GROUP_H */
