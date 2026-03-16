/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://www.hdfgroup.org/licenses.               *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*-------------------------------------------------------------------------
 *
 * Created:		H5B2int.c
 *
 * Purpose:		Internal routines for managing v2 B-trees.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5B2module.h" /* This source code file is part of the H5B2 module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5B2pkg.h"     /* v2 B-trees				*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5VMprivate.h" /* Vectors and arrays 			*/

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/
static herr_t H5B2__update_child_flush_depends(H5B2_hdr_t *hdr, unsigned depth, H5B2_node_ptr_t *node_ptrs,
                                               unsigned start_idx, unsigned end_idx, void *old_parent,
                                               void *new_parent);

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Declare a free list to manage the 'H5B2_node_info_t' sequence information */
H5FL_SEQ_EXTERN(H5B2_node_info_t);

/*-------------------------------------------------------------------------
 * Function:	H5B2__locate_record
 *
 * Purpose:	Performs a binary search to locate a record in a sorted
 *              array of records.
 *
 *              Sets *IDX to location of record greater than or equal to
 *              record to locate.
 *
 * Return:	Comparison value for insertion location.  Negative for record
 *              to locate being less than value in *IDX.  Zero for record to
 *              locate equal to value in *IDX.  Positive for record to locate
 *              being greater than value in *IDX (which should only happen when
 *              record to locate is greater than all records to search).
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__locate_record(const H5B2_class_t *type, unsigned nrec, size_t *rec_off, const uint8_t *native,
                    const void *udata, unsigned *idx, int *cmp)
{
    unsigned lo        = 0, hi; /* Low & high index values */
    unsigned my_idx    = 0;     /* Final index value */
    herr_t   ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    *cmp = -1;

    hi = nrec;
    while (lo < hi && *cmp) {
        my_idx = (lo + hi) / 2;
        if ((type->compare)(udata, native + rec_off[my_idx], cmp) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTCOMPARE, FAIL, "can't compare btree2 records");
        if (*cmp < 0)
            hi = my_idx;
        else
            lo = my_idx + 1;
    } /* end while */

    *idx = my_idx;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B2__locate_record */

/*-------------------------------------------------------------------------
 * Function:	H5B2__split1
 *
 * Purpose:	Perform a 1->2 node split
 *
 * Return:	Success:	Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__split1(H5B2_hdr_t *hdr, uint16_t depth, H5B2_node_ptr_t *curr_node_ptr,
             unsigned *parent_cache_info_flags_ptr, H5B2_internal_t *internal, unsigned *internal_flags_ptr,
             unsigned idx)
{
    const H5AC_class_t *child_class;                             /* Pointer to child node's class info */
    haddr_t   left_addr = HADDR_UNDEF, right_addr = HADDR_UNDEF; /* Addresses of left & right child nodes */
    void     *left_child = NULL, *right_child = NULL;            /* Pointers to child nodes */
    uint16_t *left_nrec, *right_nrec;                            /* Pointers to child # of records */
    uint8_t  *left_native, *right_native;                        /* Pointers to childs' native records */
    H5B2_node_ptr_t *left_node_ptrs  = NULL,
                    *right_node_ptrs = NULL; /* Pointers to childs' node pointer info */
    uint16_t mid_record;                     /* Index of "middle" record in current node */
    uint16_t old_node_nrec;                  /* Number of records in internal node split */
    unsigned left_child_flags  = H5AC__NO_FLAGS_SET,
             right_child_flags = H5AC__NO_FLAGS_SET; /* Flags for unprotecting child nodes */
    herr_t ret_value           = SUCCEED;            /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(hdr);
    assert(internal);
    assert(internal_flags_ptr);

    /* Slide records in parent node up one space, to make room for promoted record */
    if (idx < internal->nrec) {
        memmove(H5B2_INT_NREC(internal, hdr, idx + 1), H5B2_INT_NREC(internal, hdr, idx),
                hdr->cls->nrec_size * (internal->nrec - idx));
        memmove(&(internal->node_ptrs[idx + 2]), &(internal->node_ptrs[idx + 1]),
                sizeof(H5B2_node_ptr_t) * (internal->nrec - idx));
    } /* end if */

    /* Check for the kind of B-tree node to split */
    if (depth > 1) {
        H5B2_internal_t *left_int = NULL, *right_int = NULL; /* Pointers to old & new internal nodes */

        /* Create new internal node */
        internal->node_ptrs[idx + 1].all_nrec = internal->node_ptrs[idx + 1].node_nrec = 0;
        if (H5B2__create_internal(hdr, internal, &(internal->node_ptrs[idx + 1]), (uint16_t)(depth - 1)) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTINIT, FAIL, "unable to create new internal node");

        /* Setup information for unlocking child nodes */
        child_class = H5AC_BT2_INT;

        /* Protect both leaves */
        /* (Shadow left node if doing SWMR writes) */
        if (NULL ==
            (left_int = H5B2__protect_internal(hdr, internal, &internal->node_ptrs[idx],
                                               (uint16_t)(depth - 1), hdr->swmr_write, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree internal node");
        left_addr = internal->node_ptrs[idx].addr;
        if (NULL == (right_int = H5B2__protect_internal(hdr, internal, &internal->node_ptrs[idx + 1],
                                                        (uint16_t)(depth - 1), false, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree internal node");
        right_addr = internal->node_ptrs[idx + 1].addr;

        /* More setup for child nodes */
        left_child      = left_int;
        right_child     = right_int;
        left_nrec       = &(left_int->nrec);
        right_nrec      = &(right_int->nrec);
        left_native     = left_int->int_native;
        right_native    = right_int->int_native;
        left_node_ptrs  = left_int->node_ptrs;
        right_node_ptrs = right_int->node_ptrs;
    } /* end if */
    else {
        H5B2_leaf_t *left_leaf = NULL, *right_leaf = NULL; /* Pointers to old & new leaf nodes */

        /* Create new leaf node */
        internal->node_ptrs[idx + 1].all_nrec = internal->node_ptrs[idx + 1].node_nrec = 0;
        if (H5B2__create_leaf(hdr, internal, &(internal->node_ptrs[idx + 1])) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTINIT, FAIL, "unable to create new leaf node");

        /* Setup information for unlocking child nodes */
        child_class = H5AC_BT2_LEAF;

        /* Protect both leaves */
        /* (Shadow the left node if doing SWMR writes) */
        if (NULL == (left_leaf = H5B2__protect_leaf(hdr, internal, &internal->node_ptrs[idx], hdr->swmr_write,
                                                    H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree leaf node");
        left_addr = internal->node_ptrs[idx].addr;
        if (NULL == (right_leaf = H5B2__protect_leaf(hdr, internal, &internal->node_ptrs[idx + 1], false,
                                                     H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree leaf node");
        right_addr = internal->node_ptrs[idx + 1].addr;

        /* More setup for child nodes */
        left_child   = left_leaf;
        right_child  = right_leaf;
        left_nrec    = &(left_leaf->nrec);
        right_nrec   = &(right_leaf->nrec);
        left_native  = left_leaf->leaf_native;
        right_native = right_leaf->leaf_native;
    } /* end if */

    /* Get the number of records in node to split */
    old_node_nrec = internal->node_ptrs[idx].node_nrec;

    /* Determine "middle" record to promote to internal node */
    mid_record = (uint16_t)(old_node_nrec / 2);

    /* Copy "upper half" of records to new child */
    H5MM_memcpy(H5B2_NAT_NREC(right_native, hdr, 0),
                H5B2_NAT_NREC(left_native, hdr, mid_record + (unsigned)1),
                hdr->cls->nrec_size * (old_node_nrec - (mid_record + (unsigned)1)));

    /* Copy "upper half" of node pointers, if the node is an internal node */
    if (depth > 1)
        H5MM_memcpy(&(right_node_ptrs[0]), &(left_node_ptrs[mid_record + (unsigned)1]),
                    sizeof(H5B2_node_ptr_t) * (size_t)(old_node_nrec - mid_record));

    /* Copy "middle" record to internal node */
    H5MM_memcpy(H5B2_INT_NREC(internal, hdr, idx), H5B2_NAT_NREC(left_native, hdr, mid_record),
                hdr->cls->nrec_size);

    /* Mark nodes as dirty */
    left_child_flags |= H5AC__DIRTIED_FLAG;
    right_child_flags |= H5AC__DIRTIED_FLAG;

    /* Update record counts in child nodes */
    internal->node_ptrs[idx].node_nrec = *left_nrec = mid_record;
    internal->node_ptrs[idx + 1].node_nrec = *right_nrec = (uint16_t)(old_node_nrec - (mid_record + 1));

    /* Determine total number of records in new child nodes */
    if (depth > 1) {
        unsigned u;                  /* Local index variable */
        hsize_t  new_left_all_nrec;  /* New total number of records in left child */
        hsize_t  new_right_all_nrec; /* New total number of records in right child */

        /* Compute total of all records in each child node */
        new_left_all_nrec = internal->node_ptrs[idx].node_nrec;
        for (u = 0; u < (*left_nrec + (unsigned)1); u++)
            new_left_all_nrec += left_node_ptrs[u].all_nrec;

        new_right_all_nrec = internal->node_ptrs[idx + 1].node_nrec;
        for (u = 0; u < (*right_nrec + (unsigned)1); u++)
            new_right_all_nrec += right_node_ptrs[u].all_nrec;

        internal->node_ptrs[idx].all_nrec     = new_left_all_nrec;
        internal->node_ptrs[idx + 1].all_nrec = new_right_all_nrec;
    } /* end if */
    else {
        internal->node_ptrs[idx].all_nrec     = internal->node_ptrs[idx].node_nrec;
        internal->node_ptrs[idx + 1].all_nrec = internal->node_ptrs[idx + 1].node_nrec;
    } /* end else */

    /* Update # of records in parent node */
    internal->nrec++;

    /* Mark parent as dirty */
    *internal_flags_ptr |= H5AC__DIRTIED_FLAG;

    /* Update grandparent info */
    curr_node_ptr->node_nrec++;

    /* Mark grandparent as dirty, if given */
    if (parent_cache_info_flags_ptr)
        *parent_cache_info_flags_ptr |= H5AC__DIRTIED_FLAG;

    /* Update flush dependencies for grandchildren, if using SWMR */
    if (hdr->swmr_write && depth > 1)
        if (H5B2__update_child_flush_depends(hdr, depth, right_node_ptrs, 0, (unsigned)(*right_nrec + 1),
                                             left_child, right_child) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTUPDATE, FAIL, "unable to update child nodes to new parent");

#ifdef H5B2_DEBUG
    H5B2__assert_internal((hsize_t)0, hdr, internal);
    if (depth > 1) {
        H5B2__assert_internal2(internal->node_ptrs[idx].all_nrec, hdr, (H5B2_internal_t *)left_child,
                               (H5B2_internal_t *)right_child);
        H5B2__assert_internal2(internal->node_ptrs[idx + 1].all_nrec, hdr, (H5B2_internal_t *)right_child,
                               (H5B2_internal_t *)left_child);
    } /* end if */
    else {
        H5B2__assert_leaf2(hdr, (H5B2_leaf_t *)left_child, (H5B2_leaf_t *)right_child);
        H5B2__assert_leaf(hdr, (H5B2_leaf_t *)right_child);
    }  /* end else */
#endif /* H5B2_DEBUG */

done:
    /* Release child nodes (marked as dirty) */
    if (left_child && H5AC_unprotect(hdr->f, child_class, left_addr, left_child, left_child_flags) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree leaf node");
    if (right_child && H5AC_unprotect(hdr->f, child_class, right_addr, right_child, right_child_flags) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree leaf node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B2__split1() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__split_root
 *
 * Purpose:	Split the root node
 *
 * Return:	Success:	Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__split_root(H5B2_hdr_t *hdr)
{
    H5B2_internal_t *new_root       = NULL;               /* Pointer to new root node */
    unsigned         new_root_flags = H5AC__NO_FLAGS_SET; /* Cache flags for new root node */
    H5B2_node_ptr_t  old_root_ptr;                        /* Old node pointer to root node in B-tree */
    size_t           sz_max_nrec;                         /* Temporary variable for range checking */
    unsigned         u_max_nrec_size;                     /* Temporary variable for range checking */
    herr_t           ret_value = SUCCEED;                 /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(hdr);

    /* Update depth of B-tree */
    hdr->depth++;

    /* Re-allocate array of node info structs */
    if (NULL ==
        (hdr->node_info = H5FL_SEQ_REALLOC(H5B2_node_info_t, hdr->node_info, (size_t)(hdr->depth + 1))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");

    /* Update node info for new depth of tree */
    sz_max_nrec = H5B2_NUM_INT_REC(hdr, hdr->depth);
    H5_CHECKED_ASSIGN(hdr->node_info[hdr->depth].max_nrec, unsigned, sz_max_nrec, size_t);
    hdr->node_info[hdr->depth].split_nrec = (hdr->node_info[hdr->depth].max_nrec * hdr->split_percent) / 100;
    hdr->node_info[hdr->depth].merge_nrec = (hdr->node_info[hdr->depth].max_nrec * hdr->merge_percent) / 100;
    hdr->node_info[hdr->depth].cum_max_nrec =
        ((hdr->node_info[hdr->depth].max_nrec + 1) * hdr->node_info[hdr->depth - 1].cum_max_nrec) +
        hdr->node_info[hdr->depth].max_nrec;
    u_max_nrec_size = H5VM_limit_enc_size((uint64_t)hdr->node_info[hdr->depth].cum_max_nrec);
    H5_CHECKED_ASSIGN(hdr->node_info[hdr->depth].cum_max_nrec_size, uint8_t, u_max_nrec_size, unsigned);
    if (NULL == (hdr->node_info[hdr->depth].nat_rec_fac =
                     H5FL_fac_init(hdr->cls->nrec_size * hdr->node_info[hdr->depth].max_nrec)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTINIT, FAIL, "can't create node native key block factory");
    if (NULL == (hdr->node_info[hdr->depth].node_ptr_fac =
                     H5FL_fac_init(sizeof(H5B2_node_ptr_t) * (hdr->node_info[hdr->depth].max_nrec + 1))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTINIT, FAIL,
                    "can't create internal 'branch' node node pointer block factory");

    /* Keep old root node pointer info */
    old_root_ptr = hdr->root;

    /* Create new internal node to use as root */
    hdr->root.node_nrec = 0;
    if (H5B2__create_internal(hdr, hdr, &(hdr->root), hdr->depth) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTINIT, FAIL, "unable to create new internal node");

    /* Protect new root node */
    if (NULL ==
        (new_root = H5B2__protect_internal(hdr, hdr, &hdr->root, hdr->depth, false, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree internal node");

    /* Set first node pointer in root node to old root node pointer info */
    new_root->node_ptrs[0] = old_root_ptr;

    /* Split original root node */
    if (H5B2__split1(hdr, hdr->depth, &(hdr->root), NULL, new_root, &new_root_flags, 0) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTSPLIT, FAIL, "unable to split old root node");

done:
    /* Release new root node (marked as dirty) */
    if (new_root && H5AC_unprotect(hdr->f, H5AC_BT2_INT, hdr->root.addr, new_root, new_root_flags) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree internal node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B2__split_root() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__redistribute2
 *
 * Purpose:	Redistribute records between two nodes
 *
 * Return:	Success:	Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__redistribute2(H5B2_hdr_t *hdr, uint16_t depth, H5B2_internal_t *internal, unsigned idx)
{
    const H5AC_class_t *child_class;                             /* Pointer to child node's class info */
    haddr_t   left_addr = HADDR_UNDEF, right_addr = HADDR_UNDEF; /* Addresses of left & right child nodes */
    void     *left_child = NULL, *right_child = NULL;            /* Pointers to child nodes */
    uint16_t *left_nrec, *right_nrec;                            /* Pointers to child # of records */
    uint8_t  *left_native, *right_native;                        /* Pointers to childs' native records */
    H5B2_node_ptr_t *left_node_ptrs  = NULL,
                    *right_node_ptrs = NULL;            /* Pointers to childs' node pointer info */
    hssize_t left_moved_nrec = 0, right_moved_nrec = 0; /* Number of records moved, for internal redistrib */
    unsigned left_child_flags  = H5AC__NO_FLAGS_SET,
             right_child_flags = H5AC__NO_FLAGS_SET; /* Flags for unprotecting child nodes */
    herr_t ret_value           = SUCCEED;            /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(hdr);
    assert(internal);

    /* Check for the kind of B-tree node to redistribute */
    if (depth > 1) {
        H5B2_internal_t *left_internal;  /* Pointer to left internal node */
        H5B2_internal_t *right_internal; /* Pointer to right internal node */

        /* Setup information for unlocking child nodes */
        child_class = H5AC_BT2_INT;

        /* Lock left & right B-tree child nodes */
        /* (Shadow both nodes if doing SWMR writes) */
        if (NULL == (left_internal =
                         H5B2__protect_internal(hdr, internal, &internal->node_ptrs[idx],
                                                (uint16_t)(depth - 1), hdr->swmr_write, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree leaf node");
        left_addr = internal->node_ptrs[idx].addr;
        if (NULL == (right_internal =
                         H5B2__protect_internal(hdr, internal, &internal->node_ptrs[idx + 1],
                                                (uint16_t)(depth - 1), hdr->swmr_write, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree leaf node");
        right_addr = internal->node_ptrs[idx + 1].addr;

        /* More setup for child nodes */
        left_child      = left_internal;
        right_child     = right_internal;
        left_nrec       = &(left_internal->nrec);
        right_nrec      = &(right_internal->nrec);
        left_native     = left_internal->int_native;
        right_native    = right_internal->int_native;
        left_node_ptrs  = left_internal->node_ptrs;
        right_node_ptrs = right_internal->node_ptrs;
    } /* end if */
    else {
        H5B2_leaf_t *left_leaf;  /* Pointer to left leaf node */
        H5B2_leaf_t *right_leaf; /* Pointer to right leaf node */

        /* Setup information for unlocking child nodes */
        child_class = H5AC_BT2_LEAF;

        /* Lock left & right B-tree child nodes */
        /* (Shadow both nodes if doing SWMR writes) */
        if (NULL == (left_leaf = H5B2__protect_leaf(hdr, internal, &internal->node_ptrs[idx], hdr->swmr_write,
                                                    H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree leaf node");
        left_addr = internal->node_ptrs[idx].addr;
        if (NULL == (right_leaf = H5B2__protect_leaf(hdr, internal, &internal->node_ptrs[idx + 1],
                                                     hdr->swmr_write, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree leaf node");
        right_addr = internal->node_ptrs[idx + 1].addr;

        /* More setup for child nodes */
        left_child   = left_leaf;
        right_child  = right_leaf;
        left_nrec    = &(left_leaf->nrec);
        right_nrec   = &(right_leaf->nrec);
        left_native  = left_leaf->leaf_native;
        right_native = right_leaf->leaf_native;
    } /* end else */

#ifdef H5B2_DEBUG
    H5B2__assert_internal((hsize_t)0, hdr, internal);
    if (depth > 1) {
        H5B2__assert_internal2(internal->node_ptrs[idx].all_nrec, hdr, (H5B2_internal_t *)left_child,
                               (H5B2_internal_t *)right_child);
        H5B2__assert_internal2(internal->node_ptrs[idx + 1].all_nrec, hdr, (H5B2_internal_t *)right_child,
                               (H5B2_internal_t *)left_child);
    } /* end if */
    else {
        H5B2__assert_leaf2(hdr, (H5B2_leaf_t *)left_child, (H5B2_leaf_t *)right_child);
        H5B2__assert_leaf(hdr, (H5B2_leaf_t *)right_child);
    }  /* end else */
#endif /* H5B2_DEBUG */

    /* Determine whether to shuffle records left or right */
    if (*left_nrec < *right_nrec) {
        /* Moving record from right node to left */

        uint16_t new_right_nrec =
            (uint16_t)((*left_nrec + *right_nrec) / 2); /* New number of records for right child */
        uint16_t move_nrec =
            (uint16_t)(*right_nrec - new_right_nrec); /* Number of records to move from right node to left */

        /* Copy record from parent node down into left child */
        H5MM_memcpy(H5B2_NAT_NREC(left_native, hdr, *left_nrec), H5B2_INT_NREC(internal, hdr, idx),
                    hdr->cls->nrec_size);

        /* See if we need to move records from right node */
        if (move_nrec > 1)
            H5MM_memcpy(H5B2_NAT_NREC(left_native, hdr, (*left_nrec + 1)),
                        H5B2_NAT_NREC(right_native, hdr, 0), hdr->cls->nrec_size * (size_t)(move_nrec - 1));

        /* Move record from right node into parent node */
        H5MM_memcpy(H5B2_INT_NREC(internal, hdr, idx), H5B2_NAT_NREC(right_native, hdr, (move_nrec - 1)),
                    hdr->cls->nrec_size);

        /* Slide records in right node down */
        memmove(H5B2_NAT_NREC(right_native, hdr, 0), H5B2_NAT_NREC(right_native, hdr, move_nrec),
                hdr->cls->nrec_size * new_right_nrec);

        /* Handle node pointers, if we have an internal node */
        if (depth > 1) {
            hsize_t  moved_nrec = move_nrec; /* Total number of records moved, for internal redistrib */
            unsigned u;                      /* Local index variable */

            /* Count the number of records being moved */
            for (u = 0; u < move_nrec; u++)
                moved_nrec += right_node_ptrs[u].all_nrec;
            H5_CHECKED_ASSIGN(left_moved_nrec, hssize_t, moved_nrec, hsize_t);
            right_moved_nrec -= (hssize_t)moved_nrec;

            /* Copy node pointers from right node to left */
            H5MM_memcpy(&(left_node_ptrs[*left_nrec + 1]), &(right_node_ptrs[0]),
                        sizeof(H5B2_node_ptr_t) * move_nrec);

            /* Slide node pointers in right node down */
            memmove(&(right_node_ptrs[0]), &(right_node_ptrs[move_nrec]),
                    sizeof(H5B2_node_ptr_t) * (new_right_nrec + (unsigned)1));
        } /* end if */

        /* Update flush dependencies for grandchildren, if using SWMR */
        if (hdr->swmr_write && depth > 1)
            if (H5B2__update_child_flush_depends(hdr, depth, left_node_ptrs, (unsigned)(*left_nrec + 1),
                                                 (unsigned)(*left_nrec + move_nrec + 1), right_child,
                                                 left_child) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTUPDATE, FAIL, "unable to update child nodes to new parent");

        /* Update number of records in child nodes */
        *left_nrec  = (uint16_t)(*left_nrec + move_nrec);
        *right_nrec = new_right_nrec;

        /* Mark nodes as dirty */
        left_child_flags |= H5AC__DIRTIED_FLAG;
        right_child_flags |= H5AC__DIRTIED_FLAG;
    } /* end if */
    else {
        /* Moving record from left node to right */

        uint16_t new_left_nrec =
            (uint16_t)((*left_nrec + *right_nrec) / 2); /* New number of records for left child */
        uint16_t move_nrec =
            (uint16_t)(*left_nrec - new_left_nrec); /* Number of records to move from left node to right */

        /* Sanity check */
        assert(*left_nrec > *right_nrec);

        /* Slide records in right node up */
        memmove(H5B2_NAT_NREC(right_native, hdr, move_nrec), H5B2_NAT_NREC(right_native, hdr, 0),
                hdr->cls->nrec_size * (*right_nrec));

        /* Copy record from parent node down into right child */
        H5MM_memcpy(H5B2_NAT_NREC(right_native, hdr, (move_nrec - 1)), H5B2_INT_NREC(internal, hdr, idx),
                    hdr->cls->nrec_size);

        /* See if we need to move records from left node */
        if (move_nrec > 1)
            H5MM_memcpy(H5B2_NAT_NREC(right_native, hdr, 0),
                        H5B2_NAT_NREC(left_native, hdr, ((*left_nrec - move_nrec) + 1)),
                        hdr->cls->nrec_size * (size_t)(move_nrec - 1));

        /* Move record from left node into parent node */
        H5MM_memcpy(H5B2_INT_NREC(internal, hdr, idx),
                    H5B2_NAT_NREC(left_native, hdr, (*left_nrec - move_nrec)), hdr->cls->nrec_size);

        /* Handle node pointers, if we have an internal node */
        if (depth > 1) {
            hsize_t  moved_nrec = move_nrec; /* Total number of records moved, for internal redistrib */
            unsigned u;                      /* Local index variable */

            /* Slide node pointers in right node up */
            memmove(&(right_node_ptrs[move_nrec]), &(right_node_ptrs[0]),
                    sizeof(H5B2_node_ptr_t) * (size_t)(*right_nrec + 1));

            /* Copy node pointers from left node to right */
            H5MM_memcpy(&(right_node_ptrs[0]), &(left_node_ptrs[new_left_nrec + 1]),
                        sizeof(H5B2_node_ptr_t) * move_nrec);

            /* Count the number of records being moved */
            for (u = 0; u < move_nrec; u++)
                moved_nrec += right_node_ptrs[u].all_nrec;
            left_moved_nrec -= (hssize_t)moved_nrec;
            H5_CHECKED_ASSIGN(right_moved_nrec, hssize_t, moved_nrec, hsize_t);
        } /* end if */

        /* Update flush dependencies for grandchildren, if using SWMR */
        if (hdr->swmr_write && depth > 1)
            if (H5B2__update_child_flush_depends(hdr, depth, right_node_ptrs, 0, (unsigned)move_nrec,
                                                 left_child, right_child) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTUPDATE, FAIL, "unable to update child nodes to new parent");

        /* Update number of records in child nodes */
        *left_nrec  = new_left_nrec;
        *right_nrec = (uint16_t)(*right_nrec + move_nrec);

        /* Mark nodes as dirty */
        left_child_flags |= H5AC__DIRTIED_FLAG;
        right_child_flags |= H5AC__DIRTIED_FLAG;
    } /* end else */

    /* Update # of records in child nodes */
    internal->node_ptrs[idx].node_nrec     = *left_nrec;
    internal->node_ptrs[idx + 1].node_nrec = *right_nrec;

    /* Update total # of records in child B-trees */
    if (depth > 1) {
        internal->node_ptrs[idx].all_nrec =
            (hsize_t)((hssize_t)internal->node_ptrs[idx].all_nrec + left_moved_nrec);
        internal->node_ptrs[idx + 1].all_nrec =
            (hsize_t)((hssize_t)internal->node_ptrs[idx + 1].all_nrec + right_moved_nrec);
    } /* end if */
    else {
        internal->node_ptrs[idx].all_nrec     = internal->node_ptrs[idx].node_nrec;
        internal->node_ptrs[idx + 1].all_nrec = internal->node_ptrs[idx + 1].node_nrec;
    } /* end else */

#ifdef H5B2_DEBUG
    H5B2__assert_internal((hsize_t)0, hdr, internal);
    if (depth > 1) {
        H5B2__assert_internal2(internal->node_ptrs[idx].all_nrec, hdr, (H5B2_internal_t *)left_child,
                               (H5B2_internal_t *)right_child);
        H5B2__assert_internal2(internal->node_ptrs[idx + 1].all_nrec, hdr, (H5B2_internal_t *)right_child,
                               (H5B2_internal_t *)left_child);
    } /* end if */
    else {
        H5B2__assert_leaf2(hdr, (H5B2_leaf_t *)left_child, (H5B2_leaf_t *)right_child);
        H5B2__assert_leaf(hdr, (H5B2_leaf_t *)right_child);
    }  /* end else */
#endif /* H5B2_DEBUG */

done:
    /* Release child nodes (marked as dirty) */
    if (left_child && H5AC_unprotect(hdr->f, child_class, left_addr, left_child, left_child_flags) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree child node");
    if (right_child && H5AC_unprotect(hdr->f, child_class, right_addr, right_child, right_child_flags) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree child node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B2__redistribute2() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__redistribute3
 *
 * Purpose:	Redistribute records between three nodes
 *
 * Return:	Success:	Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__redistribute3(H5B2_hdr_t *hdr, uint16_t depth, H5B2_internal_t *internal, unsigned *internal_flags_ptr,
                    unsigned idx)
{
    H5B2_node_ptr_t *left_node_ptrs      = NULL,
                    *right_node_ptrs     = NULL;                 /* Pointers to childs' node pointer info */
    H5B2_node_ptr_t    *middle_node_ptrs = NULL;                 /* Pointers to childs' node pointer info */
    const H5AC_class_t *child_class;                             /* Pointer to child node's class info */
    haddr_t   left_addr = HADDR_UNDEF, right_addr = HADDR_UNDEF; /* Addresses of left & right child nodes */
    haddr_t   middle_addr = HADDR_UNDEF;                         /* Address of middle child node */
    void     *left_child = NULL, *right_child = NULL;            /* Pointers to child nodes */
    void     *middle_child = NULL;                               /* Pointers to middle child node */
    uint16_t *left_nrec, *right_nrec;                            /* Pointers to child # of records */
    uint16_t *middle_nrec;                                       /* Pointers to middle child # of records */
    uint8_t  *left_native, *right_native;                        /* Pointers to childs' native records */
    uint8_t  *middle_native;                             /* Pointers to middle child's native records */
    hssize_t  left_moved_nrec = 0, right_moved_nrec = 0; /* Number of records moved, for internal split */
    hssize_t  middle_moved_nrec = 0;                     /* Number of records moved, for internal split */
    unsigned  left_child_flags  = H5AC__NO_FLAGS_SET,
             right_child_flags  = H5AC__NO_FLAGS_SET; /* Flags for unprotecting child nodes */
    unsigned middle_child_flags = H5AC__NO_FLAGS_SET; /* Flags for unprotecting child nodes */
    herr_t   ret_value          = SUCCEED;            /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(hdr);
    assert(internal);
    assert(internal_flags_ptr);

    /* Check for the kind of B-tree node to redistribute */
    if (depth > 1) {
        H5B2_internal_t *left_internal;   /* Pointer to left internal node */
        H5B2_internal_t *middle_internal; /* Pointer to middle internal node */
        H5B2_internal_t *right_internal;  /* Pointer to right internal node */

        /* Setup information for unlocking child nodes */
        child_class = H5AC_BT2_INT;

        /* Lock B-tree child nodes */
        /* (Shadow all nodes if doing SWMR writes) */
        if (NULL == (left_internal =
                         H5B2__protect_internal(hdr, internal, &internal->node_ptrs[idx - 1],
                                                (uint16_t)(depth - 1), hdr->swmr_write, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree internal node");
        left_addr = internal->node_ptrs[idx - 1].addr;
        if (NULL == (middle_internal =
                         H5B2__protect_internal(hdr, internal, &internal->node_ptrs[idx],
                                                (uint16_t)(depth - 1), hdr->swmr_write, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree internal node");
        middle_addr = internal->node_ptrs[idx].addr;
        if (NULL == (right_internal =
                         H5B2__protect_internal(hdr, internal, &internal->node_ptrs[idx + 1],
                                                (uint16_t)(depth - 1), hdr->swmr_write, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree internal node");
        right_addr = internal->node_ptrs[idx + 1].addr;

        /* More setup for child nodes */
        left_child       = left_internal;
        middle_child     = middle_internal;
        right_child      = right_internal;
        left_nrec        = &(left_internal->nrec);
        middle_nrec      = &(middle_internal->nrec);
        right_nrec       = &(right_internal->nrec);
        left_native      = left_internal->int_native;
        middle_native    = middle_internal->int_native;
        right_native     = right_internal->int_native;
        left_node_ptrs   = left_internal->node_ptrs;
        middle_node_ptrs = middle_internal->node_ptrs;
        right_node_ptrs  = right_internal->node_ptrs;
    } /* end if */
    else {
        H5B2_leaf_t *left_leaf;   /* Pointer to left leaf node */
        H5B2_leaf_t *middle_leaf; /* Pointer to middle leaf node */
        H5B2_leaf_t *right_leaf;  /* Pointer to right leaf node */

        /* Setup information for unlocking child nodes */
        child_class = H5AC_BT2_LEAF;

        /* Lock B-tree child nodes */
        /* (Shadow all nodes if doing SWMR writes) */
        if (NULL == (left_leaf = H5B2__protect_leaf(hdr, internal, &internal->node_ptrs[idx - 1],
                                                    hdr->swmr_write, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree leaf node");
        left_addr = internal->node_ptrs[idx - 1].addr;
        if (NULL == (middle_leaf = H5B2__protect_leaf(hdr, internal, &internal->node_ptrs[idx],
                                                      hdr->swmr_write, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree leaf node");
        middle_addr = internal->node_ptrs[idx].addr;
        if (NULL == (right_leaf = H5B2__protect_leaf(hdr, internal, &internal->node_ptrs[idx + 1],
                                                     hdr->swmr_write, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree leaf node");
        right_addr = internal->node_ptrs[idx + 1].addr;

        /* More setup for child nodes */
        left_child    = left_leaf;
        middle_child  = middle_leaf;
        right_child   = right_leaf;
        left_nrec     = &(left_leaf->nrec);
        middle_nrec   = &(middle_leaf->nrec);
        right_nrec    = &(right_leaf->nrec);
        left_native   = left_leaf->leaf_native;
        middle_native = middle_leaf->leaf_native;
        right_native  = right_leaf->leaf_native;
    } /* end else */

    /* Redistribute records */
    {
        /* Compute new # of records in each node */
        unsigned total_nrec      = (unsigned)(*left_nrec + *middle_nrec + *right_nrec + 2);
        uint16_t new_middle_nrec = (uint16_t)((total_nrec - 2) / 3);
        uint16_t new_left_nrec   = (uint16_t)(((total_nrec - 2) - new_middle_nrec) / 2);
        uint16_t new_right_nrec  = (uint16_t)((total_nrec - 2) - (unsigned)(new_left_nrec + new_middle_nrec));
        uint16_t curr_middle_nrec = *middle_nrec;

        /* Sanity check rounding */
        assert(new_middle_nrec <= new_left_nrec);
        assert(new_middle_nrec <= new_right_nrec);

        /* Move records into left node */
        if (new_left_nrec > *left_nrec) {
            uint16_t moved_middle_nrec = 0; /* Number of records moved into left node */

            /* Move left parent record down to left node */
            H5MM_memcpy(H5B2_NAT_NREC(left_native, hdr, *left_nrec), H5B2_INT_NREC(internal, hdr, idx - 1),
                        hdr->cls->nrec_size);

            /* Move records from middle node into left node */
            if ((new_left_nrec - 1) > *left_nrec) {
                moved_middle_nrec = (uint16_t)(new_left_nrec - (*left_nrec + 1));
                H5MM_memcpy(H5B2_NAT_NREC(left_native, hdr, *left_nrec + 1),
                            H5B2_NAT_NREC(middle_native, hdr, 0), hdr->cls->nrec_size * moved_middle_nrec);
            } /* end if */

            /* Move record from middle node up to parent node */
            H5MM_memcpy(H5B2_INT_NREC(internal, hdr, idx - 1),
                        H5B2_NAT_NREC(middle_native, hdr, moved_middle_nrec), hdr->cls->nrec_size);
            moved_middle_nrec++;

            /* Slide records in middle node down */
            memmove(H5B2_NAT_NREC(middle_native, hdr, 0),
                    H5B2_NAT_NREC(middle_native, hdr, moved_middle_nrec),
                    hdr->cls->nrec_size * (size_t)(*middle_nrec - moved_middle_nrec));

            /* Move node pointers also if this is an internal node */
            if (depth > 1) {
                hsize_t  moved_nrec; /* Total number of records moved, for internal redistrib */
                unsigned move_nptrs; /* Number of node pointers to move */
                unsigned u;          /* Local index variable */

                /* Move middle node pointers into left node */
                move_nptrs = (unsigned)(new_left_nrec - *left_nrec);
                H5MM_memcpy(&(left_node_ptrs[*left_nrec + 1]), &(middle_node_ptrs[0]),
                            sizeof(H5B2_node_ptr_t) * move_nptrs);

                /* Count the number of records being moved into the left node */
                for (u = 0, moved_nrec = 0; u < move_nptrs; u++)
                    moved_nrec += middle_node_ptrs[u].all_nrec;
                left_moved_nrec = (hssize_t)(moved_nrec + move_nptrs);
                middle_moved_nrec -= (hssize_t)(moved_nrec + move_nptrs);

                /* Slide the node pointers in middle node down */
                memmove(&(middle_node_ptrs[0]), &(middle_node_ptrs[move_nptrs]),
                        sizeof(H5B2_node_ptr_t) * ((*middle_nrec - move_nptrs) + 1));
            } /* end if */

            /* Update flush dependencies for grandchildren, if using SWMR */
            if (hdr->swmr_write && depth > 1)
                if (H5B2__update_child_flush_depends(hdr, depth, left_node_ptrs, (unsigned)(*left_nrec + 1),
                                                     (unsigned)(*left_nrec + moved_middle_nrec + 1),
                                                     middle_child, left_child) < 0)
                    HGOTO_ERROR(H5E_BTREE, H5E_CANTUPDATE, FAIL,
                                "unable to update child nodes to new parent");

            /* Update the current number of records in middle node */
            curr_middle_nrec = (uint16_t)(curr_middle_nrec - moved_middle_nrec);

            /* Mark nodes as dirty */
            left_child_flags |= H5AC__DIRTIED_FLAG;
            middle_child_flags |= H5AC__DIRTIED_FLAG;
        } /* end if */

        /* Move records into right node */
        if (new_right_nrec > *right_nrec) {
            unsigned right_nrec_move =
                (unsigned)(new_right_nrec - *right_nrec); /* Number of records to move out of right node */

            /* Slide records in right node up */
            memmove(H5B2_NAT_NREC(right_native, hdr, right_nrec_move), H5B2_NAT_NREC(right_native, hdr, 0),
                    hdr->cls->nrec_size * (*right_nrec));

            /* Move right parent record down to right node */
            H5MM_memcpy(H5B2_NAT_NREC(right_native, hdr, right_nrec_move - 1),
                        H5B2_INT_NREC(internal, hdr, idx), hdr->cls->nrec_size);

            /* Move records from middle node into right node */
            if (right_nrec_move > 1)
                H5MM_memcpy(H5B2_NAT_NREC(right_native, hdr, 0),
                            H5B2_NAT_NREC(middle_native, hdr, ((curr_middle_nrec - right_nrec_move) + 1)),
                            hdr->cls->nrec_size * (right_nrec_move - 1));

            /* Move record from middle node up to parent node */
            H5MM_memcpy(H5B2_INT_NREC(internal, hdr, idx),
                        H5B2_NAT_NREC(middle_native, hdr, (curr_middle_nrec - right_nrec_move)),
                        hdr->cls->nrec_size);

            /* Move node pointers also if this is an internal node */
            if (depth > 1) {
                hsize_t  moved_nrec; /* Total number of records moved, for internal redistrib */
                unsigned u;          /* Local index variable */

                /* Slide the node pointers in right node up */
                memmove(&(right_node_ptrs[right_nrec_move]), &(right_node_ptrs[0]),
                        sizeof(H5B2_node_ptr_t) * (size_t)(*right_nrec + 1));

                /* Move middle node pointers into right node */
                H5MM_memcpy(&(right_node_ptrs[0]),
                            &(middle_node_ptrs[(curr_middle_nrec - right_nrec_move) + 1]),
                            sizeof(H5B2_node_ptr_t) * right_nrec_move);

                /* Count the number of records being moved into the right node */
                for (u = 0, moved_nrec = 0; u < right_nrec_move; u++)
                    moved_nrec += right_node_ptrs[u].all_nrec;
                right_moved_nrec = (hssize_t)(moved_nrec + right_nrec_move);
                middle_moved_nrec -= (hssize_t)(moved_nrec + right_nrec_move);
            } /* end if */

            /* Update flush dependencies for grandchildren, if using SWMR */
            if (hdr->swmr_write && depth > 1)
                if (H5B2__update_child_flush_depends(hdr, depth, right_node_ptrs, 0,
                                                     (unsigned)right_nrec_move, middle_child,
                                                     right_child) < 0)
                    HGOTO_ERROR(H5E_BTREE, H5E_CANTUPDATE, FAIL,
                                "unable to update child nodes to new parent");

            /* Update the current number of records in middle node */
            curr_middle_nrec = (uint16_t)(curr_middle_nrec - right_nrec_move);

            /* Mark nodes as dirty */
            middle_child_flags |= H5AC__DIRTIED_FLAG;
            right_child_flags |= H5AC__DIRTIED_FLAG;
        } /* end if */

        /* Move records out of left node */
        if (new_left_nrec < *left_nrec) {
            unsigned left_nrec_move =
                (unsigned)(*left_nrec - new_left_nrec); /* Number of records to move out of left node */

            /* Slide middle records up */
            memmove(H5B2_NAT_NREC(middle_native, hdr, left_nrec_move), H5B2_NAT_NREC(middle_native, hdr, 0),
                    hdr->cls->nrec_size * curr_middle_nrec);

            /* Move left parent record down to middle node */
            H5MM_memcpy(H5B2_NAT_NREC(middle_native, hdr, left_nrec_move - 1),
                        H5B2_INT_NREC(internal, hdr, idx - 1), hdr->cls->nrec_size);

            /* Move left records to middle node */
            if (left_nrec_move > 1)
                memmove(H5B2_NAT_NREC(middle_native, hdr, 0),
                        H5B2_NAT_NREC(left_native, hdr, new_left_nrec + 1),
                        hdr->cls->nrec_size * (left_nrec_move - 1));

            /* Move left parent record up from left node */
            H5MM_memcpy(H5B2_INT_NREC(internal, hdr, idx - 1), H5B2_NAT_NREC(left_native, hdr, new_left_nrec),
                        hdr->cls->nrec_size);

            /* Move node pointers also if this is an internal node */
            if (depth > 1) {
                hsize_t  moved_nrec; /* Total number of records moved, for internal redistrib */
                unsigned u;          /* Local index variable */

                /* Slide the node pointers in middle node up */
                memmove(&(middle_node_ptrs[left_nrec_move]), &(middle_node_ptrs[0]),
                        sizeof(H5B2_node_ptr_t) * (size_t)(curr_middle_nrec + 1));

                /* Move left node pointers into middle node */
                H5MM_memcpy(&(middle_node_ptrs[0]), &(left_node_ptrs[new_left_nrec + 1]),
                            sizeof(H5B2_node_ptr_t) * left_nrec_move);

                /* Count the number of records being moved into the left node */
                for (u = 0, moved_nrec = 0; u < left_nrec_move; u++)
                    moved_nrec += middle_node_ptrs[u].all_nrec;
                left_moved_nrec -= (hssize_t)(moved_nrec + left_nrec_move);
                middle_moved_nrec += (hssize_t)(moved_nrec + left_nrec_move);
            } /* end if */

            /* Update flush dependencies for grandchildren, if using SWMR */
            if (hdr->swmr_write && depth > 1)
                if (H5B2__update_child_flush_depends(hdr, depth, middle_node_ptrs, 0,
                                                     (unsigned)left_nrec_move, left_child, middle_child) < 0)
                    HGOTO_ERROR(H5E_BTREE, H5E_CANTUPDATE, FAIL,
                                "unable to update child nodes to new parent");

            /* Update the current number of records in middle node */
            curr_middle_nrec = (uint16_t)(curr_middle_nrec + left_nrec_move);

            /* Mark nodes as dirty */
            left_child_flags |= H5AC__DIRTIED_FLAG;
            middle_child_flags |= H5AC__DIRTIED_FLAG;
        } /* end if */

        /* Move records out of right node */
        if (new_right_nrec < *right_nrec) {
            unsigned right_nrec_move =
                (unsigned)(*right_nrec - new_right_nrec); /* Number of records to move out of right node */

            /* Move right parent record down to middle node */
            H5MM_memcpy(H5B2_NAT_NREC(middle_native, hdr, curr_middle_nrec),
                        H5B2_INT_NREC(internal, hdr, idx), hdr->cls->nrec_size);

            /* Move right records to middle node */
            memmove(H5B2_NAT_NREC(middle_native, hdr, (curr_middle_nrec + 1)),
                    H5B2_NAT_NREC(right_native, hdr, 0), hdr->cls->nrec_size * (right_nrec_move - 1));

            /* Move right parent record up from right node */
            H5MM_memcpy(H5B2_INT_NREC(internal, hdr, idx),
                        H5B2_NAT_NREC(right_native, hdr, right_nrec_move - 1), hdr->cls->nrec_size);

            /* Slide right records down */
            memmove(H5B2_NAT_NREC(right_native, hdr, 0), H5B2_NAT_NREC(right_native, hdr, right_nrec_move),
                    hdr->cls->nrec_size * new_right_nrec);

            /* Move node pointers also if this is an internal node */
            if (depth > 1) {
                hsize_t  moved_nrec; /* Total number of records moved, for internal redistrib */
                unsigned u;          /* Local index variable */

                /* Move right node pointers into middle node */
                H5MM_memcpy(&(middle_node_ptrs[curr_middle_nrec + 1]), &(right_node_ptrs[0]),
                            sizeof(H5B2_node_ptr_t) * right_nrec_move);

                /* Count the number of records being moved into the right node */
                for (u = 0, moved_nrec = 0; u < right_nrec_move; u++)
                    moved_nrec += right_node_ptrs[u].all_nrec;
                right_moved_nrec -= (hssize_t)(moved_nrec + right_nrec_move);
                middle_moved_nrec += (hssize_t)(moved_nrec + right_nrec_move);

                /* Slide the node pointers in right node down */
                memmove(&(right_node_ptrs[0]), &(right_node_ptrs[right_nrec_move]),
                        sizeof(H5B2_node_ptr_t) * (size_t)(new_right_nrec + 1));
            } /* end if */

            /* Update flush dependencies for grandchildren, if using SWMR */
            if (hdr->swmr_write && depth > 1)
                if (H5B2__update_child_flush_depends(
                        hdr, depth, middle_node_ptrs, (unsigned)(curr_middle_nrec + 1),
                        (unsigned)(curr_middle_nrec + right_nrec_move + 1), right_child, middle_child) < 0)
                    HGOTO_ERROR(H5E_BTREE, H5E_CANTUPDATE, FAIL,
                                "unable to update child nodes to new parent");

            /* Mark nodes as dirty */
            middle_child_flags |= H5AC__DIRTIED_FLAG;
            right_child_flags |= H5AC__DIRTIED_FLAG;
        } /* end if */

        /* Update # of records in nodes */
        *left_nrec   = new_left_nrec;
        *middle_nrec = new_middle_nrec;
        *right_nrec  = new_right_nrec;
    } /* end block */

    /* Update # of records in child nodes */
    internal->node_ptrs[idx - 1].node_nrec = *left_nrec;
    internal->node_ptrs[idx].node_nrec     = *middle_nrec;
    internal->node_ptrs[idx + 1].node_nrec = *right_nrec;

    /* Update total # of records in child B-trees */
    if (depth > 1) {
        internal->node_ptrs[idx - 1].all_nrec =
            (hsize_t)((hssize_t)internal->node_ptrs[idx - 1].all_nrec + left_moved_nrec);
        internal->node_ptrs[idx].all_nrec =
            (hsize_t)((hssize_t)internal->node_ptrs[idx].all_nrec + middle_moved_nrec);
        internal->node_ptrs[idx + 1].all_nrec =
            (hsize_t)((hssize_t)internal->node_ptrs[idx + 1].all_nrec + right_moved_nrec);
    } /* end if */
    else {
        internal->node_ptrs[idx - 1].all_nrec = internal->node_ptrs[idx - 1].node_nrec;
        internal->node_ptrs[idx].all_nrec     = internal->node_ptrs[idx].node_nrec;
        internal->node_ptrs[idx + 1].all_nrec = internal->node_ptrs[idx + 1].node_nrec;
    } /* end else */

    /* Mark parent as dirty */
    *internal_flags_ptr |= H5AC__DIRTIED_FLAG;

#ifdef H5B2_DEBUG
    H5B2__assert_internal((hsize_t)0, hdr, internal);
    if (depth > 1) {
        H5B2__assert_internal2(internal->node_ptrs[idx - 1].all_nrec, hdr, (H5B2_internal_t *)left_child,
                               (H5B2_internal_t *)middle_child);
        H5B2__assert_internal2(internal->node_ptrs[idx].all_nrec, hdr, (H5B2_internal_t *)middle_child,
                               (H5B2_internal_t *)left_child);
        H5B2__assert_internal2(internal->node_ptrs[idx].all_nrec, hdr, (H5B2_internal_t *)middle_child,
                               (H5B2_internal_t *)right_child);
        H5B2__assert_internal2(internal->node_ptrs[idx + 1].all_nrec, hdr, (H5B2_internal_t *)right_child,
                               (H5B2_internal_t *)middle_child);
    } /* end if */
    else {
        H5B2__assert_leaf2(hdr, (H5B2_leaf_t *)left_child, (H5B2_leaf_t *)middle_child);
        H5B2__assert_leaf2(hdr, (H5B2_leaf_t *)middle_child, (H5B2_leaf_t *)right_child);
        H5B2__assert_leaf(hdr, (H5B2_leaf_t *)right_child);
    }  /* end else */
#endif /* H5B2_DEBUG */

done:
    /* Unlock child nodes (marked as dirty) */
    if (left_child && H5AC_unprotect(hdr->f, child_class, left_addr, left_child, left_child_flags) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree child node");
    if (middle_child &&
        H5AC_unprotect(hdr->f, child_class, middle_addr, middle_child, middle_child_flags) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree child node");
    if (right_child && H5AC_unprotect(hdr->f, child_class, right_addr, right_child, right_child_flags) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree child node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B2__redistribute3() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__merge2
 *
 * Purpose:	Perform a 2->1 node merge
 *
 * Return:	Success:	Non-negative
 *
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__merge2(H5B2_hdr_t *hdr, uint16_t depth, H5B2_node_ptr_t *curr_node_ptr,
             unsigned *parent_cache_info_flags_ptr, H5B2_internal_t *internal, unsigned *internal_flags_ptr,
             unsigned idx)
{
    const H5AC_class_t *child_class;                             /* Pointer to child node's class info */
    haddr_t   left_addr = HADDR_UNDEF, right_addr = HADDR_UNDEF; /* Addresses of left & right child nodes */
    void     *left_child = NULL, *right_child = NULL;            /* Pointers to left & right child nodes */
    uint16_t *left_nrec, *right_nrec;     /* Pointers to left & right child # of records */
    uint8_t  *left_native, *right_native; /* Pointers to left & right children's native records */
    H5B2_node_ptr_t *left_node_ptrs  = NULL,
                    *right_node_ptrs = NULL; /* Pointers to childs' node pointer info */
    unsigned left_child_flags        = H5AC__NO_FLAGS_SET,
             right_child_flags       = H5AC__NO_FLAGS_SET; /* Flags for unprotecting child nodes */
    herr_t ret_value                 = SUCCEED;            /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(hdr);
    assert(curr_node_ptr);
    assert(internal);
    assert(internal_flags_ptr);

    /* Check for the kind of B-tree node to split */
    if (depth > 1) {
        H5B2_internal_t *left_internal;  /* Pointer to left internal node */
        H5B2_internal_t *right_internal; /* Pointer to right internal node */

        /* Setup information for unlocking child nodes */
        child_class = H5AC_BT2_INT;

        /* Lock left & right B-tree child nodes */
        /* (Shadow the left node if doing SWMR writes) */
        if (NULL == (left_internal =
                         H5B2__protect_internal(hdr, internal, &internal->node_ptrs[idx],
                                                (uint16_t)(depth - 1), hdr->swmr_write, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree internal node");
        left_addr = internal->node_ptrs[idx].addr;
        if (NULL ==
            (right_internal = H5B2__protect_internal(hdr, internal, &internal->node_ptrs[idx + 1],
                                                     (uint16_t)(depth - 1), false, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree internal node");
        right_addr = internal->node_ptrs[idx + 1].addr;

        /* More setup for accessing child node information */
        left_child      = left_internal;
        right_child     = right_internal;
        left_nrec       = &(left_internal->nrec);
        right_nrec      = &(right_internal->nrec);
        left_native     = left_internal->int_native;
        right_native    = right_internal->int_native;
        left_node_ptrs  = left_internal->node_ptrs;
        right_node_ptrs = right_internal->node_ptrs;
    } /* end if */
    else {
        H5B2_leaf_t *left_leaf;  /* Pointer to left leaf node */
        H5B2_leaf_t *right_leaf; /* Pointer to right leaf node */

        /* Setup information for unlocking child nodes */
        child_class = H5AC_BT2_LEAF;

        /* Lock left & right B-tree child nodes */
        /* (Shadow the left node if doing SWMR writes) */
        if (NULL == (left_leaf = H5B2__protect_leaf(hdr, internal, &internal->node_ptrs[idx], hdr->swmr_write,
                                                    H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree leaf node");
        left_addr = internal->node_ptrs[idx].addr;
        if (NULL == (right_leaf = H5B2__protect_leaf(hdr, internal, &internal->node_ptrs[idx + 1], false,
                                                     H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree leaf node");
        right_addr = internal->node_ptrs[idx + 1].addr;

        /* More setup for accessing child node information */
        left_child   = left_leaf;
        right_child  = right_leaf;
        left_nrec    = &(left_leaf->nrec);
        right_nrec   = &(right_leaf->nrec);
        left_native  = left_leaf->leaf_native;
        right_native = right_leaf->leaf_native;
    } /* end else */

    /* Redistribute records into left node */
    {
        /* Copy record from parent node to proper location */
        H5MM_memcpy(H5B2_NAT_NREC(left_native, hdr, *left_nrec), H5B2_INT_NREC(internal, hdr, idx),
                    hdr->cls->nrec_size);

        /* Copy records from right node to left node */
        H5MM_memcpy(H5B2_NAT_NREC(left_native, hdr, *left_nrec + 1), H5B2_NAT_NREC(right_native, hdr, 0),
                    hdr->cls->nrec_size * (*right_nrec));

        /* Copy node pointers from right node into left node */
        if (depth > 1)
            H5MM_memcpy(&(left_node_ptrs[*left_nrec + 1]), &(right_node_ptrs[0]),
                        sizeof(H5B2_node_ptr_t) * (size_t)(*right_nrec + 1));

        /* Update flush dependencies for grandchildren, if using SWMR */
        if (hdr->swmr_write && depth > 1)
            if (H5B2__update_child_flush_depends(hdr, depth, left_node_ptrs, (unsigned)(*left_nrec + 1),
                                                 (unsigned)(*left_nrec + *right_nrec + 2), right_child,
                                                 left_child) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTUPDATE, FAIL, "unable to update child nodes to new parent");

        /* Update # of records in left node */
        *left_nrec = (uint16_t)(*left_nrec + *right_nrec + 1);

        /* Mark nodes as dirty */
        left_child_flags |= H5AC__DIRTIED_FLAG;
        right_child_flags |= H5AC__DELETED_FLAG;
        if (!(hdr->swmr_write))
            right_child_flags |= H5AC__DIRTIED_FLAG | H5AC__FREE_FILE_SPACE_FLAG;
    } /* end block */

    /* Update # of records in child nodes */
    internal->node_ptrs[idx].node_nrec = *left_nrec;

    /* Update total # of records in child B-trees */
    internal->node_ptrs[idx].all_nrec += internal->node_ptrs[idx + 1].all_nrec + 1;

    /* Slide records in parent node down, to eliminate demoted record */
    if ((idx + 1) < internal->nrec) {
        memmove(H5B2_INT_NREC(internal, hdr, idx), H5B2_INT_NREC(internal, hdr, idx + 1),
                hdr->cls->nrec_size * (internal->nrec - (idx + 1)));
        memmove(&(internal->node_ptrs[idx + 1]), &(internal->node_ptrs[idx + 2]),
                sizeof(H5B2_node_ptr_t) * (internal->nrec - (idx + 1)));
    } /* end if */

    /* Update # of records in parent node */
    internal->nrec--;

    /* Mark parent as dirty */
    *internal_flags_ptr |= H5AC__DIRTIED_FLAG;

    /* Update grandparent info */
    curr_node_ptr->node_nrec--;

    /* Mark grandparent as dirty, if given */
    if (parent_cache_info_flags_ptr)
        *parent_cache_info_flags_ptr |= H5AC__DIRTIED_FLAG;

#ifdef H5B2_DEBUG
    H5B2__assert_internal((hsize_t)0, hdr, internal);
    if (depth > 1)
        H5B2__assert_internal(internal->node_ptrs[idx].all_nrec, hdr, (H5B2_internal_t *)left_child);
    else
        H5B2__assert_leaf(hdr, (H5B2_leaf_t *)left_child);
#endif /* H5B2_DEBUG */

done:
    /* Unlock left node (marked as dirty) */
    if (left_child && H5AC_unprotect(hdr->f, child_class, left_addr, left_child, left_child_flags) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree child node");

    /* Delete right node & remove from cache (marked as dirty) */
    if (right_child && H5AC_unprotect(hdr->f, child_class, right_addr, right_child, right_child_flags) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree child node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B2__merge2() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__merge3
 *
 * Purpose:	Perform a 3->2 node merge
 *
 * Return:	Success:	Non-negative
 *
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__merge3(H5B2_hdr_t *hdr, uint16_t depth, H5B2_node_ptr_t *curr_node_ptr,
             unsigned *parent_cache_info_flags_ptr, H5B2_internal_t *internal, unsigned *internal_flags_ptr,
             unsigned idx)
{
    const H5AC_class_t *child_class;                             /* Pointer to child node's class info */
    haddr_t   left_addr = HADDR_UNDEF, right_addr = HADDR_UNDEF; /* Addresses of left & right child nodes */
    haddr_t   middle_addr = HADDR_UNDEF;                         /* Address of middle child node */
    void     *left_child = NULL, *right_child = NULL;            /* Pointers to left & right child nodes */
    void     *middle_child = NULL;                               /* Pointer to middle child node */
    uint16_t *left_nrec, *right_nrec;     /* Pointers to left & right child # of records */
    uint16_t *middle_nrec;                /* Pointer to middle child # of records */
    uint8_t  *left_native, *right_native; /* Pointers to left & right children's native records */
    uint8_t  *middle_native;              /* Pointer to middle child's native records */
    H5B2_node_ptr_t *left_node_ptrs   = NULL,
                    *right_node_ptrs  = NULL; /* Pointers to childs' node pointer info */
    H5B2_node_ptr_t *middle_node_ptrs = NULL; /* Pointer to child's node pointer info */
    hsize_t          middle_moved_nrec;       /* Number of records moved, for internal split */
    unsigned         left_child_flags = H5AC__NO_FLAGS_SET,
             right_child_flags        = H5AC__NO_FLAGS_SET; /* Flags for unprotecting child nodes */
    unsigned middle_child_flags       = H5AC__NO_FLAGS_SET; /* Flags for unprotecting child nodes */
    herr_t   ret_value                = SUCCEED;            /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(hdr);
    assert(curr_node_ptr);
    assert(internal);
    assert(internal_flags_ptr);

    /* Check for the kind of B-tree node to split */
    if (depth > 1) {
        H5B2_internal_t *left_internal;   /* Pointer to left internal node */
        H5B2_internal_t *middle_internal; /* Pointer to middle internal node */
        H5B2_internal_t *right_internal;  /* Pointer to right internal node */

        /* Setup information for unlocking child nodes */
        child_class = H5AC_BT2_INT;

        /* Lock B-tree child nodes */
        /* (Shadow left and middle nodes if doing SWMR writes) */
        if (NULL == (left_internal =
                         H5B2__protect_internal(hdr, internal, &internal->node_ptrs[idx - 1],
                                                (uint16_t)(depth - 1), hdr->swmr_write, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree internal node");
        left_addr = internal->node_ptrs[idx - 1].addr;
        if (NULL == (middle_internal =
                         H5B2__protect_internal(hdr, internal, &internal->node_ptrs[idx],
                                                (uint16_t)(depth - 1), hdr->swmr_write, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree internal node");
        middle_addr = internal->node_ptrs[idx].addr;
        if (NULL ==
            (right_internal = H5B2__protect_internal(hdr, internal, &internal->node_ptrs[idx + 1],
                                                     (uint16_t)(depth - 1), false, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree internal node");
        right_addr = internal->node_ptrs[idx + 1].addr;

        /* More setup for accessing child node information */
        left_child       = left_internal;
        middle_child     = middle_internal;
        right_child      = right_internal;
        left_nrec        = &(left_internal->nrec);
        middle_nrec      = &(middle_internal->nrec);
        right_nrec       = &(right_internal->nrec);
        left_native      = left_internal->int_native;
        middle_native    = middle_internal->int_native;
        right_native     = right_internal->int_native;
        left_node_ptrs   = left_internal->node_ptrs;
        middle_node_ptrs = middle_internal->node_ptrs;
        right_node_ptrs  = right_internal->node_ptrs;
    } /* end if */
    else {
        H5B2_leaf_t *left_leaf;   /* Pointer to left leaf node */
        H5B2_leaf_t *middle_leaf; /* Pointer to middle leaf node */
        H5B2_leaf_t *right_leaf;  /* Pointer to right leaf node */

        /* Setup information for unlocking child nodes */
        child_class = H5AC_BT2_LEAF;

        /* Lock B-tree child nodes */
        /* (Shadow left and middle nodes if doing SWMR writes) */
        if (NULL == (left_leaf = H5B2__protect_leaf(hdr, internal, &internal->node_ptrs[idx - 1],
                                                    hdr->swmr_write, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree leaf node");
        left_addr = internal->node_ptrs[idx - 1].addr;
        if (NULL == (middle_leaf = H5B2__protect_leaf(hdr, internal, &internal->node_ptrs[idx],
                                                      hdr->swmr_write, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree leaf node");
        middle_addr = internal->node_ptrs[idx].addr;
        if (NULL == (right_leaf = H5B2__protect_leaf(hdr, internal, &internal->node_ptrs[idx + 1], false,
                                                     H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree leaf node");
        right_addr = internal->node_ptrs[idx + 1].addr;

        /* More setup for accessing child node information */
        left_child    = left_leaf;
        middle_child  = middle_leaf;
        right_child   = right_leaf;
        left_nrec     = &(left_leaf->nrec);
        middle_nrec   = &(middle_leaf->nrec);
        right_nrec    = &(right_leaf->nrec);
        left_native   = left_leaf->leaf_native;
        middle_native = middle_leaf->leaf_native;
        right_native  = right_leaf->leaf_native;
    } /* end else */

    /* Redistribute records into left node */
    {
        unsigned total_nrec       = (unsigned)(*left_nrec + *middle_nrec + *right_nrec + 2);
        unsigned middle_nrec_move = ((total_nrec - 1) / 2) - *left_nrec;

        /* Set the base number of records moved from middle node */
        middle_moved_nrec = middle_nrec_move;

        /* Copy record from parent node to proper location in left node */
        H5MM_memcpy(H5B2_NAT_NREC(left_native, hdr, *left_nrec), H5B2_INT_NREC(internal, hdr, idx - 1),
                    hdr->cls->nrec_size);

        /* Copy records from middle node to left node */
        H5MM_memcpy(H5B2_NAT_NREC(left_native, hdr, *left_nrec + 1), H5B2_NAT_NREC(middle_native, hdr, 0),
                    hdr->cls->nrec_size * (middle_nrec_move - 1));

        /* Copy record from middle node to proper location in parent node */
        H5MM_memcpy(H5B2_INT_NREC(internal, hdr, idx - 1),
                    H5B2_NAT_NREC(middle_native, hdr, (middle_nrec_move - 1)), hdr->cls->nrec_size);

        /* Slide records in middle node down */
        memmove(H5B2_NAT_NREC(middle_native, hdr, 0), H5B2_NAT_NREC(middle_native, hdr, middle_nrec_move),
                hdr->cls->nrec_size * (*middle_nrec - middle_nrec_move));

        /* Move node pointers also if this is an internal node */
        if (depth > 1) {
            unsigned u; /* Local index variable */

            /* Copy node pointers from middle node into left node */
            H5MM_memcpy(&(left_node_ptrs[*left_nrec + 1]), &(middle_node_ptrs[0]),
                        sizeof(H5B2_node_ptr_t) * middle_nrec_move);

            /* Count the number of records being moved into the left node */
            for (u = 0; u < middle_nrec_move; u++)
                middle_moved_nrec += middle_node_ptrs[u].all_nrec;

            /* Slide the node pointers in middle node down */
            memmove(&(middle_node_ptrs[0]), &(middle_node_ptrs[middle_nrec_move]),
                    sizeof(H5B2_node_ptr_t) * (size_t)((unsigned)(*middle_nrec + 1) - middle_nrec_move));
        } /* end if */

        /* Update flush dependencies for grandchildren, if using SWMR */
        if (hdr->swmr_write && depth > 1)
            if (H5B2__update_child_flush_depends(hdr, depth, left_node_ptrs, (unsigned)(*left_nrec + 1),
                                                 (unsigned)(*left_nrec + middle_nrec_move + 1), middle_child,
                                                 left_child) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTUPDATE, FAIL, "unable to update child nodes to new parent");

        /* Update # of records in left & middle nodes */
        *left_nrec   = (uint16_t)(*left_nrec + middle_nrec_move);
        *middle_nrec = (uint16_t)(*middle_nrec - middle_nrec_move);

        /* Mark nodes as dirty */
        left_child_flags |= H5AC__DIRTIED_FLAG;
        middle_child_flags |= H5AC__DIRTIED_FLAG;
    } /* end block */

    /* Redistribute records into middle node */
    {
        /* Copy record from parent node to proper location in middle node */
        H5MM_memcpy(H5B2_NAT_NREC(middle_native, hdr, *middle_nrec), H5B2_INT_NREC(internal, hdr, idx),
                    hdr->cls->nrec_size);

        /* Copy records from right node to middle node */
        H5MM_memcpy(H5B2_NAT_NREC(middle_native, hdr, *middle_nrec + 1), H5B2_NAT_NREC(right_native, hdr, 0),
                    hdr->cls->nrec_size * (*right_nrec));

        /* Move node pointers also if this is an internal node */
        if (depth > 1)
            /* Copy node pointers from right node into middle node */
            H5MM_memcpy(&(middle_node_ptrs[*middle_nrec + 1]), &(right_node_ptrs[0]),
                        sizeof(H5B2_node_ptr_t) * (size_t)(*right_nrec + 1));

        /* Update flush dependencies for grandchildren, if using SWMR */
        if (hdr->swmr_write && depth > 1)
            if (H5B2__update_child_flush_depends(hdr, depth, middle_node_ptrs, (unsigned)(*middle_nrec + 1),
                                                 (unsigned)(*middle_nrec + *right_nrec + 2), right_child,
                                                 middle_child) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTUPDATE, FAIL, "unable to update child nodes to new parent");

        /* Update # of records in middle node */
        *middle_nrec = (uint16_t)(*middle_nrec + (*right_nrec + 1));

        /* Mark nodes as dirty */
        middle_child_flags |= H5AC__DIRTIED_FLAG;
        right_child_flags |= H5AC__DELETED_FLAG;
        if (!(hdr->swmr_write))
            right_child_flags |= H5AC__DIRTIED_FLAG | H5AC__FREE_FILE_SPACE_FLAG;
    } /* end block */

    /* Update # of records in child nodes */
    internal->node_ptrs[idx - 1].node_nrec = *left_nrec;
    internal->node_ptrs[idx].node_nrec     = *middle_nrec;

    /* Update total # of records in child B-trees */
    internal->node_ptrs[idx - 1].all_nrec += middle_moved_nrec;
    internal->node_ptrs[idx].all_nrec += (internal->node_ptrs[idx + 1].all_nrec + 1) - middle_moved_nrec;

    /* Slide records in parent node down, to eliminate demoted record */
    if ((idx + 1) < internal->nrec) {
        memmove(H5B2_INT_NREC(internal, hdr, idx), H5B2_INT_NREC(internal, hdr, idx + 1),
                hdr->cls->nrec_size * (internal->nrec - (idx + 1)));
        memmove(&(internal->node_ptrs[idx + 1]), &(internal->node_ptrs[idx + 2]),
                sizeof(H5B2_node_ptr_t) * (internal->nrec - (idx + 1)));
    } /* end if */

    /* Update # of records in parent node */
    internal->nrec--;

    /* Mark parent as dirty */
    *internal_flags_ptr |= H5AC__DIRTIED_FLAG;

    /* Update grandparent info */
    curr_node_ptr->node_nrec--;

    /* Mark grandparent as dirty, if given */
    if (parent_cache_info_flags_ptr)
        *parent_cache_info_flags_ptr |= H5AC__DIRTIED_FLAG;

#ifdef H5B2_DEBUG
    H5B2__assert_internal((hsize_t)0, hdr, internal);
    if (depth > 1) {
        H5B2__assert_internal2(internal->node_ptrs[idx - 1].all_nrec, hdr, (H5B2_internal_t *)left_child,
                               (H5B2_internal_t *)middle_child);
        H5B2__assert_internal(internal->node_ptrs[idx].all_nrec, hdr, (H5B2_internal_t *)middle_child);
    } /* end if */
    else {
        H5B2__assert_leaf2(hdr, (H5B2_leaf_t *)left_child, (H5B2_leaf_t *)middle_child);
        H5B2__assert_leaf(hdr, (H5B2_leaf_t *)middle_child);
    }  /* end else */
#endif /* H5B2_DEBUG */

done:
    /* Unlock left & middle nodes (marked as dirty) */
    if (left_child && H5AC_unprotect(hdr->f, child_class, left_addr, left_child, left_child_flags) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree child node");
    if (middle_child &&
        H5AC_unprotect(hdr->f, child_class, middle_addr, middle_child, middle_child_flags) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree child node");

    /* Delete right node & remove from cache (marked as dirty) */
    if (right_child && H5AC_unprotect(hdr->f, child_class, right_addr, right_child, right_child_flags) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree child node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B2__merge3() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__insert
 *
 * Purpose:	Adds a new record to the B-tree.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__insert(H5B2_hdr_t *hdr, void *udata)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(hdr);
    assert(udata);

    /* Check if the root node is allocated yet */
    if (!H5_addr_defined(hdr->root.addr)) {
        /* Create root node as leaf node in B-tree */
        if (H5B2__create_leaf(hdr, hdr, &(hdr->root)) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTINIT, FAIL, "unable to create root node");
    } /* end if */
    /* Check if we need to split the root node (equiv. to a 1->2 node split) */
    else if (hdr->root.node_nrec == hdr->node_info[hdr->depth].split_nrec) {
        /* Split root node */
        if (H5B2__split_root(hdr) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTSPLIT, FAIL, "unable to split root node");
    } /* end if */

    /* Attempt to insert record into B-tree */
    if (hdr->depth > 0) {
        if (H5B2__insert_internal(hdr, hdr->depth, NULL, &hdr->root, H5B2_POS_ROOT, hdr, udata) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTINSERT, FAIL, "unable to insert record into B-tree internal node");
    } /* end if */
    else {
        if (H5B2__insert_leaf(hdr, &hdr->root, H5B2_POS_ROOT, hdr, udata) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTINSERT, FAIL, "unable to insert record into B-tree leaf node");
    } /* end else */

    /* Mark B-tree header as dirty */
    if (H5B2__hdr_dirty(hdr) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTMARKDIRTY, FAIL, "unable to mark B-tree header dirty");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5B2__insert() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__iterate_node
 *
 * Purpose:	Iterate over all the records from a B-tree node, in "in-order"
 *		order, making a callback for each record.
 *
 *              If the callback returns non-zero, the iteration breaks out
 *              without finishing all the records.
 *
 * Return:	Value from callback, non-negative on success, negative on error
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__iterate_node(H5B2_hdr_t *hdr, uint16_t depth, H5B2_node_ptr_t *curr_node, void *parent,
                   H5B2_operator_t op, void *op_data)
{
    const H5AC_class_t *curr_node_class = NULL;   /* Pointer to current node's class info */
    void               *node            = NULL;   /* Pointers to current node */
    uint8_t            *node_native;              /* Pointers to node's native records */
    uint8_t            *native      = NULL;       /* Pointers to copy of node's native records */
    H5B2_node_ptr_t    *node_ptrs   = NULL;       /* Pointers to node's node pointers */
    bool                node_pinned = false;      /* Whether node is pinned */
    unsigned            u;                        /* Local index */
    herr_t              ret_value = H5_ITER_CONT; /* Iterator return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(hdr);
    assert(curr_node);
    assert(op);

    /* Protect current node & set up variables */
    if (depth > 0) {
        H5B2_internal_t *internal; /* Pointer to internal node */

        /* Lock the current B-tree node */
        if (NULL ==
            (internal = H5B2__protect_internal(hdr, parent, curr_node, depth, false, H5AC__READ_ONLY_FLAG)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree internal node");

        /* Set up information about current node */
        curr_node_class = H5AC_BT2_INT;
        node            = internal;
        node_native     = internal->int_native;

        /* Allocate space for the node pointers in memory */
        if (NULL == (node_ptrs = (H5B2_node_ptr_t *)H5FL_FAC_MALLOC(hdr->node_info[depth].node_ptr_fac)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                        "memory allocation failed for B-tree internal node pointers");

        /* Copy the node pointers */
        H5MM_memcpy(node_ptrs, internal->node_ptrs,
                    (sizeof(H5B2_node_ptr_t) * (size_t)(curr_node->node_nrec + 1)));
    } /* end if */
    else {
        H5B2_leaf_t *leaf; /* Pointer to leaf node */

        /* Lock the current B-tree node */
        if (NULL == (leaf = H5B2__protect_leaf(hdr, parent, (H5B2_node_ptr_t *)curr_node, false,
                                               H5AC__READ_ONLY_FLAG)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree leaf node");

        /* Set up information about current node */
        curr_node_class = H5AC_BT2_LEAF;
        node            = leaf;
        node_native     = leaf->leaf_native;
    } /* end else */

    /* Allocate space for the native keys in memory */
    if (NULL == (native = (uint8_t *)H5FL_FAC_MALLOC(hdr->node_info[depth].nat_rec_fac)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                    "memory allocation failed for B-tree internal native keys");

    /* Copy the native keys */
    H5MM_memcpy(native, node_native, (hdr->cls->nrec_size * curr_node->node_nrec));

    /* Unlock the node */
    if (H5AC_unprotect(hdr->f, curr_node_class, curr_node->addr, node,
                       (unsigned)(hdr->swmr_write ? H5AC__PIN_ENTRY_FLAG : H5AC__NO_FLAGS_SET)) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree node");
    if (hdr->swmr_write)
        node_pinned = true;
    else
        node = NULL;

    /* Iterate through records, in order */
    for (u = 0; u < curr_node->node_nrec && !ret_value; u++) {
        /* Descend into child node, if current node is an internal node */
        if (depth > 0)
            if ((ret_value =
                     H5B2__iterate_node(hdr, (uint16_t)(depth - 1), &(node_ptrs[u]), node, op, op_data)) < 0)
                HERROR(H5E_BTREE, H5E_CANTLIST, "node iteration failed");

        /* Make callback for current record */
        if (!ret_value)
            if ((ret_value = (op)(H5B2_NAT_NREC(native, hdr, u), op_data)) < 0)
                HERROR(H5E_BTREE, H5E_CANTLIST, "iterator function failed");
    } /* end for */

    /* Descend into last child node, if current node is an internal node */
    if (!ret_value && depth > 0)
        if ((ret_value = H5B2__iterate_node(hdr, (uint16_t)(depth - 1), &(node_ptrs[u]), node, op, op_data)) <
            0)
            HERROR(H5E_BTREE, H5E_CANTLIST, "node iteration failed");

done:
    /* Unpin the node if it was pinned */
    if (node_pinned && H5AC_unpin_entry(node) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPIN, FAIL, "can't unpin node");

    /* Release the node pointers & native records, if they were copied */
    if (node_ptrs)
        node_ptrs = (H5B2_node_ptr_t *)H5FL_FAC_FREE(hdr->node_info[depth].node_ptr_fac, node_ptrs);
    if (native)
        native = (uint8_t *)H5FL_FAC_FREE(hdr->node_info[depth].nat_rec_fac, native);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5B2__iterate_node() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__delete_node
 *
 * Purpose:	Iterate over all the nodes in a B-tree node deleting them
 *		after they no longer have any children
 *
 * Return:	Value from callback, non-negative on success, negative on error
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__delete_node(H5B2_hdr_t *hdr, uint16_t depth, H5B2_node_ptr_t *curr_node, void *parent, H5B2_remove_t op,
                  void *op_data)
{
    const H5AC_class_t *curr_node_class = NULL; /* Pointer to current node's class info */
    void               *node            = NULL; /* Pointers to current node */
    uint8_t            *native;                 /* Pointers to node's native records */
    herr_t              ret_value = SUCCEED;    /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(hdr);
    assert(curr_node);

    if (depth > 0) {
        H5B2_internal_t *internal; /* Pointer to internal node */
        unsigned         u;        /* Local index */

        /* Lock the current B-tree node */
        if (NULL ==
            (internal = H5B2__protect_internal(hdr, parent, curr_node, depth, false, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree internal node");

        /* Set up information about current node */
        curr_node_class = H5AC_BT2_INT;
        node            = internal;
        native          = internal->int_native;

        /* Descend into children */
        for (u = 0; u < internal->nrec + (unsigned)1; u++)
            if (H5B2__delete_node(hdr, (uint16_t)(depth - 1), &(internal->node_ptrs[u]), internal, op,
                                  op_data) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTLIST, FAIL, "node descent failed");
    } /* end if */
    else {
        H5B2_leaf_t *leaf; /* Pointer to leaf node */

        /* Lock the current B-tree node */
        if (NULL ==
            (leaf = H5B2__protect_leaf(hdr, parent, (H5B2_node_ptr_t *)curr_node, false, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree leaf node");

        /* Set up information about current node */
        curr_node_class = H5AC_BT2_LEAF;
        node            = leaf;
        native          = leaf->leaf_native;
    } /* end else */

    /* If there's a callback defined, iterate over the records in this node */
    if (op) {
        unsigned u; /* Local index */

        /* Iterate through records in this node */
        for (u = 0; u < curr_node->node_nrec; u++) {
            /* Make callback for each record */
            if ((op)(H5B2_NAT_NREC(native, hdr, u), op_data) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTLIST, FAIL, "iterator function failed");
        } /* end for */
    }     /* end if */

done:
    /* Unlock & delete current node */
    if (node && H5AC_unprotect(
                    hdr->f, curr_node_class, curr_node->addr, node,
                    (unsigned)(H5AC__DELETED_FLAG | (hdr->swmr_write ? 0 : H5AC__FREE_FILE_SPACE_FLAG))) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5B2__delete_node() */

/*-------------------------------------------------------------------------
 * Function:    H5B2__node_size
 *
 * Purpose:     Iterate over all the records from a B-tree node, collecting
 *		btree storage info.
 *
 * Return:      non-negative on success, negative on error
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__node_size(H5B2_hdr_t *hdr, uint16_t depth, H5B2_node_ptr_t *curr_node, void *parent,
                hsize_t *btree_size)
{
    H5B2_internal_t *internal  = NULL;    /* Pointer to internal node */
    herr_t           ret_value = SUCCEED; /* Iterator return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(hdr);
    assert(curr_node);
    assert(btree_size);
    assert(depth > 0);

    /* Lock the current B-tree node */
    if (NULL ==
        (internal = H5B2__protect_internal(hdr, parent, curr_node, depth, false, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree internal node");

    /* Recursively descend into child nodes, if we are above the "twig" level in the B-tree */
    if (depth > 1) {
        unsigned u; /* Local index */

        /* Descend into children */
        for (u = 0; u < internal->nrec + (unsigned)1; u++)
            if (H5B2__node_size(hdr, (uint16_t)(depth - 1), &(internal->node_ptrs[u]), internal, btree_size) <
                0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTLIST, FAIL, "node iteration failed");
    }    /* end if */
    else /* depth is 1: count all the leaf nodes from this node */
        *btree_size += (hsize_t)(internal->nrec + 1) * hdr->node_size;

    /* Count this node */
    *btree_size += hdr->node_size;

done:
    if (internal && H5AC_unprotect(hdr->f, H5AC_BT2_INT, curr_node->addr, internal, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5B2__node_size() */

/*-------------------------------------------------------------------------
 * Function:    H5B2__create_flush_depend
 *
 * Purpose:     Create a flush dependency between two data structure components
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__create_flush_depend(H5AC_info_t *parent_entry, H5AC_info_t *child_entry)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Sanity check */
    assert(parent_entry);
    assert(child_entry);

    /* Create a flush dependency between parent and child entry */
    if (H5AC_create_flush_dependency(parent_entry, child_entry) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTDEPEND, FAIL, "unable to create flush dependency");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B2__create_flush_depend() */

/*-------------------------------------------------------------------------
 * Function:    H5B2__update_flush_depend
 *
 * Purpose:     Update flush dependencies for children of a node
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__update_flush_depend(H5B2_hdr_t *hdr, unsigned depth, H5B2_node_ptr_t *node_ptr, void *old_parent,
                          void *new_parent)
{
    const H5AC_class_t *child_class;           /* Pointer to child node's class info */
    void               *child       = NULL;    /* Pointer to child node */
    unsigned            node_status = 0;       /* Node's status in the metadata cache */
    herr_t              ret_value   = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(hdr);
    assert(depth > 0);
    assert(node_ptr);
    assert(old_parent);
    assert(new_parent);

    /* Check the node's entry status in the metadata cache */
    if (H5AC_get_entry_status(hdr->f, node_ptr->addr, &node_status) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTGET, FAIL, "unable to check status of B-tree node");

    /* If the node is in the cache, check for retargeting its parent */
    if (node_status & H5AC_ES__IN_CACHE) {
        void **parent_ptr  = NULL;  /* Pointer to child node's parent */
        bool   update_deps = false; /* Whether to update flush dependencies */

        /* Get child node pointer */
        if (depth > 1) {
            H5B2_internal_t *child_int;

            /* Protect child */
            if (NULL == (child_int = H5B2__protect_internal(hdr, new_parent, node_ptr, (uint16_t)(depth - 1),
                                                            false, H5AC__NO_FLAGS_SET)))
                HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree internal node");
            child_class = H5AC_BT2_INT;
            child       = child_int;

            if (child_int->parent == old_parent) {
                parent_ptr  = &child_int->parent;
                update_deps = true;
            } /* end if */
            else
                assert(child_int->parent == new_parent);
        } /* end if */
        else {
            H5B2_leaf_t *child_leaf;

            /* Protect child */
            if (NULL == (child_leaf = H5B2__protect_leaf(hdr, new_parent, (H5B2_node_ptr_t *)node_ptr, false,
                                                         H5AC__NO_FLAGS_SET)))
                HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree leaf node");
            child_class = H5AC_BT2_LEAF;
            child       = child_leaf;

            if (child_leaf->parent == old_parent) {
                parent_ptr  = &child_leaf->parent;
                update_deps = true;
            } /* end if */
            else
                assert(child_leaf->parent == new_parent);
        } /* end else */

        /* Update flush dependencies if necessary */
        if (update_deps) {
            /* Sanity check */
            assert(parent_ptr);

            /* Switch the flush dependency for the node */
            if (H5B2__destroy_flush_depend((H5AC_info_t *)old_parent, (H5AC_info_t *)child) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTUNDEPEND, FAIL, "unable to destroy flush dependency");
            *parent_ptr = new_parent;
            if (H5B2__create_flush_depend((H5AC_info_t *)new_parent, (H5AC_info_t *)child) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTDEPEND, FAIL, "unable to create flush dependency");
        } /* end if */
    }     /* end if */

done:
    /* Unprotect the child */
    if (child)
        if (H5AC_unprotect(hdr->f, child_class, node_ptr->addr, child, H5AC__NO_FLAGS_SET) < 0)
            HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B2__update_flush_depend() */

/*-------------------------------------------------------------------------
 * Function:    H5B2__update_child_flush_depends
 *
 * Purpose:     Update flush dependencies for children of a node
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5B2__update_child_flush_depends(H5B2_hdr_t *hdr, unsigned depth, H5B2_node_ptr_t *node_ptrs,
                                 unsigned start_idx, unsigned end_idx, void *old_parent, void *new_parent)
{
    unsigned u;                   /* Local index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(hdr);
    assert(depth > 1);
    assert(node_ptrs);
    assert(start_idx <= end_idx);
    assert(old_parent);
    assert(new_parent);

    /* Loop over children */
    for (u = start_idx; u < end_idx; u++)
        /* Update parent for children */
        if (H5B2__update_flush_depend(hdr, depth - 1, &node_ptrs[u], old_parent, new_parent) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTUPDATE, FAIL, "unable to update child node to new parent");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B2__update_child_flush_depends() */

/*-------------------------------------------------------------------------
 * Function:    H5B2__destroy_flush_depend
 *
 * Purpose:     Destroy a flush dependency between two data structure components
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__destroy_flush_depend(H5AC_info_t *parent_entry, H5AC_info_t *child_entry)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Sanity check */
    assert(parent_entry);
    assert(child_entry);

    /* Destroy a flush dependency between parent and child entry */
    if (H5AC_destroy_flush_dependency(parent_entry, child_entry) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTUNDEPEND, FAIL, "unable to destroy flush dependency");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B2__destroy_flush_depend() */
