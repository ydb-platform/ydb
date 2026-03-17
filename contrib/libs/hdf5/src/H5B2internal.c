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
 * Created:		H5B2internal.c
 *
 * Purpose:		Routines for managing v2 B-tree internal nodes.
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
#include "H5MFprivate.h" /* File memory management		*/

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
static herr_t H5B2__shadow_internal(H5B2_internal_t *internal, H5B2_node_ptr_t *curr_node_ptr);

/*********************/
/* Package Variables */
/*********************/

/* Declare a free list to manage the H5B2_internal_t struct */
H5FL_DEFINE(H5B2_internal_t);

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:	H5B2__create_internal
 *
 * Purpose:	Creates empty internal node of a B-tree and update node pointer
 *              to point to it.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__create_internal(H5B2_hdr_t *hdr, void *parent, H5B2_node_ptr_t *node_ptr, uint16_t depth)
{
    H5B2_internal_t *internal  = NULL;    /* Pointer to new internal node created */
    bool             inserted  = false;   /* Whether the internal node was inserted into cache */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(hdr);
    assert(node_ptr);
    assert(depth > 0);

    /* Allocate memory for internal node information */
    if (NULL == (internal = H5FL_CALLOC(H5B2_internal_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed for B-tree internal info");

    /* Increment ref. count on B-tree header */
    if (H5B2__hdr_incr(hdr) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTINC, FAIL, "can't increment ref. count on B-tree header");

    /* Share B-tree header information */
    internal->hdr = hdr;

    /* Allocate space for the native keys in memory */
    if (NULL == (internal->int_native = (uint8_t *)H5FL_FAC_MALLOC(hdr->node_info[depth].nat_rec_fac)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                    "memory allocation failed for B-tree internal native keys");
    memset(internal->int_native, 0, hdr->cls->nrec_size * hdr->node_info[depth].max_nrec);

    /* Allocate space for the node pointers in memory */
    if (NULL ==
        (internal->node_ptrs = (H5B2_node_ptr_t *)H5FL_FAC_MALLOC(hdr->node_info[depth].node_ptr_fac)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                    "memory allocation failed for B-tree internal node pointers");
    memset(internal->node_ptrs, 0, sizeof(H5B2_node_ptr_t) * (hdr->node_info[depth].max_nrec + 1));

    /* Set depth of the node */
    internal->depth = depth;

    /* Set parent */
    internal->parent = parent;

    /* Set shadowed epoch to header's epoch */
    internal->shadow_epoch = hdr->shadow_epoch;

    /* Allocate space on disk for the internal node */
    if (HADDR_UNDEF == (node_ptr->addr = H5MF_alloc(hdr->f, H5FD_MEM_BTREE, (hsize_t)hdr->node_size)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "file allocation failed for B-tree internal node");

    /* Cache the new B-tree node */
    if (H5AC_insert_entry(hdr->f, H5AC_BT2_INT, node_ptr->addr, internal, H5AC__NO_FLAGS_SET) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTINIT, FAIL, "can't add B-tree internal node to cache");
    inserted = true;

    /* Add internal node as child of 'top' proxy */
    if (hdr->top_proxy) {
        if (H5AC_proxy_entry_add_child(hdr->top_proxy, hdr->f, internal) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTSET, FAIL, "unable to add v2 B-tree node as child of proxy");
        internal->top_proxy = hdr->top_proxy;
    } /* end if */

done:
    if (ret_value < 0) {
        if (internal) {
            /* Remove from cache, if inserted */
            if (inserted)
                if (H5AC_remove_entry(internal) < 0)
                    HDONE_ERROR(H5E_BTREE, H5E_CANTREMOVE, FAIL,
                                "unable to remove v2 B-tree internal node from cache");

            /* Release internal node's disk space */
            if (H5_addr_defined(node_ptr->addr) &&
                H5MF_xfree(hdr->f, H5FD_MEM_BTREE, node_ptr->addr, (hsize_t)hdr->node_size) < 0)
                HDONE_ERROR(H5E_BTREE, H5E_CANTFREE, FAIL,
                            "unable to release file space for v2 B-tree internal node");

            /* Destroy internal node */
            if (H5B2__internal_free(internal) < 0)
                HDONE_ERROR(H5E_BTREE, H5E_CANTFREE, FAIL, "unable to release v2 B-tree internal node");
        } /* end if */
    }     /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5B2__create_internal() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__protect_internal
 *
 * Purpose:	"Protect" an internal node in the metadata cache
 *
 * Return:	Pointer to internal node on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5B2_internal_t *
H5B2__protect_internal(H5B2_hdr_t *hdr, void *parent, H5B2_node_ptr_t *node_ptr, uint16_t depth, bool shadow,
                       unsigned flags)
{
    H5B2_internal_cache_ud_t udata;            /* User data to pass through to cache 'deserialize' callback */
    H5B2_internal_t         *internal  = NULL; /* v2 B-tree internal node */
    H5B2_internal_t         *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(hdr);
    assert(node_ptr);
    assert(H5_addr_defined(node_ptr->addr));
    assert(depth > 0);

    /* only H5AC__READ_ONLY_FLAG may appear in flags */
    assert((flags & (unsigned)(~H5AC__READ_ONLY_FLAG)) == 0);

    /* Set up user data for callback */
    udata.f      = hdr->f;
    udata.hdr    = hdr;
    udata.parent = parent;
    udata.nrec   = node_ptr->node_nrec;
    udata.depth  = depth;

    /* Protect the internal node */
    if (NULL ==
        (internal = (H5B2_internal_t *)H5AC_protect(hdr->f, H5AC_BT2_INT, node_ptr->addr, &udata, flags)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, NULL, "unable to protect B-tree internal node");

    /* Create top proxy, if it doesn't exist */
    if (hdr->top_proxy && NULL == internal->top_proxy) {
        /* Add internal node as child of 'top' proxy */
        if (H5AC_proxy_entry_add_child(hdr->top_proxy, hdr->f, internal) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTSET, NULL,
                        "unable to add v2 B-tree internal node as child of proxy");
        internal->top_proxy = hdr->top_proxy;
    } /* end if */

    /* Shadow the node, if requested */
    if (shadow)
        if (H5B2__shadow_internal(internal, node_ptr) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTCOPY, NULL, "unable to shadow internal node");

    /* Set return value */
    ret_value = internal;

done:
    /* Clean up on error */
    if (!ret_value) {
        /* Release the internal node, if it was protected */
        if (internal) {
            /* Remove from v2 B-tree's proxy, if added */
            if (internal->top_proxy) {
                if (H5AC_proxy_entry_remove_child(internal->top_proxy, internal) < 0)
                    HDONE_ERROR(
                        H5E_BTREE, H5E_CANTUNDEPEND, NULL,
                        "unable to destroy flush dependency between internal node and v2 B-tree 'top' proxy");
                internal->top_proxy = NULL;
            } /* end if */

            /* Unprotect internal node */
            if (H5AC_unprotect(hdr->f, H5AC_BT2_INT, node_ptr->addr, internal, H5AC__NO_FLAGS_SET) < 0)
                HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, NULL,
                            "unable to unprotect v2 B-tree internal node, address = %llu",
                            (unsigned long long)node_ptr->addr);
        } /* end if */
    }     /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5B2__protect_internal() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__neighbor_internal
 *
 * Purpose:	Locate a record relative to the specified information in a
 *              B-tree internal node and return that information by filling in
 *              fields of the
 *              caller-supplied UDATA pointer depending on the type of leaf node
 *		requested.  The UDATA can point to additional data passed
 *		to the key comparison function.
 *
 *              The 'OP' routine is called with the record found and the
 *              OP_DATA pointer, to allow caller to return information about
 *              the record.
 *
 *              The RANGE indicates whether to search for records less than or
 *              equal to, or greater than or equal to the information passed
 *              in with UDATA.
 *
 * Return:	Non-negative on success, negative on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__neighbor_internal(H5B2_hdr_t *hdr, uint16_t depth, H5B2_node_ptr_t *curr_node_ptr, void *neighbor_loc,
                        H5B2_compare_t comp, void *parent, void *udata, H5B2_found_t op, void *op_data)
{
    H5B2_internal_t *internal;            /* Pointer to internal node */
    unsigned         idx       = 0;       /* Location of record which matches key */
    int              cmp       = 0;       /* Comparison value of records */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(hdr);
    assert(depth > 0);
    assert(curr_node_ptr);
    assert(H5_addr_defined(curr_node_ptr->addr));
    assert(op);

    /* Lock current B-tree node */
    if (NULL ==
        (internal = H5B2__protect_internal(hdr, parent, curr_node_ptr, depth, false, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree internal node");

    /* Locate node pointer for child */
    if (H5B2__locate_record(hdr->cls, internal->nrec, hdr->nat_off, internal->int_native, udata, &idx, &cmp) <
        0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTCOMPARE, FAIL, "can't compare btree2 records");
    if (cmp > 0)
        idx++;

    /* Set the neighbor location, if appropriate */
    if (comp == H5B2_COMPARE_LESS) {
        if (idx > 0)
            neighbor_loc = H5B2_INT_NREC(internal, hdr, idx - 1);
    } /* end if */
    else {
        assert(comp == H5B2_COMPARE_GREATER);

        if (idx < internal->nrec)
            neighbor_loc = H5B2_INT_NREC(internal, hdr, idx);
    } /* end else */

    /* Attempt to find neighboring record */
    if (depth > 1) {
        if (H5B2__neighbor_internal(hdr, (uint16_t)(depth - 1), &internal->node_ptrs[idx], neighbor_loc, comp,
                                    internal, udata, op, op_data) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_NOTFOUND, FAIL,
                        "unable to find neighbor record in B-tree internal node");
    } /* end if */
    else {
        if (H5B2__neighbor_leaf(hdr, &internal->node_ptrs[idx], neighbor_loc, comp, internal, udata, op,
                                op_data) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_NOTFOUND, FAIL, "unable to find neighbor record in B-tree leaf node");
    } /* end else */

done:
    /* Release the B-tree internal node */
    if (internal &&
        H5AC_unprotect(hdr->f, H5AC_BT2_INT, curr_node_ptr->addr, internal, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release internal B-tree node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5B2__neighbor_internal() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__insert_internal
 *
 * Purpose:	Adds a new record to a B-tree node.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__insert_internal(H5B2_hdr_t *hdr, uint16_t depth, unsigned *parent_cache_info_flags_ptr,
                      H5B2_node_ptr_t *curr_node_ptr, H5B2_nodepos_t curr_pos, void *parent, void *udata)
{
    H5B2_internal_t *internal       = NULL; /* Pointer to internal node */
    unsigned         internal_flags = H5AC__NO_FLAGS_SET;
    unsigned         idx            = 0;               /* Location of record which matches key */
    H5B2_nodepos_t   next_pos       = H5B2_POS_MIDDLE; /* Position of node */
    herr_t           ret_value      = SUCCEED;         /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(hdr);
    assert(depth > 0);
    assert(curr_node_ptr);
    assert(H5_addr_defined(curr_node_ptr->addr));

    /* Lock current B-tree node */
    if (NULL ==
        (internal = H5B2__protect_internal(hdr, parent, curr_node_ptr, depth, false, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree internal node");

    /* Sanity check number of records */
    assert(internal->nrec == curr_node_ptr->node_nrec);

    /* Split or redistribute child node pointers, if necessary */
    {
        int      cmp;        /* Comparison value of records */
        unsigned retries;    /* Number of times to attempt redistribution */
        size_t   split_nrec; /* Number of records to split node at */

        /* Locate node pointer for child */
        if (H5B2__locate_record(hdr->cls, internal->nrec, hdr->nat_off, internal->int_native, udata, &idx,
                                &cmp) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTCOMPARE, FAIL, "can't compare btree2 records");
        if (cmp == 0)
            HGOTO_ERROR(H5E_BTREE, H5E_EXISTS, FAIL, "record is already in B-tree");
        if (cmp > 0)
            idx++;

        /* Set the number of redistribution retries */
        /* This takes care of the case where a B-tree node needs to be
         * redistributed, but redistributing the node causes the index
         * for insertion to move to another node, which also needs to be
         * redistributed.  Now, we loop trying to redistribute and then
         * eventually force a split */
        retries = 2;

        /* Determine the correct number of records to split child node at */
        split_nrec = hdr->node_info[depth - 1].split_nrec;

        /* Preemptively split/redistribute a node we will enter */
        while (internal->node_ptrs[idx].node_nrec == split_nrec) {
            /* Attempt to redistribute records among children */
            if (idx == 0) { /* Left-most child */
                if (retries > 0 && (internal->node_ptrs[idx + 1].node_nrec < split_nrec)) {
                    if (H5B2__redistribute2(hdr, depth, internal, idx) < 0)
                        HGOTO_ERROR(H5E_BTREE, H5E_CANTREDISTRIBUTE, FAIL,
                                    "unable to redistribute child node records");
                } /* end if */
                else {
                    if (H5B2__split1(hdr, depth, curr_node_ptr, parent_cache_info_flags_ptr, internal,
                                     &internal_flags, idx) < 0)
                        HGOTO_ERROR(H5E_BTREE, H5E_CANTSPLIT, FAIL, "unable to split child node");
                }                             /* end else */
            }                                 /* end if */
            else if (idx == internal->nrec) { /* Right-most child */
                if (retries > 0 && (internal->node_ptrs[idx - 1].node_nrec < split_nrec)) {
                    if (H5B2__redistribute2(hdr, depth, internal, (idx - 1)) < 0)
                        HGOTO_ERROR(H5E_BTREE, H5E_CANTREDISTRIBUTE, FAIL,
                                    "unable to redistribute child node records");
                } /* end if */
                else {
                    if (H5B2__split1(hdr, depth, curr_node_ptr, parent_cache_info_flags_ptr, internal,
                                     &internal_flags, idx) < 0)
                        HGOTO_ERROR(H5E_BTREE, H5E_CANTSPLIT, FAIL, "unable to split child node");
                }  /* end else */
            }      /* end if */
            else { /* Middle child */
                if (retries > 0 && ((internal->node_ptrs[idx + 1].node_nrec < split_nrec) ||
                                    (internal->node_ptrs[idx - 1].node_nrec < split_nrec))) {
                    if (H5B2__redistribute3(hdr, depth, internal, &internal_flags, idx) < 0)
                        HGOTO_ERROR(H5E_BTREE, H5E_CANTREDISTRIBUTE, FAIL,
                                    "unable to redistribute child node records");
                } /* end if */
                else {
                    if (H5B2__split1(hdr, depth, curr_node_ptr, parent_cache_info_flags_ptr, internal,
                                     &internal_flags, idx) < 0)
                        HGOTO_ERROR(H5E_BTREE, H5E_CANTSPLIT, FAIL, "unable to split child node");
                } /* end else */
            }     /* end else */

            /* Locate node pointer for child (after split/redistribute) */
            /* Actually, this can be easily updated (for 2-node redistrib.) and shouldn't require re-searching
             */
            if (H5B2__locate_record(hdr->cls, internal->nrec, hdr->nat_off, internal->int_native, udata, &idx,
                                    &cmp) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTCOMPARE, FAIL, "can't compare btree2 records");
            if (cmp == 0)
                HGOTO_ERROR(H5E_BTREE, H5E_EXISTS, FAIL, "record is already in B-tree");
            if (cmp > 0)
                idx++;

            /* Decrement the number of redistribution retries left */
            retries--;
        } /* end while */
    }     /* end block */

    /* Check if this node is left/right-most */
    if (H5B2_POS_MIDDLE != curr_pos) {
        if (idx == 0) {
            if (H5B2_POS_LEFT == curr_pos || H5B2_POS_ROOT == curr_pos)
                next_pos = H5B2_POS_LEFT;
        } /* end if */
        else if (idx == internal->nrec) {
            if (H5B2_POS_RIGHT == curr_pos || H5B2_POS_ROOT == curr_pos)
                next_pos = H5B2_POS_RIGHT;
        } /* end else */
    }     /* end if */

    /* Attempt to insert node */
    if (depth > 1) {
        if (H5B2__insert_internal(hdr, (uint16_t)(depth - 1), &internal_flags, &internal->node_ptrs[idx],
                                  next_pos, internal, udata) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTINSERT, FAIL, "unable to insert record into B-tree internal node");
    } /* end if */
    else {
        if (H5B2__insert_leaf(hdr, &internal->node_ptrs[idx], next_pos, internal, udata) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTINSERT, FAIL, "unable to insert record into B-tree leaf node");
    } /* end else */

    /* Update record count for node pointer to current node */
    curr_node_ptr->all_nrec++;

    /* Mark node as dirty */
    internal_flags |= H5AC__DIRTIED_FLAG;

done:
    /* Release the B-tree internal node */
    if (internal) {
        /* Shadow the node if doing SWMR writes */
        if (hdr->swmr_write && (internal_flags & H5AC__DIRTIED_FLAG))
            if (H5B2__shadow_internal(internal, curr_node_ptr) < 0)
                HDONE_ERROR(H5E_BTREE, H5E_CANTCOPY, FAIL, "unable to shadow internal B-tree node");

        /* Unprotect node */
        if (H5AC_unprotect(hdr->f, H5AC_BT2_INT, curr_node_ptr->addr, internal, internal_flags) < 0)
            HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release internal B-tree node");
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5B2__insert_internal() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__update_internal
 *
 * Purpose:	Insert or modify a record in a B-tree internal node.
 *		If the record exists already, it is modified as if H5B2_modify
 *		was called).  If it doesn't exist, it is inserted as if
 *		H5B2_insert was called.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__update_internal(H5B2_hdr_t *hdr, uint16_t depth, unsigned *parent_cache_info_flags_ptr,
                      H5B2_node_ptr_t *curr_node_ptr, H5B2_update_status_t *status, H5B2_nodepos_t curr_pos,
                      void *parent, void *udata, H5B2_modify_t op, void *op_data)
{
    H5B2_internal_t *internal       = NULL; /* Pointer to internal node */
    unsigned         internal_flags = H5AC__NO_FLAGS_SET;
    int              cmp;                         /* Comparison value of records */
    unsigned         idx       = 0;               /* Location of record which matches key */
    H5B2_nodepos_t   next_pos  = H5B2_POS_MIDDLE; /* Position of node */
    herr_t           ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(hdr);
    assert(depth > 0);
    assert(curr_node_ptr);
    assert(H5_addr_defined(curr_node_ptr->addr));

    /* Lock current B-tree node */
    if (NULL ==
        (internal = H5B2__protect_internal(hdr, parent, curr_node_ptr, depth, false, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree internal node");

    /* Sanity check number of records */
    assert(internal->nrec == curr_node_ptr->node_nrec);

    /* Locate node pointer for child */
    if (H5B2__locate_record(hdr->cls, internal->nrec, hdr->nat_off, internal->int_native, udata, &idx, &cmp) <
        0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTCOMPARE, FAIL, "can't compare btree2 records");

    /* Check for modifying existing record */
    if (0 == cmp) {
        bool changed = false; /* Whether the 'modify' callback changed the record */

        /* Make callback for current record */
        if ((op)(H5B2_INT_NREC(internal, hdr, idx), op_data, &changed) < 0) {
            /* Make certain that the callback didn't modify the value if it failed */
            assert(changed == false);

            HGOTO_ERROR(H5E_BTREE, H5E_CANTMODIFY, FAIL,
                        "'modify' callback failed for B-tree update operation");
        } /* end if */

        /* Mark the node as dirty if it changed */
        internal_flags |= (changed ? H5AC__DIRTIED_FLAG : 0);

        /* Indicate that the record was modified */
        *status = H5B2_UPDATE_MODIFY_DONE;
    } /* end if */
    else {
        /* Adjust index to leave room for node to insert */
        if (cmp > 0)
            idx++;

        /* Check if this node is left/right-most */
        if (H5B2_POS_MIDDLE != curr_pos) {
            if (idx == 0) {
                if (H5B2_POS_LEFT == curr_pos || H5B2_POS_ROOT == curr_pos)
                    next_pos = H5B2_POS_LEFT;
            } /* end if */
            else if (idx == internal->nrec) {
                if (H5B2_POS_RIGHT == curr_pos || H5B2_POS_ROOT == curr_pos)
                    next_pos = H5B2_POS_RIGHT;
            } /* end else */
        }     /* end if */

        /* Attempt to update record in child */
        if (depth > 1) {
            if (H5B2__update_internal(hdr, (uint16_t)(depth - 1), &internal_flags, &internal->node_ptrs[idx],
                                      status, next_pos, internal, udata, op, op_data) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTUPDATE, FAIL,
                            "unable to update record in internal B-tree node");
        } /* end if */
        else {
            if (H5B2__update_leaf(hdr, &internal->node_ptrs[idx], status, next_pos, internal, udata, op,
                                  op_data) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTUPDATE, FAIL, "unable to update record in leaf B-tree node");
        } /* end else */

        /* Take actions based on child's status report */
        switch (*status) {
            case H5B2_UPDATE_MODIFY_DONE:
                /* No action */
                break;

            case H5B2_UPDATE_SHADOW_DONE:
                /* If child node was shadowed (if SWMR is enabled), mark this node dirty */
                if (hdr->swmr_write)
                    internal_flags |= H5AC__DIRTIED_FLAG;

                /* No further modifications up the tree are necessary though, so downgrade to merely
                 * "modified" */
                *status = H5B2_UPDATE_MODIFY_DONE;
                break;

            case H5B2_UPDATE_INSERT_DONE:
                /* Mark node as dirty */
                internal_flags |= H5AC__DIRTIED_FLAG;

                /* Update total record count for node pointer to current node */
                curr_node_ptr->all_nrec++;
                break;

            case H5B2_UPDATE_INSERT_CHILD_FULL:
                /* Split/redistribute this node */
                if (internal->nrec == hdr->node_info[depth].split_nrec) {
                    bool could_split = false; /* Whether the child node could split */

                    if (idx == 0) { /* Left-most child */
                        /* Check for left-most child and its neighbor being close to full */
                        if ((internal->node_ptrs[idx].node_nrec + internal->node_ptrs[idx + 1].node_nrec) >=
                            (unsigned)((hdr->node_info[depth - 1].split_nrec * 2) - 1))
                            could_split = true;
                    }
                    else if (idx == internal->nrec) { /* Right-most child */
                        /* Check for right-most child and its neighbor being close to full */
                        if ((internal->node_ptrs[idx - 1].node_nrec + internal->node_ptrs[idx].node_nrec) >=
                            (unsigned)((hdr->node_info[depth - 1].split_nrec * 2) - 1))
                            could_split = true;
                    }
                    else { /* Middle child */
                        /* Check for middle child and its left neighbor being close to full */
                        if ((internal->node_ptrs[idx - 1].node_nrec + internal->node_ptrs[idx].node_nrec) >=
                            (unsigned)((hdr->node_info[depth - 1].split_nrec * 2) - 1))
                            could_split = true;
                        /* Check for middle child and its right neighbor being close to full */
                        else if ((internal->node_ptrs[idx].node_nrec +
                                  internal->node_ptrs[idx + 1].node_nrec) >=
                                 (unsigned)((hdr->node_info[depth - 1].split_nrec * 2) - 1))
                            could_split = true;
                    }

                    /* If this node is full and the child node insertion could
                     *  cause a split, punt back up to caller, leaving the
                     *  "insert child full" status.
                     */
                    if (could_split) {
                        /* Release the internal B-tree node */
                        if (H5AC_unprotect(hdr->f, H5AC_BT2_INT, curr_node_ptr->addr, internal,
                                           internal_flags) < 0)
                            HGOTO_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL,
                                        "unable to release internal B-tree node");
                        internal = NULL;

                        /* Punt back to caller */
                        HGOTO_DONE(SUCCEED);
                    }
                }

                /* Release the internal B-tree node */
                if (H5AC_unprotect(hdr->f, H5AC_BT2_INT, curr_node_ptr->addr, internal, internal_flags) < 0)
                    HGOTO_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release internal B-tree node");
                internal = NULL;

                /* Indicate that the record was inserted */
                *status = H5B2_UPDATE_INSERT_DONE;

                /* Dodge sideways into inserting a record into this node */
                if (H5B2__insert_internal(hdr, depth, parent_cache_info_flags_ptr, curr_node_ptr, curr_pos,
                                          parent, udata) < 0)
                    HGOTO_ERROR(H5E_BTREE, H5E_CANTINSERT, FAIL,
                                "unable to insert record into internal B-tree node");
                break;

            case H5B2_UPDATE_UNKNOWN:
            default:
                assert(0 && "Invalid update status");
                HGOTO_ERROR(H5E_BTREE, H5E_CANTUPDATE, FAIL, "invalid update status");
        } /* end switch */
    }     /* end else */

done:
    /* Release the internal B-tree node */
    if (internal) {
        /* Check if we should shadow this node */
        if (hdr->swmr_write && (internal_flags & H5AC__DIRTIED_FLAG)) {
            /* Attempt to shadow the node if doing SWMR writes */
            if (H5B2__shadow_internal(internal, curr_node_ptr) < 0)
                HDONE_ERROR(H5E_BTREE, H5E_CANTCOPY, FAIL, "unable to shadow internal B-tree node");

            /* Change the state to "shadowed" if only modified currently */
            /* (Triggers parent to be marked dirty) */
            if (*status == H5B2_UPDATE_MODIFY_DONE)
                *status = H5B2_UPDATE_SHADOW_DONE;
        } /* end if */

        /* Unprotect node */
        if (H5AC_unprotect(hdr->f, H5AC_BT2_INT, curr_node_ptr->addr, internal, internal_flags) < 0)
            HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release internal B-tree node");
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5B2__update_internal() */

/*-------------------------------------------------------------------------
 * Function:    H5B2__shadow_internal
 *
 * Purpose:     "Shadow" an internal node - copy it to a new location,
 *              leaving the data in the old location intact (for now).
 *              This is done when writing in SWMR mode to ensure that
 *              readers do not see nodes that are out of date with
 *              respect to each other and thereby inconsistent.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5B2__shadow_internal(H5B2_internal_t *internal, H5B2_node_ptr_t *curr_node_ptr)
{
    H5B2_hdr_t *hdr;                 /* B-tree header */
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(internal);
    assert(curr_node_ptr);
    assert(H5_addr_defined(curr_node_ptr->addr));
    hdr = internal->hdr;
    assert(hdr);
    assert(hdr->swmr_write);

    /* We only need to shadow the node if it has not been shadowed since the
     * last time the header was flushed, as otherwise it will be unreachable by
     * the readers so there will be no need to shadow.  To check if it has been
     * shadowed, compare the epoch of this node and the header.  If this node's
     * epoch is <= to the header's, it hasn't been shadowed yet. */
    if (internal->shadow_epoch <= hdr->shadow_epoch) {
        haddr_t new_node_addr; /* Address to move node to */

        /*
         * We must clone the old node so readers with an out-of-date version of
         * the parent can still see the correct number of children, via the
         * shadowed node.  Remove it from cache but do not mark it free on disk.
         */
        /* Allocate space for the cloned node */
        if (HADDR_UNDEF == (new_node_addr = H5MF_alloc(hdr->f, H5FD_MEM_BTREE, (hsize_t)hdr->node_size)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTALLOC, FAIL, "unable to allocate file space to move B-tree node");

        /* Move the location of the node on the disk */
        if (H5AC_move_entry(hdr->f, H5AC_BT2_INT, curr_node_ptr->addr, new_node_addr) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTMOVE, FAIL, "unable to move B-tree node");
        curr_node_ptr->addr = new_node_addr;

        /* Should free the space in the file, but this is not supported by
         * SWMR_WRITE code yet - QAK, 2016/12/01
         */

        /* Set shadow epoch for node ahead of header */
        internal->shadow_epoch = hdr->shadow_epoch + 1;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B2__shadow_internal() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__remove_internal
 *
 * Purpose:	Removes a record from a B-tree node.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__remove_internal(H5B2_hdr_t *hdr, bool *depth_decreased, void *swap_loc, void *swap_parent,
                      uint16_t depth, H5AC_info_t *parent_cache_info, unsigned *parent_cache_info_flags_ptr,
                      H5B2_nodepos_t curr_pos, H5B2_node_ptr_t *curr_node_ptr, void *udata, H5B2_remove_t op,
                      void *op_data)
{
    H5AC_info_t     *new_cache_info; /* Pointer to new cache info */
    unsigned        *new_cache_info_flags_ptr = NULL;
    H5B2_node_ptr_t *new_node_ptr;                     /* Pointer to new node pointer */
    H5B2_internal_t *internal;                         /* Pointer to internal node */
    H5B2_nodepos_t   next_pos       = H5B2_POS_MIDDLE; /* Position of next node */
    unsigned         internal_flags = H5AC__NO_FLAGS_SET;
    haddr_t          internal_addr  = HADDR_UNDEF; /* Address of internal node */
    size_t           merge_nrec;                   /* Number of records to merge node at */
    bool             collapsed_root = false;       /* Whether the root was collapsed */
    herr_t           ret_value      = SUCCEED;     /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(hdr);
    assert(depth > 0);
    assert(parent_cache_info);
    assert(curr_node_ptr);
    assert(H5_addr_defined(curr_node_ptr->addr));

    /* Lock current B-tree node */
    if (NULL == (internal = H5B2__protect_internal(hdr, parent_cache_info, curr_node_ptr, depth, false,
                                                   H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree internal node");
    internal_addr = curr_node_ptr->addr;

    /* Determine the correct number of records to merge at */
    merge_nrec = hdr->node_info[depth - 1].merge_nrec;

    /* Check for needing to collapse the root node */
    /* (The root node is the only internal node allowed to have 1 record) */
    if (internal->nrec == 1 &&
        ((internal->node_ptrs[0].node_nrec + internal->node_ptrs[1].node_nrec) <= ((merge_nrec * 2) + 1))) {

        /* Merge children of root node */
        if (H5B2__merge2(hdr, depth, curr_node_ptr, parent_cache_info_flags_ptr, internal, &internal_flags,
                         0) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTSPLIT, FAIL, "unable to merge child node");

        /* Let the cache know that the object is deleted */
        internal_flags |= H5AC__DELETED_FLAG;
        if (!hdr->swmr_write)
            internal_flags |= H5AC__FREE_FILE_SPACE_FLAG;

        /* Reset information in header's root node pointer */
        curr_node_ptr->addr      = internal->node_ptrs[0].addr;
        curr_node_ptr->node_nrec = internal->node_ptrs[0].node_nrec;

        /* Update flush dependency for child, if using SWMR */
        if (hdr->swmr_write)
            if (H5B2__update_flush_depend(hdr, depth, curr_node_ptr, internal, hdr) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTUPDATE, FAIL, "unable to update child node to new parent");

        /* Indicate that the level of the B-tree decreased */
        *depth_decreased = true;

        /* Set pointers for advancing to child node */
        new_cache_info           = parent_cache_info;
        new_cache_info_flags_ptr = parent_cache_info_flags_ptr;
        new_node_ptr             = curr_node_ptr;

        /* Set flag to indicate root was collapsed */
        collapsed_root = true;

        /* Indicate position of next node */
        next_pos = H5B2_POS_ROOT;
    } /* end if */
    /* Merge or redistribute child node pointers, if necessary */
    else {
        unsigned idx = 0; /* Location of record which matches key */
        int      cmp = 0; /* Comparison value of records */
        unsigned retries; /* Number of times to attempt redistribution */

        /* Shadow the node if doing SWMR writes */
        if (hdr->swmr_write) {
            if (H5B2__shadow_internal(internal, curr_node_ptr) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTCOPY, FAIL, "unable to shadow internal node");
            internal_addr = curr_node_ptr->addr;
        } /* end if */

        /* Locate node pointer for child */
        if (swap_loc)
            idx = 0;
        else {
            if (H5B2__locate_record(hdr->cls, internal->nrec, hdr->nat_off, internal->int_native, udata, &idx,
                                    &cmp) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTCOMPARE, FAIL, "can't compare btree2 records");
            if (cmp >= 0)
                idx++;
        } /* end else */

        /* Set the number of redistribution retries */
        /* This takes care of the case where a B-tree node needs to be
         * redistributed, but redistributing the node causes the index
         * for removal to move to another node, which also needs to be
         * redistributed.  Now, we loop trying to redistribute and then
         * eventually force a merge */
        retries = 2;

        /* Preemptively merge/redistribute a node we will enter */
        while (internal->node_ptrs[idx].node_nrec == merge_nrec) {
            /* Attempt to redistribute records among children */
            /* (NOTE: These 2-node redistributions should actually get the
             *  record to promote from the node with more records. - QAK)
             */
            /* (NOTE: This code is the same in both H5B2__remove_internal() and
             *  H5B2__remove_internal_by_idx(), fix bugs in both places! - QAK)
             */
            if (idx == 0) { /* Left-most child */
                if (retries > 0 && (internal->node_ptrs[idx + 1].node_nrec > merge_nrec)) {
                    if (H5B2__redistribute2(hdr, depth, internal, idx) < 0)
                        HGOTO_ERROR(H5E_BTREE, H5E_CANTREDISTRIBUTE, FAIL,
                                    "unable to redistribute child node records");
                } /* end if */
                else {
                    if (H5B2__merge2(hdr, depth, curr_node_ptr, parent_cache_info_flags_ptr, internal,
                                     &internal_flags, idx) < 0)
                        HGOTO_ERROR(H5E_BTREE, H5E_CANTSPLIT, FAIL, "unable to merge child node");
                }                             /* end else */
            }                                 /* end if */
            else if (idx == internal->nrec) { /* Right-most child */
                if (retries > 0 && (internal->node_ptrs[idx - 1].node_nrec > merge_nrec)) {
                    if (H5B2__redistribute2(hdr, depth, internal, (idx - 1)) < 0)
                        HGOTO_ERROR(H5E_BTREE, H5E_CANTREDISTRIBUTE, FAIL,
                                    "unable to redistribute child node records");
                } /* end if */
                else {
                    if (H5B2__merge2(hdr, depth, curr_node_ptr, parent_cache_info_flags_ptr, internal,
                                     &internal_flags, (idx - 1)) < 0)
                        HGOTO_ERROR(H5E_BTREE, H5E_CANTSPLIT, FAIL, "unable to merge child node");
                }  /* end else */
            }      /* end if */
            else { /* Middle child */
                if (retries > 0 && ((internal->node_ptrs[idx + 1].node_nrec > merge_nrec) ||
                                    (internal->node_ptrs[idx - 1].node_nrec > merge_nrec))) {
                    if (H5B2__redistribute3(hdr, depth, internal, &internal_flags, idx) < 0)
                        HGOTO_ERROR(H5E_BTREE, H5E_CANTREDISTRIBUTE, FAIL,
                                    "unable to redistribute child node records");
                } /* end if */
                else {
                    if (H5B2__merge3(hdr, depth, curr_node_ptr, parent_cache_info_flags_ptr, internal,
                                     &internal_flags, idx) < 0)
                        HGOTO_ERROR(H5E_BTREE, H5E_CANTSPLIT, FAIL, "unable to merge child node");
                } /* end else */
            }     /* end else */

            /* Locate node pointer for child (after merge/redistribute) */
            if (swap_loc)
                idx = 0;
            else {
                /* Actually, this can be easily updated (for 2-node redistrib.) and shouldn't require
                 * re-searching */
                if (H5B2__locate_record(hdr->cls, internal->nrec, hdr->nat_off, internal->int_native, udata,
                                        &idx, &cmp) < 0)
                    HGOTO_ERROR(H5E_BTREE, H5E_CANTCOMPARE, FAIL, "can't compare btree2 records");
                if (cmp >= 0)
                    idx++;
            } /* end else */

            /* Decrement the number of redistribution retries left */
            retries--;
        } /* end while */

        /* Handle deleting a record from an internal node */
        if (!swap_loc && cmp == 0) {
            swap_loc    = H5B2_INT_NREC(internal, hdr, idx - 1);
            swap_parent = internal;
        } /* end if */

        /* Swap record to delete with record from leaf, if we are the last internal node */
        if (swap_loc && depth == 1)
            if (H5B2__swap_leaf(hdr, depth, internal, &internal_flags, idx, swap_loc) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTSWAP, FAIL, "Can't swap records in B-tree");

        /* Set pointers for advancing to child node */
        new_cache_info_flags_ptr = &internal_flags;
        new_cache_info           = &internal->cache_info;
        new_node_ptr             = &internal->node_ptrs[idx];

        /* Indicate position of next node */
        if (H5B2_POS_MIDDLE != curr_pos) {
            if (idx == 0) {
                if (H5B2_POS_LEFT == curr_pos || H5B2_POS_ROOT == curr_pos)
                    next_pos = H5B2_POS_LEFT;
            } /* end if */
            else if (idx == internal->nrec) {
                if (H5B2_POS_RIGHT == curr_pos || H5B2_POS_ROOT == curr_pos)
                    next_pos = H5B2_POS_RIGHT;
            } /* end if */
        }     /* end if */
    }         /* end else */

    /* Attempt to remove record from child node */
    if (depth > 1) {
        if (H5B2__remove_internal(hdr, depth_decreased, swap_loc, swap_parent, (uint16_t)(depth - 1),
                                  new_cache_info, new_cache_info_flags_ptr, next_pos, new_node_ptr, udata, op,
                                  op_data) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTDELETE, FAIL, "unable to remove record from B-tree internal node");
    } /* end if */
    else {
        if (H5B2__remove_leaf(hdr, new_node_ptr, next_pos, new_cache_info, udata, op, op_data) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTDELETE, FAIL, "unable to remove record from B-tree leaf node");
    } /* end else */

    /* Update record count for node pointer to current node */
    if (!collapsed_root)
        new_node_ptr->all_nrec--;

    /* Mark node as dirty */
    if (!(hdr->swmr_write && collapsed_root))
        internal_flags |= H5AC__DIRTIED_FLAG;

#ifdef H5B2_DEBUG
    H5B2__assert_internal((!collapsed_root ? (curr_node_ptr->all_nrec - 1) : new_node_ptr->all_nrec), hdr,
                          internal);
#endif /* H5B2_DEBUG */

done:
    /* Release the B-tree internal node */
    if (internal && H5AC_unprotect(hdr->f, H5AC_BT2_INT, internal_addr, internal, internal_flags) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release internal B-tree node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5B2__remove_internal() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__remove_internal_by_idx
 *
 * Purpose:	Removes a record from a B-tree node, according to the offset
 *              in the B-tree records
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__remove_internal_by_idx(H5B2_hdr_t *hdr, bool *depth_decreased, void *swap_loc, void *swap_parent,
                             uint16_t depth, H5AC_info_t *parent_cache_info,
                             unsigned *parent_cache_info_flags_ptr, H5B2_node_ptr_t *curr_node_ptr,
                             H5B2_nodepos_t curr_pos, hsize_t n, H5B2_remove_t op, void *op_data)
{
    H5AC_info_t     *new_cache_info; /* Pointer to new cache info */
    unsigned        *new_cache_info_flags_ptr = NULL;
    H5B2_node_ptr_t *new_node_ptr;                     /* Pointer to new node pointer */
    H5B2_internal_t *internal;                         /* Pointer to internal node */
    H5B2_nodepos_t   next_pos       = H5B2_POS_MIDDLE; /* Position of next node */
    unsigned         internal_flags = H5AC__NO_FLAGS_SET;
    haddr_t          internal_addr  = HADDR_UNDEF; /* Address of internal node */
    size_t           merge_nrec;                   /* Number of records to merge node at */
    bool             collapsed_root = false;       /* Whether the root was collapsed */
    herr_t           ret_value      = SUCCEED;     /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(hdr);
    assert(depth > 0);
    assert(parent_cache_info);
    assert(curr_node_ptr);
    assert(H5_addr_defined(curr_node_ptr->addr));

    /* Lock current B-tree node */
    if (NULL == (internal = H5B2__protect_internal(hdr, parent_cache_info, curr_node_ptr, depth, false,
                                                   H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree internal node");
    internal_addr = curr_node_ptr->addr;
    assert(internal->nrec == curr_node_ptr->node_nrec);
    assert(depth == hdr->depth || internal->nrec > 1);

    /* Determine the correct number of records to merge at */
    merge_nrec = hdr->node_info[depth - 1].merge_nrec;

    /* Check for needing to collapse the root node */
    /* (The root node is the only internal node allowed to have 1 record) */
    if (internal->nrec == 1 &&
        ((internal->node_ptrs[0].node_nrec + internal->node_ptrs[1].node_nrec) <= ((merge_nrec * 2) + 1))) {
        assert(depth == hdr->depth);

        /* Merge children of root node */
        if (H5B2__merge2(hdr, depth, curr_node_ptr, parent_cache_info_flags_ptr, internal, &internal_flags,
                         0) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTSPLIT, FAIL, "unable to merge child node");

        /* Let the cache know that the object is deleted */
        internal_flags |= H5AC__DELETED_FLAG;
        if (!hdr->swmr_write)
            internal_flags |= H5AC__FREE_FILE_SPACE_FLAG;

        /* Reset information in header's root node pointer */
        curr_node_ptr->addr      = internal->node_ptrs[0].addr;
        curr_node_ptr->node_nrec = internal->node_ptrs[0].node_nrec;

        /* Update flush dependency for child, if using SWMR */
        if (hdr->swmr_write)
            if (H5B2__update_flush_depend(hdr, depth, curr_node_ptr, internal, hdr) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTUPDATE, FAIL, "unable to update child node to new parent");

        /* Indicate that the level of the B-tree decreased */
        *depth_decreased = true;

        /* Set pointers for advancing to child node */
        new_cache_info           = parent_cache_info;
        new_cache_info_flags_ptr = parent_cache_info_flags_ptr;
        new_node_ptr             = curr_node_ptr;

        /* Set flag to indicate root was collapsed */
        collapsed_root = true;

        /* Indicate position of next node */
        next_pos = H5B2_POS_ROOT;
    } /* end if */
    /* Merge or redistribute child node pointers, if necessary */
    else {
        hsize_t  orig_n = n;    /* Original index looked for */
        unsigned idx;           /* Location of record which matches key */
        bool     found = false; /* Comparison value of records */
        unsigned retries;       /* Number of times to attempt redistribution */

        /* Shadow the node if doing SWMR writes */
        if (hdr->swmr_write) {
            if (H5B2__shadow_internal(internal, curr_node_ptr) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTCOPY, FAIL, "unable to shadow internal node");
            internal_addr = curr_node_ptr->addr;
        } /* end if */

        /* Locate node pointer for child */
        if (swap_loc)
            idx = 0;
        else {
            /* Search for record with correct index */
            for (idx = 0; idx < internal->nrec; idx++) {
                /* Check which child node contains indexed record */
                if (internal->node_ptrs[idx].all_nrec >= n) {
                    /* Check if record is in this node */
                    if (internal->node_ptrs[idx].all_nrec == n) {
                        /* Indicate the record was found and that the index
                         *      in child nodes is zero from now on
                         */
                        found = true;
                        n     = 0;

                        /* Increment to next record */
                        idx++;
                    } /* end if */

                    /* Break out of loop early */
                    break;
                } /* end if */

                /* Decrement index we are looking for to account for the node we
                 * just advanced past.
                 */
                n -= (internal->node_ptrs[idx].all_nrec + 1);
            } /* end for */
        }     /* end else */

        /* Set the number of redistribution retries */
        /* This takes care of the case where a B-tree node needs to be
         * redistributed, but redistributing the node causes the index
         * for removal to move to another node, which also needs to be
         * redistributed.  Now, we loop trying to redistribute and then
         * eventually force a merge */
        retries = 2;

        /* Preemptively merge/redistribute a node we will enter */
        while (internal->node_ptrs[idx].node_nrec == merge_nrec) {
            /* Attempt to redistribute records among children */
            /* (NOTE: These 2-node redistributions should actually get the
             *  record to promote from the node with more records. - QAK)
             */
            /* (NOTE: This code is the same in both H5B2__remove_internal() and
             *  H5B2__remove_internal_by_idx(), fix bugs in both places! - QAK)
             */
            if (idx == 0) { /* Left-most child */
                if (retries > 0 && (internal->node_ptrs[idx + 1].node_nrec > merge_nrec)) {
                    if (H5B2__redistribute2(hdr, depth, internal, idx) < 0)
                        HGOTO_ERROR(H5E_BTREE, H5E_CANTREDISTRIBUTE, FAIL,
                                    "unable to redistribute child node records");
                } /* end if */
                else {
                    if (H5B2__merge2(hdr, depth, curr_node_ptr, parent_cache_info_flags_ptr, internal,
                                     &internal_flags, idx) < 0)
                        HGOTO_ERROR(H5E_BTREE, H5E_CANTSPLIT, FAIL, "unable to merge child node");
                }                             /* end else */
            }                                 /* end if */
            else if (idx == internal->nrec) { /* Right-most child */
                if (retries > 0 && (internal->node_ptrs[idx - 1].node_nrec > merge_nrec)) {
                    if (H5B2__redistribute2(hdr, depth, internal, (idx - 1)) < 0)
                        HGOTO_ERROR(H5E_BTREE, H5E_CANTREDISTRIBUTE, FAIL,
                                    "unable to redistribute child node records");
                } /* end if */
                else {
                    if (H5B2__merge2(hdr, depth, curr_node_ptr, parent_cache_info_flags_ptr, internal,
                                     &internal_flags, (idx - 1)) < 0)
                        HGOTO_ERROR(H5E_BTREE, H5E_CANTSPLIT, FAIL, "unable to merge child node");
                }  /* end else */
            }      /* end if */
            else { /* Middle child */
                if (retries > 0 && ((internal->node_ptrs[idx + 1].node_nrec > merge_nrec) ||
                                    (internal->node_ptrs[idx - 1].node_nrec > merge_nrec))) {
                    if (H5B2__redistribute3(hdr, depth, internal, &internal_flags, idx) < 0)
                        HGOTO_ERROR(H5E_BTREE, H5E_CANTREDISTRIBUTE, FAIL,
                                    "unable to redistribute child node records");
                } /* end if */
                else {
                    if (H5B2__merge3(hdr, depth, curr_node_ptr, parent_cache_info_flags_ptr, internal,
                                     &internal_flags, idx) < 0)
                        HGOTO_ERROR(H5E_BTREE, H5E_CANTSPLIT, FAIL, "unable to merge child node");
                } /* end else */
            }     /* end else */

            /* Locate node pointer for child (after merge/redistribute) */
            if (swap_loc)
                idx = 0;
            else {
                /* Count from the original index value again */
                n = orig_n;

                /* Reset "found" flag - the record may have shifted during the
                 *      redistribute/merge
                 */
                found = false;

                /* Search for record with correct index */
                for (idx = 0; idx < internal->nrec; idx++) {
                    /* Check which child node contains indexed record */
                    if (internal->node_ptrs[idx].all_nrec >= n) {
                        /* Check if record is in this node */
                        if (internal->node_ptrs[idx].all_nrec == n) {
                            /* Indicate the record was found and that the index
                             *      in child nodes is zero from now on
                             */
                            found = true;
                            n     = 0;

                            /* Increment to next record */
                            idx++;
                        } /* end if */

                        /* Break out of loop early */
                        break;
                    } /* end if */

                    /* Decrement index we are looking for to account for the node we
                     * just advanced past.
                     */
                    n -= (internal->node_ptrs[idx].all_nrec + 1);
                } /* end for */
            }     /* end else */

            /* Decrement the number of redistribution retries left */
            retries--;
        } /* end while */

        /* Handle deleting a record from an internal node */
        if (!swap_loc && found) {
            swap_loc    = H5B2_INT_NREC(internal, hdr, idx - 1);
            swap_parent = internal;
        } /* end if */

        /* Swap record to delete with record from leaf, if we are the last internal node */
        if (swap_loc && depth == 1)
            if (H5B2__swap_leaf(hdr, depth, internal, &internal_flags, idx, swap_loc) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTSWAP, FAIL, "can't swap records in B-tree");

        /* Set pointers for advancing to child node */
        new_cache_info_flags_ptr = &internal_flags;
        new_cache_info           = &internal->cache_info;
        new_node_ptr             = &internal->node_ptrs[idx];

        /* Indicate position of next node */
        if (H5B2_POS_MIDDLE != curr_pos) {
            if (idx == 0) {
                if (H5B2_POS_LEFT == curr_pos || H5B2_POS_ROOT == curr_pos)
                    next_pos = H5B2_POS_LEFT;
            } /* end if */
            else if (idx == internal->nrec) {
                if (H5B2_POS_RIGHT == curr_pos || H5B2_POS_ROOT == curr_pos)
                    next_pos = H5B2_POS_RIGHT;
            } /* end if */
        }     /* end if */
    }         /* end else */

    /* Attempt to remove record from child node */
    if (depth > 1) {
        if (H5B2__remove_internal_by_idx(hdr, depth_decreased, swap_loc, swap_parent, (uint16_t)(depth - 1),
                                         new_cache_info, new_cache_info_flags_ptr, new_node_ptr, next_pos, n,
                                         op, op_data) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTDELETE, FAIL, "unable to remove record from B-tree internal node");
    } /* end if */
    else {
        if (H5B2__remove_leaf_by_idx(hdr, new_node_ptr, next_pos, new_cache_info, (unsigned)n, op, op_data) <
            0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTDELETE, FAIL, "unable to remove record from B-tree leaf node");
    } /* end else */

    /* Update record count for node pointer to child node */
    if (!collapsed_root)
        new_node_ptr->all_nrec--;

    /* Mark node as dirty */
    if (!(hdr->swmr_write && collapsed_root))
        internal_flags |= H5AC__DIRTIED_FLAG;

#ifdef H5B2_DEBUG
    H5B2__assert_internal((!collapsed_root ? (curr_node_ptr->all_nrec - 1) : new_node_ptr->all_nrec), hdr,
                          internal);
#endif /* H5B2_DEBUG */

done:
    /* Release the B-tree internal node */
    if (internal && H5AC_unprotect(hdr->f, H5AC_BT2_INT, internal_addr, internal, internal_flags) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release internal B-tree node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5B2__remove_internal_by_idx() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__internal_free
 *
 * Purpose:	Destroys a B-tree internal node in memory.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__internal_free(H5B2_internal_t *internal)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(internal);

    /* Release internal node's native key buffer */
    if (internal->int_native)
        internal->int_native = (uint8_t *)H5FL_FAC_FREE(internal->hdr->node_info[internal->depth].nat_rec_fac,
                                                        internal->int_native);

    /* Release internal node's node pointer buffer */
    if (internal->node_ptrs)
        internal->node_ptrs = (H5B2_node_ptr_t *)H5FL_FAC_FREE(
            internal->hdr->node_info[internal->depth].node_ptr_fac, internal->node_ptrs);

    /* Decrement ref. count on B-tree header */
    if (H5B2__hdr_decr(internal->hdr) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTDEC, FAIL, "can't decrement ref. count on B-tree header");

    /* Sanity check */
    assert(NULL == internal->top_proxy);

    /* Free B-tree internal node info */
    internal = H5FL_FREE(H5B2_internal_t, internal);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B2__internal_free() */

#ifdef H5B2_DEBUG

/*-------------------------------------------------------------------------
 * Function:	H5B2__assert_internal
 *
 * Purpose:	Verify than an internal node is mostly sane
 *
 * Return:	Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
H5_ATTR_PURE herr_t
H5B2__assert_internal(hsize_t parent_all_nrec, const H5B2_hdr_t H5_ATTR_NDEBUG_UNUSED *hdr,
                      const H5B2_internal_t *internal)
{
    hsize_t  tot_all_nrec; /* Total number of records at or below this node */
    uint16_t u, v;         /* Local index variables */

    /* General sanity checking on node */
    assert(internal->nrec <= hdr->node_info->split_nrec);

    /* Sanity checking on node pointers */
    tot_all_nrec = internal->nrec;
    for (u = 0; u < internal->nrec + 1; u++) {
        tot_all_nrec += internal->node_ptrs[u].all_nrec;

        assert(H5_addr_defined(internal->node_ptrs[u].addr));
        assert(internal->node_ptrs[u].addr > 0);
        for (v = 0; v < u; v++)
            assert(internal->node_ptrs[u].addr != internal->node_ptrs[v].addr);
    } /* end for */

    /* Sanity check all_nrec total in parent */
    if (parent_all_nrec > 0)
        assert(tot_all_nrec == parent_all_nrec);

    return (0);
} /* end H5B2__assert_internal() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__assert_internal2
 *
 * Purpose:	Verify than internal nodes are mostly sane
 *
 * Return:	Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
H5_ATTR_PURE herr_t
H5B2__assert_internal2(hsize_t parent_all_nrec, const H5B2_hdr_t H5_ATTR_NDEBUG_UNUSED *hdr,
                       const H5B2_internal_t *internal, const H5B2_internal_t *internal2)
{
    hsize_t  tot_all_nrec; /* Total number of records at or below this node */
    uint16_t u, v;         /* Local index variables */

    /* General sanity checking on node */
    assert(internal->nrec <= hdr->node_info->split_nrec);

    /* Sanity checking on node pointers */
    tot_all_nrec = internal->nrec;
    for (u = 0; u < internal->nrec + 1; u++) {
        tot_all_nrec += internal->node_ptrs[u].all_nrec;

        assert(H5_addr_defined(internal->node_ptrs[u].addr));
        assert(internal->node_ptrs[u].addr > 0);
        for (v = 0; v < u; v++)
            assert(internal->node_ptrs[u].addr != internal->node_ptrs[v].addr);
        for (v = 0; v < internal2->nrec + 1; v++)
            assert(internal->node_ptrs[u].addr != internal2->node_ptrs[v].addr);
    } /* end for */

    /* Sanity check all_nrec total in parent */
    if (parent_all_nrec > 0)
        assert(tot_all_nrec == parent_all_nrec);

    return (0);
} /* end H5B2__assert_internal2() */
#endif /* H5B2_DEBUG */
