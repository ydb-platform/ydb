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
 * Created:		H5B2leaf.c
 *
 * Purpose:		Routines for managing v2 B-tree leaf nodes.
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
#include "H5MMprivate.h" /* Memory management			*/

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
static herr_t H5B2__shadow_leaf(H5B2_leaf_t *leaf, H5B2_node_ptr_t *curr_node_ptr);

/*********************/
/* Package Variables */
/*********************/

/* Declare a free list to manage the H5B2_leaf_t struct */
H5FL_DEFINE(H5B2_leaf_t);

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:	H5B2__create_leaf
 *
 * Purpose:	Creates empty leaf node of a B-tree and update node pointer
 *              to point to it.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__create_leaf(H5B2_hdr_t *hdr, void *parent, H5B2_node_ptr_t *node_ptr)
{
    H5B2_leaf_t *leaf      = NULL;    /* Pointer to new leaf node created */
    bool         inserted  = false;   /* Whether the leaf node was inserted into cache */
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(hdr);
    assert(node_ptr);

    /* Allocate memory for leaf information */
    if (NULL == (leaf = H5FL_CALLOC(H5B2_leaf_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed for B-tree leaf info");

    /* Increment ref. count on B-tree header */
    if (H5B2__hdr_incr(hdr) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTINC, FAIL, "can't increment ref. count on B-tree header");

    /* Share B-tree header information */
    leaf->hdr = hdr;

    /* Allocate space for the native keys in memory */
    if (NULL == (leaf->leaf_native = (uint8_t *)H5FL_FAC_MALLOC(hdr->node_info[0].nat_rec_fac)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed for B-tree leaf native keys");
    memset(leaf->leaf_native, 0, hdr->cls->nrec_size * hdr->node_info[0].max_nrec);

    /* Set parent */
    leaf->parent = parent;

    /* Set shadowed epoch to header's epoch */
    leaf->shadow_epoch = hdr->shadow_epoch;

    /* Allocate space on disk for the leaf */
    if (HADDR_UNDEF == (node_ptr->addr = H5MF_alloc(hdr->f, H5FD_MEM_BTREE, (hsize_t)hdr->node_size)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "file allocation failed for B-tree leaf node");

    /* Cache the new B-tree node */
    if (H5AC_insert_entry(hdr->f, H5AC_BT2_LEAF, node_ptr->addr, leaf, H5AC__NO_FLAGS_SET) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTINIT, FAIL, "can't add B-tree leaf to cache");
    inserted = true;

    /* Add leaf node as child of 'top' proxy */
    if (hdr->top_proxy) {
        if (H5AC_proxy_entry_add_child(hdr->top_proxy, hdr->f, leaf) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTSET, FAIL, "unable to add v2 B-tree node as child of proxy");
        leaf->top_proxy = hdr->top_proxy;
    } /* end if */

done:
    if (ret_value < 0) {
        if (leaf) {
            /* Remove from cache, if inserted */
            if (inserted)
                if (H5AC_remove_entry(leaf) < 0)
                    HDONE_ERROR(H5E_BTREE, H5E_CANTREMOVE, FAIL,
                                "unable to remove v2 B-tree leaf node from cache");

            /* Release leaf node's disk space */
            if (H5_addr_defined(node_ptr->addr) &&
                H5MF_xfree(hdr->f, H5FD_MEM_BTREE, node_ptr->addr, (hsize_t)hdr->node_size) < 0)
                HDONE_ERROR(H5E_BTREE, H5E_CANTFREE, FAIL,
                            "unable to release file space for v2 B-tree leaf node");

            /* Destroy leaf node */
            if (H5B2__leaf_free(leaf) < 0)
                HDONE_ERROR(H5E_BTREE, H5E_CANTFREE, FAIL, "unable to release v2 B-tree leaf node");
        } /* end if */
    }     /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5B2__create_leaf() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__protect_leaf
 *
 * Purpose:	"Protect" an leaf node in the metadata cache
 *
 * Return:	Pointer to leaf node on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5B2_leaf_t *
H5B2__protect_leaf(H5B2_hdr_t *hdr, void *parent, H5B2_node_ptr_t *node_ptr, bool shadow, unsigned flags)
{
    H5B2_leaf_cache_ud_t udata;            /* User-data for callback */
    H5B2_leaf_t         *leaf;             /* v2 B-tree leaf node */
    H5B2_leaf_t         *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(hdr);
    assert(node_ptr);
    assert(H5_addr_defined(node_ptr->addr));

    /* only H5AC__READ_ONLY_FLAG may appear in flags */
    assert((flags & (unsigned)(~H5AC__READ_ONLY_FLAG)) == 0);

    /* Set up user data for callback */
    udata.f      = hdr->f;
    udata.hdr    = hdr;
    udata.parent = parent;
    udata.nrec   = node_ptr->node_nrec;

    /* Protect the leaf node */
    if (NULL == (leaf = (H5B2_leaf_t *)H5AC_protect(hdr->f, H5AC_BT2_LEAF, node_ptr->addr, &udata, flags)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, NULL, "unable to protect B-tree leaf node");

    /* Create top proxy, if it doesn't exist */
    if (hdr->top_proxy && NULL == leaf->top_proxy) {
        /* Add leaf node as child of 'top' proxy */
        if (H5AC_proxy_entry_add_child(hdr->top_proxy, hdr->f, leaf) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTSET, NULL, "unable to add v2 B-tree leaf node as child of proxy");
        leaf->top_proxy = hdr->top_proxy;
    } /* end if */

    /* Shadow the node, if requested */
    if (shadow)
        if (H5B2__shadow_leaf(leaf, node_ptr) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTCOPY, NULL, "unable to shadow leaf node");

    /* Set return value */
    ret_value = leaf;

done:
    /* Clean up on error */
    if (!ret_value) {
        /* Release the leaf node, if it was protected */
        if (leaf) {
            /* Remove from v2 B-tree's proxy, if added */
            if (leaf->top_proxy) {
                if (H5AC_proxy_entry_remove_child(leaf->top_proxy, leaf) < 0)
                    HDONE_ERROR(
                        H5E_BTREE, H5E_CANTUNDEPEND, NULL,
                        "unable to destroy flush dependency between leaf node and v2 B-tree 'top' proxy");
                leaf->top_proxy = NULL;
            } /* end if */

            /* Unprotect leaf node */
            if (H5AC_unprotect(hdr->f, H5AC_BT2_LEAF, node_ptr->addr, leaf, H5AC__NO_FLAGS_SET) < 0)
                HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, NULL,
                            "unable to unprotect v2 B-tree leaf node, address = %llu",
                            (unsigned long long)node_ptr->addr);
        } /* end if */
    }     /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5B2__protect_leaf() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__neighbor_leaf
 *
 * Purpose:	Locate a record relative to the specified information in a
 *              B-tree leaf node and return that information by filling in
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
H5B2__neighbor_leaf(H5B2_hdr_t *hdr, H5B2_node_ptr_t *curr_node_ptr, void *neighbor_loc, H5B2_compare_t comp,
                    void *parent, void *udata, H5B2_found_t op, void *op_data)
{
    H5B2_leaf_t *leaf;                /* Pointer to leaf node */
    unsigned     idx       = 0;       /* Location of record which matches key */
    int          cmp       = 0;       /* Comparison value of records */
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(hdr);
    assert(curr_node_ptr);
    assert(H5_addr_defined(curr_node_ptr->addr));
    assert(op);

    /* Lock current B-tree node */
    if (NULL == (leaf = H5B2__protect_leaf(hdr, parent, curr_node_ptr, false, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree leaf node");

    /* Locate node pointer for child */
    if (H5B2__locate_record(hdr->cls, leaf->nrec, hdr->nat_off, leaf->leaf_native, udata, &idx, &cmp) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTCOMPARE, FAIL, "can't compare btree2 records");
    if (cmp > 0)
        idx++;
    else if (cmp == 0 && comp == H5B2_COMPARE_GREATER)
        idx++;

    /* Set the neighbor location, if appropriate */
    if (comp == H5B2_COMPARE_LESS) {
        if (idx > 0)
            neighbor_loc = H5B2_LEAF_NREC(leaf, hdr, idx - 1);
    } /* end if */
    else {
        assert(comp == H5B2_COMPARE_GREATER);

        if (idx < leaf->nrec)
            neighbor_loc = H5B2_LEAF_NREC(leaf, hdr, idx);
    } /* end else */

    /* Make callback if neighbor record has been found */
    if (neighbor_loc) {
        /* Make callback for current record */
        if ((op)(neighbor_loc, op_data) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_NOTFOUND, FAIL,
                        "'found' callback failed for B-tree neighbor operation");
    } /* end if */
    else
        HGOTO_ERROR(H5E_BTREE, H5E_NOTFOUND, FAIL, "unable to find neighbor record in B-tree");

done:
    /* Release the B-tree leaf node */
    if (leaf && H5AC_unprotect(hdr->f, H5AC_BT2_LEAF, curr_node_ptr->addr, leaf, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree leaf node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5B2__neighbor_leaf() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__insert_leaf
 *
 * Purpose:	Adds a new record to a B-tree leaf node.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__insert_leaf(H5B2_hdr_t *hdr, H5B2_node_ptr_t *curr_node_ptr, H5B2_nodepos_t curr_pos, void *parent,
                  void *udata)
{
    H5B2_leaf_t *leaf;                            /* Pointer to leaf node */
    unsigned     leaf_flags = H5AC__NO_FLAGS_SET; /* Flags for unprotecting the leaf node */
    int          cmp;                             /* Comparison value of records */
    unsigned     idx       = 0;                   /* Location of record which matches key */
    herr_t       ret_value = SUCCEED;             /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(hdr);
    assert(curr_node_ptr);
    assert(H5_addr_defined(curr_node_ptr->addr));

    /* Lock current B-tree node */
    if (NULL == (leaf = H5B2__protect_leaf(hdr, parent, curr_node_ptr, false, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree leaf node");

    /* Must have a leaf node with enough space to insert a record now */
    assert(curr_node_ptr->node_nrec < hdr->node_info[0].max_nrec);

    /* Sanity check number of records */
    assert(curr_node_ptr->all_nrec == curr_node_ptr->node_nrec);
    assert(leaf->nrec == curr_node_ptr->node_nrec);

    /* Check for inserting into empty leaf */
    if (leaf->nrec == 0)
        idx = 0;
    else {
        /* Find correct location to insert this record */
        if (H5B2__locate_record(hdr->cls, leaf->nrec, hdr->nat_off, leaf->leaf_native, udata, &idx, &cmp) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTCOMPARE, FAIL, "can't compare btree2 records");
        if (cmp == 0)
            HGOTO_ERROR(H5E_BTREE, H5E_EXISTS, FAIL, "record is already in B-tree");
        if (cmp > 0)
            idx++;

        /* Make room for new record */
        if (idx < leaf->nrec)
            memmove(H5B2_LEAF_NREC(leaf, hdr, idx + 1), H5B2_LEAF_NREC(leaf, hdr, idx),
                    hdr->cls->nrec_size * (leaf->nrec - idx));
    } /* end else */

    /* Make callback to store record in native form */
    if ((hdr->cls->store)(H5B2_LEAF_NREC(leaf, hdr, idx), udata) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTINSERT, FAIL, "unable to insert record into leaf node");

    /* Mark the node as dirty */
    leaf_flags |= H5AC__DIRTIED_FLAG;

    /* Update record count for node pointer to current node */
    curr_node_ptr->all_nrec++;
    curr_node_ptr->node_nrec++;

    /* Update record count for current node */
    leaf->nrec++;

    /* Check for new record being the min or max for the tree */
    /* (Don't use 'else' for the idx check, to allow for root leaf node) */
    if (H5B2_POS_MIDDLE != curr_pos) {
        if (idx == 0) {
            if (H5B2_POS_LEFT == curr_pos || H5B2_POS_ROOT == curr_pos) {
                if (hdr->min_native_rec == NULL)
                    if (NULL == (hdr->min_native_rec = H5MM_malloc(hdr->cls->nrec_size)))
                        HGOTO_ERROR(H5E_BTREE, H5E_CANTALLOC, FAIL,
                                    "memory allocation failed for v2 B-tree min record info");
                H5MM_memcpy(hdr->min_native_rec, H5B2_LEAF_NREC(leaf, hdr, idx), hdr->cls->nrec_size);
            } /* end if */
        }     /* end if */
        if (idx == (unsigned)(leaf->nrec - 1)) {
            if (H5B2_POS_RIGHT == curr_pos || H5B2_POS_ROOT == curr_pos) {
                if (hdr->max_native_rec == NULL)
                    if (NULL == (hdr->max_native_rec = H5MM_malloc(hdr->cls->nrec_size)))
                        HGOTO_ERROR(H5E_BTREE, H5E_CANTALLOC, FAIL,
                                    "memory allocation failed for v2 B-tree max record info");
                H5MM_memcpy(hdr->max_native_rec, H5B2_LEAF_NREC(leaf, hdr, idx), hdr->cls->nrec_size);
            } /* end if */
        }     /* end if */
    }         /* end if */

done:
    /* Release the B-tree leaf node (marked as dirty) */
    if (leaf) {
        /* Shadow the node if doing SWMR writes */
        if (hdr->swmr_write && (leaf_flags & H5AC__DIRTIED_FLAG))
            if (H5B2__shadow_leaf(leaf, curr_node_ptr) < 0)
                HDONE_ERROR(H5E_BTREE, H5E_CANTCOPY, FAIL, "unable to shadow leaf B-tree node");

        /* Unprotect leaf node */
        if (H5AC_unprotect(hdr->f, H5AC_BT2_LEAF, curr_node_ptr->addr, leaf, leaf_flags) < 0)
            HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release leaf B-tree node");
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5B2__insert_leaf() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__update_leaf
 *
 * Purpose:	Insert or modify a record in a B-tree leaf node.
 *		If the record exists already, it is modified as if H5B2_modify
 *		was called).  If it doesn't exist, it is inserted as if
 *		H5B2_insert was called.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__update_leaf(H5B2_hdr_t *hdr, H5B2_node_ptr_t *curr_node_ptr, H5B2_update_status_t *status,
                  H5B2_nodepos_t curr_pos, void *parent, void *udata, H5B2_modify_t op, void *op_data)
{
    H5B2_leaf_t *leaf;                            /* Pointer to leaf node */
    unsigned     leaf_flags = H5AC__NO_FLAGS_SET; /* Flags for unprotecting the leaf node */
    int          cmp        = -1;                 /* Comparison value of records */
    unsigned     idx        = 0;                  /* Location of record which matches key */
    herr_t       ret_value  = SUCCEED;            /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(hdr);
    assert(curr_node_ptr);
    assert(H5_addr_defined(curr_node_ptr->addr));

    /* Lock current B-tree node */
    if (NULL == (leaf = H5B2__protect_leaf(hdr, parent, curr_node_ptr, false, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree leaf node");

    /* Sanity check number of records */
    assert(curr_node_ptr->all_nrec == curr_node_ptr->node_nrec);
    assert(leaf->nrec == curr_node_ptr->node_nrec);

    /* Check for inserting into empty leaf */
    if (leaf->nrec == 0)
        idx = 0;
    else {
        /* Find correct location to insert this record */
        if (H5B2__locate_record(hdr->cls, leaf->nrec, hdr->nat_off, leaf->leaf_native, udata, &idx, &cmp) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTCOMPARE, FAIL, "can't compare btree2 records");

        /* Check for inserting a record */
        if (0 != cmp) {
            /* Check if the leaf node is full */
            if (curr_node_ptr->node_nrec == hdr->node_info[0].split_nrec) {
                /* Indicate that the leaf is full, but we need to insert */
                *status = H5B2_UPDATE_INSERT_CHILD_FULL;

                /* Let calling routine handle insertion */
                HGOTO_DONE(SUCCEED);
            } /* end if */

            /* Adjust index to leave room for record to insert */
            if (cmp > 0)
                idx++;

            /* Make room for new record */
            if (idx < leaf->nrec)
                memmove(H5B2_LEAF_NREC(leaf, hdr, idx + 1), H5B2_LEAF_NREC(leaf, hdr, idx),
                        hdr->cls->nrec_size * (leaf->nrec - idx));
        } /* end if */
    }     /* end else */

    /* Check for modifying existing record */
    if (0 == cmp) {
        bool changed = false; /* Whether the 'modify' callback changed the record */

        /* Make callback for current record */
        if ((op)(H5B2_LEAF_NREC(leaf, hdr, idx), op_data, &changed) < 0) {
            /* Make certain that the callback didn't modify the value if it failed */
            assert(changed == false);

            HGOTO_ERROR(H5E_BTREE, H5E_CANTMODIFY, FAIL,
                        "'modify' callback failed for B-tree update operation");
        } /* end if */

        /* Mark the node as dirty if it changed */
        leaf_flags |= (changed ? H5AC__DIRTIED_FLAG : 0);

        /* Indicate that the record was modified */
        *status = H5B2_UPDATE_MODIFY_DONE;
    } /* end if */
    else {
        /* Must have a leaf node with enough space to insert a record now */
        assert(curr_node_ptr->node_nrec < hdr->node_info[0].max_nrec);

        /* Make callback to store record in native form */
        if ((hdr->cls->store)(H5B2_LEAF_NREC(leaf, hdr, idx), udata) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTINSERT, FAIL, "unable to insert record into leaf node");

        /* Mark the node as dirty */
        leaf_flags |= H5AC__DIRTIED_FLAG;

        /* Indicate that the record was inserted */
        *status = H5B2_UPDATE_INSERT_DONE;

        /* Update record count for node pointer to current node */
        curr_node_ptr->all_nrec++;
        curr_node_ptr->node_nrec++;

        /* Update record count for current node */
        leaf->nrec++;
    } /* end else */

    /* Check for new record being the min or max for the tree */
    /* (Don't use 'else' for the idx check, to allow for root leaf node) */
    if (H5B2_POS_MIDDLE != curr_pos) {
        if (idx == 0) {
            if (H5B2_POS_LEFT == curr_pos || H5B2_POS_ROOT == curr_pos) {
                if (hdr->min_native_rec == NULL)
                    if (NULL == (hdr->min_native_rec = H5MM_malloc(hdr->cls->nrec_size)))
                        HGOTO_ERROR(H5E_BTREE, H5E_CANTALLOC, FAIL,
                                    "memory allocation failed for v2 B-tree min record info");
                H5MM_memcpy(hdr->min_native_rec, H5B2_LEAF_NREC(leaf, hdr, idx), hdr->cls->nrec_size);
            } /* end if */
        }     /* end if */
        if (idx == (unsigned)(leaf->nrec - 1)) {
            if (H5B2_POS_RIGHT == curr_pos || H5B2_POS_ROOT == curr_pos) {
                if (hdr->max_native_rec == NULL)
                    if (NULL == (hdr->max_native_rec = H5MM_malloc(hdr->cls->nrec_size)))
                        HGOTO_ERROR(H5E_BTREE, H5E_CANTALLOC, FAIL,
                                    "memory allocation failed for v2 B-tree max record info");
                H5MM_memcpy(hdr->max_native_rec, H5B2_LEAF_NREC(leaf, hdr, idx), hdr->cls->nrec_size);
            } /* end if */
        }     /* end if */
    }         /* end if */

done:
    /* Release the B-tree leaf node */
    if (leaf) {
        /* Check if we should shadow this node */
        if (hdr->swmr_write && (leaf_flags & H5AC__DIRTIED_FLAG)) {
            /* Attempt to shadow the node if doing SWMR writes */
            if (H5B2__shadow_leaf(leaf, curr_node_ptr) < 0)
                HDONE_ERROR(H5E_BTREE, H5E_CANTCOPY, FAIL, "unable to shadow leaf B-tree node");

            /* Change the state to "shadowed" if only modified currently */
            /* (Triggers parent to be marked dirty) */
            if (*status == H5B2_UPDATE_MODIFY_DONE)
                *status = H5B2_UPDATE_SHADOW_DONE;
        } /* end if */

        /* Unprotect leaf node */
        if (H5AC_unprotect(hdr->f, H5AC_BT2_LEAF, curr_node_ptr->addr, leaf, leaf_flags) < 0)
            HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release leaf B-tree node");
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5B2__update_leaf() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__swap_leaf
 *
 * Purpose:	Swap a record in a node with a record in a leaf node
 *
 * Return:	Success:	Non-negative
 *
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__swap_leaf(H5B2_hdr_t *hdr, uint16_t depth, H5B2_internal_t *internal, unsigned *internal_flags_ptr,
                unsigned idx, void *swap_loc)
{
    const H5AC_class_t *child_class;              /* Pointer to child node's class info */
    haddr_t             child_addr = HADDR_UNDEF; /* Address of child node */
    void               *child      = NULL;        /* Pointer to child node */
    uint8_t            *child_native;             /* Pointer to child's native records */
    herr_t              ret_value = SUCCEED;      /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(hdr);
    assert(internal);
    assert(internal_flags_ptr);
    assert(idx <= internal->nrec);

    /* Check for the kind of B-tree node to swap */
    if (depth > 1) {
        H5B2_internal_t *child_internal; /* Pointer to internal node */

        /* Setup information for unlocking child node */
        child_class = H5AC_BT2_INT;

        /* Lock B-tree child nodes */
        if (NULL ==
            (child_internal = H5B2__protect_internal(hdr, internal, &internal->node_ptrs[idx],
                                                     (uint16_t)(depth - 1), false, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree internal node");
        child_addr = internal->node_ptrs[idx].addr;

        /* More setup for accessing child node information */
        child        = child_internal;
        child_native = child_internal->int_native;
    } /* end if */
    else {
        H5B2_leaf_t *child_leaf; /* Pointer to leaf node */

        /* Setup information for unlocking child nodes */
        child_class = H5AC_BT2_LEAF;

        /* Lock B-tree child node */
        if (NULL == (child_leaf = H5B2__protect_leaf(hdr, internal, &internal->node_ptrs[idx], false,
                                                     H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree leaf node");
        child_addr = internal->node_ptrs[idx].addr;

        /* More setup for accessing child node information */
        child        = child_leaf;
        child_native = child_leaf->leaf_native;
    } /* end else */

    /* Swap records (use disk page as temporary buffer) */
    H5MM_memcpy(hdr->page, H5B2_NAT_NREC(child_native, hdr, 0), hdr->cls->nrec_size);
    H5MM_memcpy(H5B2_NAT_NREC(child_native, hdr, 0), swap_loc, hdr->cls->nrec_size);
    H5MM_memcpy(swap_loc, hdr->page, hdr->cls->nrec_size);

    /* Mark parent as dirty */
    *internal_flags_ptr |= H5AC__DIRTIED_FLAG;

#ifdef H5B2_DEBUG
    H5B2__assert_internal((hsize_t)0, hdr, internal);
    if (depth > 1)
        H5B2__assert_internal(internal->node_ptrs[idx].all_nrec, hdr, (H5B2_internal_t *)child);
    else
        H5B2__assert_leaf(hdr, (H5B2_leaf_t *)child);
#endif /* H5B2_DEBUG */

done:
    /* Unlock child node */
    if (child && H5AC_unprotect(hdr->f, child_class, child_addr, child, H5AC__DIRTIED_FLAG) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree child node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B2__swap_leaf() */

/*-------------------------------------------------------------------------
 * Function:    H5B2__shadow_leaf
 *
 * Purpose:     "Shadow" a leaf node - copy it to a new location, leaving
 *              the data in the old location intact (for now).  This is
 *              done when writing in SWMR mode to ensure that readers do
 *              not see nodes that are out of date with respect to each
 *              other and thereby inconsistent.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5B2__shadow_leaf(H5B2_leaf_t *leaf, H5B2_node_ptr_t *curr_node_ptr)
{
    H5B2_hdr_t *hdr;                 /* B-tree header */
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(leaf);
    assert(curr_node_ptr);
    assert(H5_addr_defined(curr_node_ptr->addr));
    hdr = leaf->hdr;
    assert(hdr);
    assert(hdr->swmr_write);

    /* We only need to shadow the node if it has not been shadowed since the
     * last time the header was flushed, as otherwise it will be unreachable by
     * the readers so there will be no need to shadow.  To check if it has been
     * shadowed, compare the epoch of this node and the header.  If this node's
     * epoch is <= to the header's, it hasn't been shadowed yet. */
    if (leaf->shadow_epoch <= hdr->shadow_epoch) {
        haddr_t new_node_addr; /* Address to move node to */

        /*
         * We must clone the old node so readers with an out-of-date version of
         * the parent can still see the correct number of children, via the
         * shadowed node.  Remove it from cache but do not mark it free on disk.
         */
        /* Allocate space for the cloned node */
        if (HADDR_UNDEF == (new_node_addr = H5MF_alloc(hdr->f, H5FD_MEM_BTREE, (hsize_t)hdr->node_size)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTALLOC, FAIL, "unable to allocate file space to move B-tree node");

        /* Move the location of the old child on the disk */
        if (H5AC_move_entry(hdr->f, H5AC_BT2_LEAF, curr_node_ptr->addr, new_node_addr) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTMOVE, FAIL, "unable to move B-tree node");
        curr_node_ptr->addr = new_node_addr;

        /* Should free the space in the file, but this is not supported by
         * SWMR_WRITE code yet - QAK, 2016/12/01
         */

        /* Set shadow epoch for node ahead of header */
        leaf->shadow_epoch = hdr->shadow_epoch + 1;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B2__shadow_leaf() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__remove_leaf
 *
 * Purpose:	Removes a record from a B-tree leaf node.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__remove_leaf(H5B2_hdr_t *hdr, H5B2_node_ptr_t *curr_node_ptr, H5B2_nodepos_t curr_pos, void *parent,
                  void *udata, H5B2_remove_t op, void *op_data)
{
    H5B2_leaf_t *leaf;                            /* Pointer to leaf node */
    haddr_t      leaf_addr  = HADDR_UNDEF;        /* Leaf address on disk */
    unsigned     leaf_flags = H5AC__NO_FLAGS_SET; /* Flags for unprotecting leaf node */
    unsigned     idx        = 0;                  /* Location of record which matches key */
    int          cmp;                             /* Comparison value of records */
    herr_t       ret_value = SUCCEED;             /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(hdr);
    assert(curr_node_ptr);
    assert(H5_addr_defined(curr_node_ptr->addr));

    /* Lock current B-tree node */
    if (NULL == (leaf = H5B2__protect_leaf(hdr, parent, curr_node_ptr, false, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree leaf node");
    leaf_addr = curr_node_ptr->addr;

    /* Sanity check number of records */
    assert(curr_node_ptr->all_nrec == curr_node_ptr->node_nrec);
    assert(leaf->nrec == curr_node_ptr->node_nrec);

    /* Find correct location to remove this record */
    if (H5B2__locate_record(hdr->cls, leaf->nrec, hdr->nat_off, leaf->leaf_native, udata, &idx, &cmp) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTCOMPARE, FAIL, "can't compare btree2 records");
    if (cmp != 0)
        HGOTO_ERROR(H5E_BTREE, H5E_NOTFOUND, FAIL, "record is not in B-tree");

    /* Check for invalidating the min/max record for the tree */
    if (H5B2_POS_MIDDLE != curr_pos) {
        /* (Don't use 'else' for the idx check, to allow for root leaf node) */
        if (idx == 0) {
            if (H5B2_POS_LEFT == curr_pos || H5B2_POS_ROOT == curr_pos) {
                if (hdr->min_native_rec)
                    hdr->min_native_rec = H5MM_xfree(hdr->min_native_rec);
            } /* end if */
        }     /* end if */
        if (idx == (unsigned)(leaf->nrec - 1)) {
            if (H5B2_POS_RIGHT == curr_pos || H5B2_POS_ROOT == curr_pos) {
                if (hdr->max_native_rec)
                    hdr->max_native_rec = H5MM_xfree(hdr->max_native_rec);
            } /* end if */
        }     /* end if */
    }         /* end if */

    /* Make 'remove' callback if there is one */
    if (op)
        if ((op)(H5B2_LEAF_NREC(leaf, hdr, idx), op_data) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTDELETE, FAIL, "unable to remove record into leaf node");

    /* Update number of records in node */
    leaf->nrec--;

    if (leaf->nrec > 0) {
        /* Shadow the node if doing SWMR writes */
        if (hdr->swmr_write) {
            if (H5B2__shadow_leaf(leaf, curr_node_ptr) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTCOPY, FAIL, "unable to shadow leaf node");
            leaf_addr = curr_node_ptr->addr;
        } /* end if */

        /* Pack record out of leaf */
        if (idx < leaf->nrec)
            memmove(H5B2_LEAF_NREC(leaf, hdr, idx), H5B2_LEAF_NREC(leaf, hdr, (idx + 1)),
                    hdr->cls->nrec_size * (leaf->nrec - idx));

        /* Mark leaf node as dirty also */
        leaf_flags |= H5AC__DIRTIED_FLAG;
    } /* end if */
    else {
        /* Let the cache know that the object is deleted */
        leaf_flags |= H5AC__DELETED_FLAG;
        if (!hdr->swmr_write)
            leaf_flags |= H5AC__DIRTIED_FLAG | H5AC__FREE_FILE_SPACE_FLAG;

        /* Reset address of parent node pointer */
        curr_node_ptr->addr = HADDR_UNDEF;
    } /* end else */

    /* Update record count for parent of leaf node */
    curr_node_ptr->node_nrec--;

done:
    /* Release the B-tree leaf node */
    if (leaf && H5AC_unprotect(hdr->f, H5AC_BT2_LEAF, leaf_addr, leaf, leaf_flags) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release leaf B-tree node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5B2__remove_leaf() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__remove_leaf_by_idx
 *
 * Purpose:	Removes a record from a B-tree leaf node, according to the
 *              offset in the B-tree records.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__remove_leaf_by_idx(H5B2_hdr_t *hdr, H5B2_node_ptr_t *curr_node_ptr, H5B2_nodepos_t curr_pos,
                         void *parent, unsigned idx, H5B2_remove_t op, void *op_data)
{
    H5B2_leaf_t *leaf;                            /* Pointer to leaf node */
    haddr_t      leaf_addr  = HADDR_UNDEF;        /* Leaf address on disk */
    unsigned     leaf_flags = H5AC__NO_FLAGS_SET; /* Flags for unprotecting leaf node */
    herr_t       ret_value  = SUCCEED;            /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(hdr);
    assert(curr_node_ptr);
    assert(H5_addr_defined(curr_node_ptr->addr));

    /* Lock B-tree leaf node */
    if (NULL == (leaf = H5B2__protect_leaf(hdr, parent, curr_node_ptr, false, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree leaf node");
    leaf_addr = curr_node_ptr->addr;

    /* Sanity check number of records */
    assert(curr_node_ptr->all_nrec == curr_node_ptr->node_nrec);
    assert(leaf->nrec == curr_node_ptr->node_nrec);
    assert(idx < leaf->nrec);

    /* Check for invalidating the min/max record for the tree */
    if (H5B2_POS_MIDDLE != curr_pos) {
        /* (Don't use 'else' for the idx check, to allow for root leaf node) */
        if (idx == 0) {
            if (H5B2_POS_LEFT == curr_pos || H5B2_POS_ROOT == curr_pos) {
                if (hdr->min_native_rec)
                    hdr->min_native_rec = H5MM_xfree(hdr->min_native_rec);
            } /* end if */
        }     /* end if */
        if (idx == (unsigned)(leaf->nrec - 1)) {
            if (H5B2_POS_RIGHT == curr_pos || H5B2_POS_ROOT == curr_pos) {
                if (hdr->max_native_rec)
                    hdr->max_native_rec = H5MM_xfree(hdr->max_native_rec);
            } /* end if */
        }     /* end if */
    }         /* end if */

    /* Make 'remove' callback if there is one */
    if (op)
        if ((op)(H5B2_LEAF_NREC(leaf, hdr, idx), op_data) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTDELETE, FAIL, "unable to remove record into leaf node");

    /* Update number of records in node */
    leaf->nrec--;

    if (leaf->nrec > 0) {
        /* Shadow the node if doing SWMR writes */
        if (hdr->swmr_write) {
            if (H5B2__shadow_leaf(leaf, curr_node_ptr) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTCOPY, FAIL, "unable to shadow leaf node");
            leaf_addr = curr_node_ptr->addr;
        } /* end if */

        /* Pack record out of leaf */
        if (idx < leaf->nrec)
            memmove(H5B2_LEAF_NREC(leaf, hdr, idx), H5B2_LEAF_NREC(leaf, hdr, (idx + 1)),
                    hdr->cls->nrec_size * (leaf->nrec - idx));

        /* Mark leaf node as dirty also */
        leaf_flags |= H5AC__DIRTIED_FLAG;
    } /* end if */
    else {
        /* Let the cache know that the object is deleted */
        leaf_flags |= H5AC__DELETED_FLAG;
        if (!hdr->swmr_write)
            leaf_flags |= H5AC__DIRTIED_FLAG | H5AC__FREE_FILE_SPACE_FLAG;

        /* Reset address of parent node pointer */
        curr_node_ptr->addr = HADDR_UNDEF;
    } /* end else */

    /* Update record count for parent of leaf node */
    curr_node_ptr->node_nrec--;

done:
    /* Release the B-tree leaf node */
    if (leaf && H5AC_unprotect(hdr->f, H5AC_BT2_LEAF, leaf_addr, leaf, leaf_flags) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release leaf B-tree node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5B2__remove_leaf_by_idx() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__leaf_free
 *
 * Purpose:	Destroys a B-tree leaf node in memory.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__leaf_free(H5B2_leaf_t *leaf)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(leaf);

    /* Release leaf's native key buffer */
    if (leaf->leaf_native)
        leaf->leaf_native = (uint8_t *)H5FL_FAC_FREE(leaf->hdr->node_info[0].nat_rec_fac, leaf->leaf_native);

    /* Decrement ref. count on B-tree header */
    if (H5B2__hdr_decr(leaf->hdr) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTDEC, FAIL, "can't decrement ref. count on B-tree header");

    /* Sanity check */
    assert(NULL == leaf->top_proxy);

    /* Free B-tree leaf node info */
    leaf = H5FL_FREE(H5B2_leaf_t, leaf);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B2__leaf_free() */

#ifdef H5B2_DEBUG

/*-------------------------------------------------------------------------
 * Function:	H5B2__assert_leaf
 *
 * Purpose:	Verify than a leaf node is mostly sane
 *
 * Return:	Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
H5_ATTR_PURE herr_t
H5B2__assert_leaf(const H5B2_hdr_t H5_ATTR_NDEBUG_UNUSED *hdr, const H5B2_leaf_t H5_ATTR_NDEBUG_UNUSED *leaf)
{
    /* General sanity checking on node */
    assert(leaf->nrec <= hdr->node_info->split_nrec);

    return (0);
} /* end H5B2__assert_leaf() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__assert_leaf2
 *
 * Purpose:	Verify than a leaf node is mostly sane
 *
 * Return:	Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
H5_ATTR_PURE herr_t
H5B2__assert_leaf2(const H5B2_hdr_t H5_ATTR_NDEBUG_UNUSED *hdr, const H5B2_leaf_t H5_ATTR_NDEBUG_UNUSED *leaf,
                   const H5B2_leaf_t H5_ATTR_UNUSED *leaf2)
{
    /* General sanity checking on node */
    assert(leaf->nrec <= hdr->node_info->split_nrec);

    return (0);
} /* end H5B2__assert_leaf2() */
#endif /* H5B2_DEBUG */
