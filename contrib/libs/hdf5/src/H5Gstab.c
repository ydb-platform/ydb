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

/****************/
/* Module Setup */
/****************/

#include "H5Gmodule.h" /* This source code file is part of the H5G module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Fprivate.h"  /* File access				*/
#include "H5Gpkg.h"      /* Groups		  		*/
#include "H5HLprivate.h" /* Local Heaps				*/
#include "H5MMprivate.h" /* Memory management			*/

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/* User data for finding link information from B-tree */
typedef struct {
    /* downward */
    const char *name; /* Name to search for */
    H5HL_t     *heap; /* Local heap for group */

    /* upward */
    H5O_link_t *lnk; /* Caller's link location */
} H5G_stab_fnd_ud_t;

/* Data passed through B-tree iteration for looking up a name by index */
typedef struct H5G_bt_it_gnbi_t {
    /* downward */
    H5G_bt_it_idx_common_t common; /* Common information for "by index" lookup  */
    H5HL_t                *heap;   /*symbol table heap 			     */

    /* upward */
    char *name; /*member name to be returned                 */
} H5G_bt_it_gnbi_t;

#ifndef H5_NO_DEPRECATED_SYMBOLS
/* Data passed through B-tree iteration for looking up a type by index */
typedef struct H5G_bt_it_gtbi_t {
    /* downward */
    H5G_bt_it_idx_common_t common; /* Common information for "by index" lookup  */
    H5F_t                 *f;      /* Pointer to file that symbol table is in */

    /* upward */
    H5G_obj_t type; /*member type to be returned                 */
} H5G_bt_it_gtbi_t;
#endif /* H5_NO_DEPRECATED_SYMBOLS */

/* Data passed through B-tree iteration for looking up a link by index */
typedef struct H5G_bt_it_lbi_t {
    /* downward */
    H5G_bt_it_idx_common_t common; /* Common information for "by index" lookup  */
    H5HL_t                *heap;   /*symbol table heap 			     */

    /* upward */
    H5O_link_t *lnk;   /*link to be returned                        */
    bool        found; /*whether we found the link                  */
} H5G_bt_it_lbi_t;

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:	H5G__stab_create_components
 *
 * Purpose:	Creates the components for a new, empty, symbol table (name heap
 *		and B-tree).  The caller can specify an initial size for the
 *		name heap.
 *
 *		In order for the B-tree to operate correctly, the first
 *		item in the heap is the empty string, and must appear at
 *		heap offset zero.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__stab_create_components(H5F_t *f, H5O_stab_t *stab, size_t size_hint)
{
    H5HL_t *heap = NULL;         /* Pointer to local heap */
    size_t  name_offset;         /* Offset of "" name */
    herr_t  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(stab);
    assert(size_hint > 0);

    /* Create the B-tree */
    if (H5B_create(f, H5B_SNODE, NULL, &(stab->btree_addr) /*out*/) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create B-tree");

    /* Create symbol table private heap */
    if (H5HL_create(f, size_hint, &(stab->heap_addr) /*out*/) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create heap");

    /* Pin the heap down in memory */
    if (NULL == (heap = H5HL_protect(f, stab->heap_addr, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_SYM, H5E_PROTECT, FAIL, "unable to protect symbol table heap");

    /* Insert name into the heap */
    if (H5HL_insert(f, heap, (size_t)1, "", &name_offset) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINSERT, FAIL, "can't insert name into heap");

    /* B-trees won't work if the first name isn't at the beginning
     * of the heap.
     */
    assert(0 == name_offset);

done:
    /* Release resources */
    if (heap && FAIL == H5HL_unprotect(heap))
        HDONE_ERROR(H5E_SYM, H5E_PROTECT, FAIL, "unable to unprotect symbol table heap");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__stab_create_components() */

/*-------------------------------------------------------------------------
 * Function:	H5G__stab_create
 *
 * Purpose:	Creates a new empty symbol table (object header, name heap,
 *		and B-tree).  The caller can specify an initial size for the
 *		name heap.  The object header of the group is opened for
 *		write access.
 *
 *		In order for the B-tree to operate correctly, the first
 *		item in the heap is the empty string, and must appear at
 *		heap offset zero.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__stab_create(H5O_loc_t *grp_oloc, const H5O_ginfo_t *ginfo, H5O_stab_t *stab)
{
    size_t heap_hint;           /* Local heap size hint */
    size_t size_hint;           /* Local heap size hint */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_TAG(grp_oloc->addr)

    /*
     * Check arguments.
     */
    assert(grp_oloc);
    assert(stab);

    /* Adjust the size hint, if necessary */
    if (ginfo->lheap_size_hint == 0)
        heap_hint =
            8 + /* "null" name inserted for B-tree */
            (ginfo->est_num_entries *
             H5HL_ALIGN(ginfo->est_name_len +
                        1)) + /* estimated size of names for links, aligned for inserting into local heap */
            H5HL_SIZEOF_FREE(grp_oloc->file); /* Free list entry in local heap */
    else
        heap_hint = ginfo->lheap_size_hint;
    size_hint = MAX(heap_hint, H5HL_SIZEOF_FREE(grp_oloc->file) + 2);

    /* Go create the B-tree & local heap */
    if (H5G__stab_create_components(grp_oloc->file, stab, size_hint) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create symbol table components");

    /*
     * Insert the symbol table message into the object header and the symbol
     * table entry.
     */
    if (H5O_msg_create(grp_oloc, H5O_STAB_ID, 0, H5O_UPDATE_TIME, stab) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create message");

done:
    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5G__stab_create() */

/*-------------------------------------------------------------------------
 * Function:	H5G__stab_insert_real
 *
 * Purpose:	Insert a new symbol into a table.
 *		The name of the new symbol is NAME and its symbol
 *		table entry is OBJ_LNK.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__stab_insert_real(H5F_t *f, const H5O_stab_t *stab, const char *name, H5O_link_t *obj_lnk,
                      H5O_type_t obj_type, const void *crt_info)
{
    H5HL_t      *heap = NULL;         /* Pointer to local heap */
    H5G_bt_ins_t udata;               /* Data to pass through B-tree	*/
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check arguments */
    assert(f);
    assert(stab);
    assert(name && *name);
    assert(obj_lnk);

    /* Pin the heap down in memory */
    if (NULL == (heap = H5HL_protect(f, stab->heap_addr, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_SYM, H5E_PROTECT, FAIL, "unable to protect symbol table heap");

    /* Initialize data to pass through B-tree */
    udata.common.name = name;
    udata.common.heap = heap;
    udata.lnk         = obj_lnk;
    udata.obj_type    = obj_type;
    udata.crt_info    = crt_info;

    /* Insert into symbol table */
    if (H5B_insert(f, H5B_SNODE, stab->btree_addr, &udata) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINSERT, FAIL, "unable to insert entry");

done:
    /* Release resources */
    if (heap && H5HL_unprotect(heap) < 0)
        HDONE_ERROR(H5E_SYM, H5E_PROTECT, FAIL, "unable to unprotect symbol table heap");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__stab_insert_real() */

/*-------------------------------------------------------------------------
 * Function:	H5G__stab_insert
 *
 * Purpose:	Insert a new symbol into the table described by GRP_ENT in
 *		file F.	 The name of the new symbol is NAME and its symbol
 *		table entry is OBJ_ENT.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__stab_insert(const H5O_loc_t *grp_oloc, const char *name, H5O_link_t *obj_lnk, H5O_type_t obj_type,
                 const void *crt_info)
{
    H5O_stab_t stab;                /* Symbol table message		*/
    herr_t     ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check arguments */
    assert(grp_oloc && grp_oloc->file);
    assert(name && *name);
    assert(obj_lnk);

    /* Retrieve symbol table message */
    if (NULL == H5O_msg_read(grp_oloc, H5O_STAB_ID, &stab))
        HGOTO_ERROR(H5E_SYM, H5E_BADMESG, FAIL, "not a symbol table");

    if (H5G__stab_insert_real(grp_oloc->file, &stab, name, obj_lnk, obj_type, crt_info) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, H5_ITER_ERROR, "unable to insert the name");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__stab_insert() */

/*-------------------------------------------------------------------------
 * Function:	H5G__stab_remove
 *
 * Purpose:	Remove NAME from a symbol table.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__stab_remove(const H5O_loc_t *loc, H5RS_str_t *grp_full_path_r, const char *name)
{
    H5HL_t     *heap = NULL;         /* Pointer to local heap */
    H5O_stab_t  stab;                /*symbol table message		*/
    H5G_bt_rm_t udata;               /*data to pass through B-tree	*/
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(loc && loc->file);
    assert(name && *name);

    /* Read in symbol table message */
    if (NULL == H5O_msg_read(loc, H5O_STAB_ID, &stab))
        HGOTO_ERROR(H5E_SYM, H5E_BADMESG, FAIL, "not a symbol table");

    /* Pin the heap down in memory */
    if (NULL == (heap = H5HL_protect(loc->file, stab.heap_addr, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_SYM, H5E_PROTECT, FAIL, "unable to protect symbol table heap");

    /* Initialize data to pass through B-tree */
    udata.common.name     = name;
    udata.common.heap     = heap;
    udata.grp_full_path_r = grp_full_path_r;

    /* Remove from symbol table */
    if (H5B_remove(loc->file, H5B_SNODE, stab.btree_addr, &udata) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to remove entry");

done:
    /* Release resources */
    if (heap && H5HL_unprotect(heap) < 0)
        HDONE_ERROR(H5E_SYM, H5E_PROTECT, FAIL, "unable to unprotect symbol table heap");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__stab_remove() */

/*-------------------------------------------------------------------------
 * Function:	H5G__stab_remove_by_idx
 *
 * Purpose:	Remove NAME from a symbol table, according to the name index.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__stab_remove_by_idx(const H5O_loc_t *grp_oloc, H5RS_str_t *grp_full_path_r, H5_iter_order_t order,
                        hsize_t n)
{
    H5HL_t     *heap = NULL;          /* Pointer to local heap */
    H5O_stab_t  stab;                 /* Symbol table message		*/
    H5G_bt_rm_t udata;                /* Data to pass through B-tree	*/
    H5O_link_t  obj_lnk;              /* Object's link within group */
    bool        lnk_copied = false;   /* Whether the link was copied */
    herr_t      ret_value  = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(grp_oloc && grp_oloc->file);

    /* Look up name of link to remove, by index */
    if (H5G__stab_lookup_by_idx(grp_oloc, order, n, &obj_lnk) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't get link information");
    lnk_copied = true;

    /* Read in symbol table message */
    if (NULL == H5O_msg_read(grp_oloc, H5O_STAB_ID, &stab))
        HGOTO_ERROR(H5E_SYM, H5E_BADMESG, FAIL, "not a symbol table");

    /* Pin the heap down in memory */
    if (NULL == (heap = H5HL_protect(grp_oloc->file, stab.heap_addr, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_SYM, H5E_PROTECT, FAIL, "unable to protect symbol table heap");

    /* Initialize data to pass through B-tree */
    udata.common.name     = obj_lnk.name;
    udata.common.heap     = heap;
    udata.grp_full_path_r = grp_full_path_r;

    /* Remove link from symbol table */
    if (H5B_remove(grp_oloc->file, H5B_SNODE, stab.btree_addr, &udata) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to remove entry");

done:
    /* Release resources */
    if (heap && H5HL_unprotect(heap) < 0)
        HDONE_ERROR(H5E_SYM, H5E_PROTECT, FAIL, "unable to unprotect symbol table heap");

    /* Reset the link information, if we have a copy */
    if (lnk_copied)
        H5O_msg_reset(H5O_LINK_ID, &obj_lnk);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__stab_remove_by_idx() */

/*-------------------------------------------------------------------------
 * Function:	H5G__stab_delete
 *
 * Purpose:	Delete entire symbol table information from file
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__stab_delete(H5F_t *f, const H5O_stab_t *stab)
{
    H5HL_t     *heap = NULL;         /* Pointer to local heap */
    H5G_bt_rm_t udata;               /*data to pass through B-tree	*/
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(stab);
    assert(H5_addr_defined(stab->btree_addr));
    assert(H5_addr_defined(stab->heap_addr));

    /* Pin the heap down in memory */
    if (NULL == (heap = H5HL_protect(f, stab->heap_addr, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_SYM, H5E_PROTECT, FAIL, "unable to protect symbol table heap");

    /* Set up user data for B-tree deletion */
    udata.common.name = NULL;
    udata.common.heap = heap;

    /* Delete entire B-tree */
    if (H5B_delete(f, H5B_SNODE, stab->btree_addr, &udata) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTDELETE, FAIL, "unable to delete symbol table B-tree");

    /* Release resources */
    if (H5HL_unprotect(heap) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_PROTECT, FAIL, "unable to unprotect symbol table heap");
    heap = NULL;

    /* Delete local heap for names */
    if (H5HL_delete(f, stab->heap_addr) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTDELETE, FAIL, "unable to delete symbol table heap");

done:
    /* Release resources */
    if (heap && H5HL_unprotect(heap) < 0)
        HDONE_ERROR(H5E_SYM, H5E_PROTECT, FAIL, "unable to unprotect symbol table heap");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__stab_delete() */

/*-------------------------------------------------------------------------
 * Function:	H5G__stab_iterate
 *
 * Purpose:	Iterate over the objects in a group
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__stab_iterate(const H5O_loc_t *oloc, H5_iter_order_t order, hsize_t skip, hsize_t *last_lnk,
                  H5G_lib_iterate_t op, void *op_data)
{
    H5HL_t          *heap = NULL;           /* Local heap for group */
    H5O_stab_t       stab;                  /* Info about symbol table */
    H5G_link_table_t ltable    = {0, NULL}; /* Link table */
    herr_t           ret_value = FAIL;      /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(oloc);
    assert(op);

    /* Get the B-tree info */
    if (NULL == H5O_msg_read(oloc, H5O_STAB_ID, &stab))
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "unable to determine local heap address");

    /* Pin the heap down in memory */
    if (NULL == (heap = H5HL_protect(oloc->file, stab.heap_addr, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_SYM, H5E_PROTECT, FAIL, "unable to protect symbol table heap");

    /* Check on iteration order */
    /* ("native" iteration order is increasing for this link storage mechanism) */
    if (order != H5_ITER_DEC) {
        H5G_bt_it_it_t udata; /* User data to pass to B-tree callback */

        /* Build udata to pass through H5B_iterate() to H5G__node_iterate() */
        udata.heap      = heap;
        udata.skip      = skip;
        udata.final_ent = last_lnk;
        udata.op        = op;
        udata.op_data   = op_data;

        /* Iterate over the group members */
        if ((ret_value = H5B_iterate(oloc->file, H5B_SNODE, stab.btree_addr, H5G__node_iterate, &udata)) < 0)
            HERROR(H5E_SYM, H5E_CANTNEXT, "iteration operator failed");

        /* Check for too high of a starting index (ex post facto :-) */
        /* (Skipping exactly as many entries as are in the group is currently an error) */
        if (skip > 0 && skip >= *last_lnk)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid index specified");
    } /* end if */
    else {
        H5G_bt_it_bt_t udata; /* User data to pass to B-tree callback */

        /* Build udata to pass through H5B_iterate() to H5G__node_build_table() */
        udata.alloc_nlinks = 0;
        udata.heap         = heap;
        udata.ltable       = &ltable;

        /* Iterate over the group members */
        if (H5B_iterate(oloc->file, H5B_SNODE, stab.btree_addr, H5G__node_build_table, &udata) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "unable to build link table");

        /* Check for skipping out of bounds */
        if (skip > 0 && (size_t)skip >= ltable.nlinks)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "index out of bound");

        /* Sort link table in correct iteration order */
        if (H5G__link_sort_table(&ltable, H5_INDEX_NAME, order) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTSORT, FAIL, "error sorting link messages");

        /* Iterate over links in table */
        if ((ret_value = H5G__link_iterate_table(&ltable, skip, last_lnk, op, op_data)) < 0)
            HERROR(H5E_SYM, H5E_CANTNEXT, "iteration operator failed");
    } /* end else */

done:
    /* Release resources */
    if (heap && H5HL_unprotect(heap) < 0)
        HDONE_ERROR(H5E_SYM, H5E_PROTECT, FAIL, "unable to unprotect symbol table heap");
    if (ltable.lnks && H5G__link_release_table(&ltable) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTFREE, FAIL, "unable to release link table");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__stab_iterate() */

/*-------------------------------------------------------------------------
 * Function:	H5G__stab_count
 *
 * Purpose:	Count the # of links in a group
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__stab_count(const H5O_loc_t *oloc, hsize_t *num_objs)
{
    H5O_stab_t stab; /* Info about symbol table */
    herr_t     ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE_TAG(oloc->addr)

    /* Sanity check */
    assert(oloc);
    assert(num_objs);

    /* Reset the number of objects in the group */
    *num_objs = 0;

    /* Get the B-tree info */
    if (NULL == H5O_msg_read(oloc, H5O_STAB_ID, &stab))
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "unable to determine local heap address");

    /* Iterate over the group members */
    if (H5B_iterate(oloc->file, H5B_SNODE, stab.btree_addr, H5G__node_sumup, num_objs) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "iteration operator failed");

done:
    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5G__stab_count() */

/*-------------------------------------------------------------------------
 * Function:	H5G__stab_bh_size
 *
 * Purpose:	Retrieve storage for btree and heap (1.6)
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__stab_bh_size(H5F_t *f, const H5O_stab_t *stab, H5_ih_info_t *bh_info)
{
    hsize_t    snode_size; /* Symbol table node size */
    H5B_info_t bt_info;    /* B-tree node info */
    herr_t     ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(f);
    assert(stab);
    assert(bh_info);

    /* Set up user data for B-tree iteration */
    snode_size = 0;

    /* Get the B-tree & symbol table node size info */
    if (H5B_get_info(f, H5B_SNODE, stab->btree_addr, &bt_info, H5G__node_iterate_size, &snode_size) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTINIT, FAIL, "iteration operator failed");

    /* Add symbol table & B-tree node sizes to index info */
    bh_info->index_size += snode_size + bt_info.size;

    /* Get the size of the local heap for the group */
    if (H5HL_heapsize(f, stab->heap_addr, &(bh_info->heap_size)) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "iteration operator failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__stab_bh_size() */

/*-------------------------------------------------------------------------
 * Function:	H5G__stab_get_name_by_idx_cb
 *
 * Purpose:     Callback for B-tree iteration 'by index' info query to
 *              retrieve the name of a link
 *
 * Return:	Success:        Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__stab_get_name_by_idx_cb(const H5G_entry_t *ent, void *_udata)
{
    H5G_bt_it_gnbi_t *udata = (H5G_bt_it_gnbi_t *)_udata;
    size_t            name_off;            /* Offset of name in heap */
    const char       *name;                /* Pointer to name string in heap */
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(ent);
    assert(udata && udata->heap);

    /* Get name offset in heap */
    name_off = ent->name_off;

    if ((name = (const char *)H5HL_offset_into(udata->heap, name_off)) == NULL)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "unable to get symbol table link name");

    if ((udata->name = H5MM_strdup(name)) == NULL)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "unable to duplicate symbol table link name");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__stab_get_name_by_idx_cb */

/*-------------------------------------------------------------------------
 * Function:	H5G__stab_get_name_by_idx
 *
 * Purpose:     Returns the name of objects in the group by giving index.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__stab_get_name_by_idx(const H5O_loc_t *oloc, H5_iter_order_t order, hsize_t n, char *name,
                          size_t name_size, size_t *name_len)
{
    H5HL_t          *heap = NULL;           /* Pointer to local heap */
    H5O_stab_t       stab;                  /* Info about local heap & B-tree */
    H5G_bt_it_gnbi_t udata;                 /* Iteration information */
    bool             udata_valid = false;   /* Whether iteration information is valid */
    herr_t           ret_value   = SUCCEED; /* Return value */

    /* Portably clear udata struct (before FUNC_ENTER) */
    memset(&udata, 0, sizeof(udata));

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(oloc);

    /* Get the B-tree & local heap info */
    if (NULL == H5O_msg_read(oloc, H5O_STAB_ID, &stab))
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "unable to determine local heap address");

    /* Pin the heap down in memory */
    if (NULL == (heap = H5HL_protect(oloc->file, stab.heap_addr, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_SYM, H5E_PROTECT, FAIL, "unable to protect symbol table heap");

    /* Remap index for decreasing iteration order */
    if (order == H5_ITER_DEC) {
        hsize_t nlinks = 0; /* Number of links in group */

        /* Iterate over the symbol table nodes, to count the links */
        if (H5B_iterate(oloc->file, H5B_SNODE, stab.btree_addr, H5G__node_sumup, &nlinks) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "iteration operator failed");

        /* Map decreasing iteration order index to increasing iteration order index */
        n = nlinks - (n + 1);
    } /* end if */

    /* Set iteration information */
    udata.common.idx      = n;
    udata.common.num_objs = 0;
    udata.common.op       = H5G__stab_get_name_by_idx_cb;
    udata.heap            = heap;
    udata.name            = NULL;
    udata_valid           = true;

    /* Iterate over the group members */
    if (H5B_iterate(oloc->file, H5B_SNODE, stab.btree_addr, H5G__node_by_idx, &udata) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "iteration operator failed");

    /* If we don't know the name now, we almost certainly went out of bounds */
    if (udata.name == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "index out of bound");

    /* Get the length of the name */
    *name_len = strlen(udata.name);

    /* Copy the name into the user's buffer, if given */
    if (name) {
        strncpy(name, udata.name, MIN((*name_len + 1), name_size));
        if (*name_len >= name_size)
            name[name_size - 1] = '\0';
    } /* end if */

done:
    /* Release resources */
    if (heap && H5HL_unprotect(heap) < 0)
        HDONE_ERROR(H5E_SYM, H5E_PROTECT, FAIL, "unable to unprotect symbol table heap");

    /* Free the duplicated name */
    if (udata_valid && udata.name != NULL)
        H5MM_xfree(udata.name);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__stab_get_name_by_idx() */

/*-------------------------------------------------------------------------
 * Function:	H5G__stab_lookup_cb
 *
 * Purpose:     B-tree 'find' callback to retrieve location for an object
 *
 * Return:	Success:        Non-negative
 *
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__stab_lookup_cb(const H5G_entry_t *ent, void *_udata)
{
    H5G_stab_fnd_ud_t *udata     = (H5G_stab_fnd_ud_t *)_udata; /* 'User data' passed in */
    herr_t             ret_value = SUCCEED;                     /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check for setting link info */
    if (udata->lnk)
        /* Convert the entry to a link */
        if (H5G__ent_to_link(udata->lnk, udata->heap, ent, udata->name) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTCONVERT, FAIL, "unable to convert symbol table entry to link");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__stab_lookup_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5G__stab_lookup
 *
 * Purpose:	Look up an object relative to a group, using symbol table
 *
 * Return:	Non-negative (true/false) on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__stab_lookup(const H5O_loc_t *grp_oloc, const char *name, bool *found, H5O_link_t *lnk)
{
    H5HL_t           *heap = NULL;         /* Pointer to local heap */
    H5G_bt_lkp_t      bt_udata;            /* Data to pass through B-tree	*/
    H5G_stab_fnd_ud_t udata;               /* 'User data' to give to callback */
    H5O_stab_t        stab;                /* Symbol table message		*/
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check arguments */
    assert(grp_oloc && grp_oloc->file);
    assert(name && *name);
    assert(found);
    assert(lnk);

    /* Retrieve the symbol table message for the group */
    if (NULL == H5O_msg_read(grp_oloc, H5O_STAB_ID, &stab))
        HGOTO_ERROR(H5E_SYM, H5E_BADMESG, FAIL, "can't read message");

    /* Pin the heap down in memory */
    if (NULL == (heap = H5HL_protect(grp_oloc->file, stab.heap_addr, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_SYM, H5E_PROTECT, FAIL, "unable to protect symbol table heap");

    /* Set up user data to pass to 'find' operation callback */
    udata.name = name;
    udata.lnk  = lnk;
    udata.heap = heap;

    /* Set up the user data for actual B-tree find operation */
    bt_udata.common.name = name;
    bt_udata.common.heap = heap;
    bt_udata.op          = H5G__stab_lookup_cb;
    bt_udata.op_data     = &udata;

    /* Search the B-tree */
    if (H5B_find(grp_oloc->file, H5B_SNODE, stab.btree_addr, found, &bt_udata) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "not found");

done:
    /* Release resources */
    if (heap && H5HL_unprotect(heap) < 0)
        HDONE_ERROR(H5E_SYM, H5E_PROTECT, FAIL, "unable to unprotect symbol table heap");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__stab_lookup() */

/*-------------------------------------------------------------------------
 * Function:	H5G__stab_lookup_by_idx_cb
 *
 * Purpose:     Callback for B-tree iteration 'by index' info query to
 *              retrieve the link
 *
 * Return:	Success:        Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__stab_lookup_by_idx_cb(const H5G_entry_t *ent, void *_udata)
{
    H5G_bt_it_lbi_t *udata = (H5G_bt_it_lbi_t *)_udata;
    const char      *name;                /* Pointer to name string in heap */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(ent);
    assert(udata && udata->heap);

    /* Get a pointer to the link name */
    if ((name = (const char *)H5HL_offset_into(udata->heap, ent->name_off)) == NULL)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "unable to get symbol table link name");

    /* Convert the entry to a link */
    if (H5G__ent_to_link(udata->lnk, udata->heap, ent, name) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTCONVERT, FAIL, "unable to convert symbol table entry to link");
    udata->found = true;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__stab_lookup_by_idx_cb */

/*-------------------------------------------------------------------------
 * Function:	H5G__stab_lookup_by_idx
 *
 * Purpose:	Look up an object in a group, according to the name index
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__stab_lookup_by_idx(const H5O_loc_t *grp_oloc, H5_iter_order_t order, hsize_t n, H5O_link_t *lnk)
{
    H5HL_t         *heap = NULL;         /* Pointer to local heap */
    H5G_bt_it_lbi_t udata;               /* Iteration information */
    H5O_stab_t      stab;                /* Symbol table message */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check arguments */
    assert(grp_oloc && grp_oloc->file);
    assert(lnk);

    /* Get the B-tree & local heap info */
    if (NULL == H5O_msg_read(grp_oloc, H5O_STAB_ID, &stab))
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "unable to determine local heap address");

    /* Pin the heap down in memory */
    if (NULL == (heap = H5HL_protect(grp_oloc->file, stab.heap_addr, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_SYM, H5E_PROTECT, FAIL, "unable to protect symbol table heap");

    /* Remap index for decreasing iteration order */
    if (order == H5_ITER_DEC) {
        hsize_t nlinks = 0; /* Number of links in group */

        /* Iterate over the symbol table nodes, to count the links */
        if (H5B_iterate(grp_oloc->file, H5B_SNODE, stab.btree_addr, H5G__node_sumup, &nlinks) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "iteration operator failed");

        /* Map decreasing iteration order index to increasing iteration order index */
        n = nlinks - (n + 1);
    } /* end if */

    /* Set iteration information */
    udata.common.idx      = n;
    udata.common.num_objs = 0;
    udata.common.op       = H5G__stab_lookup_by_idx_cb;
    udata.heap            = heap;
    udata.lnk             = lnk;
    udata.found           = false;

    /* Iterate over the group members */
    if (H5B_iterate(grp_oloc->file, H5B_SNODE, stab.btree_addr, H5G__node_by_idx, &udata) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "iteration operator failed");

    /* If we didn't find the link, we almost certainly went out of bounds */
    if (!udata.found)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "index out of bound");

done:
    /* Release resources */
    if (heap && H5HL_unprotect(heap) < 0)
        HDONE_ERROR(H5E_SYM, H5E_PROTECT, FAIL, "unable to unprotect symbol table heap");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__stab_lookup_by_idx() */

#ifndef H5_STRICT_FORMAT_CHECKS

/*-------------------------------------------------------------------------
 * Function:	H5G__stab_valid
 *
 * Purpose:	Verify that a group's symbol table message is valid.  If
 *              provided, the addresses in alt_stab will be tried if the
 *              addresses in the group's stab message are invalid, and
 *              the stab message will be updated if necessary.
 *
 *              NOTE: This function is only called when strict format
 *              checks are disabled.  This is so that, when strict
 *              format checks are enabled,  errors in the symbol table
 *              messages are not fixed by this function and are instead
 *              reported by the library.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__stab_valid(H5O_loc_t *grp_oloc, H5O_stab_t *alt_stab)
{
    H5O_stab_t stab;                /* Current symbol table */
    H5HL_t    *heap      = NULL;    /* Pointer to local heap */
    bool       changed   = false;   /* Whether stab has been modified */
    herr_t     ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_TAG(grp_oloc->addr)

    /* Read the symbol table message */
    if (NULL == H5O_msg_read(grp_oloc, H5O_STAB_ID, &stab))
        HGOTO_ERROR(H5E_SYM, H5E_BADMESG, FAIL, "unable to read symbol table message");

    /* Check if the symbol table message's b-tree address is valid */
    if (H5B_valid(grp_oloc->file, H5B_SNODE, stab.btree_addr) < 0) {
        /* Address is invalid, try the b-tree address in the alternate symbol
         * table message */
        if (!alt_stab || H5B_valid(grp_oloc->file, H5B_SNODE, alt_stab->btree_addr) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_NOTFOUND, FAIL, "unable to locate b-tree");
        else {
            /* The alternate symbol table's b-tree address is valid.  Adjust the
             * symbol table message in the group. */
            stab.btree_addr = alt_stab->btree_addr;
            changed         = true;
        } /* end else */
    }     /* end if */

    /* Check if the symbol table message's heap address is valid */
    if (NULL == (heap = H5HL_protect(grp_oloc->file, stab.heap_addr, H5AC__READ_ONLY_FLAG))) {
        /* Address is invalid, try the heap address in the alternate symbol
         * table message */
        if (!alt_stab ||
            NULL == (heap = H5HL_protect(grp_oloc->file, alt_stab->heap_addr, H5AC__READ_ONLY_FLAG)))
            HGOTO_ERROR(H5E_HEAP, H5E_NOTFOUND, FAIL, "unable to locate heap");
        else {
            /* The alternate symbol table's heap address is valid.  Adjust the
             * symbol table message in the group. */
            stab.heap_addr = alt_stab->heap_addr;
            changed        = true;
        } /* end else */
    }     /* end if */

    /* Update the symbol table message and clear errors if necessary */
    if (changed) {
        H5E_clear_stack(NULL);
        if (H5O_msg_write(grp_oloc, H5O_STAB_ID, 0, H5O_UPDATE_TIME | H5O_UPDATE_FORCE, &stab) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to correct symbol table message");
    } /* end if */

done:
    /* Release resources */
    if (heap && H5HL_unprotect(heap) < 0)
        HDONE_ERROR(H5E_SYM, H5E_PROTECT, FAIL, "unable to unprotect symbol table heap");

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5G__stab_valid */
#endif /* H5_STRICT_FORMAT_CHECKS */
