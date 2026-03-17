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

#define H5O_FRIEND      /*suppress error about including H5Opkg	  */
#include "H5SMmodule.h" /* This source code file is part of the H5SM module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Fprivate.h"  /* File access                          */
#include "H5FLprivate.h" /* Free Lists                           */
#include "H5MFprivate.h" /* File memory management		*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5Opkg.h"      /* Object Headers                       */
#include "H5SMpkg.h"     /* Shared object header messages        */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/* Udata struct for calls to H5SM__read_iter_op */
typedef struct H5SM_read_udata_t {
    H5F_t            *file;         /* File in which sharing is happening (in) */
    H5O_msg_crt_idx_t idx;          /* Creation index of this message (in) */
    size_t            buf_size;     /* Size of the encoded message (out) */
    void             *encoding_buf; /* The encoded message (out) */
} H5SM_read_udata_t;

/********************/
/* Local Prototypes */
/********************/
static herr_t  H5SM__create_index(H5F_t *f, H5SM_index_header_t *header);
static herr_t  H5SM__delete_index(H5F_t *f, H5SM_index_header_t *header, bool delete_heap);
static haddr_t H5SM__create_list(H5F_t *f, H5SM_index_header_t *header);
static herr_t  H5SM__find_in_list(const H5SM_list_t *list, const H5SM_mesg_key_t *key, size_t *empty_pos,
                                  size_t *list_pos);
static herr_t  H5SM__convert_list_to_btree(H5F_t *f, H5SM_index_header_t *header, H5SM_list_t **_list,
                                           H5HF_t *fheap, H5O_t *open_oh);
static herr_t  H5SM__bt2_convert_to_list_op(const void *record, void *op_data);
static herr_t  H5SM__convert_btree_to_list(H5F_t *f, H5SM_index_header_t *header);
static herr_t  H5SM__incr_ref(void *record, void *_op_data, bool *changed);
static herr_t  H5SM__write_mesg(H5F_t *f, H5O_t *open_oh, H5SM_index_header_t *header, bool defer,
                                unsigned type_id, void *mesg, unsigned *cache_flags_ptr);
static herr_t  H5SM__decr_ref(void *record, void *op_data, bool *changed);
static herr_t  H5SM__delete_from_index(H5F_t *f, H5O_t *open_oh, H5SM_index_header_t *header,
                                       const H5O_shared_t *mesg, unsigned *cache_flags,
                                       size_t  */*out*/ mesg_size, void  **/*out*/ encoded_mesg);
static herr_t  H5SM__type_to_flag(unsigned type_id, unsigned *type_flag);
static herr_t  H5SM__read_iter_op(H5O_t *oh, H5O_mesg_t *mesg, unsigned sequence, unsigned *oh_modified,
                                  void *_udata);
static herr_t  H5SM__read_mesg_fh_cb(const void *obj, size_t obj_len, void *_udata);
static herr_t  H5SM__read_mesg(H5F_t *f, const H5SM_sohm_t *mesg, H5HF_t *fheap, H5O_t *open_oh,
                               size_t *encoding_size /*out*/, void **encoded_mesg /*out*/);

/*********************/
/* Package Variables */
/*********************/

H5FL_DEFINE(H5SM_master_table_t);
H5FL_ARR_DEFINE(H5SM_index_header_t, H5O_SHMESG_MAX_NINDEXES);
H5FL_DEFINE(H5SM_list_t);
H5FL_ARR_DEFINE(H5SM_sohm_t, H5O_SHMESG_MAX_LIST_SIZE);

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:    H5SM_init
 *
 * Purpose:     Initializes the Shared Message interface.
 *
 *              Creates a master SOHM table in the file and in the cache.
 *              This function should not be called for files that have
 *              SOHMs disabled in the FCPL.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5SM_init(H5F_t *f, H5P_genplist_t *fc_plist, const H5O_loc_t *ext_loc)
{
    H5O_shmesg_table_t   sohm_table;                 /* SOHM message for superblock extension */
    H5SM_master_table_t *table      = NULL;          /* SOHM master table for file */
    H5AC_ring_t          orig_ring  = H5AC_RING_INV; /* Original ring value */
    haddr_t              table_addr = HADDR_UNDEF;   /* Address of SOHM master table in file */
    unsigned             list_max, btree_min;        /* Phase change limits for SOHM indices */
    unsigned             index_type_flags[H5O_SHMESG_MAX_NINDEXES]; /* Messages types stored in each index */
    unsigned minsizes[H5O_SHMESG_MAX_NINDEXES]; /* Message size sharing threshold for each index */
    unsigned type_flags_used;                   /* Message type flags used, for sanity checking */
    unsigned x;                                 /* Local index variable */
    herr_t   ret_value = SUCCEED;               /* Return value */

    FUNC_ENTER_NOAPI_TAG(H5AC__SOHM_TAG, FAIL)

    assert(f);
    /* File should not already have a SOHM table */
    assert(!H5_addr_defined(H5F_SOHM_ADDR(f)));

    /* Set the ring type in the DXPL */
    H5AC_set_ring(H5AC_RING_USER, &orig_ring);

    /* Initialize master table */
    if (NULL == (table = H5FL_CALLOC(H5SM_master_table_t)))
        HGOTO_ERROR(H5E_SOHM, H5E_CANTALLOC, FAIL, "memory allocation failed for SOHM table");
    table->num_indexes = H5F_SOHM_NINDEXES(f);
    table->table_size  = H5SM_TABLE_SIZE(f);

    /* Get information from fcpl */
    if (H5P_get(fc_plist, H5F_CRT_SHMSG_INDEX_TYPES_NAME, &index_type_flags) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_CANTGET, FAIL, "can't get SOHM type flags");
    if (H5P_get(fc_plist, H5F_CRT_SHMSG_LIST_MAX_NAME, &list_max) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_CANTGET, FAIL, "can't get SOHM list maximum");
    if (H5P_get(fc_plist, H5F_CRT_SHMSG_BTREE_MIN_NAME, &btree_min) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_CANTGET, FAIL, "can't get SOHM btree minimum");
    if (H5P_get(fc_plist, H5F_CRT_SHMSG_INDEX_MINSIZE_NAME, &minsizes) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_CANTGET, FAIL, "can't get SOHM message min sizes");

    /* Verify that values are valid */
    if (table->num_indexes > H5O_SHMESG_MAX_NINDEXES)
        HGOTO_ERROR(H5E_SOHM, H5E_BADRANGE, FAIL, "number of indexes in property list is too large");

    /* Check that type flags weren't duplicated anywhere */
    type_flags_used = 0;
    for (x = 0; x < table->num_indexes; ++x) {
        if (index_type_flags[x] & type_flags_used)
            HGOTO_ERROR(H5E_SOHM, H5E_BADVALUE, FAIL,
                        "the same shared message type flag is assigned to more than one index");
        type_flags_used |= index_type_flags[x];
    } /* end for */

    /* Check that number of indexes in table and in superblock make sense.
     * Right now we just use one byte to hold the number of indexes.
     */
    assert(table->num_indexes < 256);

    /* Check that list and btree cutoffs make sense.  There can't be any
     * values greater than the list max but less than the btree min; the
     * list max has to be greater than or equal to one less than the btree
     * min.
     */
    assert(list_max + 1 >= btree_min);
    assert(table->num_indexes > 0 && table->num_indexes <= H5O_SHMESG_MAX_NINDEXES);

    /* Allocate the SOHM indexes as an array. */
    if (NULL == (table->indexes =
                     (H5SM_index_header_t *)H5FL_ARR_MALLOC(H5SM_index_header_t, (size_t)table->num_indexes)))
        HGOTO_ERROR(H5E_SOHM, H5E_NOSPACE, FAIL, "memory allocation failed for SOHM indexes");

    /* Initialize all of the indexes, but don't allocate space for them to
     * hold messages until we actually need to write to them.
     */
    for (x = 0; x < table->num_indexes; x++) {
        table->indexes[x].btree_min     = btree_min;
        table->indexes[x].list_max      = list_max;
        table->indexes[x].mesg_types    = index_type_flags[x];
        table->indexes[x].min_mesg_size = minsizes[x];
        table->indexes[x].index_addr    = HADDR_UNDEF;
        table->indexes[x].heap_addr     = HADDR_UNDEF;
        table->indexes[x].num_messages  = 0;

        /* Indexes start as lists unless the list-to-btree threshold is zero */
        if (table->indexes[x].list_max > 0)
            table->indexes[x].index_type = H5SM_LIST;
        else
            table->indexes[x].index_type = H5SM_BTREE;

        /* Compute the size of a list index for this SOHM index */
        table->indexes[x].list_size = H5SM_LIST_SIZE(f, list_max);
    } /* end for */

    /* Allocate space for the table on disk */
    if (HADDR_UNDEF == (table_addr = H5MF_alloc(f, H5FD_MEM_SOHM_TABLE, (hsize_t)table->table_size)))
        HGOTO_ERROR(H5E_SOHM, H5E_NOSPACE, FAIL, "file allocation failed for SOHM table");

    /* Cache the new table */
    if (H5AC_insert_entry(f, H5AC_SOHM_TABLE, table_addr, table, H5AC__NO_FLAGS_SET) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_CANTINS, FAIL, "can't add SOHM table to cache");

    /* Record the address of the master table in the file */
    H5F_SET_SOHM_ADDR(f, table_addr);

    /* Check for sharing attributes in this file, which means that creation
     *  indices must be tracked on object header message in the file.
     */
    if (type_flags_used & H5O_SHMESG_ATTR_FLAG)
        H5F_SET_STORE_MSG_CRT_IDX(f, true);

    /* Set the ring type to superblock extension */
    H5AC_set_ring(H5AC_RING_SBE, NULL);

    /* Write shared message information to the superblock extension */
    sohm_table.addr     = H5F_SOHM_ADDR(f);
    sohm_table.version  = H5F_SOHM_VERS(f);
    sohm_table.nindexes = H5F_SOHM_NINDEXES(f);
    if (H5O_msg_create(ext_loc, H5O_SHMESG_ID, H5O_MSG_FLAG_CONSTANT | H5O_MSG_FLAG_DONTSHARE,
                       H5O_UPDATE_TIME, &sohm_table) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_CANTINIT, FAIL, "unable to update SOHM header message");

done:
    /* Reset the ring in the API context */
    if (orig_ring != H5AC_RING_INV)
        H5AC_set_ring(orig_ring, NULL);

    if (ret_value < 0) {
        if (table_addr != HADDR_UNDEF)
            H5MF_xfree(f, H5FD_MEM_SOHM_TABLE, table_addr, (hsize_t)table->table_size);
        if (table != NULL)
            table = H5FL_FREE(H5SM_master_table_t, table);
    } /* end if */

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5SM_init() */

/*-------------------------------------------------------------------------
 * Function:    H5SM__type_to_flag
 *
 * Purpose:     Get the shared message flag for a given message type.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__type_to_flag(unsigned type_id, unsigned *type_flag)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Translate the H5O type_id into an H5SM type flag */
    switch (type_id) {
        case H5O_FILL_ID:
            type_id = H5O_FILL_NEW_ID;
            /* FALLTHROUGH */
            H5_ATTR_FALLTHROUGH

        case H5O_SDSPACE_ID:
        case H5O_DTYPE_ID:
        case H5O_FILL_NEW_ID:
        case H5O_PLINE_ID:
        case H5O_ATTR_ID:
            *type_flag = (unsigned)1 << type_id;
            break;

        default:
            HGOTO_ERROR(H5E_SOHM, H5E_BADTYPE, FAIL, "unknown message type ID");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SM__type_to_flag() */

/*-------------------------------------------------------------------------
 * Function:    H5SM__get_index
 *
 * Purpose:     Get the index number for a given message type.
 *
 *              Returns the number of the index in the supplied table
 *              that holds messages of type type_id, or negative if
 *              there is no index for this message type.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
ssize_t
H5SM__get_index(const H5SM_master_table_t *table, unsigned type_id)
{
    size_t   x;
    unsigned type_flag;
    ssize_t  ret_value = FAIL;

    FUNC_ENTER_PACKAGE

    /* Translate the H5O type_id into an H5SM type flag */
    if (H5SM__type_to_flag(type_id, &type_flag) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_CANTGET, FAIL, "can't map message type to flag");

    /* Search the indexes until we find one that matches this flag or we've
     * searched them all.
     */
    for (x = 0; x < table->num_indexes; ++x)
        if (table->indexes[x].mesg_types & type_flag)
            HGOTO_DONE((ssize_t)x);

    /* At this point, ret_value is either the location of the correct
     * index or it's still FAIL because we didn't find an index.
     */
done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SM__get_index() */

/*-------------------------------------------------------------------------
 * Function:    H5SM_type_shared
 *
 * Purpose:     Check if a given message type is shared in a file.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5SM_type_shared(H5F_t *f, unsigned type_id)
{
    H5SM_master_table_t *table = NULL;      /* Shared object master table */
    unsigned             type_flag;         /* Flag corresponding to message type */
    size_t               u;                 /* Local index variable */
    htri_t               ret_value = false; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_TAG(H5AC__SOHM_TAG)

    /* Translate the H5O type_id into an H5SM type flag */
    if (H5SM__type_to_flag(type_id, &type_flag) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_CANTGET, FAIL, "can't map message type to flag");

    /* Look up the master SOHM table */
    if (H5_addr_defined(H5F_SOHM_ADDR(f))) {
        H5SM_table_cache_ud_t cache_udata; /* User-data for callback */

        /* Set up user data for callback */
        cache_udata.f = f;

        if (NULL == (table = (H5SM_master_table_t *)H5AC_protect(f, H5AC_SOHM_TABLE, H5F_SOHM_ADDR(f),
                                                                 &cache_udata, H5AC__READ_ONLY_FLAG)))
            HGOTO_ERROR(H5E_SOHM, H5E_CANTPROTECT, FAIL, "unable to load SOHM master table");
    } /* end if */
    else
        /* No shared messages of any type */
        HGOTO_DONE(false);

    /* Search the indexes until we find one that matches this flag or we've
     * searched them all.
     */
    for (u = 0; u < table->num_indexes; u++)
        if (table->indexes[u].mesg_types & type_flag)
            HGOTO_DONE(true);

done:
    /* Release the master SOHM table */
    if (table && H5AC_unprotect(f, H5AC_SOHM_TABLE, H5F_SOHM_ADDR(f), table, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTUNPROTECT, FAIL, "unable to close SOHM master table");

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5SM_type_shared() */

/*-------------------------------------------------------------------------
 * Function:    H5SM_get_fheap_addr
 *
 * Purpose:     Gets the address of the fractal heap used to store
 *              messages of type type_id.
 *
 * Return:      Non-negative on success/negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5SM_get_fheap_addr(H5F_t *f, unsigned type_id, haddr_t *fheap_addr)
{
    H5SM_master_table_t  *table = NULL;        /* Shared object master table */
    H5SM_table_cache_ud_t cache_udata;         /* User-data for callback */
    ssize_t               index_num;           /* Which index */
    herr_t                ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_TAG(H5AC__SOHM_TAG, FAIL)

    /* Sanity checks */
    assert(f);
    assert(fheap_addr);

    /* Set up user data for callback */
    cache_udata.f = f;

    /* Look up the master SOHM table */
    if (NULL == (table = (H5SM_master_table_t *)H5AC_protect(f, H5AC_SOHM_TABLE, H5F_SOHM_ADDR(f),
                                                             &cache_udata, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_SOHM, H5E_CANTPROTECT, FAIL, "unable to load SOHM master table");

    /* Look up index for message type */
    if ((index_num = H5SM__get_index(table, type_id)) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_CANTPROTECT, FAIL, "unable to find correct SOHM index");

    /* Retrieve heap address for index */
    *fheap_addr = table->indexes[index_num].heap_addr;

done:
    /* Release the master SOHM table */
    if (table && H5AC_unprotect(f, H5AC_SOHM_TABLE, H5F_SOHM_ADDR(f), table, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTUNPROTECT, FAIL, "unable to close SOHM master table");

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5SM_get_fheap_addr() */

/*-------------------------------------------------------------------------
 * Function:    H5SM__create_index
 *
 * Purpose:     Allocates storage for an index, populating the HEADER struct.
 *
 * Return:      Non-negative on success/negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__create_index(H5F_t *f, H5SM_index_header_t *header)
{
    H5HF_create_t fheap_cparam;     /* Fractal heap creation parameters */
    H5HF_t       *fheap     = NULL; /* Fractal heap handle */
    H5B2_t       *bt2       = NULL; /* v2 B-tree handle for index */
    herr_t        ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(header);
    assert(header->index_addr == HADDR_UNDEF);
    assert(header->btree_min <= header->list_max + 1);

    /* In most cases, the index starts as a list */
    if (header->list_max > 0) {
        haddr_t list_addr = HADDR_UNDEF; /* Address of SOHM list */

        /* Create the list index */
        if (HADDR_UNDEF == (list_addr = H5SM__create_list(f, header)))
            HGOTO_ERROR(H5E_SOHM, H5E_CANTCREATE, FAIL, "list creation failed for SOHM index");

        /* Set the index type & address */
        header->index_type = H5SM_LIST;
        header->index_addr = list_addr;
    } /* end if */
    /* index is a B-tree */
    else {
        H5B2_create_t bt2_cparam;              /* v2 B-tree creation parameters */
        haddr_t       tree_addr = HADDR_UNDEF; /* Address of SOHM B-tree */

        /* Create the v2 B-tree index */
        bt2_cparam.cls           = H5SM_INDEX;
        bt2_cparam.node_size     = (uint32_t)H5SM_B2_NODE_SIZE;
        bt2_cparam.rrec_size     = (uint32_t)H5SM_SOHM_ENTRY_SIZE(f);
        bt2_cparam.split_percent = H5SM_B2_SPLIT_PERCENT;
        bt2_cparam.merge_percent = H5SM_B2_MERGE_PERCENT;
        if (NULL == (bt2 = H5B2_create(f, &bt2_cparam, f)))
            HGOTO_ERROR(H5E_SOHM, H5E_CANTCREATE, FAIL, "B-tree creation failed for SOHM index");

        /* Retrieve the v2 B-tree's address in the file */
        if (H5B2_get_addr(bt2, &tree_addr) < 0)
            HGOTO_ERROR(H5E_SOHM, H5E_CANTGET, FAIL, "can't get v2 B-tree address for SOHM index");

        /* Set the index type & address */
        header->index_type = H5SM_BTREE;
        header->index_addr = tree_addr;
    } /* end else */

    /* Create a heap to hold the shared messages that the list or B-tree will index */
    memset(&fheap_cparam, 0, sizeof(fheap_cparam));
    fheap_cparam.managed.width            = H5O_FHEAP_MAN_WIDTH;
    fheap_cparam.managed.start_block_size = H5O_FHEAP_MAN_START_BLOCK_SIZE;
    fheap_cparam.managed.max_direct_size  = H5O_FHEAP_MAN_MAX_DIRECT_SIZE;
    fheap_cparam.managed.max_index        = H5O_FHEAP_MAN_MAX_INDEX;
    fheap_cparam.managed.start_root_rows  = H5O_FHEAP_MAN_START_ROOT_ROWS;
    fheap_cparam.checksum_dblocks         = H5O_FHEAP_CHECKSUM_DBLOCKS;
    fheap_cparam.id_len                   = 0;
    fheap_cparam.max_man_size             = H5O_FHEAP_MAX_MAN_SIZE;
    if (NULL == (fheap = H5HF_create(f, &fheap_cparam)))
        HGOTO_ERROR(H5E_SOHM, H5E_CANTINIT, FAIL, "unable to create fractal heap");

    if (H5HF_get_heap_addr(fheap, &(header->heap_addr)) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_CANTGETSIZE, FAIL, "can't get fractal heap address");

#ifndef NDEBUG
    {
        size_t fheap_id_len; /* Size of a fractal heap ID */

        /* Sanity check ID length */
        if (H5HF_get_id_len(fheap, &fheap_id_len) < 0)
            HGOTO_ERROR(H5E_SOHM, H5E_CANTGETSIZE, FAIL, "can't get fractal heap ID length");
        assert(fheap_id_len == H5O_FHEAP_ID_LEN);
    }
#endif /* NDEBUG */

done:
    /* Release resources */
    if (fheap && H5HF_close(fheap) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTCLOSEOBJ, FAIL, "can't close fractal heap");
    if (bt2 && H5B2_close(bt2) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTCLOSEOBJ, FAIL, "can't close v2 B-tree for SOHM index");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SM__create_index */

/*-------------------------------------------------------------------------
 * Function:    H5SM__delete_index
 *
 * Purpose:     De-allocates storage for an index whose header is HEADER.
 *
 *              If DELETE_HEAP is true, deletes the index's heap, eliminating
 *              it completely.
 *
 *              If DELETE_HEAP is false, the heap is not deleted.  This is
 *              useful when deleting only the index header as the index is
 *              converted from a list to a B-tree and back again.
 *
 * Return:      Non-negative on success/negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__delete_index(H5F_t *f, H5SM_index_header_t *header, bool delete_heap)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Determine whether index is a list or a B-tree. */
    if (header->index_type == H5SM_LIST) {
        unsigned index_status = 0; /* Index list's status in the metadata cache */

        /* Check the index list's status in the metadata cache */
        if (H5AC_get_entry_status(f, header->index_addr, &index_status) < 0)
            HGOTO_ERROR(H5E_SOHM, H5E_CANTGET, FAIL,
                        "unable to check metadata cache status for direct block");

        /* If the index list is in the cache, expunge it now */
        if (index_status & H5AC_ES__IN_CACHE) {
            /* Sanity checks on index list */
            assert(!(index_status & H5AC_ES__IS_PINNED));
            assert(!(index_status & H5AC_ES__IS_PROTECTED));

            /* Evict the index list from the metadata cache */
            if (H5AC_expunge_entry(f, H5AC_SOHM_LIST, header->index_addr, H5AC__FREE_FILE_SPACE_FLAG) < 0)
                HGOTO_ERROR(H5E_SOHM, H5E_CANTREMOVE, FAIL, "unable to remove list index from cache");
        } /* end if */
    }     /* end if */
    else {
        assert(header->index_type == H5SM_BTREE);

        /* Delete the B-tree. */
        if (H5B2_delete(f, header->index_addr, f, NULL, NULL) < 0)
            HGOTO_ERROR(H5E_SOHM, H5E_CANTDELETE, FAIL, "unable to delete B-tree");

        /* Revert to list unless B-trees can have zero records */
        if (header->btree_min > 0)
            header->index_type = H5SM_LIST;
    } /* end else */

    /* Free the index's heap if requested. */
    if (delete_heap == true) {
        if (H5HF_delete(f, header->heap_addr) < 0)
            HGOTO_ERROR(H5E_SOHM, H5E_CANTDELETE, FAIL, "unable to delete fractal heap");
        header->heap_addr = HADDR_UNDEF;
    } /* end if */

    /* Reset index info */
    header->index_addr   = HADDR_UNDEF;
    header->num_messages = 0;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SM__delete_index */

/*-------------------------------------------------------------------------
 * Function:    H5SM__create_list
 *
 * Purpose:     Creates a list of SOHM messages.
 *
 *              Called when a new index is created from scratch or when a
 *              B-tree needs to be converted back into a list.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5SM__create_list(H5F_t *f, H5SM_index_header_t *header)
{
    H5SM_list_t *list = NULL;             /* List of messages */
    hsize_t      x;                       /* Counter variable */
    size_t       num_entries;             /* Number of messages to create in list */
    haddr_t      addr      = HADDR_UNDEF; /* Address of the list on disk */
    haddr_t      ret_value = HADDR_UNDEF; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(header);

    num_entries = header->list_max;

    /* Allocate list in memory */
    if (NULL == (list = H5FL_CALLOC(H5SM_list_t)))
        HGOTO_ERROR(H5E_SOHM, H5E_NOSPACE, HADDR_UNDEF, "file allocation failed for SOHM list");
    if (NULL == (list->messages = (H5SM_sohm_t *)H5FL_ARR_CALLOC(H5SM_sohm_t, num_entries)))
        HGOTO_ERROR(H5E_SOHM, H5E_NOSPACE, HADDR_UNDEF, "file allocation failed for SOHM list");

    /* Initialize messages in list */
    for (x = 0; x < num_entries; x++)
        list->messages[x].location = H5SM_NO_LOC;

    /* Point list at header passed in */
    list->header = header;

    /* Allocate space for the list on disk */
    if (HADDR_UNDEF == (addr = H5MF_alloc(f, H5FD_MEM_SOHM_INDEX, (hsize_t)header->list_size)))
        HGOTO_ERROR(H5E_SOHM, H5E_NOSPACE, HADDR_UNDEF, "file allocation failed for SOHM list");

    /* Put the list into the cache */
    if (H5AC_insert_entry(f, H5AC_SOHM_LIST, addr, list, H5AC__NO_FLAGS_SET) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_CANTINS, HADDR_UNDEF, "can't add SOHM list to cache");

    /* Set return value */
    ret_value = addr;

done:
    if (ret_value == HADDR_UNDEF) {
        if (list != NULL) {
            if (list->messages != NULL)
                list->messages = H5FL_ARR_FREE(H5SM_sohm_t, list->messages);
            list = H5FL_FREE(H5SM_list_t, list);
        } /* end if */
        if (addr != HADDR_UNDEF)
            H5MF_xfree(f, H5FD_MEM_SOHM_INDEX, addr, (hsize_t)header->list_size);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SM__create_list */

/*-------------------------------------------------------------------------
 * Function:    H5SM__convert_list_to_btree
 *
 * Purpose:     Given a list index, turns it into a B-tree index.  This is
 *              done when too many messages are added to the list.
 *
 *              Requires that *_LIST be a valid list and currently protected
 *              in the cache.  Unprotects (and expunges) *_LIST from the cache.
 *
 *              _LIST needs to be a double pointer so that the calling function
 *              knows if it is released from the cache if this function exits
 *              in error.  Trying to free it again will trigger an assert.
 *
 * Return:      Non-negative on success
 *              Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__convert_list_to_btree(H5F_t *f, H5SM_index_header_t *header, H5SM_list_t **_list, H5HF_t *fheap,
                            H5O_t *open_oh)
{
    H5SM_list_t    *list;         /* Pointer to the existing message list */
    H5SM_mesg_key_t key;          /* Key for inserting records in v2 B-tree */
    H5B2_create_t   bt2_cparam;   /* v2 B-tree creation parameters */
    H5B2_t         *bt2 = NULL;   /* v2 B-tree handle for index */
    haddr_t         tree_addr;    /* New v2 B-tree's address */
    size_t          num_messages; /* Number of messages being tracked */
    size_t          x;
    void           *encoding_buf = NULL;
    herr_t          ret_value    = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(_list && *_list);
    assert(header);

    /* Get pointer to list of messages to convert */
    list = *_list;

    /* Create the new v2 B-tree for tracking the messages */
    bt2_cparam.cls           = H5SM_INDEX;
    bt2_cparam.node_size     = (uint32_t)H5SM_B2_NODE_SIZE;
    bt2_cparam.rrec_size     = (uint32_t)H5SM_SOHM_ENTRY_SIZE(f);
    bt2_cparam.split_percent = H5SM_B2_SPLIT_PERCENT;
    bt2_cparam.merge_percent = H5SM_B2_MERGE_PERCENT;
    if (NULL == (bt2 = H5B2_create(f, &bt2_cparam, f)))
        HGOTO_ERROR(H5E_SOHM, H5E_CANTCREATE, FAIL, "B-tree creation failed for SOHM index");

    /* Retrieve the v2 B-tree's address in the file */
    if (H5B2_get_addr(bt2, &tree_addr) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_CANTGET, FAIL, "can't get v2 B-tree address for SOHM index");

    /* Set up key values that all messages will use.  Since these messages
     * are in the heap, they have a heap ID and no encoding or type_id.
     */
    key.file          = f;
    key.fheap         = fheap;
    key.encoding_size = 0;
    key.encoding      = NULL;

    /* Insert each record into the new B-tree */
    for (x = 0; x < header->list_max; x++) {
        if (list->messages[x].location != H5SM_NO_LOC) {
            /* Copy message into key */
            key.message = list->messages[x];

            /* Get the encoded message */
            if (H5SM__read_mesg(f, &(key.message), fheap, open_oh, &key.encoding_size, &encoding_buf) < 0)
                HGOTO_ERROR(H5E_SOHM, H5E_CANTLOAD, FAIL, "Couldn't read SOHM message in list");

            key.encoding = encoding_buf;

            /* Insert the message into the B-tree */
            if (H5B2_insert(bt2, &key) < 0)
                HGOTO_ERROR(H5E_SOHM, H5E_CANTINSERT, FAIL, "couldn't add SOHM to B-tree");

            /* Free buffer from H5SM__read_mesg */
            if (encoding_buf)
                encoding_buf = H5MM_xfree(encoding_buf);
        } /* end if */
    }     /* end for */

    /* Unprotect list in cache and release heap */
    if (H5AC_unprotect(f, H5AC_SOHM_LIST, header->index_addr, list,
                       H5AC__DELETED_FLAG | H5AC__FREE_FILE_SPACE_FLAG) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_CANTUNPROTECT, FAIL, "unable to release SOHM list");
    *_list = list = NULL;

    /* Delete the old list index (but not its heap, which the new index is
     * still using!)
     */
    num_messages = header->num_messages; /* preserve this across the index deletion */
    if (H5SM__delete_index(f, header, false) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_CANTDELETE, FAIL, "can't free list index");

    /* Set/restore header info */
    header->index_addr   = tree_addr;
    header->index_type   = H5SM_BTREE;
    header->num_messages = num_messages;

done:
    /* Release resources */
    if (bt2 && H5B2_close(bt2) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTCLOSEOBJ, FAIL, "can't close v2 B-tree for SOHM index");
    if (encoding_buf)
        encoding_buf = H5MM_xfree(encoding_buf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5SM__convert_list_to_btree() */

/*-------------------------------------------------------------------------
 * Function:	H5SM__bt2_convert_to_list_op
 *
 * Purpose:	An H5B2_remove_t callback function to convert a SOHM
 *              B-tree index to a list.
 *
 *              Inserts this record into the list passed through op_data.
 *
 * Return:	Non-negative on success
 *              Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__bt2_convert_to_list_op(const void *record, void *op_data)
{
    const H5SM_sohm_t *message = (const H5SM_sohm_t *)record;
    const H5SM_list_t *list    = (const H5SM_list_t *)op_data;
    size_t             mesg_idx; /* Index of message to modify */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(record);
    assert(op_data);

    /* Get the message index, and increment the # of messages in list */
    mesg_idx = list->header->num_messages++;
    assert(list->header->num_messages <= list->header->list_max);

    /* Insert this message at the end of the list */
    assert(list->messages[mesg_idx].location == H5SM_NO_LOC);
    assert(message->location != H5SM_NO_LOC);
    H5MM_memcpy(&(list->messages[mesg_idx]), message, sizeof(H5SM_sohm_t));

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5SM__bt2_convert_to_list_op() */

/*-------------------------------------------------------------------------
 * Function:    H5SM__convert_btree_to_list
 *
 * Purpose:     Given a B-tree index, turns it into a list index.  This is
 *              done when too many messages are deleted from the B-tree.
 *
 * Return:      Non-negative on success
 *              Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__convert_btree_to_list(H5F_t *f, H5SM_index_header_t *header)
{
    H5SM_list_t         *list = NULL;
    H5SM_list_cache_ud_t cache_udata; /* User-data for metadata cache callback */
    haddr_t              btree_addr;
    herr_t               ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Remember the address of the old B-tree, but change the header over to be
     * a list..
     */
    btree_addr = header->index_addr;

    header->num_messages = 0;
    header->index_type   = H5SM_LIST;

    /* Create a new list index */
    if (HADDR_UNDEF == (header->index_addr = H5SM__create_list(f, header)))
        HGOTO_ERROR(H5E_SOHM, H5E_CANTINIT, FAIL, "unable to create shared message list");

    /* Set up user data for metadata cache callback */
    cache_udata.f      = f;
    cache_udata.header = header;

    /* Protect the SOHM list */
    if (NULL == (list = (H5SM_list_t *)H5AC_protect(f, H5AC_SOHM_LIST, header->index_addr, &cache_udata,
                                                    H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_SOHM, H5E_CANTPROTECT, FAIL, "unable to load SOHM list index");

    /* Delete the B-tree and have messages copy themselves to the
     * list as they're deleted
     */
    if (H5B2_delete(f, btree_addr, f, H5SM__bt2_convert_to_list_op, list) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_CANTDELETE, FAIL, "unable to delete B-tree");

done:
    /* Release the SOHM list from the cache */
    if (list && H5AC_unprotect(f, H5AC_SOHM_LIST, header->index_addr, list, H5AC__DIRTIED_FLAG) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTUNPROTECT, FAIL, "unable to unprotect SOHM index");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SM__convert_btree_to_list() */

/*-------------------------------------------------------------------------
 * Function:    H5SM__can_share_common
 *
 * Purpose:     "trivial" checks for determining if a message can be shared.
 *
 * Note:	These checks are common to the "can share" and "try share"
 *		routines and are the "fast" checks before we need to protect
 *		the SOHM master table.
 *
 * Return:      true if message could be a SOHM
 *              false if this message couldn't be a SOHM
 *              Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5SM__can_share_common(const H5F_t *f, unsigned type_id, const void *mesg)
{
    htri_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check whether this message ought to be shared or not */
    /* If sharing is disabled in this file, don't share the message */
    if (!H5_addr_defined(H5F_SOHM_ADDR(f)))
        HGOTO_DONE(false);

    /* Type-specific check */
    if ((ret_value = H5O_msg_can_share(type_id, mesg)) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_BADTYPE, FAIL, "can_share callback returned error");
    if (ret_value == false)
        HGOTO_DONE(false);

    /* At this point, the message passes the "trivial" checks and is worth
     *  further checks.
     */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SM__can_share_common() */

/*-------------------------------------------------------------------------
 * Function:    H5SM_can_share
 *
 * Purpose:     Checks if an object header message would be shared or is
 *		already shared.
 *
 *              If not, returns false and does nothing.
 *
 * Return:      true if message will be a SOHM
 *              false if this message won't be a SOHM
 *              Negative on failure
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5SM_can_share(H5F_t *f, H5SM_master_table_t *table, ssize_t *sohm_index_num, unsigned type_id,
               const void *mesg)
{
    size_t               mesg_size;
    H5SM_master_table_t *my_table = NULL;
    ssize_t              index_num;
    htri_t               tri_ret;
    htri_t               ret_value = true;

    FUNC_ENTER_NOAPI_TAG(H5AC__SOHM_TAG, FAIL)

    /* "trivial" sharing checks */
    if ((tri_ret = H5SM__can_share_common(f, type_id, mesg)) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_BADTYPE, FAIL, "'trivial' sharing checks returned error");
    if (tri_ret == false)
        HGOTO_DONE(false);

    /* Look up the master SOHM table */
    /* (use incoming master SOHM table if possible) */
    if (table)
        my_table = table;
    else {
        H5SM_table_cache_ud_t cache_udata; /* User-data for callback */

        /* Set up user data for callback */
        cache_udata.f = f;

        if (NULL == (my_table = (H5SM_master_table_t *)H5AC_protect(f, H5AC_SOHM_TABLE, H5F_SOHM_ADDR(f),
                                                                    &cache_udata, H5AC__READ_ONLY_FLAG)))
            HGOTO_ERROR(H5E_SOHM, H5E_CANTPROTECT, FAIL, "unable to load SOHM master table");
    } /* end if */

    /* Find the right index for this message type.  If there is no such index
     * then this type of message isn't shareable
     */
    if ((index_num = H5SM__get_index(my_table, type_id)) < 0) {
        H5E_clear_stack(NULL); /*ignore error*/
        HGOTO_DONE(false);
    } /* end if */

    /* If the message isn't big enough, don't bother sharing it */
    if (0 == (mesg_size = H5O_msg_raw_size(f, type_id, true, mesg)))
        HGOTO_ERROR(H5E_SOHM, H5E_BADMESG, FAIL, "unable to get OH message size");
    if (mesg_size < my_table->indexes[index_num].min_mesg_size)
        HGOTO_DONE(false);

    /* At this point, the message will be shared, set the index number if requested. */
    if (sohm_index_num)
        *sohm_index_num = index_num;

done:
    /* Release the master SOHM table, if we protected it */
    if (my_table && my_table != table &&
        H5AC_unprotect(f, H5AC_SOHM_TABLE, H5F_SOHM_ADDR(f), my_table, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTUNPROTECT, FAIL, "unable to close SOHM master table");

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5SM_can_share() */

/*-------------------------------------------------------------------------
 * Function:    H5SM_try_share
 *
 * Purpose:     Attempts to share an object header message.
 *
 *              MESG_LOC is an H5O_mesg_loc_t struct that gives the message's
 *              location in an object header (address and index).  This
 *              function sets the type_id in MESG_LOC.
 *              If MESG_LOC is not NULL, this message will be "unique but
 *              shareable" and will be entered in the index but not actually
 *              shared.  If it is NULL, this message will be fully shared if
 *              it is shareable at all.
 *
 *              OPEN_OH is the object header that is currently open and
 *              protected.  If NULL, the SM module will protect any object
 *              header it needs (which can cause an error if that OH is
 *              already protected!).
 *
 *              DEFER_FLAGS indicates whether the sharing operation should
 *              actually occur, or whether this is just a set up call for a
 *              future sharing operation.  In the latter case this argument
 *              should be H5SM_DEFER.  If the message was previously deferred
 *              this argument should be H5SM_WAS_DEFERRED.
 *
 *              MESG_FLAGS will have the H5O_MSG_FLAG_SHAREABLE or
 *              H5O_MSG_FLAG_SHARED flag set if one is appropriate.  This is
 *              the only way to tell the difference between a message that
 *              has just been fully shared and a message that is only
 *              "shareable" and is still in the object header, so it cannot
 *              be NULL if MESG_LOC is not NULL.  If MESG_LOC is NULL, then
 *              the message won't be "unique but shareable" and MESG_FLAGS
 *              can be NULL as well.
 *
 *              If the message should be shared (if sharing has been
 *              enabled and this message qualifies), this function turns the
 *              message into a shared message, sets the H5O_MSG_FLAG_SHARED
 *              flag in mesg_flags, and returns true.
 *
 *              If the message isn't shared, returns false.  If the message
 *              isn't shared but was entered in the shared message index,
 *              the H5O_MSG_FLAG_SHAREABLE flag will be set in mesg_flags
 *              and returns true.
 *
 *              If this message was already shared, increments its reference
 *              count, and leaves it otherwise unchanged, returning true and
 *              setting the H5O_MSG_FLAG_SHARED flag in mesg_flags.
 *
 *              If mesg_flags is NULL, the calling function should just look
 *              at the return value to determine if the message was shared
 *              or not.  Such messages should never be "unique but shareable."
 *
 * Return:      true if message is now a SOHM
 *              false if this message is not a SOHM
 *              Negative on failure
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5SM_try_share(H5F_t *f, H5O_t *open_oh, unsigned defer_flags, unsigned type_id, void *mesg,
               unsigned *mesg_flags)
{
    H5SM_master_table_t  *table = NULL;
    H5SM_table_cache_ud_t cache_udata; /* User-data for callback */
    unsigned              cache_flags = H5AC__NO_FLAGS_SET;
    ssize_t               index_num;
    htri_t                tri_ret;
#ifndef NDEBUG
    unsigned deferred_type = UINT_MAX;
#endif
    htri_t ret_value = true;

    FUNC_ENTER_NOAPI_TAG(H5AC__SOHM_TAG, FAIL)

    /* If we previously deferred this operation, the saved message type should
     * be the same as the one we get here.  In debug mode, we make sure this
     * holds true; otherwise we can leave now if it wasn't shared in the DEFER
     * pass. */
    if (defer_flags & H5SM_WAS_DEFERRED)
#ifndef NDEBUG
        deferred_type = ((H5O_shared_t *)mesg)->type;
#else  /* NDEBUG */
        if ((((H5O_shared_t *)mesg)->type != H5O_SHARE_TYPE_HERE) &&
            (((H5O_shared_t *)mesg)->type != H5O_SHARE_TYPE_SOHM))
            HGOTO_DONE(false);
#endif /* NDEBUG */

    /* "trivial" sharing checks */
    if (mesg_flags && (*mesg_flags & H5O_MSG_FLAG_DONTSHARE))
        HGOTO_DONE(false);
    if ((tri_ret = H5SM__can_share_common(f, type_id, mesg)) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_BADTYPE, FAIL, "'trivial' sharing checks returned error");
    if (tri_ret == false)
        HGOTO_DONE(false);

    /* Set up user data for callback */
    cache_udata.f = f;

    /* Look up the master SOHM table */
    if (NULL == (table = (H5SM_master_table_t *)H5AC_protect(f, H5AC_SOHM_TABLE, H5F_SOHM_ADDR(f),
                                                             &cache_udata, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_SOHM, H5E_CANTPROTECT, FAIL, "unable to load SOHM master table");

    /* "complex" sharing checks */
    if ((tri_ret = H5SM_can_share(f, table, &index_num, type_id, mesg)) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_BADTYPE, FAIL, "'complex' sharing checks returned error");
    if (tri_ret == false)
        HGOTO_DONE(false);

    /* At this point, the message will be shared. */

    /* If the index hasn't been allocated yet, create it */
    if (table->indexes[index_num].index_addr == HADDR_UNDEF) {
        if (H5SM__create_index(f, &(table->indexes[index_num])) < 0)
            HGOTO_ERROR(H5E_SOHM, H5E_CANTINIT, FAIL, "unable to create SOHM index");
        cache_flags |= H5AC__DIRTIED_FLAG;
    } /* end if */

    /* Write the message as a shared message.  This may or may not cause the
     * message to become shared (if it is unique, it will not be shared).
     */
    if (H5SM__write_mesg(f, open_oh, &(table->indexes[index_num]), (defer_flags & H5SM_DEFER) != 0, type_id,
                         mesg, &cache_flags) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_CANTINSERT, FAIL, "can't write shared message");

    /* Set flags if this message was "written" without error and wasn't a
     * 'defer' attempt; it is now either fully shared or "shareable".
     */
    if (mesg_flags) {
        if (((H5O_shared_t *)mesg)->type == H5O_SHARE_TYPE_HERE)
            *mesg_flags |= H5O_MSG_FLAG_SHAREABLE;
        else {
            assert(((H5O_shared_t *)mesg)->type == H5O_SHARE_TYPE_SOHM);
            *mesg_flags |= H5O_MSG_FLAG_SHARED;
        } /* end else */
    }     /* end if */

done:
    assert((ret_value != true) || ((H5O_shared_t *)mesg)->type == H5O_SHARE_TYPE_HERE ||
           ((H5O_shared_t *)mesg)->type == H5O_SHARE_TYPE_SOHM);
#ifndef NDEBUG
    /* If we previously deferred this operation, make sure the saved message
     * type is the same as the one we get here. */
    if (defer_flags & H5SM_WAS_DEFERRED)
        assert(deferred_type == ((H5O_shared_t *)mesg)->type);
#endif /* NDEBUG */

    /* Release the master SOHM table */
    if (table && H5AC_unprotect(f, H5AC_SOHM_TABLE, H5F_SOHM_ADDR(f), table, cache_flags) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTUNPROTECT, FAIL, "unable to close SOHM master table");

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5SM_try_share() */

/*-------------------------------------------------------------------------
 * Function:	H5SM__incr_ref
 *
 * Purpose:	Increment the reference count for a SOHM message and return
 *              the message's heap ID.
 *
 *              The message pointer is actually returned via op_data, which
 *              should be a pointer to a H5SM_fheap_id_t.
 *
 * Return:	Non-negative on success
 *              Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__incr_ref(void *record, void *_op_data, bool *changed)
{
    H5SM_sohm_t          *message   = (H5SM_sohm_t *)record;
    H5SM_incr_ref_opdata *op_data   = (H5SM_incr_ref_opdata *)_op_data;
    herr_t                ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(record);
    assert(op_data);
    assert(changed);

    /* If the message was previously shared in an object header, share
     * it in the heap now.
     */
    if (message->location == H5SM_IN_OH) {
        assert(op_data->key && op_data->key->fheap);

        /* Put the message in the heap and record its new heap ID */
        if (H5HF_insert(op_data->key->fheap, op_data->key->encoding_size, op_data->key->encoding,
                        &message->u.heap_loc.fheap_id) < 0)
            HGOTO_ERROR(H5E_SOHM, H5E_CANTINSERT, FAIL, "unable to insert message into fractal heap");

        message->location             = H5SM_IN_HEAP;
        message->u.heap_loc.ref_count = 2;
    } /* end if */
    else {
        assert(message->location == H5SM_IN_HEAP);
        /* If it's already in the heap, just increment the ref count */
        ++message->u.heap_loc.ref_count;
    } /* end else */

    /* If we got here, the message has changed */
    *changed = true;

    /* Check for retrieving the heap ID */
    if (op_data)
        op_data->fheap_id = message->u.heap_loc.fheap_id;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SM__incr_ref() */

/*-------------------------------------------------------------------------
 * Function:    H5SM__write_mesg
 *
 * Purpose:     This routine adds a shareable message to an index.
 *              The behavior is controlled by the DEFER parameter:
 *
 *              If DEFER is true, this routine Simulates adding a shareable
 *              message to an index.  It determines what the outcome would
 *              be with DEFER set the false and updates the shared message
 *              info, but does not actually add the message to a heap, list,
 *              or b-tree.  Assumes that an open object header will be
 *              available when H5SM__write_mesg is called with DEFER set to
 *              false.
 *
 *              If DEFER is false, this routine adds a shareable message to
 *              an index.  If this is the first such message and we have an
 *              object header location for this message, we record it in the
 *              index but don't modify the message passed in.  If the message
 *              is already in the index or we don't have an object header
 *              location for it, it is shared in the heap and this function
 *              sets its sharing struct to reflect this.
 *
 *              The index could be a list or a B-tree.
 *
 * Return:      Non-negative on success
 *              Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__write_mesg(H5F_t *f, H5O_t *open_oh, H5SM_index_header_t *header, bool defer, unsigned type_id,
                 void *mesg, unsigned *cache_flags_ptr)
{
    H5SM_list_t         *list = NULL;             /* List index */
    H5SM_mesg_key_t      key;                     /* Key used to search the index */
    H5SM_list_cache_ud_t cache_udata;             /* User-data for metadata cache callback */
    H5O_shared_t         shared;                  /* Shared H5O message */
    bool                 found = false;           /* Was the message in the index? */
    H5HF_t              *fheap = NULL;            /* Fractal heap handle */
    H5B2_t              *bt2   = NULL;            /* v2 B-tree handle for index */
    size_t               buf_size;                /* Size of the encoded message */
    void                *encoding_buf = NULL;     /* Buffer for encoded message */
    size_t               empty_pos    = SIZE_MAX; /* Empty entry in list */
    herr_t               ret_value    = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(header);
    assert(header->index_type != H5SM_BADTYPE);
    assert(cache_flags_ptr);

    /* Encode the message to be written */
    if ((buf_size = H5O_msg_raw_size(f, type_id, true, mesg)) == 0)
        HGOTO_ERROR(H5E_SOHM, H5E_BADSIZE, FAIL, "can't find message size");
    if (NULL == (encoding_buf = H5MM_malloc(buf_size)))
        HGOTO_ERROR(H5E_SOHM, H5E_NOSPACE, FAIL, "can't allocate buffer for encoding");
    if (H5O_msg_encode(f, type_id, true, (unsigned char *)encoding_buf, mesg) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_CANTENCODE, FAIL, "can't encode message to be shared");

    /* Open the fractal heap for this index */
    if (NULL == (fheap = H5HF_open(f, header->heap_addr)))
        HGOTO_ERROR(H5E_SOHM, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");

    /* Set up a key for the message to be written */
    key.file             = f;
    key.fheap            = fheap;
    key.encoding         = encoding_buf;
    key.encoding_size    = buf_size;
    key.message.hash     = H5_checksum_lookup3(encoding_buf, buf_size, type_id);
    key.message.location = H5SM_NO_LOC;

    /* Assume the message is already in the index and try to increment its
     * reference count.  If this fails, the message isn't in the index after
     * all and we'll need to add it.
     */
    if (header->index_type == H5SM_LIST) {
        size_t list_pos; /* Position in a list index */

        /* Set up user data for metadata cache callback */
        cache_udata.f      = f;
        cache_udata.header = header;

        /* The index is a list; get it from the cache */
        if (NULL == (list = (H5SM_list_t *)H5AC_protect(f, H5AC_SOHM_LIST, header->index_addr, &cache_udata,
                                                        defer ? H5AC__READ_ONLY_FLAG : H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_SOHM, H5E_CANTPROTECT, FAIL, "unable to load SOHM index");

        /* See if the message is already in the index and get its location.
         * Also record the first empty list position we find in case we need it
         * later.
         */
        if (H5SM__find_in_list(list, &key, &empty_pos, &list_pos) < 0)
            HGOTO_ERROR(H5E_SOHM, H5E_CANTINSERT, FAIL, "unable to search for message in list");

        if (defer) {
            if (list_pos != SIZE_MAX)
                found = true;
        } /* end if */
        else {
            if (list_pos != SIZE_MAX) {
                /* If the message was previously shared in an object header, share
                 * it in the heap now.
                 */
                if (list->messages[list_pos].location == H5SM_IN_OH) {
                    /* Put the message in the heap and record its new heap ID */
                    if (H5HF_insert(fheap, key.encoding_size, key.encoding, &shared.u.heap_id) < 0)
                        HGOTO_ERROR(H5E_SOHM, H5E_CANTINSERT, FAIL,
                                    "unable to insert message into fractal heap");

                    list->messages[list_pos].location             = H5SM_IN_HEAP;
                    list->messages[list_pos].u.heap_loc.fheap_id  = shared.u.heap_id;
                    list->messages[list_pos].u.heap_loc.ref_count = 2;
                } /* end if */
                else {
                    /* If the message was already in the heap, increase its ref count */
                    assert(list->messages[list_pos].location == H5SM_IN_HEAP);
                    ++(list->messages[list_pos].u.heap_loc.ref_count);
                } /* end else */

                /* Set up the shared location to point to the shared location */
                shared.u.heap_id = list->messages[list_pos].u.heap_loc.fheap_id;
                found            = true;
            } /* end if */
        }     /* end else */
    }         /* end if */
    /* Index is a B-tree */
    else {
        assert(header->index_type == H5SM_BTREE);

        /* Open the index v2 B-tree */
        if (NULL == (bt2 = H5B2_open(f, header->index_addr, f)))
            HGOTO_ERROR(H5E_SOHM, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for SOHM index");

        if (defer) {
            /* If this returns 0, it means that the message wasn't found. */
            /* If it return 1, set the heap_id in the shared struct.  It will
             * return a heap ID, since a message with a reference count greater
             * than 1 is always shared in the heap.
             */
            if (H5B2_find(bt2, &key, &found, NULL, NULL) < 0)
                HGOTO_ERROR(H5E_SOHM, H5E_NOTFOUND, FAIL, "can't search for message in index");
        } /* end if */
        else {
            H5SM_incr_ref_opdata op_data;

            /* Set up callback info */
            op_data.key = &key;

            /* If this returns failure, it means that the message wasn't found. */
            /* If it succeeds, set the heap_id in the shared struct.  It will
             * return a heap ID, since a message with a reference count greater
             * than 1 is always shared in the heap.
             */
            if (H5B2_modify(bt2, &key, H5SM__incr_ref, &op_data) >= 0) {
                shared.u.heap_id = op_data.fheap_id;
                found            = true;
            } /* end if */
            else
                H5E_clear_stack(NULL); /*ignore error*/
        }                              /* end else */
    }                                  /* end else */

    if (found) {
        /* If the message was found, it's shared in the heap (now).  Set up a
         * shared message so we can mark it as shared.
         */
        shared.type = H5O_SHARE_TYPE_SOHM;

#ifdef H5_USING_MEMCHECKER
        /* Reset the shared message payload if deferring.  This doesn't matter
         *      in the long run since the payload will get overwritten when the
         *      non-deferred call to this routine occurs, but it stops memory
         *      checkers like valgrind from whining when the partially initialized
         *      shared message is serialized. -QAK
         */
        if (defer)
            memset(&shared.u, 0, sizeof(shared.u));
#endif /* H5_USING_MEMCHECKER */
    }  /* end if */
    else {
        htri_t share_in_ohdr; /* Whether the new message can be shared in another object's header */

        /* Add the message to the index */

        /* Check if the message can be shared in another object's header */
        if ((share_in_ohdr = H5O_msg_can_share_in_ohdr(type_id)) < 0)
            HGOTO_ERROR(H5E_SOHM, H5E_BADTYPE, FAIL, "'share in ohdr' check returned error");

        /* If this message can be shared in an object header location, it is
         *      "shareable" but not shared in the heap.
         *
         * If 'defer' flag is set:
         *      We will insert it in the index but not modify the original
         *              message.
         *      If it can't be shared in an object header location, we will
         *              insert it in the heap.  Note that we will only share
         *              the message in the object header if there is an
         *              "open_oh" available.
         *
         * If 'defer' flag is not set:
         *      Insert it in the index but don't modify the original message.
         *      If it can't be shared in an object header location or there's
         *              no object header location available, insert it in the
         *              heap.
         */
        if (share_in_ohdr && open_oh) {
            /* Set up shared component info */
            shared.type = H5O_SHARE_TYPE_HERE;

            /* Retrieve any creation index from the native message */
            if (H5O_msg_get_crt_index(type_id, mesg, &shared.u.loc.index) < 0)
                HGOTO_ERROR(H5E_SOHM, H5E_CANTGET, FAIL, "unable to retrieve creation index");

            if (defer)
                shared.u.loc.oh_addr = HADDR_UNDEF;
            else {
                shared.u.loc.oh_addr = H5O_OH_GET_ADDR(open_oh);

                /* Copy shared component info into key for inserting into index */
                key.message.location   = H5SM_IN_OH;
                key.message.u.mesg_loc = shared.u.loc;
            } /* end else */
        }     /* end if */
        else {
            /* Set up shared component info */
            /* (heap ID set below, if not deferred) */
            shared.type = H5O_SHARE_TYPE_SOHM;

            if (!defer) {
                /* Put the message in the heap and record its new heap ID */
                if (H5HF_insert(fheap, key.encoding_size, key.encoding, &shared.u.heap_id) < 0)
                    HGOTO_ERROR(H5E_SOHM, H5E_CANTINSERT, FAIL, "unable to insert message into fractal heap");

                key.message.location             = H5SM_IN_HEAP;
                key.message.u.heap_loc.fheap_id  = shared.u.heap_id;
                key.message.u.heap_loc.ref_count = 1;
            } /* end if */
        }     /* end else */

        if (!defer) {
            /* Set common information */
            key.message.msg_type_id = type_id;

            /* Check whether the list has grown enough that it needs to become a B-tree */
            if (header->index_type == H5SM_LIST && header->num_messages >= header->list_max)
                if (H5SM__convert_list_to_btree(f, header, &list, fheap, open_oh) < 0)
                    HGOTO_ERROR(H5E_SOHM, H5E_CANTDELETE, FAIL, "unable to convert list to B-tree");

            /* Insert the new message into the SOHM index */
            if (header->index_type == H5SM_LIST) {
                /* Index is a list.  Find an empty spot if we haven't already */
                if (empty_pos == SIZE_MAX) {
                    size_t pos;

                    if (H5SM__find_in_list(list, NULL, &empty_pos, &pos) < 0)
                        HGOTO_ERROR(H5E_SOHM, H5E_CANTINSERT, FAIL, "unable to search for message in list");

                    if (pos == SIZE_MAX || empty_pos == SIZE_MAX)
                        HGOTO_ERROR(H5E_SOHM, H5E_CANTINSERT, FAIL, "unable to find empty entry in list");
                }
                /* Insert message into list */
                assert(list->messages[empty_pos].location == H5SM_NO_LOC);
                assert(key.message.location != H5SM_NO_LOC);
                list->messages[empty_pos] = key.message;
            } /* end if */
            /* Index is a B-tree */
            else {
                assert(header->index_type == H5SM_BTREE);

                /* Open the index v2 B-tree, if it isn't already */
                if (NULL == bt2) {
                    if (NULL == (bt2 = H5B2_open(f, header->index_addr, f)))
                        HGOTO_ERROR(H5E_SOHM, H5E_CANTOPENOBJ, FAIL,
                                    "unable to open v2 B-tree for SOHM index");
                } /* end if */

                if (H5B2_insert(bt2, &key) < 0)
                    HGOTO_ERROR(H5E_SOHM, H5E_CANTINSERT, FAIL, "couldn't add SOHM to B-tree");
            } /* end else */

            ++(header->num_messages);
            (*cache_flags_ptr) |= H5AC__DIRTIED_FLAG;
        } /* end if */
    }     /* end else */

    /* Set the file pointer & message type for the shared component */
    shared.file        = f;
    shared.msg_type_id = type_id;

    /* Update the original message's shared component */
    if (H5O_msg_set_share(type_id, &shared, mesg) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_BADMESG, FAIL, "unable to set sharing information");

done:
    /* Release the fractal heap & v2 B-tree if we opened them */
    if (fheap && H5HF_close(fheap) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTCLOSEOBJ, FAIL, "can't close fractal heap");
    if (bt2 && H5B2_close(bt2) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTCLOSEOBJ, FAIL, "can't close v2 B-tree for SOHM index");

    /* If we got a list out of the cache, release it (it is always dirty after writing a message) */
    if (list && H5AC_unprotect(f, H5AC_SOHM_LIST, header->index_addr, list,
                               defer ? H5AC__NO_FLAGS_SET : H5AC__DIRTIED_FLAG) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTUNPROTECT, FAIL, "unable to close SOHM index");

    if (encoding_buf)
        encoding_buf = H5MM_xfree(encoding_buf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SM__write_mesg() */

/*-------------------------------------------------------------------------
 * Function:    H5SM_delete
 *
 * Purpose:     Given an object header message that is being deleted,
 *              checks if it is a SOHM.  If so, decrements its reference
 *              count.
 *
 *              If an object header is currently protected, it needs to
 *              be passed in as open_oh so the SM code doesn't try to
 *              re-protect it.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5SM_delete(H5F_t *f, H5O_t *open_oh, H5O_shared_t *sh_mesg)
{
    H5SM_master_table_t  *table       = NULL;
    unsigned              cache_flags = H5AC__NO_FLAGS_SET;
    H5SM_table_cache_ud_t cache_udata; /* User-data for callback */
    ssize_t               index_num;
    size_t                mesg_size   = 0;
    void                 *mesg_buf    = NULL;
    void                 *native_mesg = NULL;
    unsigned              type_id; /* Message type ID to operate on */
    herr_t                ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_TAG(H5AC__SOHM_TAG, FAIL)

    assert(f);
    assert(H5_addr_defined(H5F_SOHM_ADDR(f)));
    assert(sh_mesg);

    /* Get message type */
    type_id = sh_mesg->msg_type_id;

    /* Set up user data for callback */
    cache_udata.f = f;

    /* Look up the master SOHM table */
    if (NULL == (table = (H5SM_master_table_t *)H5AC_protect(f, H5AC_SOHM_TABLE, H5F_SOHM_ADDR(f),
                                                             &cache_udata, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_SOHM, H5E_CANTPROTECT, FAIL, "unable to load SOHM master table");

    /* Find the correct index and try to delete from it */
    if ((index_num = H5SM__get_index(table, type_id)) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_NOTFOUND, FAIL, "unable to find correct SOHM index");

    /* If mesg_buf is not NULL, the message's reference count has reached
     * zero and any file space it uses needs to be freed.  mesg_buf holds the
     * serialized form of the message.
     */
    if (H5SM__delete_from_index(f, open_oh, &(table->indexes[index_num]), sh_mesg, &cache_flags, &mesg_size,
                                &mesg_buf) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_CANTDELETE, FAIL, "unable to delete message from SOHM index");

    /* Release the master SOHM table */
    if (H5AC_unprotect(f, H5AC_SOHM_TABLE, H5F_SOHM_ADDR(f), table, cache_flags) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_CANTUNPROTECT, FAIL, "unable to close SOHM master table");
    table = NULL;

    /* If buf was allocated, delete the message it holds.  This message may
     * reference other shared messages that also need to be deleted, so the
     * master table needs to be unprotected when we do this.
     */
    if (mesg_buf) {
        if (NULL ==
            (native_mesg = H5O_msg_decode(f, open_oh, type_id, mesg_size, (const unsigned char *)mesg_buf)))
            HGOTO_ERROR(H5E_SOHM, H5E_CANTDECODE, FAIL, "can't decode shared message.");

        if (H5O_msg_delete(f, open_oh, type_id, native_mesg) < 0)
            HGOTO_ERROR(H5E_SOHM, H5E_CANTFREE, FAIL, "can't delete shared message.");
    } /* end if */

done:
    /* Release the master SOHM table (should only happen on error) */
    if (table && H5AC_unprotect(f, H5AC_SOHM_TABLE, H5F_SOHM_ADDR(f), table, cache_flags) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTUNPROTECT, FAIL, "unable to close SOHM master table");

    /* Release any native message we decoded */
    if (native_mesg)
        H5O_msg_free(type_id, native_mesg);

    /* Free encoding buf */
    if (mesg_buf)
        mesg_buf = H5MM_xfree(mesg_buf);

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5SM_delete() */

/*-------------------------------------------------------------------------
 * Function:    H5SM__find_in_list
 *
 * Purpose:     Find a message's location in a list.  Also find the first
 *              empty location in the list (since if we don't find the
 *              message, we may want to insert it into an open spot).
 *
 *              If KEY is NULL, simply find the first empty location in the
 *              list.
 *
 *              If EMPTY_POS is NULL, don't store anything in it.
 *
 * Return:      Success:    SUCCEED
 *                          pos = position (if found)
 *                          pos = SIZE_MAX (if not found)
 *                          empty_pos = indeterminate (if found)
 *                          empty_pos = 1st empty position (if not found)
 *
 *              Failure:    FAIL
 *                          pos & empty_pos indeterminate
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__find_in_list(const H5SM_list_t *list, const H5SM_mesg_key_t *key, size_t *empty_pos, size_t *pos)
{
    size_t x;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(list);
    /* Both key and empty_pos can be NULL, but not both! */
    assert(key || empty_pos);

    /* Initialize empty_pos to an invalid value */
    if (empty_pos)
        *empty_pos = SIZE_MAX;

    /* Find the first (only) message equal to the key passed in.
     * Also record the first empty position we find.
     */
    for (x = 0; x < list->header->list_max; x++) {
        if (list->messages[x].location != H5SM_NO_LOC) {
            int cmp;

            if (H5SM__message_compare(key, &(list->messages[x]), &cmp) < 0)
                HGOTO_ERROR(H5E_SOHM, H5E_CANTCOMPARE, FAIL, "can't compare message records");

            if (0 == cmp) {
                *pos = x;
                HGOTO_DONE(SUCCEED);
            }
        }
        else if (empty_pos && list->messages[x].location == H5SM_NO_LOC) {
            /* Note position */
            *empty_pos = x;

            /* Found earlier position possible, don't check any more */
            empty_pos = NULL;
        }
    }

    /* If we reached this point, we didn't find the message */
    *pos = SIZE_MAX;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SM__find_in_list */

/*-------------------------------------------------------------------------
 * Function:	H5SM__decr_ref
 *
 * Purpose:	Decrement the reference count for a SOHM message.  Doesn't
 *              remove the record from the B-tree even if the refcount
 *              reaches zero.
 *
 *              The new message is returned through op_data.  If its
 *              reference count is zero, the calling function should
 *              remove this record from the B-tree.
 *
 * Return:	Non-negative on success
 *              Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__decr_ref(void *record, void *op_data, bool *changed)
{
    H5SM_sohm_t *message = (H5SM_sohm_t *)record;

    FUNC_ENTER_PACKAGE_NOERR

    assert(record);
    assert(op_data);
    assert(changed);

    /* Adjust the message's reference count if it's stored in the heap.
     * Messages stored in object headers always have refcounts of 1,
     * so the calling function should know to just delete such a message
     */
    if (message->location == H5SM_IN_HEAP) {
        --message->u.heap_loc.ref_count;
        *changed = true;
    } /* end if */

    if (op_data)
        *(H5SM_sohm_t *)op_data = *message;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5SM__decr_ref() */

/*-------------------------------------------------------------------------
 * Function:    H5SM__delete_from_index
 *
 * Purpose:     Decrement the reference count for a particular message in this
 *              index.  If the reference count reaches zero, allocate a buffer
 *              to hold the serialized form of this message so that any
 *              resources it uses can be freed, and return this buffer in
 *              ENCODED_MESG.
 *
 * Return:      Non-negative on success
 *              Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__delete_from_index(H5F_t *f, H5O_t *open_oh, H5SM_index_header_t *header, const H5O_shared_t *mesg,
                        unsigned *cache_flags, size_t *mesg_size /*out*/, void **encoded_mesg /*out*/)
{
    H5SM_list_t    *list = NULL;
    H5SM_mesg_key_t key;
    H5SM_sohm_t     message;             /* Deleted message returned from index */
    H5SM_sohm_t    *message_ptr;         /* Pointer to deleted message returned from index */
    H5HF_t         *fheap = NULL;        /* Fractal heap that contains the message */
    H5B2_t         *bt2   = NULL;        /* v2 B-tree handle for index */
    size_t          buf_size;            /* Size of the encoded message (out) */
    void           *encoding_buf = NULL; /* The encoded message (out) */
    unsigned        type_id;             /* Message type to operate on */
    herr_t          ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(f);
    assert(header);
    assert(mesg);
    assert(cache_flags);
    assert(*encoded_mesg == NULL);

    /* Get the message type for later */
    type_id = mesg->msg_type_id;

    /* Open the heap for this type of message. */
    if (NULL == (fheap = H5HF_open(f, header->heap_addr)))
        HGOTO_ERROR(H5E_SOHM, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");

    /* Get the message size and encoded message for the message to be deleted,
     * either from its OH or from the heap.
     */
    if (mesg->type == H5O_SHARE_TYPE_HERE) {
        key.message.location    = H5SM_IN_OH;
        key.message.msg_type_id = type_id;
        key.message.u.mesg_loc  = mesg->u.loc;
    } /* end if */
    else {
        key.message.location             = H5SM_IN_HEAP;
        key.message.msg_type_id          = type_id;
        key.message.u.heap_loc.ref_count = 0; /* Refcount isn't relevant here */
        key.message.u.heap_loc.fheap_id  = mesg->u.heap_id;
    } /* end else */

    /* Get the encoded message */
    if (H5SM__read_mesg(f, &key.message, fheap, open_oh, &buf_size, &encoding_buf) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");

    /* Set up key for message to be deleted. */
    key.file          = f;
    key.fheap         = fheap;
    key.encoding      = encoding_buf;
    key.encoding_size = buf_size;
    key.message.hash  = H5_checksum_lookup3(encoding_buf, buf_size, type_id);

    /* Try to find the message in the index */
    if (header->index_type == H5SM_LIST) {
        H5SM_list_cache_ud_t cache_udata; /* User-data for metadata cache callback */
        size_t               list_pos;    /* Position of the message in the list */

        /* Set up user data for metadata cache callback */
        cache_udata.f      = f;
        cache_udata.header = header;

        /* If the index is stored as a list, get it from the cache */
        if (NULL == (list = (H5SM_list_t *)H5AC_protect(f, H5AC_SOHM_LIST, header->index_addr, &cache_udata,
                                                        H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_SOHM, H5E_CANTPROTECT, FAIL, "unable to load SOHM index");

        /* Find the message in the list */
        if (H5SM__find_in_list(list, &key, NULL, &list_pos) < 0)
            HGOTO_ERROR(H5E_SOHM, H5E_NOTFOUND, FAIL, "unable to search for message in list");
        if (list_pos == SIZE_MAX)
            HGOTO_ERROR(H5E_SOHM, H5E_NOTFOUND, FAIL, "message not in index");

        if (list->messages[list_pos].location == H5SM_IN_HEAP)
            --(list->messages[list_pos].u.heap_loc.ref_count);

        /* Point to the message */
        message_ptr = &list->messages[list_pos];
    } /* end if */
    else {
        /* Index is a B-tree */
        assert(header->index_type == H5SM_BTREE);

        /* Open the index v2 B-tree */
        if (NULL == (bt2 = H5B2_open(f, header->index_addr, f)))
            HGOTO_ERROR(H5E_SOHM, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for SOHM index");

        /* If this returns failure, it means that the message wasn't found.
         * If it succeeds, a copy of the modified message will be returned.
         */
        if (H5B2_modify(bt2, &key, H5SM__decr_ref, &message) < 0)
            HGOTO_ERROR(H5E_SOHM, H5E_NOTFOUND, FAIL, "message not in index");

        /* Point to the message */
        message_ptr = &message;
    } /* end else */

    /* If the ref count is zero or this message was in an OH (which always
     * has a ref count of 1) delete the message from the index
     */
    if (message_ptr->location == H5SM_IN_OH || message_ptr->u.heap_loc.ref_count == 0) {
        /* Save the location */
        H5SM_storage_loc_t old_loc = message_ptr->location;

        /* Updated the index header, so set its dirty flag */
        --header->num_messages;
        *cache_flags |= H5AC__DIRTIED_FLAG;

        /* Remove the message from the index */
        if (header->index_type == H5SM_LIST)
            message_ptr->location = H5SM_NO_LOC;
        else {
            /* Open the index v2 B-tree, if it isn't already */
            if (NULL == bt2) {
                if (NULL == (bt2 = H5B2_open(f, header->index_addr, f)))
                    HGOTO_ERROR(H5E_SOHM, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for SOHM index");
            } /* end if */

            if (H5B2_remove(bt2, &key, NULL, NULL) < 0)
                HGOTO_ERROR(H5E_SOHM, H5E_CANTREMOVE, FAIL, "unable to delete message from index");
        } /* end else */

        /* Remove the message from the heap if it was stored in the heap*/
        if (old_loc == H5SM_IN_HEAP)
            if (H5HF_remove(fheap, &(message_ptr->u.heap_loc.fheap_id)) < 0)
                HGOTO_ERROR(H5E_SOHM, H5E_CANTREMOVE, FAIL, "unable to remove message from heap");

        /* Return the message's encoding so anything it references can be freed */
        *encoded_mesg = encoding_buf;
        *mesg_size    = buf_size;

        /* If there are no messages left in the index, delete it */
        if (header->num_messages == 0) {

            /* Unprotect cache and release heap */
            if (list && H5AC_unprotect(f, H5AC_SOHM_LIST, header->index_addr, list,
                                       H5AC__DELETED_FLAG | H5AC__FREE_FILE_SPACE_FLAG) < 0)
                HGOTO_ERROR(H5E_SOHM, H5E_CANTUNPROTECT, FAIL, "unable to release SOHM list");
            list = NULL;

            assert(fheap);
            if (H5HF_close(fheap) < 0)
                HGOTO_ERROR(H5E_SOHM, H5E_CANTCLOSEOBJ, FAIL, "can't close fractal heap");
            fheap = NULL;

            /* Delete the index and its heap */
            if (H5SM__delete_index(f, header, true) < 0)
                HGOTO_ERROR(H5E_SOHM, H5E_CANTDELETE, FAIL, "can't delete empty index");
        } /* end if */
        else if (header->index_type == H5SM_BTREE && header->num_messages < header->btree_min) {
            /* Otherwise, if we've just passed the btree-to-list cutoff, convert
             * this B-tree into a list
             */
            if (H5SM__convert_btree_to_list(f, header) < 0)
                HGOTO_ERROR(H5E_SOHM, H5E_CANTINIT, FAIL, "unable to convert btree to list");
        } /* end if */
    }     /* end if */

done:
    /* Release the SOHM list */
    if (list && H5AC_unprotect(f, H5AC_SOHM_LIST, header->index_addr, list, H5AC__DIRTIED_FLAG) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTUNPROTECT, FAIL, "unable to close SOHM index");

    /* Release the fractal heap & v2 B-tree if we opened them */
    if (fheap && H5HF_close(fheap) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTCLOSEOBJ, FAIL, "can't close fractal heap");
    if (bt2 && H5B2_close(bt2) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTCLOSEOBJ, FAIL, "can't close v2 B-tree for SOHM index");

    /* Free the message encoding, if we're not returning it in encoded_mesg
     * or if there's been an error.
     */
    if (encoding_buf && (NULL == *encoded_mesg || ret_value < 0)) {
        encoding_buf = H5MM_xfree(encoding_buf);
        *mesg_size   = 0;
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SM__delete_from_index() */

/*-------------------------------------------------------------------------
 * Function:    H5SM_get_info
 *
 * Purpose:     Get the shared message info for a file, if there is any.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5SM_get_info(const H5O_loc_t *ext_loc, H5P_genplist_t *fc_plist)
{
    H5F_t               *f = ext_loc->file;         /* File pointer (convenience variable) */
    H5O_shmesg_table_t   sohm_table;                /* SOHM message from superblock extension */
    H5SM_master_table_t *table     = NULL;          /* SOHM master table */
    H5AC_ring_t          orig_ring = H5AC_RING_INV; /* Original ring value */
    unsigned             tmp_sohm_nindexes;         /* Number of shared messages indexes in the table */
    htri_t               status;                    /* Status for message existing */
    herr_t               ret_value = SUCCEED;       /* Return value */

    FUNC_ENTER_NOAPI_TAG(H5AC__SOHM_TAG, FAIL)

    /* Sanity check */
    assert(ext_loc);
    assert(f);
    assert(fc_plist);

    /* Check for the extension having a 'shared message info' message */
    if ((status = H5O_msg_exists(ext_loc, H5O_SHMESG_ID)) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_CANTGET, FAIL, "unable to read object header");
    if (status) {
        H5SM_table_cache_ud_t cache_udata;                          /* User-data for callback */
        unsigned              index_flags[H5O_SHMESG_MAX_NINDEXES]; /* Message flags for each index */
        unsigned              minsizes[H5O_SHMESG_MAX_NINDEXES];    /* Minimum message size for each index */
        unsigned              sohm_l2b;                             /* SOHM list-to-btree cutoff */
        unsigned              sohm_b2l;                             /* SOHM btree-to-list cutoff */
        unsigned              u;                                    /* Local index variable */

        /* Retrieve the 'shared message info' structure */
        if (NULL == H5O_msg_read(ext_loc, H5O_SHMESG_ID, &sohm_table))
            HGOTO_ERROR(H5E_SOHM, H5E_CANTGET, FAIL, "shared message info message not present");

        /* Portably initialize the arrays */
        memset(index_flags, 0, sizeof(index_flags));
        memset(minsizes, 0, sizeof(minsizes));

        /* Set SOHM info from file */
        H5F_SET_SOHM_ADDR(f, sohm_table.addr);
        H5F_SET_SOHM_VERS(f, sohm_table.version);
        H5F_SET_SOHM_NINDEXES(f, sohm_table.nindexes);
        assert(H5_addr_defined(H5F_SOHM_ADDR(f)));
        assert(H5F_SOHM_NINDEXES(f) > 0 && H5F_SOHM_NINDEXES(f) <= H5O_SHMESG_MAX_NINDEXES);

        /* Set up user data for callback */
        cache_udata.f = f;

        /* Set the ring type in the DXPL */
        H5AC_set_ring(H5AC_RING_USER, &orig_ring);

        /* Read the rest of the SOHM table information from the cache */
        if (NULL == (table = (H5SM_master_table_t *)H5AC_protect(f, H5AC_SOHM_TABLE, H5F_SOHM_ADDR(f),
                                                                 &cache_udata, H5AC__READ_ONLY_FLAG)))
            HGOTO_ERROR(H5E_SOHM, H5E_CANTPROTECT, FAIL, "unable to load SOHM master table");

        /* Get index conversion limits */
        sohm_l2b = (unsigned)table->indexes[0].list_max;
        sohm_b2l = (unsigned)table->indexes[0].btree_min;

        /* Iterate through all indices */
        for (u = 0; u < table->num_indexes; ++u) {
            /* Pack information about the individual SOHM index */
            index_flags[u] = table->indexes[u].mesg_types;
            minsizes[u]    = (unsigned)table->indexes[u].min_mesg_size;

            /* Sanity check */
            assert(sohm_l2b == table->indexes[u].list_max);
            assert(sohm_b2l == table->indexes[u].btree_min);

            /* Check for sharing attributes in this file, which means that creation
             *  indices must be tracked on object header message in the file.
             */
            if (index_flags[u] & H5O_SHMESG_ATTR_FLAG)
                H5F_SET_STORE_MSG_CRT_IDX(f, true);
        } /* end for */

        /* Set values in the property list */
        tmp_sohm_nindexes = H5F_SOHM_NINDEXES(f);
        if (H5P_set(fc_plist, H5F_CRT_SHMSG_NINDEXES_NAME, &tmp_sohm_nindexes) < 0)
            HGOTO_ERROR(H5E_SOHM, H5E_CANTSET, FAIL, "can't set number of SOHM indexes");
        if (H5P_set(fc_plist, H5F_CRT_SHMSG_INDEX_TYPES_NAME, index_flags) < 0)
            HGOTO_ERROR(H5E_SOHM, H5E_CANTSET, FAIL, "can't set type flags for indexes");
        if (H5P_set(fc_plist, H5F_CRT_SHMSG_INDEX_MINSIZE_NAME, minsizes) < 0)
            HGOTO_ERROR(H5E_SOHM, H5E_CANTSET, FAIL, "can't set type flags for indexes");
        if (H5P_set(fc_plist, H5F_CRT_SHMSG_LIST_MAX_NAME, &sohm_l2b) < 0)
            HGOTO_ERROR(H5E_SOHM, H5E_CANTGET, FAIL, "can't set SOHM cutoff in property list");
        if (H5P_set(fc_plist, H5F_CRT_SHMSG_BTREE_MIN_NAME, &sohm_b2l) < 0)
            HGOTO_ERROR(H5E_SOHM, H5E_CANTGET, FAIL, "can't set SOHM cutoff in property list");
    } /* end if */
    else {
        /* No SOHM info in file */
        H5F_SET_SOHM_ADDR(f, HADDR_UNDEF);
        H5F_SET_SOHM_VERS(f, 0);
        H5F_SET_SOHM_NINDEXES(f, 0);

        /* Shared object header messages are disabled */
        tmp_sohm_nindexes = H5F_SOHM_NINDEXES(f);
        if (H5P_set(fc_plist, H5F_CRT_SHMSG_NINDEXES_NAME, &tmp_sohm_nindexes) < 0)
            HGOTO_ERROR(H5E_SOHM, H5E_CANTSET, FAIL, "can't set number of SOHM indexes");
    } /* end else */

done:
    /* Reset the ring in the API context */
    if (orig_ring != H5AC_RING_INV)
        H5AC_set_ring(orig_ring, NULL);

    /* Release the master SOHM table if we took it out of the cache */
    if (table && H5AC_unprotect(f, H5AC_SOHM_TABLE, H5F_SOHM_ADDR(f), table, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTUNPROTECT, FAIL, "unable to close SOHM master table");

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5SM_get_info() */

/*-------------------------------------------------------------------------
 * Function:    H5SM_reconstitute
 *
 * Purpose:     Reconstitute a shared object header message structure from
 *              a plain heap ID.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5SM_reconstitute(H5O_shared_t *sh_mesg, H5F_t *f, unsigned msg_type_id, H5O_fheap_id_t heap_id)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check args */
    assert(sh_mesg);

    /* Set flag for shared message */
    sh_mesg->type        = H5O_SHARE_TYPE_SOHM;
    sh_mesg->file        = f;
    sh_mesg->msg_type_id = msg_type_id;
    sh_mesg->u.heap_id   = heap_id;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5SM_reconstitute() */

/*-------------------------------------------------------------------------
 * Function:	H5SM__get_refcount_bt2_cb
 *
 * Purpose:	v2 B-tree 'find' callback to retrieve the record for a message
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__get_refcount_bt2_cb(const void *_record, void *_op_data)
{
    const H5SM_sohm_t *record  = (const H5SM_sohm_t *)_record; /* v2 B-tree record for message */
    H5SM_sohm_t       *op_data = (H5SM_sohm_t *)_op_data;      /* "op data" from v2 B-tree find */

    FUNC_ENTER_PACKAGE_NOERR

    /*
     * Check arguments.
     */
    assert(record);
    assert(op_data);

    /* Make a copy of the record */
    *op_data = *record;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5SM__get_refcount_bt2_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5SM_get_refcount
 *
 * Purpose:     Retrieve the reference count for a message shared in the heap
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5SM_get_refcount(H5F_t *f, unsigned type_id, const H5O_shared_t *sh_mesg, hsize_t *ref_count)
{
    H5HF_t               *fheap = NULL;           /* Fractal heap that contains shared messages */
    H5B2_t               *bt2   = NULL;           /* v2 B-tree handle for index */
    H5SM_master_table_t  *table = NULL;           /* SOHM master table */
    H5SM_table_cache_ud_t tbl_cache_udata;        /* User-data for callback */
    H5SM_list_t          *list   = NULL;          /* SOHM index list for message type (if in list form) */
    H5SM_index_header_t  *header = NULL;          /* Index header for message type */
    H5SM_mesg_key_t       key;                    /* Key for looking up message */
    H5SM_sohm_t           message;                /* Shared message returned from callback */
    ssize_t               index_num;              /* Table index for message type */
    size_t                buf_size;               /* Size of the encoded message */
    void                 *encoding_buf = NULL;    /* Buffer for encoded message */
    herr_t                ret_value    = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_TAG(H5AC__SOHM_TAG)

    /* Sanity check */
    assert(f);
    assert(sh_mesg);
    assert(ref_count);

    /* Set up user data for callback */
    tbl_cache_udata.f = f;

    /* Look up the master SOHM table */
    if (NULL == (table = (H5SM_master_table_t *)H5AC_protect(f, H5AC_SOHM_TABLE, H5F_SOHM_ADDR(f),
                                                             &tbl_cache_udata, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_SOHM, H5E_CANTPROTECT, FAIL, "unable to load SOHM master table");

    /* Find the correct index and find the message in it */
    if ((index_num = H5SM__get_index(table, type_id)) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_NOTFOUND, FAIL, "unable to find correct SOHM index");
    header = &(table->indexes[index_num]);

    /* Open the heap for this message type */
    if (NULL == (fheap = H5HF_open(f, header->heap_addr)))
        HGOTO_ERROR(H5E_SOHM, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");

    /* Set up a SOHM message to correspond to the shared message passed in */
    key.message.location             = H5SM_IN_HEAP;
    key.message.u.heap_loc.fheap_id  = sh_mesg->u.heap_id;
    key.message.u.heap_loc.ref_count = 0; /* Ref count isn't needed to find message */

    /* Get the encoded message */
    if (H5SM__read_mesg(f, &key.message, fheap, NULL, &buf_size, &encoding_buf) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");

    /* Set up key for message to locate */
    key.file          = f;
    key.fheap         = fheap;
    key.encoding      = encoding_buf;
    key.encoding_size = buf_size;
    key.message.hash  = H5_checksum_lookup3(encoding_buf, buf_size, type_id);

    /* Try to find the message in the index */
    if (header->index_type == H5SM_LIST) {
        H5SM_list_cache_ud_t lst_cache_udata; /* User-data for metadata cache callback */
        size_t               list_pos;        /* Position of the message in the list */

        /* Set up user data for metadata cache callback */
        lst_cache_udata.f      = f;
        lst_cache_udata.header = header;

        /* If the index is stored as a list, get it from the cache */
        if (NULL == (list = (H5SM_list_t *)H5AC_protect(f, H5AC_SOHM_LIST, header->index_addr,
                                                        &lst_cache_udata, H5AC__READ_ONLY_FLAG)))
            HGOTO_ERROR(H5E_SOHM, H5E_CANTPROTECT, FAIL, "unable to load SOHM index");

        /* Find the message in the list */
        if (H5SM__find_in_list(list, &key, NULL, &list_pos) < 0)
            HGOTO_ERROR(H5E_SOHM, H5E_NOTFOUND, FAIL, "unable to search for message in list");
        if (list_pos == SIZE_MAX)
            HGOTO_ERROR(H5E_SOHM, H5E_NOTFOUND, FAIL, "message not in index");

        /* Copy the message */
        message = list->messages[list_pos];
    } /* end if */
    else {
        bool msg_exists; /* Whether the message exists in the v2 B-tree */

        /* Index is a B-tree */
        assert(header->index_type == H5SM_BTREE);

        /* Open the index v2 B-tree */
        if (NULL == (bt2 = H5B2_open(f, header->index_addr, f)))
            HGOTO_ERROR(H5E_SOHM, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for SOHM index");

        /* Look up the message in the v2 B-tree */
        msg_exists = false;
        if (H5B2_find(bt2, &key, &msg_exists, H5SM__get_refcount_bt2_cb, &message) < 0)
            HGOTO_ERROR(H5E_SOHM, H5E_CANTGET, FAIL, "error finding message in index");
        if (!msg_exists)
            HGOTO_ERROR(H5E_SOHM, H5E_NOTFOUND, FAIL, "message not in index");
    } /* end else */

    /* Set the refcount for the message */
    assert(message.location == H5SM_IN_HEAP);
    *ref_count = message.u.heap_loc.ref_count;

done:
    /* Release resources */
    if (list && H5AC_unprotect(f, H5AC_SOHM_LIST, header->index_addr, list, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTUNPROTECT, FAIL, "unable to close SOHM index");
    if (table && H5AC_unprotect(f, H5AC_SOHM_TABLE, H5F_SOHM_ADDR(f), table, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTUNPROTECT, FAIL, "unable to close SOHM master table");
    if (fheap && H5HF_close(fheap) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTCLOSEOBJ, FAIL, "can't close fractal heap");
    if (bt2 && H5B2_close(bt2) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTCLOSEOBJ, FAIL, "can't close v2 B-tree for SOHM index");
    if (encoding_buf)
        encoding_buf = H5MM_xfree(encoding_buf);

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5SM_get_refcount() */

/*-------------------------------------------------------------------------
 * Function:	H5SM__read_iter_op
 *
 * Purpose:	OH iteration callback to get the encoded version of a message
 *              by index.
 *
 *              The buffer needs to be freed.
 *
 * Return:	0 if this is not the message we're searching for
 *              1 if this is the message we're searching for (with encoded
 *                      value returned in udata)
 *              negative on error
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__read_iter_op(H5O_t *oh, H5O_mesg_t *mesg /*in,out*/, unsigned sequence,
                   unsigned H5_ATTR_UNUSED *oh_modified, void *_udata /*in,out*/)
{
    H5SM_read_udata_t *udata     = (H5SM_read_udata_t *)_udata;
    herr_t             ret_value = H5_ITER_CONT;

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(oh);
    assert(mesg);
    assert(udata);
    assert(NULL == udata->encoding_buf);

    /* Check the creation index for this message */
    if (sequence == udata->idx) {
        /* Check if the message is dirty & flush it to the object header if so */
        if (mesg->dirty)
            if (H5O_msg_flush(udata->file, oh, mesg) < 0)
                HGOTO_ERROR(H5E_SOHM, H5E_CANTENCODE, H5_ITER_ERROR,
                            "unable to encode object header message");

        /* Get the message's encoded size */
        udata->buf_size = mesg->raw_size;
        assert(udata->buf_size);

        /* Allocate buffer to return the message in */
        if (NULL == (udata->encoding_buf = H5MM_malloc(udata->buf_size)))
            HGOTO_ERROR(H5E_SOHM, H5E_NOSPACE, H5_ITER_ERROR, "memory allocation failed");

        /* Copy the encoded message into the buffer to return */
        H5MM_memcpy(udata->encoding_buf, mesg->raw, udata->buf_size);

        /* Found the message we were looking for */
        ret_value = H5_ITER_STOP;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SM__read_iter_op() */

/*-------------------------------------------------------------------------
 * Function:	H5SM__read_mesg_fh_cb
 *
 * Purpose:	Callback for H5HF_op, used in H5SM__read_mesg below.
 *              Makes a copy of the message in the heap data, returned in the
 *              UDATA struct.
 *
 * Return:	Negative on error, non-negative on success
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__read_mesg_fh_cb(const void *obj, size_t obj_len, void *_udata)
{
    H5SM_read_udata_t *udata     = (H5SM_read_udata_t *)_udata;
    herr_t             ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Allocate a buffer to hold the message */
    if (NULL == (udata->encoding_buf = H5MM_malloc(obj_len)))
        HGOTO_ERROR(H5E_SOHM, H5E_NOSPACE, FAIL, "memory allocation failed");

    /* Copy the message from the heap */
    H5MM_memcpy(udata->encoding_buf, obj, obj_len);
    udata->buf_size = obj_len;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SM__read_mesg_fh_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5SM__read_mesg
 *
 * Purpose:	Given an H5SM_sohm_t sohm, encodes the message into a buffer.
 *              This buffer should then be freed.
 *
 * Return:	Non-negative on success/negative on error
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__read_mesg(H5F_t *f, const H5SM_sohm_t *mesg, H5HF_t *fheap, H5O_t *open_oh,
                size_t *encoding_size /*out*/, void **encoded_mesg /*out*/)
{
    H5SM_read_udata_t udata;            /* User data for callbacks */
    H5O_loc_t         oloc;             /* Object location for message in object header */
    H5O_t            *oh        = NULL; /* Object header for message in object header */
    herr_t            ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(mesg);
    assert(fheap);

    /* Set up user data for message iteration */
    udata.file         = f;
    udata.idx          = mesg->u.mesg_loc.index;
    udata.encoding_buf = NULL;
    udata.idx          = 0;

    /* Get the message size and encoded message for the message to be deleted,
     * either from its OH or from the heap.
     */
    if (mesg->location == H5SM_IN_OH) {
        /* Read message from object header */
        const H5O_msg_class_t *type = NULL; /* Actual H5O class type for the ID */
        H5O_mesg_operator_t    op;          /* Wrapper for operator */

        type = H5O_msg_class_g[mesg->msg_type_id]; /* map the type ID to the actual type object */
        assert(type);

        /* Reset object location for operation */
        if (H5O_loc_reset(&oloc) < 0)
            HGOTO_ERROR(H5E_SOHM, H5E_CANTRESET, FAIL, "unable to initialize location");

        if (NULL == open_oh || mesg->u.mesg_loc.oh_addr != H5O_OH_GET_ADDR(open_oh)) {
            /* Open the object in the file */
            oloc.file = f;
            oloc.addr = mesg->u.mesg_loc.oh_addr;
            if (H5O_open(&oloc) < 0)
                HGOTO_ERROR(H5E_SOHM, H5E_CANTLOAD, FAIL, "unable to open object header");

            /* Load the object header from the cache */
            if (NULL == (oh = H5O_protect(&oloc, H5AC__READ_ONLY_FLAG, false)))
                HGOTO_ERROR(H5E_SOHM, H5E_CANTPROTECT, FAIL, "unable to load object header");
        } /* end if */
        else
            oh = open_oh;

        /* Use the "real" iterate routine so it doesn't try to protect the OH */
        op.op_type  = H5O_MESG_OP_LIB;
        op.u.lib_op = H5SM__read_iter_op;
        if ((ret_value = H5O__msg_iterate_real(f, oh, type, &op, &udata)) < 0)
            HGOTO_ERROR(H5E_SOHM, H5E_BADITER, FAIL, "unable to iterate over object header messages");
    } /* end if */
    else {
        assert(mesg->location == H5SM_IN_HEAP);

        /* Copy the message from the heap */
        if (H5HF_op(fheap, &(mesg->u.heap_loc.fheap_id), H5SM__read_mesg_fh_cb, &udata) < 0)
            HGOTO_ERROR(H5E_SOHM, H5E_CANTLOAD, FAIL, "can't read message from fractal heap.");
    } /* end else */
    assert(udata.encoding_buf);
    assert(udata.buf_size);

    /* Record the returned values */
    *encoded_mesg  = udata.encoding_buf;
    *encoding_size = udata.buf_size;

done:
    /* Close the object header if we opened one and had an error */
    if (oh && oh != open_oh) {
        if (oh && H5O_unprotect(&oloc, oh, H5AC__NO_FLAGS_SET) < 0)
            HDONE_ERROR(H5E_SOHM, H5E_CANTUNPROTECT, FAIL, "unable to release object header");
        if (H5O_close(&oloc, NULL) < 0)
            HDONE_ERROR(H5E_SOHM, H5E_CANTCLOSEOBJ, FAIL, "unable to close object header");
    } /* end if */

    /* Release the encoding buffer on error */
    if (ret_value < 0 && udata.encoding_buf)
        udata.encoding_buf = H5MM_xfree(udata.encoding_buf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SM__read_mesg */

/*-------------------------------------------------------------------------
 * Function:	H5SM__table_free
 *
 * Purpose:	Frees memory used by the SOHM table.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5SM__table_free(H5SM_master_table_t *table)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(table);
    assert(table->indexes);

    table->indexes = H5FL_ARR_FREE(H5SM_index_header_t, table->indexes);

    table = H5FL_FREE(H5SM_master_table_t, table);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5SM__table_free() */

/*-------------------------------------------------------------------------
 * Function:	H5SM__list_free
 *
 * Purpose:	Frees all memory used by the list.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5SM__list_free(H5SM_list_t *list)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(list);
    assert(list->messages);

    list->messages = H5FL_ARR_FREE(H5SM_sohm_t, list->messages);

    list = H5FL_FREE(H5SM_list_t, list);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5SM__list_free() */

/*-------------------------------------------------------------------------
 * Function:    H5SM_table_debug
 *
 * Purpose:     Print debugging information for the master table.
 *
 *              If table_vers and num_indexes are not UINT_MAX, they are used
 *              instead of the values in the superblock.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5SM_table_debug(H5F_t *f, haddr_t table_addr, FILE *stream, int indent, int fwidth, unsigned table_vers,
                 unsigned num_indexes)
{
    H5SM_master_table_t  *table = NULL;        /* SOHM master table */
    H5SM_table_cache_ud_t cache_udata;         /* User-data for callback */
    unsigned              x;                   /* Counter variable */
    herr_t                ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_TAG(H5AC__SOHM_TAG, FAIL)

    assert(f);
    assert(table_addr != HADDR_UNDEF);
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    /* If table_vers and num_indexes are UINT_MAX, replace them with values from
     * userblock
     */
    if (table_vers == UINT_MAX)
        table_vers = H5F_SOHM_VERS(f);
    else if (table_vers != H5F_SOHM_VERS(f))
        fprintf(stream, "*** SOHM TABLE VERSION DOESN'T MATCH VERSION IN SUPERBLOCK!\n");
    if (num_indexes == UINT_MAX)
        num_indexes = H5F_SOHM_NINDEXES(f);
    else if (num_indexes != H5F_SOHM_NINDEXES(f))
        fprintf(stream, "*** NUMBER OF SOHM INDEXES DOESN'T MATCH VALUE IN SUPERBLOCK!\n");

    /* Check arguments.  Version must be 0, the only version implemented so far */
    if (table_vers > HDF5_SHAREDHEADER_VERSION)
        HGOTO_ERROR(H5E_SOHM, H5E_BADVALUE, FAIL, "unknown shared message table version");
    if (num_indexes == 0 || num_indexes > H5O_SHMESG_MAX_NINDEXES)
        HGOTO_ERROR(H5E_SOHM, H5E_BADVALUE, FAIL,
                    "number of indexes must be between 1 and H5O_SHMESG_MAX_NINDEXES");

    /* Set up user data for callback */
    cache_udata.f = f;

    /* Look up the master SOHM table */
    if (NULL == (table = (H5SM_master_table_t *)H5AC_protect(f, H5AC_SOHM_TABLE, table_addr, &cache_udata,
                                                             H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_SOHM, H5E_CANTPROTECT, FAIL, "unable to load SOHM master table");

    fprintf(stream, "%*sShared Message Master Table...\n", indent, "");
    for (x = 0; x < num_indexes; ++x) {
        fprintf(stream, "%*sIndex %d...\n", indent, "", x);
        fprintf(stream, "%*s%-*s %s\n", indent + 3, "", fwidth, "SOHM Index Type:",
                (table->indexes[x].index_type == H5SM_LIST
                     ? "List"
                     : (table->indexes[x].index_type == H5SM_BTREE ? "B-Tree" : "Unknown")));

        fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent + 3, "", fwidth,
                "Address of index:", table->indexes[x].index_addr);
        fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent + 3, "", fwidth,
                "Address of index's heap:", table->indexes[x].heap_addr);
        fprintf(stream, "%*s%-*s 0x%08x\n", indent + 3, "", fwidth,
                "Message type flags:", table->indexes[x].mesg_types);
        fprintf(stream, "%*s%-*s %zu\n", indent + 3, "", fwidth,
                "Minimum size of messages:", table->indexes[x].min_mesg_size);
        fprintf(stream, "%*s%-*s %zu\n", indent + 3, "", fwidth,
                "Number of messages:", table->indexes[x].num_messages);
        fprintf(stream, "%*s%-*s %zu\n", indent + 3, "", fwidth,
                "Maximum list size:", table->indexes[x].list_max);
        fprintf(stream, "%*s%-*s %zu\n", indent + 3, "", fwidth,
                "Minimum B-tree size:", table->indexes[x].btree_min);
    } /* end for */

done:
    if (table && H5AC_unprotect(f, H5AC_SOHM_TABLE, table_addr, table, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTUNPROTECT, FAIL, "unable to close SOHM master table");

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5SM_table_debug() */

/*-------------------------------------------------------------------------
 * Function:    H5SM_list_debug
 *
 * Purpose:     Print debugging information for a SOHM list.
 *
 *              Relies on the list version and number of messages passed in.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5SM_list_debug(H5F_t *f, haddr_t list_addr, FILE *stream, int indent, int fwidth, haddr_t table_addr)
{
    H5SM_master_table_t  *table = NULL;        /* SOHM master table */
    H5SM_list_t          *list  = NULL;        /* SOHM index list for message type (if in list form) */
    H5SM_list_cache_ud_t  lst_cache_udata;     /* List user-data for metadata cache callback */
    H5SM_table_cache_ud_t tbl_cache_udata;     /* Table user-data for metadata cache callback */
    H5HF_t               *fh = NULL;           /* Fractal heap for SOHM messages */
    unsigned              index_num;           /* Index of list, within master table */
    unsigned              x;                   /* Counter variable */
    herr_t                ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_TAG(H5AC__SOHM_TAG, FAIL)

    assert(f);
    assert(list_addr != HADDR_UNDEF);
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    /* Set up user data for callback */
    tbl_cache_udata.f = f;

    /* Look up the master SOHM table */
    if (NULL == (table = (H5SM_master_table_t *)H5AC_protect(f, H5AC_SOHM_TABLE, table_addr, &tbl_cache_udata,
                                                             H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_SOHM, H5E_CANTPROTECT, FAIL, "unable to load SOHM master table");

    /* Determine which index the list is part of */
    index_num = table->num_indexes;
    for (x = 0; x < table->num_indexes; x++) {
        if (H5_addr_eq(table->indexes[x].index_addr, list_addr)) {
            index_num = x;
            break;
        } /* end if */
    }     /* end for */
    if (x == table->num_indexes)
        HGOTO_ERROR(H5E_SOHM, H5E_BADVALUE, FAIL,
                    "list address doesn't match address for any indices in table");

    /* Set up user data for metadata cache callback */
    lst_cache_udata.f      = f;
    lst_cache_udata.header = &(table->indexes[index_num]);

    /* Get the list from the cache */
    if (NULL == (list = (H5SM_list_t *)H5AC_protect(f, H5AC_SOHM_LIST, list_addr, &lst_cache_udata,
                                                    H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_SOHM, H5E_CANTPROTECT, FAIL, "unable to load SOHM index");

    /* Open the heap, if one exists */
    if (H5_addr_defined(table->indexes[index_num].heap_addr))
        if (NULL == (fh = H5HF_open(f, table->indexes[index_num].heap_addr)))
            HGOTO_ERROR(H5E_SOHM, H5E_CANTOPENOBJ, FAIL, "unable to open SOHM heap");

    fprintf(stream, "%*sShared Message List Index...\n", indent, "");
    for (x = 0; x < table->indexes[index_num].num_messages; ++x) {
        fprintf(stream, "%*sShared Object Header Message %d...\n", indent, "", x);
        fprintf(stream, "%*s%-*s %08lu\n", indent + 3, "", fwidth,
                "Hash value:", (unsigned long)list->messages[x].hash);
        if (list->messages[x].location == H5SM_IN_HEAP) {
            assert(fh);

            fprintf(stream, "%*s%-*s %s\n", indent + 3, "", fwidth, "Location:", "in heap");
            fprintf(stream, "%*s%-*s 0x%" PRIx64 "\n", indent + 3, "", fwidth,
                    "Heap ID:", list->messages[x].u.heap_loc.fheap_id.val);
            fprintf(stream, "%*s%-*s %" PRIuHSIZE "\n", indent + 3, "", fwidth,
                    "Reference count:", list->messages[x].u.heap_loc.ref_count);
        } /* end if */
        else if (list->messages[x].location == H5SM_IN_OH) {
            fprintf(stream, "%*s%-*s %s\n", indent + 3, "", fwidth, "Location:", "in object header");
            fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent + 3, "", fwidth,
                    "Object header address:", list->messages[x].u.mesg_loc.oh_addr);
            fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent + 3, "", fwidth,
                    "Message creation index:", list->messages[x].u.mesg_loc.oh_addr);
            fprintf(stream, "%*s%-*s %u\n", indent + 3, "", fwidth,
                    "Message type ID:", list->messages[x].msg_type_id);
        } /* end if */
        else
            fprintf(stream, "%*s%-*s %s\n", indent + 3, "", fwidth, "Location:", "invalid");
    } /* end for */

done:
    if (fh && H5HF_close(fh) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTCLOSEOBJ, FAIL, "unable to close SOHM heap");
    if (list && H5AC_unprotect(f, H5AC_SOHM_LIST, list_addr, list, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTUNPROTECT, FAIL, "unable to close SOHM index");
    if (table && H5AC_unprotect(f, H5AC_SOHM_TABLE, table_addr, table, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTUNPROTECT, FAIL, "unable to close SOHM master table");

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5SM_list_debug() */

/*-------------------------------------------------------------------------
 * Function:    H5SM_ih_size
 *
 * Purpose:     Loop through the master SOHM table (if there is one) to:
 *			1. collect storage used for header
 *                      1. collect storage used for B-tree and List
 *			   (include btree storage used by huge objects in fractal heap)
 *                      2. collect fractal heap storage
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5SM_ih_size(H5F_t *f, hsize_t *hdr_size, H5_ih_info_t *ih_info)
{
    H5SM_master_table_t  *table = NULL;        /* SOHM master table */
    H5SM_table_cache_ud_t cache_udata;         /* User-data for callback */
    H5HF_t               *fheap = NULL;        /* Fractal heap handle */
    H5B2_t               *bt2   = NULL;        /* v2 B-tree handle for index */
    unsigned              u;                   /* Local index variable */
    herr_t                ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_TAG(H5AC__SOHM_TAG, FAIL)

    /* Sanity check */
    assert(f);
    assert(H5_addr_defined(H5F_SOHM_ADDR(f)));
    assert(hdr_size);
    assert(ih_info);

    /* Set up user data for callback */
    cache_udata.f = f;

    /* Look up the master SOHM table */
    if (NULL == (table = (H5SM_master_table_t *)H5AC_protect(f, H5AC_SOHM_TABLE, H5F_SOHM_ADDR(f),
                                                             &cache_udata, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_SOHM, H5E_CANTPROTECT, FAIL, "unable to load SOHM master table");

    /* Get SOHM header size */
    *hdr_size = table->table_size;

    /* Loop over all the indices for shared messages */
    for (u = 0; u < table->num_indexes; u++) {
        /* Get index storage size (for either B-tree or list) */
        if (table->indexes[u].index_type == H5SM_BTREE) {
            if (H5_addr_defined(table->indexes[u].index_addr)) {
                /* Open the index v2 B-tree */
                if (NULL == (bt2 = H5B2_open(f, table->indexes[u].index_addr, f)))
                    HGOTO_ERROR(H5E_SOHM, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for SOHM index");

                if (H5B2_size(bt2, &(ih_info->index_size)) < 0)
                    HGOTO_ERROR(H5E_SOHM, H5E_CANTGET, FAIL, "can't retrieve B-tree storage info");

                /* Close the v2 B-tree */
                if (H5B2_close(bt2) < 0)
                    HGOTO_ERROR(H5E_SOHM, H5E_CANTCLOSEOBJ, FAIL, "can't close v2 B-tree for SOHM index");
                bt2 = NULL;
            } /* end if */
        }     /* end if */
        else {
            assert(table->indexes[u].index_type == H5SM_LIST);
            ih_info->index_size += table->indexes[u].list_size;
        } /* end else */

        /* Check for heap for this index */
        if (H5_addr_defined(table->indexes[u].heap_addr)) {
            /* Open the fractal heap for this index */
            if (NULL == (fheap = H5HF_open(f, table->indexes[u].heap_addr)))
                HGOTO_ERROR(H5E_SOHM, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");

            /* Get heap storage size */
            if (H5HF_size(fheap, &(ih_info->heap_size)) < 0)
                HGOTO_ERROR(H5E_SOHM, H5E_CANTGET, FAIL, "can't retrieve fractal heap storage info");

            /* Close the fractal heap */
            if (H5HF_close(fheap) < 0)
                HGOTO_ERROR(H5E_SOHM, H5E_CANTCLOSEOBJ, FAIL, "can't close fractal heap");
            fheap = NULL;
        } /* end if */
    }     /* end for */

done:
    /* Release resources */
    if (fheap && H5HF_close(fheap) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTCLOSEOBJ, FAIL, "can't close fractal heap");
    if (bt2 && H5B2_close(bt2) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTCLOSEOBJ, FAIL, "can't close v2 B-tree for SOHM index");
    if (table && H5AC_unprotect(f, H5AC_SOHM_TABLE, H5F_SOHM_ADDR(f), table, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTUNPROTECT, FAIL, "unable to close SOHM master table");

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5SM_ih_size() */
